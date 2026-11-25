// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::Coordinator;
use rustlakedb_common::Result;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MaintainerConfig {
    pub interval: Duration,
    pub min_files_to_compact: usize,
    pub retain_snapshots: usize,
}

impl Default for MaintainerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            min_files_to_compact: 3,
            retain_snapshots: 2,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaintenanceCycleReport {
    pub tables_checked: usize,
    pub tables_compacted: usize,
    pub tables_vacuumed: usize,
}

/// Autonomous Iceberg v3 Table Maintenance Daemon (RFC-002):
/// Continuously monitors table file fragmentation & Deletion Vectors, running
/// `COMPACT TABLE` (bin-packing + DV materialization + Puffin rebuild) and `VACUUM TABLE` automatically.
pub struct AutonomousMaintainer {
    coordinator: Arc<Coordinator>,
    config: MaintainerConfig,
}

impl AutonomousMaintainer {
    pub fn new(coordinator: Arc<Coordinator>, config: MaintainerConfig) -> Self {
        Self {
            coordinator,
            config,
        }
    }

    pub async fn run_once(&self) -> Result<MaintenanceCycleReport> {
        let mut report = MaintenanceCycleReport::default();
        let tables = self.coordinator.catalog.list_tables().await?;

        for table_name in tables {
            report.tables_checked += 1;
            let meta = self.coordinator.catalog.get_table(&table_name).await?;
            let snap = meta.current_snapshot("main")?;

            let mut has_dvs = false;
            for df in &snap.data_files {
                let dv = self
                    .coordinator
                    .store
                    .read_deletion_vector(&df.file_path)
                    .await?;
                if !dv.is_empty() {
                    has_dvs = true;
                    break;
                }
            }

            // Only auto-compact small fragmented files (<16 MiB) on tables within in-memory compaction budget,
            // never pulling billion-row optimal Parquet tables into maintainer RAM!
            let small_files = snap
                .data_files
                .iter()
                .filter(|df| df.byte_size < 16 * 1024 * 1024)
                .count();
            let total_rows: u64 = snap.data_files.iter().map(|df| df.row_count).sum();
            let within_budget = total_rows <= 2_000_000;

            if within_budget && (small_files >= self.config.min_files_to_compact || has_dvs) {
                tracing::info!(
                    table = %table_name,
                    files = snap.data_files.len(),
                    has_dvs,
                    "AutonomousMaintainer compacting table"
                );
                let _ = self
                    .coordinator
                    .execute_sql(&format!("COMPACT TABLE {table_name}"), "main")
                    .await?;
                report.tables_compacted += 1;
            }

            let updated = self.coordinator.catalog.get_table(&table_name).await?;
            if updated.snapshots.len() > self.config.retain_snapshots {
                let _ = self
                    .coordinator
                    .execute_sql(
                        &format!(
                            "VACUUM TABLE {table_name} RETAIN {}",
                            self.config.retain_snapshots
                        ),
                        "main",
                    )
                    .await?;
                report.tables_vacuumed += 1;
            }
        }

        Ok(report)
    }

    pub async fn run_loop(&self) -> Result<()> {
        loop {
            match self.run_once().await {
                Ok(rep) => {
                    tracing::info!(
                        checked = rep.tables_checked,
                        compacted = rep.tables_compacted,
                        vacuumed = rep.tables_vacuumed,
                        "maintenance cycle complete"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "maintenance cycle encountered error");
                }
            }
            tokio::time::sleep(self.config.interval).await;
        }
    }
}
