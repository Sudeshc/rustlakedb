// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::{select_compaction_candidates, CompactionPolicy, Coordinator};
use rustlakedb_common::Result;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MaintainerConfig {
    pub interval: Duration,
    pub min_files_to_compact: usize,
    pub retain_snapshots: usize,
    pub compaction_policy: CompactionPolicy,
}

impl Default for MaintainerConfig {
    fn default() -> Self {
        let mut policy = CompactionPolicy::from_env();
        policy.min_small_files_per_partition = 3;
        Self {
            interval: Duration::from_secs(
                std::env::var("RUSTLAKEDB_MAINTAIN_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30)
                    .max(5),
            ),
            min_files_to_compact: 3,
            retain_snapshots: 2,
            compaction_policy: policy,
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
/// Continuously monitors every table and automatically triggers:
/// - **Selective bin-packing** when a partition accumulates `>= min_files_to_compact` small files (`< min_file_bytes`)
/// - **Oversized/skewed file splitting** when any file exceeds `max_file_bytes` (e.g. post-`MERGE` or giant bulk load)
/// - **Delete-ratio DV materialization** when a file's Deletion Vector bloat crosses `delete_ratio_threshold` (default 20%)
/// - **Safe `VACUUM TABLE` (`ExpireSnapshots`)** to expire old unreferenced snapshots and reclaim orphaned storage.
///
/// Works safely on billion-row+ tables because compaction is selective, partition-aware, streaming-Arrow
/// bounded (`max_bytes_per_run`), and uses non-conflicting Iceberg `RewriteFiles` OCC!
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
        let mut policy = self.config.compaction_policy.clone();
        policy.min_small_files_per_partition = self.config.min_files_to_compact;

        for table_name in tables {
            report.tables_checked += 1;
            let meta = self.coordinator.catalog.get_table(&table_name).await?;
            let snap = meta.current_snapshot("main")?;

            // O(1) zero-I/O check: triggers automatically on small-file accumulation,
            // oversized/skewed files (> max_file_bytes), or high post-delete DV bloat (>= delete_ratio)!
            let selection = select_compaction_candidates(snap, &policy, false);
            if selection.total_candidate_files > 0 {
                tracing::info!(
                    table = %table_name,
                    candidates = selection.total_candidate_files,
                    skipped_healthy = selection.skipped_healthy_files,
                    "AutonomousMaintainer triggering selective streaming compaction"
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
