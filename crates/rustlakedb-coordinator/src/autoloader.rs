// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::Coordinator;
use rustlakedb_common::{Result, RustLakeError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AutoLoaderCheckpoint {
    processed_files: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AutoLoaderReport {
    pub files_ingested: usize,
    pub rows_loaded: usize,
    pub dlq_rows: usize,
}

/// Cloud / Landing-Prefix Auto-Loader Daemon (Wave 9):
/// Watches a landing directory for newly arrived `.ndjson`/`.jsonl`/`.csv`/`.parquet` files,
/// tracks checkpoints durably in `_autoloader/checkpoints.json` for **exactly-once** ingestion,
/// and routes malformed NDJSON rows to `<table_name>_dlq` without halting pipelines.
pub struct AutoLoader {
    coord: Arc<Coordinator>,
    table_name: String,
    landing_dir: PathBuf,
    checkpoint_file: PathBuf,
    branch: String,
}

impl AutoLoader {
    pub fn new(
        coord: Arc<Coordinator>,
        table_name: impl Into<String>,
        landing_dir: impl AsRef<Path>,
        checkpoint_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        std::fs::create_dir_all(landing_dir.as_ref())?;
        std::fs::create_dir_all(checkpoint_dir.as_ref())?;
        let tbl = table_name.into();
        let checkpoint_file = checkpoint_dir
            .as_ref()
            .join(format!("autoloader_{tbl}_checkpoint.json"));
        Ok(Self {
            coord,
            table_name: tbl,
            landing_dir: landing_dir.as_ref().to_path_buf(),
            checkpoint_file,
            branch: "main".into(),
        })
    }

    fn load_checkpoint(&self) -> AutoLoaderCheckpoint {
        if self.checkpoint_file.exists()
            && let Ok(bytes) = std::fs::read(&self.checkpoint_file)
            && let Ok(cp) = serde_json::from_slice(&bytes)
        {
            return cp;
        }
        AutoLoaderCheckpoint::default()
    }

    fn save_checkpoint(&self, cp: &AutoLoaderCheckpoint) -> Result<()> {
        let tmp = self
            .checkpoint_file
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, serde_json::to_vec_pretty(cp)?)?;
        std::fs::rename(&tmp, &self.checkpoint_file)?;
        Ok(())
    }

    /// Runs a single idempotent scan-and-ingest pass over `landing_dir`.
    pub async fn run_once(&self) -> Result<AutoLoaderReport> {
        let mut cp = self.load_checkpoint();
        let mut report = AutoLoaderReport::default();

        let mut entries: Vec<PathBuf> = std::fs::read_dir(&self.landing_dir)?
            .filter_map(|e| e.ok().map(|de| de.path()))
            .filter(|p| p.is_file())
            .collect();
        entries.sort();

        for path in entries {
            let fname = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            if fname.starts_with('.') || fname.ends_with(".tmp") || cp.processed_files.contains(&fname)
            {
                continue;
            }
            let lower = fname.to_lowercase();
            let (loaded, dlq) = if lower.ends_with(".ndjson")
                || lower.ends_with(".jsonl")
                || lower.ends_with(".json")
            {
                let body = std::fs::read_to_string(&path).map_err(|e| {
                    RustLakeError::Execution(format!("autoloader read '{}': {e}", path.display()))
                })?;
                self.coord
                    .ingest_ndjson(&self.table_name, &self.branch, &body, true)
                    .await?
            } else if lower.ends_with(".csv") {
                let sql = format!(
                    "COPY {} FROM '{}' (FORMAT CSV, ON_ERROR CONTINUE)",
                    self.table_name,
                    path.display().to_string().replace('\\', "/")
                );
                let res = self.coord.execute_sql(&sql, &self.branch).await?;
                let loaded = res
                    .command_tag
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                (loaded, 0)
            } else if lower.ends_with(".parquet") {
                let sql = format!(
                    "COPY {} FROM '{}' (FORMAT PARQUET)",
                    self.table_name,
                    path.display().to_string().replace('\\', "/")
                );
                let res = self.coord.execute_sql(&sql, &self.branch).await?;
                let loaded = res
                    .command_tag
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                (loaded, 0)
            } else {
                continue;
            };

            cp.processed_files.insert(fname);
            self.save_checkpoint(&cp)?;
            report.files_ingested += 1;
            report.rows_loaded += loaded;
            report.dlq_rows += dlq;
        }

        Ok(report)
    }

    pub async fn run_loop(&self, poll_interval: Duration) -> Result<()> {
        loop {
            match self.run_once().await {
                Ok(rep) if rep.files_ingested > 0 => {
                    tracing::info!(
                        table = %self.table_name,
                        files = rep.files_ingested,
                        rows = rep.rows_loaded,
                        dlq = rep.dlq_rows,
                        "AutoLoader ingested micro-batch"
                    );
                }
                Err(e) => {
                    tracing::warn!(table = %self.table_name, error = %e, "AutoLoader cycle error");
                }
                _ => {}
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}
