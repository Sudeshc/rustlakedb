// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod raft;
pub mod rest;

use async_trait::async_trait;
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::IcebergSplit;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    Bool,
    Date,
    TimestampTz,
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// Apache Iceberg v3 VARIANT type (supports automatic Parquet sub-column shredding, RFC-003).
    Variant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub field_id: i32,
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonShreddingConfig {
    pub auto_shred: bool,
    pub max_shredded_paths: usize,
    pub shredded_paths: Vec<String>,
}

impl Default for JsonShreddingConfig {
    fn default() -> Self {
        Self {
            auto_shred: true,
            max_shredded_paths: 128,
            shredded_paths: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PuffinIndexType {
    Inverted,
    Bloom,
    ThetaSketch,
    Hnsw,
    Bm25,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PuffinIndexSpec {
    pub name: String,
    pub column: String,
    pub json_path: Option<String>,
    pub index_type: PuffinIndexType,
}

/// File-scoped Puffin secondary index blob for JSON path acceleration (RFC-003).
#[derive(Debug, Clone, Default)]
pub struct FilePuffinIndex {
    pub inverted: HashMap<(String, String), RoaringBitmap>,
    pub bloom_hashes: HashSet<u64>,
    pub ndv_estimates: HashMap<String, u64>,
}

impl FilePuffinIndex {
    pub fn index_json_field(&mut self, json_path: &str, value: &str, row_id: u32) {
        self.inverted
            .entry((json_path.to_string(), value.to_string()))
            .or_default()
            .insert(row_id);
        let key = format!("{json_path}={value}");
        self.bloom_hashes.insert(xxh3_64(key.as_bytes()));
        // Also index key presence for Postgres JSONB `?` operator (RFC-003).
        let has_key = format!("__has_key__:{json_path}");
        self.inverted
            .entry((has_key.clone(), "1".to_string()))
            .or_default()
            .insert(row_id);
        self.bloom_hashes.insert(xxh3_64(has_key.as_bytes()));
    }

    pub fn lookup_json_eq(&self, json_path: &str, value: &str) -> Option<&RoaringBitmap> {
        let key = format!("{json_path}={value}");
        if !self.bloom_hashes.contains(&xxh3_64(key.as_bytes())) {
            return None;
        }
        self.inverted
            .get(&(json_path.to_string(), value.to_string()))
    }

    pub fn lookup_json_has_key(&self, json_path: &str) -> Option<&RoaringBitmap> {
        let has_key = format!("__has_key__:{json_path}");
        if !self.bloom_hashes.contains(&xxh3_64(has_key.as_bytes())) {
            return None;
        }
        self.inverted.get(&(has_key, "1".to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFileEntry {
    pub file_path: String,
    pub row_group_id: u32,
    pub row_count: u64,
    pub byte_size: u64,
    #[serde(skip)]
    pub deletion_vector: RoaringBitmap,
    #[serde(skip)]
    pub puffin_index: FilePuffinIndex,
    #[serde(default)]
    pub ndv_estimates: HashMap<String, u64>,
    /// Per-file zone-map minimum values (serialized text) for data skipping (Wave 4).
    #[serde(default)]
    pub column_min: HashMap<String, String>,
    /// Per-file zone-map maximum values (serialized text) for data skipping (Wave 4).
    #[serde(default)]
    pub column_max: HashMap<String, String>,
    /// Iceberg hidden partition values (`spec_expr -> partition_value`, Wave 14).
    #[serde(default)]
    pub partition_values: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    pub name: String,
    pub query_sql: String,
    pub materialized: bool,
    pub last_refreshed_ms: u64,
    /// Optional base table + last processed Iceberg snapshot ID for Incremental MV Refresh (Wave 8).
    #[serde(default)]
    pub source_table: Option<String>,
    #[serde(default)]
    pub last_source_snapshot_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSnapshot {
    pub snapshot_id: u64,
    pub parent_snapshot_id: Option<u64>,
    pub timestamp_ms: u64,
    pub data_files: Vec<DataFileEntry>,
    /// Arbitrary Iceberg snapshot summary properties (e.g. `kafka.offset.<topic>.<part>`, Wave 14/15).
    #[serde(default)]
    pub summary: HashMap<String, String>,
}

/// Evaluates an Apache Iceberg hidden-partition transform expression (`col`, `days(col)`,
/// `bucket(N, col)`, `truncate(W, col)`) against a row value and returns `(source_column, partition_value)`.
pub fn eval_partition_transform(spec_expr: &str, value_str: &str) -> (String, String) {
    let trimmed = spec_expr.trim();
    let lower = trimmed.to_lowercase();
    if let Some(inner) = lower
        .strip_prefix("days(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let col = trimmed[5..trimmed.len() - 1].trim().to_string();
        let day = value_str.chars().take(10).collect::<String>();
        let _ = inner;
        return (col, day);
    }
    if lower.starts_with("bucket(") && lower.ends_with(')') {
        let inner = &trimmed[7..trimmed.len() - 1];
        if let Some((n_str, col_str)) = inner.split_once(',') {
            let n = n_str.trim().parse::<u64>().unwrap_or(16).max(1);
            let col = col_str.trim().to_string();
            let b = xxh3_64(value_str.as_bytes()) % n;
            return (col, b.to_string());
        }
    }
    if lower.starts_with("truncate(") && lower.ends_with(')') {
        let inner = &trimmed[9..trimmed.len() - 1];
        if let Some((w_str, col_str)) = inner.split_once(',') {
            let w = w_str.trim().parse::<usize>().unwrap_or(4);
            let col = col_str.trim().to_string();
            let trunc: String = value_str.chars().take(w).collect();
            return (col, trunc);
        }
    }
    (trimmed.to_string(), value_str.to_string())
}

/// Extracts the underlying source column name from a hidden partition spec expression.
pub fn partition_spec_source_col(spec_expr: &str) -> String {
    eval_partition_transform(spec_expr, "").0
}

/// Column-level data distribution statistics stored by `ANALYZE TABLE` (Wave 24/26).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    pub ndv: u64,
    pub null_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub total_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableMetadata {
    pub table_name: String,
    pub format_version: u8,
    pub schema: Vec<FieldSchema>,
    pub json_shredding: JsonShreddingConfig,
    pub indexes: Vec<PuffinIndexSpec>,
    pub snapshots: BTreeMap<u64, IcebergSnapshot>,
    /// Git-style branches & tags (`main` -> latest snapshot_id, RFC-002).
    pub refs: HashMap<String, u64>,
    pub next_snapshot_id: u64,
    /// Liquid / Hilbert multi-column clustering keys (`ALTER TABLE ... CLUSTER BY`, Wave 4).
    #[serde(default)]
    pub cluster_by: Vec<String>,
    /// Iceberg Hidden Partitioning spec expressions (`CREATE TABLE ... PARTITION BY (...)`, Wave 12).
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Zero-rewrite Iceberg field-ID rename history: `current_col_name -> [historical_parquet_names]` (Wave 22).
    #[serde(default)]
    pub column_aliases: HashMap<String, Vec<String>>,
    /// Per-column SQL `DEFAULT` literal values enforced on write (Wave 24).
    #[serde(default)]
    pub column_defaults: HashMap<String, String>,
    /// Table-level and column-level SQL `CHECK (...)` expressions enforced on write (Wave 24).
    #[serde(default)]
    pub check_constraints: Vec<String>,
    /// Centralized catalog governance tags (`pii=true`, `domain=finance`, `owner=data-eng`, Wave 24).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Column-level statistics persisted by `ANALYZE TABLE` for real CBO selectivity (Wave 26).
    #[serde(default)]
    pub column_stats: HashMap<String, ColumnStats>,
}

impl IcebergTableMetadata {
    pub fn new(table_name: impl Into<String>, schema: Vec<FieldSchema>) -> Self {
        let initial = IcebergSnapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            timestamp_ms: 0,
            data_files: Vec::new(),
            summary: HashMap::new(),
        };
        let mut snapshots = BTreeMap::new();
        snapshots.insert(1, initial);
        let mut refs = HashMap::new();
        refs.insert("main".to_string(), 1);
        Self {
            table_name: table_name.into(),
            format_version: 3,
            schema,
            json_shredding: JsonShreddingConfig::default(),
            indexes: Vec::new(),
            snapshots,
            refs,
            next_snapshot_id: 2,
            cluster_by: Vec::new(),
            partition_by: Vec::new(),
            column_aliases: HashMap::new(),
            column_defaults: HashMap::new(),
            check_constraints: Vec::new(),
            tags: BTreeMap::new(),
            column_stats: HashMap::new(),
        }
    }

    pub fn current_snapshot(&self, branch: &str) -> Result<&IcebergSnapshot> {
        let snap_id = self
            .refs
            .get(branch)
            .ok_or_else(|| RustLakeError::Catalog(format!("branch '{branch}' not found")))?;
        self.snapshots
            .get(snap_id)
            .ok_or_else(|| RustLakeError::Catalog(format!("snapshot {snap_id} missing")))
    }

    pub fn snapshot_at_version(&self, version: u64) -> Result<&IcebergSnapshot> {
        self.snapshots
            .get(&version)
            .ok_or_else(|| RustLakeError::Catalog(format!("snapshot version {version} not found")))
    }

    /// Resolves the latest Iceberg snapshot committed at or before `ts_ms` (epoch milliseconds, Wave 24).
    pub fn snapshot_at_timestamp(&self, ts_ms: u64) -> Result<&IcebergSnapshot> {
        self.snapshots
            .values()
            .filter(|s| s.timestamp_ms <= ts_ms)
            .max_by_key(|s| (s.timestamp_ms, s.snapshot_id))
            .ok_or_else(|| {
                RustLakeError::Catalog(format!(
                    "no snapshot found at or before timestamp {ts_ms} ms on '{}'",
                    self.table_name
                ))
            })
    }

    pub fn splits(&self, branch: &str) -> Result<Vec<IcebergSplit>> {
        let snap = self.current_snapshot(branch)?;
        Ok(Self::splits_from_snapshot(&self.table_name, snap))
    }

    pub fn splits_at_version(&self, version: u64) -> Result<Vec<IcebergSplit>> {
        let snap = self.snapshot_at_version(version)?;
        Ok(Self::splits_from_snapshot(&self.table_name, snap))
    }

    pub fn splits_at_timestamp(&self, ts_ms: u64) -> Result<Vec<IcebergSplit>> {
        let snap = self.snapshot_at_timestamp(ts_ms)?;
        Ok(Self::splits_from_snapshot(&self.table_name, snap))
    }

    fn splits_from_snapshot(table_name: &str, snap: &IcebergSnapshot) -> Vec<IcebergSplit> {
        snap.data_files
            .iter()
            .map(|f| IcebergSplit {
                table_name: table_name.to_string(),
                file_path: f.file_path.clone(),
                row_group_id: f.row_group_id,
                estimated_bytes: f.byte_size,
            })
            .collect()
    }

    pub fn total_rows(&self, branch: &str) -> u64 {
        self.current_snapshot(branch)
            .map(|s| s.data_files.iter().map(|f| f.row_count).sum())
            .unwrap_or(0)
    }

    pub fn total_bytes(&self, branch: &str) -> u64 {
        self.current_snapshot(branch)
            .map(|s| s.data_files.iter().map(|f| f.byte_size).sum())
            .unwrap_or(0)
    }

    pub fn referenced_file_paths(&self) -> HashSet<String> {
        let mut set = HashSet::new();
        for snap in self.snapshots.values() {
            for df in &snap.data_files {
                set.insert(df.file_path.clone());
            }
        }
        set
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCommitProposal {
    pub table_name: String,
    pub branch: String,
    pub expected_snapshot_id: u64,
    pub added_files: Vec<DataFileEntry>,
    /// Files to remove from the active snapshot (used by compaction / rewrite).
    pub removed_files: Vec<String>,
    pub is_append_only: bool,
}

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn create_table(&self, metadata: IcebergTableMetadata) -> Result<()>;
    async fn drop_table(&self, table_name: &str) -> Result<()>;
    async fn get_table(&self, table_name: &str) -> Result<IcebergTableMetadata>;
    async fn list_tables(&self) -> Result<Vec<String>>;
    async fn add_column(&self, table_name: &str, name: String, data_type: ColumnType) -> Result<i32>;
    async fn add_index(&self, table_name: &str, spec: PuffinIndexSpec) -> Result<()>;
    async fn create_branch(&self, table_name: &str, from_branch: &str, new_branch: &str) -> Result<u64>;
    async fn merge_branch(&self, table_name: &str, source_branch: &str, target_branch: &str) -> Result<u64>;
    async fn commit_table(&self, proposal: TableCommitProposal) -> Result<u64>;
    async fn commit_multi_table(&self, proposals: Vec<TableCommitProposal>) -> Result<Vec<(String, u64)>>;
    /// Expires old snapshots not referenced by any branch, keeping the latest `keep_last` snapshots.
    async fn expire_snapshots(&self, table_name: &str, keep_last: usize) -> Result<usize>;
    async fn set_cluster_by(&self, table_name: &str, columns: Vec<String>) -> Result<()>;
    async fn set_shredded_paths(&self, table_name: &str, paths: Vec<String>) -> Result<()>;
    async fn create_view(&self, def: ViewDefinition) -> Result<()>;
    async fn drop_view(&self, name: &str) -> Result<()>;
    async fn get_view(&self, name: &str) -> Result<Option<ViewDefinition>>;
    async fn list_views(&self) -> Result<Vec<ViewDefinition>>;
    async fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()>;
    async fn truncate_table(&self, table_name: &str, branch: &str) -> Result<u64>;
    /// Zero-rewrite Iceberg field-ID column rename (`ALTER TABLE t RENAME COLUMN a TO b`, Wave 22).
    async fn rename_column(&self, table_name: &str, old_col: &str, new_col: &str) -> Result<i32>;
    /// Zero-rewrite Iceberg column drop (`ALTER TABLE t DROP COLUMN c`, Wave 22).
    async fn drop_column(&self, table_name: &str, col_name: &str, if_exists: bool) -> Result<()>;
    /// Zero-rewrite Iceberg column type widening (`ALTER TABLE t ALTER COLUMN c TYPE ...`, Wave 22).
    async fn widen_column(&self, table_name: &str, col_name: &str, new_type: ColumnType) -> Result<()>;
    /// Atomic Iceberg Time Travel rollback (`RESTORE TABLE t TO VERSION <snap_id>`, Wave 24).
    async fn restore_table(&self, table_name: &str, branch: &str, target_snapshot_id: u64) -> Result<u64>;
    /// Sets governance tags on a table (`ALTER TABLE t SET TAG ('k'='v', ...)`, Wave 24).
    async fn set_table_tags(&self, table_name: &str, tags: BTreeMap<String, String>) -> Result<()>;
    /// Sets a column `DEFAULT` value on a table (Wave 24).
    async fn set_column_default(&self, table_name: &str, col_name: &str, default_val: String) -> Result<()>;
    /// Persists `ANALYZE TABLE` column-level statistics into catalog metadata (Wave 26).
    async fn set_column_stats(&self, table_name: &str, stats: HashMap<String, ColumnStats>) -> Result<()>;
}

/// Embedded Iceberg v3 catalog modeling the `openraft` state machine (RFC-002).
/// Supports both pure in-memory mode and crash-safe atomic disk persistence (`open`).
pub struct InMemoryRaftCatalog {
    tables: RwLock<HashMap<String, IcebergTableMetadata>>,
    views: RwLock<HashMap<String, ViewDefinition>>,
    state_file: Option<PathBuf>,
    views_file: Option<PathBuf>,
}

impl Default for InMemoryRaftCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRaftCatalog {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            views: RwLock::new(HashMap::new()),
            state_file: None,
            views_file: None,
        }
    }

    /// Opens or creates a crash-safe persistent catalog stored under `<dir>/_catalog/state.json`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let catalog_dir = dir.as_ref().join("_catalog");
        std::fs::create_dir_all(&catalog_dir)?;
        let state_file = catalog_dir.join("state.json");
        let views_file = catalog_dir.join("views.json");

        let tables = if state_file.exists() {
            let bytes = std::fs::read(&state_file)?;
            serde_json::from_slice(&bytes)?
        } else {
            HashMap::new()
        };
        let views = if views_file.exists() {
            let bytes = std::fs::read(&views_file)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(Self {
            tables: RwLock::new(tables),
            views: RwLock::new(views),
            state_file: Some(state_file),
            views_file: Some(views_file),
        })
    }

    fn persist_locked(&self, tables: &HashMap<String, IcebergTableMetadata>) -> Result<()> {
        if let Some(ref path) = self.state_file {
            let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let bytes = serde_json::to_vec_pretty(tables)?;
            std::fs::write(&tmp, bytes)?;
            std::fs::rename(&tmp, path)?;
        }
        Ok(())
    }

    fn persist_views_locked(&self, views: &HashMap<String, ViewDefinition>) -> Result<()> {
        if let Some(ref path) = self.views_file {
            let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let bytes = serde_json::to_vec_pretty(views)?;
            std::fs::write(&tmp, bytes)?;
            std::fs::rename(&tmp, path)?;
        }
        Ok(())
    }

    /// Refreshes in-memory catalog view from disk so independent Worker OS processes
    /// immediately observe tables and snapshots committed by the Coordinator process.
    fn sync_from_disk(&self) {
        if let Some(ref path) = self.state_file
            && path.exists()
            && let Ok(bytes) = std::fs::read(path)
            && let Ok(loaded) = serde_json::from_slice::<HashMap<String, IcebergTableMetadata>>(&bytes)
        {
            *self.tables.write() = loaded;
        }
        if let Some(ref path) = self.views_file
            && path.exists()
            && let Ok(bytes) = std::fs::read(path)
            && let Ok(loaded) = serde_json::from_slice::<HashMap<String, ViewDefinition>>(&bytes)
        {
            *self.views.write() = loaded;
        }
    }
}

/// Normalizes schema-qualified & double-quoted table names (e.g. `"public"."customers"` -> `"customers"`).
pub fn normalize_table_name(raw: &str) -> String {
    raw.trim()
        .rsplit('.')
        .next()
        .unwrap_or(raw)
        .trim()
        .trim_matches('"')
        .to_string()
}

#[async_trait]
impl Catalog for InMemoryRaftCatalog {
    async fn create_table(&self, mut metadata: IcebergTableMetadata) -> Result<()> {
        metadata.table_name = normalize_table_name(&metadata.table_name);
        let mut guard = self.tables.write();
        if guard.contains_key(&metadata.table_name) {
            return Err(RustLakeError::Catalog(format!(
                "table '{}' already exists",
                metadata.table_name
            )));
        }
        guard.insert(metadata.table_name.clone(), metadata);
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn drop_table(&self, table_name: &str) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        if guard.remove(&key).is_none() {
            return Err(RustLakeError::Catalog(format!(
                "table '{table_name}' not found"
            )));
        }
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn get_table(&self, table_name: &str) -> Result<IcebergTableMetadata> {
        let key = normalize_table_name(table_name);
        self.sync_from_disk();
        let guard = self.tables.read();
        guard
            .get(&key)
            .cloned()
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        self.sync_from_disk();
        let guard = self.tables.read();
        let mut names: Vec<String> = guard.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    async fn add_column(
        &self,
        table_name: &str,
        name: String,
        data_type: ColumnType,
    ) -> Result<i32> {
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(table_name)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        if table.schema.iter().any(|f| f.name == name) {
            return Err(RustLakeError::Catalog(format!(
                "column '{name}' already exists on '{table_name}'"
            )));
        }
        let next_id = table.schema.iter().map(|f| f.field_id).max().unwrap_or(0) + 1;
        table.schema.push(FieldSchema {
            field_id: next_id,
            name,
            data_type,
            nullable: true,
        });
        self.persist_locked(&guard)?;
        Ok(next_id)
    }

    async fn add_index(&self, table_name: &str, spec: PuffinIndexSpec) -> Result<()> {
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(table_name)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        table.indexes.push(spec);
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn create_branch(
        &self,
        table_name: &str,
        from_branch: &str,
        new_branch: &str,
    ) -> Result<u64> {
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(table_name)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let base_snap = *table
            .refs
            .get(from_branch)
            .ok_or_else(|| RustLakeError::Catalog(format!("branch '{from_branch}' not found")))?;
        table.refs.insert(new_branch.to_string(), base_snap);
        self.persist_locked(&guard)?;
        Ok(base_snap)
    }

    async fn merge_branch(
        &self,
        table_name: &str,
        source_branch: &str,
        target_branch: &str,
    ) -> Result<u64> {
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(table_name)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let src_snap = *table
            .refs
            .get(source_branch)
            .ok_or_else(|| RustLakeError::Catalog(format!("branch '{source_branch}' not found")))?;
        table.refs.insert(target_branch.to_string(), src_snap);
        self.persist_locked(&guard)?;
        Ok(src_snap)
    }

    async fn commit_table(&self, proposal: TableCommitProposal) -> Result<u64> {
        let res = self.commit_multi_table(vec![proposal]).await?;
        Ok(res[0].1)
    }

    async fn commit_multi_table(
        &self,
        proposals: Vec<TableCommitProposal>,
    ) -> Result<Vec<(String, u64)>> {
        let mut guard = self.tables.write();

        // Phase 1: Atomic OCC validation across ALL participating tables.
        for prop in &proposals {
            let table = guard.get(&prop.table_name).ok_or_else(|| {
                RustLakeError::Catalog(format!("table '{}' not found", prop.table_name))
            })?;
            let current_id = *table.refs.get(&prop.branch).ok_or_else(|| {
                RustLakeError::Catalog(format!("branch '{}' not found", prop.branch))
            })?;
            if current_id != prop.expected_snapshot_id && !prop.is_append_only {
                return Err(RustLakeError::OccConflict {
                    table: prop.table_name.clone(),
                    reason: format!(
                        "expected snapshot {}, found {}",
                        prop.expected_snapshot_id, current_id
                    ),
                });
            }
        }

        // Phase 2: All OCC checks passed -> commit all table pointers atomically.
        let mut committed = Vec::with_capacity(proposals.len());
        for prop in proposals {
            let table = guard.get_mut(&prop.table_name).expect("validated above");
            let current_id = *table.refs.get(&prop.branch).expect("validated above");
            let removed_set: HashSet<String> = prop.removed_files.into_iter().collect();
            let mut files: Vec<DataFileEntry> = table
                .snapshots
                .get(&current_id)
                .expect("validated snapshot")
                .data_files
                .iter()
                .filter(|f| !removed_set.contains(&f.file_path))
                .cloned()
                .collect();
            files.extend(prop.added_files);

            let new_id = table.next_snapshot_id;
            table.next_snapshot_id += 1;
            table.snapshots.insert(
                new_id,
                IcebergSnapshot {
                    snapshot_id: new_id,
                    parent_snapshot_id: Some(current_id),
                    timestamp_ms: new_id * 1000,
                    data_files: files,
                    summary: HashMap::new(),
                },
            );
            table.refs.insert(prop.branch, new_id);
            committed.push((prop.table_name, new_id));
        }

        self.persist_locked(&guard)?;
        Ok(committed)
    }

    async fn expire_snapshots(&self, table_name: &str, keep_last: usize) -> Result<usize> {
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(table_name)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let protected_refs: HashSet<u64> = table.refs.values().copied().collect();
        let all_ids: Vec<u64> = table.snapshots.keys().copied().collect();
        let keep_recent: HashSet<u64> = all_ids
            .iter()
            .rev()
            .take(std::cmp::max(1, keep_last))
            .copied()
            .collect();

        let before = table.snapshots.len();
        table
            .snapshots
            .retain(|id, _| protected_refs.contains(id) || keep_recent.contains(id));
        let expired = before - table.snapshots.len();
        self.persist_locked(&guard)?;
        Ok(expired)
    }

    async fn set_cluster_by(&self, table_name: &str, columns: Vec<String>) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        table.cluster_by = columns;
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn set_shredded_paths(&self, table_name: &str, paths: Vec<String>) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        table.json_shredding.shredded_paths = paths;
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn create_view(&self, mut def: ViewDefinition) -> Result<()> {
        def.name = normalize_table_name(&def.name);
        let mut guard = self.views.write();
        guard.insert(def.name.clone(), def);
        self.persist_views_locked(&guard)?;
        Ok(())
    }

    async fn drop_view(&self, name: &str) -> Result<()> {
        let key = normalize_table_name(name);
        let mut guard = self.views.write();
        if guard.remove(&key).is_none() {
            return Err(RustLakeError::Catalog(format!("view '{name}' not found")));
        }
        self.persist_views_locked(&guard)?;
        Ok(())
    }

    async fn get_view(&self, name: &str) -> Result<Option<ViewDefinition>> {
        self.sync_from_disk();
        let key = normalize_table_name(name);
        Ok(self.views.read().get(&key).cloned())
    }

    async fn list_views(&self) -> Result<Vec<ViewDefinition>> {
        self.sync_from_disk();
        let mut v: Vec<ViewDefinition> = self.views.read().values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    async fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        let old_key = normalize_table_name(old_name);
        let new_key = normalize_table_name(new_name);
        let mut guard = self.tables.write();
        if guard.contains_key(&new_key) {
            return Err(RustLakeError::Catalog(format!(
                "target table '{new_key}' already exists"
            )));
        }
        let mut meta = guard
            .remove(&old_key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{old_name}' not found")))?;
        meta.table_name = new_key.clone();
        guard.insert(new_key, meta);
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn truncate_table(&self, table_name: &str, branch: &str) -> Result<u64> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let current_id = *table
            .refs
            .get(branch)
            .ok_or_else(|| RustLakeError::Catalog(format!("branch '{branch}' not found")))?;
        let new_id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        table.snapshots.insert(
            new_id,
            IcebergSnapshot {
                snapshot_id: new_id,
                parent_snapshot_id: Some(current_id),
                timestamp_ms: new_id * 1000,
                data_files: Vec::new(),
                summary: HashMap::new(),
            },
        );
        table.refs.insert(branch.to_string(), new_id);
        self.persist_locked(&guard)?;
        Ok(new_id)
    }

    async fn rename_column(&self, table_name: &str, old_col: &str, new_col: &str) -> Result<i32> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        if table.schema.iter().any(|f| f.name.eq_ignore_ascii_case(new_col)) {
            return Err(RustLakeError::Catalog(format!(
                "column '{new_col}' already exists in '{table_name}'"
            )));
        }
        let field = table
            .schema
            .iter_mut()
            .find(|f| f.name.eq_ignore_ascii_case(old_col))
            .ok_or_else(|| {
                RustLakeError::Catalog(format!("column '{old_col}' not found in '{table_name}'"))
            })?;
        let fid = field.field_id;
        let prev_name = field.name.clone();
        field.name = new_col.to_string();
        let mut hist = table.column_aliases.remove(&prev_name).unwrap_or_default();
        if !hist.contains(&prev_name) {
            hist.push(prev_name);
        }
        table.column_aliases.insert(new_col.to_string(), hist);
        self.persist_locked(&guard)?;
        Ok(fid)
    }

    async fn drop_column(&self, table_name: &str, col_name: &str, if_exists: bool) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let before = table.schema.len();
        table.schema.retain(|f| !f.name.eq_ignore_ascii_case(col_name));
        if table.schema.len() == before {
            if if_exists {
                return Ok(());
            }
            return Err(RustLakeError::Catalog(format!(
                "column '{col_name}' not found in '{table_name}'"
            )));
        }
        table.column_aliases.remove(col_name);
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn widen_column(
        &self,
        table_name: &str,
        col_name: &str,
        new_type: ColumnType,
    ) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let field = table
            .schema
            .iter_mut()
            .find(|f| f.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                RustLakeError::Catalog(format!("column '{col_name}' not found in '{table_name}'"))
            })?;
        field.data_type = new_type;
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn restore_table(
        &self,
        table_name: &str,
        branch: &str,
        target_snapshot_id: u64,
    ) -> Result<u64> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let target_files = table
            .snapshots
            .get(&target_snapshot_id)
            .ok_or_else(|| {
                RustLakeError::Catalog(format!(
                    "snapshot version {target_snapshot_id} not found on '{table_name}'"
                ))
            })?
            .data_files
            .clone();
        let parent_id = table.refs.get(branch).copied();
        let new_id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut summary = HashMap::new();
        summary.insert("operation".into(), "restore".into());
        summary.insert("restored_from_snapshot_id".into(), target_snapshot_id.to_string());
        table.snapshots.insert(
            new_id,
            IcebergSnapshot {
                snapshot_id: new_id,
                parent_snapshot_id: parent_id,
                timestamp_ms: now_ms,
                data_files: target_files,
                summary,
            },
        );
        table.refs.insert(branch.to_string(), new_id);
        self.persist_locked(&guard)?;
        Ok(new_id)
    }

    async fn set_table_tags(
        &self,
        table_name: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        for (k, v) in tags {
            table.tags.insert(k, v);
        }
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn set_column_default(
        &self,
        table_name: &str,
        col_name: &str,
        default_val: String,
    ) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        table.column_defaults.insert(col_name.to_string(), default_val);
        self.persist_locked(&guard)?;
        Ok(())
    }

    async fn set_column_stats(
        &self,
        table_name: &str,
        stats: HashMap<String, ColumnStats>,
    ) -> Result<()> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        table.column_stats = stats;
        self.persist_locked(&guard)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multi_table_acid_rolls_back_all_on_occ_conflict() {
        let cat = InMemoryRaftCatalog::new();
        let schema = vec![FieldSchema {
            field_id: 1,
            name: "id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        }];
        cat.create_table(IcebergTableMetadata::new("accounts", schema.clone()))
            .await
            .unwrap();
        cat.create_table(IcebergTableMetadata::new("ledger", schema))
            .await
            .unwrap();

        cat.commit_table(TableCommitProposal {
            table_name: "ledger".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: vec![],
            removed_files: vec![],
            is_append_only: false,
        })
        .await
        .unwrap();

        let err = cat
            .commit_multi_table(vec![
                TableCommitProposal {
                    table_name: "accounts".into(),
                    branch: "main".into(),
                    expected_snapshot_id: 1,
                    added_files: vec![],
                    removed_files: vec![],
                    is_append_only: false,
                },
                TableCommitProposal {
                    table_name: "ledger".into(),
                    branch: "main".into(),
                    expected_snapshot_id: 1,
                    added_files: vec![],
                    removed_files: vec![],
                    is_append_only: false,
                },
            ])
            .await
            .unwrap_err();
        assert!(matches!(err, RustLakeError::OccConflict { .. }));

        let accounts = cat.get_table("accounts").await.unwrap();
        assert_eq!(accounts.current_snapshot("main").unwrap().snapshot_id, 1);
    }

    #[tokio::test]
    async fn git_style_branching_and_merge_works() {
        let cat = InMemoryRaftCatalog::new();
        cat.create_table(IcebergTableMetadata::new("events", vec![]))
            .await
            .unwrap();
        cat.create_branch("events", "main", "staging").await.unwrap();

        let new_snap = cat
            .commit_table(TableCommitProposal {
                table_name: "events".into(),
                branch: "staging".into(),
                expected_snapshot_id: 1,
                added_files: vec![],
                removed_files: vec![],
                is_append_only: true,
            })
            .await
            .unwrap();
        assert_eq!(new_snap, 2);

        let t = cat.get_table("events").await.unwrap();
        assert_eq!(t.current_snapshot("main").unwrap().snapshot_id, 1);
        assert_eq!(t.current_snapshot("staging").unwrap().snapshot_id, 2);

        cat.merge_branch("events", "staging", "main").await.unwrap();
        let t2 = cat.get_table("events").await.unwrap();
        assert_eq!(t2.current_snapshot("main").unwrap().snapshot_id, 2);
    }
}
