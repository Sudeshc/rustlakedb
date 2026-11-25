// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod raft;
pub mod rest;

use async_trait::async_trait;
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{FilterExpr, IcebergSplit};
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
    /// Immutable Iceberg v3 Deletion Vector blob path scoped to this snapshot (preserves Time Travel & RESTORE).
    #[serde(default)]
    pub dv_path: Option<String>,
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

/// Apache Iceberg v3 Equality Delete file descriptor (zero-read CDC deletes/upserts, Wave 31).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqualityDeleteFile {
    pub file_path: String,
    pub equality_column: String,
    pub row_count: u64,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSnapshot {
    pub snapshot_id: u64,
    pub parent_snapshot_id: Option<u64>,
    pub timestamp_ms: u64,
    pub data_files: Vec<DataFileEntry>,
    /// Active Apache Iceberg v3 Equality Delete Parquet files for zero-read CDC upserts/deletes (Wave 31).
    #[serde(default)]
    pub equality_deletes: Vec<EqualityDeleteFile>,
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
            equality_deletes: Vec::new(),
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
                dv_path: f.dv_path.clone(),
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
                if let Some(ref dv) = df.dv_path {
                    set.insert(dv.clone());
                }
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

/// Persisted dynamic column/JSON masking rule replicated via Raft (Wave 29).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedMaskingRule {
    pub name: String,
    pub target: String,
    pub mask_value: String,
}

/// Persisted table-level data lineage edge replicated via Raft (Wave 29).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedLineageEdge {
    pub source_table: String,
    pub target_table: String,
    pub operation: String,
    pub user: String,
    pub timestamp_ms: u64,
}

/// Crash-safe, Raft-replicated Enterprise Governance State (RBAC, RLS, Masking, Lineage, Wave 29).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistedGovernanceState {
    #[serde(default)]
    pub roles: Vec<String>,
    /// `(role, table, privilege)` tuples
    #[serde(default)]
    pub grants: Vec<(String, String, String)>,
    /// `(role, table, predicate)` RLS policies
    #[serde(default)]
    pub rls_policies: Vec<(String, String, FilterExpr)>,
    /// `(role, table, rule)` masking rules
    #[serde(default)]
    pub masking_policies: Vec<(String, String, PersistedMaskingRule)>,
    /// Table-level data lineage graph edges
    #[serde(default)]
    pub lineage: Vec<PersistedLineageEdge>,
}

/// Replicated governance mutation committed through the Raft WAL (Wave 29).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GovernanceOp {
    CreateRole(String),
    Grant {
        role: String,
        table: String,
        privilege: String,
    },
    Revoke {
        role: String,
        table: String,
        privilege: String,
    },
    AddRlsPolicy {
        role: String,
        table: String,
        predicate: FilterExpr,
    },
    AddMaskingPolicy {
        role: String,
        table: String,
        rule: PersistedMaskingRule,
    },
    RecordLineage(PersistedLineageEdge),
}

/// Live Raft consensus telemetry exported to `/healthz` and Prometheus `/metrics`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftTelemetry {
    pub node_id: u64,
    pub is_leader: bool,
    pub term: u64,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub leader_changes: u64,
}

/// Persists `ANALYZE TABLE` column-level statistics into catalog metadata (Wave 26).
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Returns live Raft consensus telemetry when backed by a multi-node `RaftCatalogNode`.
    fn raft_telemetry(&self) -> Option<RaftTelemetry> {
        None
    }
    /// Returns `(table_count, max_snapshots_on_any_table)` with zero metadata cloning for `/metrics`.
    fn catalog_stats(&self) -> (usize, usize) {
        (0, 0)
    }
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
    /// Applies a crash-safe, Raft-replicated governance mutation (RBAC, RLS, Masking, Lineage, Wave 29).
    async fn apply_governance(&self, op: GovernanceOp) -> Result<()>;
    /// Reads the latest Raft-replicated governance state snapshot (Wave 29).
    async fn get_governance(&self) -> Result<PersistedGovernanceState>;
    /// Atomically commits a zero-read Apache Iceberg v3 Equality Delete file + optional upserted data files (Wave 31).
    async fn commit_equality_delete(
        &self,
        table_name: &str,
        branch: &str,
        eq_delete: EqualityDeleteFile,
        upserted_files: Vec<DataFileEntry>,
    ) -> Result<u64>;
}

/// Atomic point-in-time catalog state snapshot used by Raft `InstallSnapshot` RPCs
/// when a partitioned or restarted follower lags behind the leader's WAL compaction index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullCatalogSnapshot {
    pub tables: HashMap<String, IcebergTableMetadata>,
    pub views: HashMap<String, ViewDefinition>,
    pub governance: PersistedGovernanceState,
}

/// Embedded Iceberg v3 catalog modeling the `openraft` state machine (RFC-002).
/// Supports both pure in-memory mode and crash-safe atomic disk persistence (`open`).
pub struct InMemoryRaftCatalog {
    tables: RwLock<HashMap<String, IcebergTableMetadata>>,
    views: RwLock<HashMap<String, ViewDefinition>>,
    governance: RwLock<PersistedGovernanceState>,
    state_file: Option<PathBuf>,
    views_file: Option<PathBuf>,
    governance_file: Option<PathBuf>,
    /// Tracks `(mtime, byte_len)` of last written/synced catalog JSON files so disk reloads
    /// execute strictly under the write lock (eliminating TOCTOU lost updates under concurrent writers!).
    last_state_stamp: RwLock<Option<(std::time::SystemTime, u64)>>,
    last_views_stamp: RwLock<Option<(std::time::SystemTime, u64)>>,
    last_governance_stamp: RwLock<Option<(std::time::SystemTime, u64)>>,
}

fn max_retained_snapshots() -> usize {
    std::env::var("RUSTLAKEDB_MAX_RETAINED_SNAPSHOTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64)
        .max(8)
}

/// Bounds in-memory & persisted historical snapshot retention to O(1) per table while always
/// preserving every snapshot pointed to by a live branch/tag in `table.refs`.
fn prune_old_snapshots(table: &mut IcebergTableMetadata) {
    let cap = max_retained_snapshots();
    if table.snapshots.len() <= cap {
        return;
    }
    let pinned: HashSet<u64> = table.refs.values().copied().collect();
    let removable: Vec<u64> = table
        .snapshots
        .keys()
        .copied()
        .filter(|sid| !pinned.contains(sid))
        .collect();
    let to_drop = table.snapshots.len().saturating_sub(cap);
    for sid in removable.into_iter().take(to_drop) {
        table.snapshots.remove(&sid);
    }
}

fn file_stamp(path: &Path) -> Option<(std::time::SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.modified().ok()?, meta.len()))
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
            governance: RwLock::new(PersistedGovernanceState::default()),
            state_file: None,
            views_file: None,
            governance_file: None,
            last_state_stamp: RwLock::new(None),
            last_views_stamp: RwLock::new(None),
            last_governance_stamp: RwLock::new(None),
        }
    }

    /// Opens or creates a crash-safe persistent catalog stored under `<dir>/_catalog/state.json`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let catalog_dir = dir.as_ref().join("_catalog");
        std::fs::create_dir_all(&catalog_dir)?;
        let state_file = catalog_dir.join("state.json");
        let views_file = catalog_dir.join("views.json");
        let governance_file = catalog_dir.join("governance.json");

        let tables = if state_file.exists() {
            let bytes = std::fs::read(&state_file)?;
            serde_json::from_slice(&bytes)?
        } else {
            HashMap::new()
        };
        let state_stamp = file_stamp(&state_file);
        let views = if views_file.exists() {
            let bytes = std::fs::read(&views_file)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            HashMap::new()
        };
        let views_stamp = file_stamp(&views_file);
        let governance = if governance_file.exists() {
            let bytes = std::fs::read(&governance_file)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            PersistedGovernanceState::default()
        };
        let governance_stamp = file_stamp(&governance_file);

        Ok(Self {
            tables: RwLock::new(tables),
            views: RwLock::new(views),
            governance: RwLock::new(governance),
            state_file: Some(state_file),
            views_file: Some(views_file),
            governance_file: Some(governance_file),
            last_state_stamp: RwLock::new(state_stamp),
            last_views_stamp: RwLock::new(views_stamp),
            last_governance_stamp: RwLock::new(governance_stamp),
        })
    }

    fn persist_locked(&self, tables: &HashMap<String, IcebergTableMetadata>) -> Result<()> {
        if let Some(ref path) = self.state_file {
            let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let bytes = serde_json::to_vec(tables)?;
            std::fs::write(&tmp, bytes)?;
            std::fs::rename(&tmp, path)?;
            *self.last_state_stamp.write() = file_stamp(path);
        }
        Ok(())
    }

    fn persist_views_locked(&self, views: &HashMap<String, ViewDefinition>) -> Result<()> {
        if let Some(ref path) = self.views_file {
            let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let bytes = serde_json::to_vec(views)?;
            std::fs::write(&tmp, bytes)?;
            std::fs::rename(&tmp, path)?;
            *self.last_views_stamp.write() = file_stamp(path);
        }
        Ok(())
    }

    fn persist_governance_locked(&self, gov: &PersistedGovernanceState) -> Result<()> {
        if let Some(ref path) = self.governance_file {
            let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let bytes = serde_json::to_vec(gov)?;
            std::fs::write(&tmp, bytes)?;
            std::fs::rename(&tmp, path)?;
            *self.last_governance_stamp.write() = file_stamp(path);
        }
        Ok(())
    }

    /// Exports an atomic point-in-time snapshot of tables, views, and governance for Raft `InstallSnapshot`.
    pub fn export_snapshot(&self) -> FullCatalogSnapshot {
        FullCatalogSnapshot {
            tables: self.tables.read().clone(),
            views: self.views.read().clone(),
            governance: self.governance.read().clone(),
        }
    }

    /// Atomically installs an authoritative Raft `InstallSnapshot` state onto this follower and persists it.
    pub fn install_snapshot(&self, snap: FullCatalogSnapshot) -> Result<()> {
        {
            let mut t = self.tables.write();
            *t = snap.tables;
            self.persist_locked(&t)?;
        }
        {
            let mut v = self.views.write();
            *v = snap.views;
            self.persist_views_locked(&v)?;
        }
        {
            let mut g = self.governance.write();
            *g = snap.governance;
            self.persist_governance_locked(&g)?;
        }
        Ok(())
    }

    /// Refreshes in-memory catalog view from disk strictly under the write lock, skipping I/O
    /// when `(mtime, len)` matches our own last commit (zero TOCTOU lost updates under concurrency!).
    fn sync_from_disk(&self) {
        if let Some(ref path) = self.state_file {
            let cur_stamp = file_stamp(path);
            if cur_stamp.is_some() && cur_stamp != *self.last_state_stamp.read() {
                let mut guard = self.tables.write();
                let recheck = file_stamp(path);
                if recheck.is_some()
                    && recheck != *self.last_state_stamp.read()
                    && let Ok(bytes) = std::fs::read(path)
                    && let Ok(loaded) =
                        serde_json::from_slice::<HashMap<String, IcebergTableMetadata>>(&bytes)
                {
                    *guard = loaded;
                    *self.last_state_stamp.write() = recheck;
                }
            }
        }
        if let Some(ref path) = self.views_file {
            let cur_stamp = file_stamp(path);
            if cur_stamp.is_some() && cur_stamp != *self.last_views_stamp.read() {
                let mut guard = self.views.write();
                let recheck = file_stamp(path);
                if recheck.is_some()
                    && recheck != *self.last_views_stamp.read()
                    && let Ok(bytes) = std::fs::read(path)
                    && let Ok(loaded) =
                        serde_json::from_slice::<HashMap<String, ViewDefinition>>(&bytes)
                {
                    *guard = loaded;
                    *self.last_views_stamp.write() = recheck;
                }
            }
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
    fn catalog_stats(&self) -> (usize, usize) {
        let guard = self.tables.read();
        let max_snaps = guard
            .values()
            .map(|t| t.snapshots.len())
            .max()
            .unwrap_or(0);
        (guard.len(), max_snaps)
    }

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
                // Wave 31: Partition-Level Disjoint OCC Resolution —
                // If this mutating commit AND every intervening commit only touched strictly disjoint
                // non-empty partition sets (and disjoint removed_files), auto-rebase cleanly!
                let mut prop_parts: HashSet<BTreeMap<String, String>> = HashSet::new();
                for f in &prop.added_files {
                    if !f.partition_values.is_empty() {
                        prop_parts.insert(f.partition_values.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                    }
                }
                if let Some(exp_snap) = table.snapshots.get(&prop.expected_snapshot_id) {
                    let rem_set: HashSet<&str> = prop.removed_files.iter().map(|s| s.as_str()).collect();
                    for f in &exp_snap.data_files {
                        if rem_set.contains(f.file_path.as_str()) && !f.partition_values.is_empty() {
                            prop_parts.insert(f.partition_values.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                        }
                    }
                }
                let mut disjoint_ok = !prop_parts.is_empty();
                if disjoint_ok {
                    let prop_rem: HashSet<&str> = prop.removed_files.iter().map(|s| s.as_str()).collect();
                    for sid in (prop.expected_snapshot_id + 1)..=current_id {
                        let Some(inter) = table.snapshots.get(&sid) else {
                            disjoint_ok = false;
                            break;
                        };
                        let Some(raw_parts) = inter.summary.get("touched_partitions") else {
                            disjoint_ok = false;
                            break;
                        };
                        let inter_parts: Vec<BTreeMap<String, String>> =
                            serde_json::from_str(raw_parts).unwrap_or_default();
                        if inter_parts.is_empty()
                            || inter_parts.iter().any(|p| prop_parts.contains(p))
                        {
                            disjoint_ok = false;
                            break;
                        }
                        if let Some(raw_rem) = inter.summary.get("removed_files") {
                            let inter_rem: Vec<String> =
                                serde_json::from_str(raw_rem).unwrap_or_default();
                            if inter_rem.iter().any(|r| prop_rem.contains(r.as_str())) {
                                disjoint_ok = false;
                                break;
                            }
                        }
                    }
                }
                if !disjoint_ok {
                    return Err(RustLakeError::OccConflict {
                        table: prop.table_name.clone(),
                        reason: format!(
                            "expected snapshot {}, found {}",
                            prop.expected_snapshot_id, current_id
                        ),
                    });
                }
            }
        }

        // Phase 2: All OCC checks passed -> commit all table pointers atomically.
        let mut committed = Vec::with_capacity(proposals.len());
        for prop in proposals {
            let table = guard.get_mut(&prop.table_name).expect("validated above");
            let current_id = *table.refs.get(&prop.branch).expect("validated above");
            let cur_snap = table
                .snapshots
                .get(&current_id)
                .expect("validated snapshot");
            let removed_set: HashSet<String> = prop.removed_files.iter().cloned().collect();
            let mut touched_parts: Vec<BTreeMap<String, String>> = Vec::new();
            for f in &cur_snap.data_files {
                if removed_set.contains(&f.file_path) && !f.partition_values.is_empty() {
                    let pm: BTreeMap<String, String> = f
                        .partition_values
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    if !touched_parts.contains(&pm) {
                        touched_parts.push(pm);
                    }
                }
            }
            for f in &prop.added_files {
                if !f.partition_values.is_empty() {
                    let pm: BTreeMap<String, String> = f
                        .partition_values
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    if !touched_parts.contains(&pm) {
                        touched_parts.push(pm);
                    }
                }
            }
            let prev_eq_deletes = cur_snap.equality_deletes.clone();
            let mut files: Vec<DataFileEntry> = cur_snap
                .data_files
                .iter()
                .filter(|f| !removed_set.contains(&f.file_path))
                .cloned()
                .collect();
            files.extend(prop.added_files);

            let mut summary = HashMap::new();
            if !touched_parts.is_empty()
                && let Ok(s) = serde_json::to_string(&touched_parts)
            {
                summary.insert("touched_partitions".into(), s);
            }
            if !prop.removed_files.is_empty()
                && let Ok(s) = serde_json::to_string(&prop.removed_files)
            {
                summary.insert("removed_files".into(), s);
            }

            let new_id = table.next_snapshot_id;
            table.next_snapshot_id += 1;
            table.snapshots.insert(
                new_id,
                IcebergSnapshot {
                    snapshot_id: new_id,
                    parent_snapshot_id: Some(current_id),
                    timestamp_ms: new_id * 1000,
                    data_files: files,
                    equality_deletes: prev_eq_deletes,
                    summary,
                },
            );
            table.refs.insert(prop.branch, new_id);
            prune_old_snapshots(table);
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
                equality_deletes: Vec::new(),
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
                equality_deletes: Vec::new(),
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

    async fn apply_governance(&self, op: GovernanceOp) -> Result<()> {
        let mut g = self.governance.write();
        match op {
            GovernanceOp::CreateRole(role) => {
                if !g.roles.contains(&role) {
                    g.roles.push(role);
                }
            }
            GovernanceOp::Grant {
                role,
                table,
                privilege,
            } => {
                if !g.roles.contains(&role) {
                    g.roles.push(role.clone());
                }
                let entry = (role, table, privilege.to_uppercase());
                if !g.grants.contains(&entry) {
                    g.grants.push(entry);
                }
            }
            GovernanceOp::Revoke {
                role,
                table,
                privilege,
            } => {
                let p = privilege.to_uppercase();
                g.grants.retain(|(r, t, priv_name)| {
                    !(r == &role && t == &table && (p == "ALL" || priv_name == &p))
                });
            }
            GovernanceOp::AddRlsPolicy {
                role,
                table,
                predicate,
            } => {
                g.rls_policies.retain(|(r, t, _)| !(r == &role && t == &table));
                g.rls_policies.push((role, table, predicate));
            }
            GovernanceOp::AddMaskingPolicy { role, table, rule } => {
                g.masking_policies.push((role, table, rule));
            }
            GovernanceOp::RecordLineage(edge) => {
                if g.lineage.len() >= 1000 {
                    g.lineage.remove(0);
                }
                g.lineage.push(edge);
            }
        }
        self.persist_governance_locked(&g)?;
        Ok(())
    }

    async fn get_governance(&self) -> Result<PersistedGovernanceState> {
        if let Some(ref path) = self.governance_file {
            let cur_stamp = file_stamp(path);
            if cur_stamp.is_some() && cur_stamp != *self.last_governance_stamp.read() {
                let mut guard = self.governance.write();
                let recheck = file_stamp(path);
                if recheck.is_some()
                    && recheck != *self.last_governance_stamp.read()
                    && let Ok(bytes) = std::fs::read(path)
                    && let Ok(disk_gov) = serde_json::from_slice::<PersistedGovernanceState>(&bytes)
                {
                    *guard = disk_gov;
                    *self.last_governance_stamp.write() = recheck;
                }
            }
        }
        Ok(self.governance.read().clone())
    }

    async fn commit_equality_delete(
        &self,
        table_name: &str,
        branch: &str,
        mut eq_delete: EqualityDeleteFile,
        upserted_files: Vec<DataFileEntry>,
    ) -> Result<u64> {
        let key = normalize_table_name(table_name);
        let mut guard = self.tables.write();
        let table = guard
            .get_mut(&key)
            .ok_or_else(|| RustLakeError::Catalog(format!("table '{table_name}' not found")))?;
        let current_id = *table
            .refs
            .get(branch)
            .ok_or_else(|| RustLakeError::Catalog(format!("branch '{branch}' not found")))?;
        let cur_snap = table
            .snapshots
            .get(&current_id)
            .ok_or_else(|| RustLakeError::Catalog(format!("snapshot {current_id} missing")))?;
        let mut files = cur_snap.data_files.clone();
        files.extend(upserted_files);
        let mut eq_deletes = cur_snap.equality_deletes.clone();
        let new_id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        eq_delete.sequence_number = new_id;
        eq_deletes.push(eq_delete);
        let mut summary = HashMap::new();
        summary.insert("operation".into(), "equality-delete-upsert".into());
        table.snapshots.insert(
            new_id,
            IcebergSnapshot {
                snapshot_id: new_id,
                parent_snapshot_id: Some(current_id),
                timestamp_ms: new_id * 1000,
                data_files: files,
                equality_deletes: eq_deletes,
                summary,
            },
        );
        table.refs.insert(branch.to_string(), new_id);
        self.persist_locked(&guard)?;
        Ok(new_id)
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

    #[tokio::test]
    async fn concurrent_persistent_commits_conserve_every_file_zero_lost_updates() {
        let dir = std::env::temp_dir().join(format!("rustlakedb_cat_occ_{}", uuid::Uuid::new_v4()));
        let cat = std::sync::Arc::new(InMemoryRaftCatalog::open(&dir).unwrap());
        cat.create_table(IcebergTableMetadata::new("ledger_occ", vec![]))
            .await
            .unwrap();

        let mut handles = Vec::new();
        for w in 0..12usize {
            let c = std::sync::Arc::clone(&cat);
            handles.push(tokio::spawn(async move {
                for i in 0..10usize {
                    let meta = c.get_table("ledger_occ").await.unwrap();
                    let snap = meta.current_snapshot("main").unwrap().snapshot_id;
                    c.commit_table(TableCommitProposal {
                        table_name: "ledger_occ".into(),
                        branch: "main".into(),
                        expected_snapshot_id: snap,
                        added_files: vec![DataFileEntry {
                            file_path: format!("ledger_occ/w{w}_i{i}.parquet"),
                            row_group_id: 0,
                            row_count: 1,
                            byte_size: 128,
                            deletion_vector: RoaringBitmap::new(),
                            puffin_index: FilePuffinIndex::default(),
                            ndv_estimates: HashMap::new(),
                            column_min: HashMap::new(),
                            column_max: HashMap::new(),
                            partition_values: HashMap::new(),
                            dv_path: None,
                        }],
                        removed_files: vec![],
                        is_append_only: true,
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let final_meta = cat.get_table("ledger_occ").await.unwrap();
        let files = &final_meta.current_snapshot("main").unwrap().data_files;
        assert_eq!(files.len(), 120, "all 120 concurrent commits must be conserved");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
