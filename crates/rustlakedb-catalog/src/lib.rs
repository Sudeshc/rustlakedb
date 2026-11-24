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
    TimestampTz,
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
}

impl IcebergTableMetadata {
    pub fn new(table_name: impl Into<String>, schema: Vec<FieldSchema>) -> Self {
        let initial = IcebergSnapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            timestamp_ms: 0,
            data_files: Vec::new(),
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

    pub fn splits(&self, branch: &str) -> Result<Vec<IcebergSplit>> {
        let snap = self.current_snapshot(branch)?;
        Ok(Self::splits_from_snapshot(&self.table_name, snap))
    }

    pub fn splits_at_version(&self, version: u64) -> Result<Vec<IcebergSplit>> {
        let snap = self.snapshot_at_version(version)?;
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
}
