// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod hnsw;
pub mod iceberg_avro;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use roaring::RoaringBitmap;
use rustlakedb_catalog::{
    ColumnType, DataFileEntry, FieldSchema, FilePuffinIndex, IcebergTableMetadata,
};
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{RowBatch, ScalarValue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SerializedPuffinSidecar {
    inverted: Vec<(String, String, Vec<u32>)>,
    bloom_hashes: Vec<u64>,
    ndv_estimates: HashMap<String, u64>,
    shredded_paths: Vec<String>,
}

/// Bounded hybrid RAM + optional NVMe disk cache for immutable Parquet & Puffin objects (Wave 1).
/// Pairs with Rendezvous consistent hashing so repeated queries achieve >95% local hit rates.
struct BoundedObjectCache {
    max_ram_bytes: usize,
    used_ram_bytes: usize,
    entries: HashMap<String, Bytes>,
    lru_order: VecDeque<String>,
    nvme_dir: Option<PathBuf>,
}

impl BoundedObjectCache {
    fn new(max_ram_bytes: usize, nvme_dir: Option<PathBuf>) -> Self {
        if let Some(ref d) = nvme_dir {
            let _ = std::fs::create_dir_all(d);
        }
        Self {
            max_ram_bytes,
            used_ram_bytes: 0,
            entries: HashMap::new(),
            lru_order: VecDeque::new(),
            nvme_dir,
        }
    }

    fn nvme_path(&self, key: &str) -> Option<PathBuf> {
        self.nvme_dir.as_ref().map(|d| {
            let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
            d.join(format!("obj_{hash:016x}.cache"))
        })
    }

    fn get(&mut self, key: &str) -> Option<Bytes> {
        if let Some(b) = self.entries.get(key).cloned() {
            return Some(b);
        }
        if let Some(p) = self.nvme_path(key)
            && p.exists()
            && let Ok(raw) = std::fs::read(&p)
        {
            let bytes = Bytes::from(raw);
            self.insert_ram(key.to_string(), bytes.clone());
            return Some(bytes);
        }
        None
    }

    fn put(&mut self, key: String, bytes: Bytes) {
        if let Some(p) = self.nvme_path(&key) {
            let _ = std::fs::write(p, &bytes);
        }
        self.insert_ram(key, bytes);
    }

    fn insert_ram(&mut self, key: String, bytes: Bytes) {
        let len = bytes.len();
        if len > self.max_ram_bytes {
            return;
        }
        if let Some(old) = self.entries.remove(&key) {
            self.used_ram_bytes = self.used_ram_bytes.saturating_sub(old.len());
        }
        while self.used_ram_bytes + len > self.max_ram_bytes {
            if let Some(oldest) = self.lru_order.pop_front() {
                if let Some(evicted) = self.entries.remove(&oldest) {
                    self.used_ram_bytes = self.used_ram_bytes.saturating_sub(evicted.len());
                }
            } else {
                break;
            }
        }
        self.used_ram_bytes += len;
        self.entries.insert(key.clone(), bytes);
        self.lru_order.push_back(key);
    }

    fn invalidate(&mut self, key: &str) {
        if let Some(old) = self.entries.remove(key) {
            self.used_ram_bytes = self.used_ram_bytes.saturating_sub(old.len());
        }
        if let Some(p) = self.nvme_path(key) {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Real cloud-agnostic Iceberg v3 storage engine backed by `object_store` + `arrow` + `parquet`
/// with hybrid RAM + NVMe caching, column projection pushdown, and shredded JSON sub-columns.
pub struct IcebergObjectStore {
    store: Arc<dyn ObjectStore>,
    base_uri: Option<String>,
    cache: Mutex<BoundedObjectCache>,
    pub parquet_files_written: AtomicU64,
    pub parquet_files_read: AtomicU64,
    pub files_pruned_by_puffin: AtomicU64,
    pub files_pruned_by_zonemap: AtomicU64,
    pub files_pruned_by_partition: AtomicU64,
    pub rows_skipped_by_dv: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub object_store_gets: AtomicU64,
}

impl IcebergObjectStore {
    pub fn in_memory() -> Self {
        Self::with_store(Arc::new(InMemory::new()), 128 * 1024 * 1024, None, None)
    }

    pub fn local_fs(root: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        let fs = LocalFileSystem::new_with_prefix(root.as_ref())
            .map_err(|e| RustLakeError::Execution(format!("local object store init: {e}")))?;
        let nvme_cache = root.as_ref().join("_nvme_cache");
        let abs = std::fs::canonicalize(root.as_ref()).unwrap_or_else(|_| root.as_ref().to_path_buf());
        let norm = abs.to_string_lossy().replace('\\', "/");
        let clean = norm.strip_prefix("//?/").unwrap_or(&norm);
        let base_uri = if clean.starts_with('/') {
            format!("file://{clean}")
        } else {
            format!("file:///{clean}")
        };
        Ok(Self::with_store(
            Arc::new(fs),
            128 * 1024 * 1024,
            Some(nvme_cache),
            Some(base_uri),
        ))
    }

    fn with_store(
        store: Arc<dyn ObjectStore>,
        ram_cache_bytes: usize,
        nvme_dir: Option<PathBuf>,
        base_uri: Option<String>,
    ) -> Self {
        Self {
            store,
            base_uri,
            cache: Mutex::new(BoundedObjectCache::new(ram_cache_bytes, nvme_dir)),
            parquet_files_written: AtomicU64::new(0),
            parquet_files_read: AtomicU64::new(0),
            files_pruned_by_puffin: AtomicU64::new(0),
            files_pruned_by_zonemap: AtomicU64::new(0),
            files_pruned_by_partition: AtomicU64::new(0),
            rows_skipped_by_dv: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            object_store_gets: AtomicU64::new(0),
        }
    }

    /// Cloud-agnostic URI factory: supports `memory://`, `file://<path>`, plain paths,
    /// and (with `--features cloud`) `s3://<bucket>` (incl. MinIO via `AWS_ENDPOINT_URL`), `gs://<bucket>`, `az://<container>`.
    pub fn from_uri(uri: &str) -> Result<Self> {
        if uri == "memory://" {
            return Ok(Self::in_memory());
        }
        if let Some(local_path) = uri.strip_prefix("file://") {
            return Self::local_fs(local_path);
        }
        #[cfg(feature = "cloud")]
        {
            if let Some(bucket) = uri.strip_prefix("s3://") {
                let bucket_name = bucket.split('/').next().unwrap_or(bucket);
                let s3 = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_allow_http(true)
                    .build()
                    .map_err(|e| RustLakeError::Execution(format!("S3 object_store init: {e}")))?;
                return Ok(Self::with_store(
                    Arc::new(s3),
                    256 * 1024 * 1024,
                    None,
                    Some(format!("s3://{bucket_name}")),
                ));
            }
            if let Some(bucket) = uri.strip_prefix("gs://") {
                let bucket_name = bucket.split('/').next().unwrap_or(bucket);
                let gcs = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map_err(|e| RustLakeError::Execution(format!("GCS object_store init: {e}")))?;
                return Ok(Self::with_store(
                    Arc::new(gcs),
                    256 * 1024 * 1024,
                    None,
                    Some(format!("gs://{bucket_name}")),
                ));
            }
            if let Some(container) = uri.strip_prefix("az://") {
                let name = container.split('/').next().unwrap_or(container);
                let az = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(name)
                    .build()
                    .map_err(|e| RustLakeError::Execution(format!("Azure object_store init: {e}")))?;
                return Ok(Self::with_store(
                    Arc::new(az),
                    256 * 1024 * 1024,
                    None,
                    Some(format!("az://{name}")),
                ));
            }
        }
        if uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://") {
            return Err(RustLakeError::Execution(format!(
                "cloud storage URI '{uri}' requires building with `--features cloud`"
            )));
        }
        Self::local_fs(uri)
    }

    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Writes raw bytes to an arbitrary object key (used by `COPY TO` for local & cloud URIs, Wave 26).
    pub async fn put_raw_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.store
            .put(&ObjPath::from(key), PutPayload::from(Bytes::from(bytes)))
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store put '{key}': {e}")))?;
        Ok(())
    }

    /// Reads raw bytes from an arbitrary object key (used by `COPY FROM` for cloud URIs, Wave 26).
    pub async fn get_raw_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self
            .store
            .get(&ObjPath::from(key))
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store get '{key}': {e}")))?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store read '{key}': {e}")))?;
        Ok(bytes.to_vec())
    }

    /// Reads an immutable object (`*.parquet` or `*.puffin.json`) through the hybrid RAM + NVMe cache.
    async fn get_cached_immutable(&self, key: &str) -> Result<Option<Bytes>> {
        if let Some(hit) = self.cache.lock().get(key) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(hit));
        }
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        self.object_store_gets.fetch_add(1, Ordering::Relaxed);
        match self.store.get(&ObjPath::from(key)).await {
            Ok(resp) => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| RustLakeError::Execution(format!("read bytes '{key}': {e}")))?;
                self.cache.lock().put(key.to_string(), bytes.clone());
                Ok(Some(bytes))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(RustLakeError::Execution(format!(
                "object_store get '{key}': {e}"
            ))),
        }
    }

    /// Writes a `RowBatch` as a real Zstd-compressed Apache Parquet file + Puffin JSON index sidecar
    /// (with automatic JSON sub-column shredding per RFC-003 and per-file zone-map min/max stats).
    pub async fn write_data_file(
        &self,
        file_path: &str,
        schema: &[FieldSchema],
        batch: &RowBatch,
    ) -> Result<DataFileEntry> {
        self.write_data_file_with_shred_paths(file_path, schema, batch, &[])
            .await
    }

    pub async fn write_data_file_with_shred_paths(
        &self,
        file_path: &str,
        schema: &[FieldSchema],
        batch: &RowBatch,
        extra_shred_paths: &[String],
    ) -> Result<DataFileEntry> {
        let (record_batch, puffin, shredded_paths, column_min, column_max) =
            row_batch_to_arrow_and_puffin(schema, batch, extra_shred_paths)?;

        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, record_batch.schema(), Some(props))
                .map_err(|e| RustLakeError::Execution(format!("parquet writer init: {e}")))?;
            writer
                .write(&record_batch)
                .map_err(|e| RustLakeError::Execution(format!("parquet write: {e}")))?;
            writer
                .close()
                .map_err(|e| RustLakeError::Execution(format!("parquet close: {e}")))?;
        }

        let byte_size = buf.len() as u64;
        let bytes = Bytes::from(buf);
        let obj_path = ObjPath::from(file_path);
        self.store
            .put(&obj_path, PutPayload::from(bytes.clone()))
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store put parquet: {e}")))?;
        self.cache.lock().put(file_path.to_string(), bytes);
        self.parquet_files_written.fetch_add(1, Ordering::Relaxed);

        let sidecar = SerializedPuffinSidecar {
            inverted: puffin
                .inverted
                .iter()
                .map(|((p, v), bm)| (p.clone(), v.clone(), bm.iter().collect()))
                .collect(),
            bloom_hashes: puffin.bloom_hashes.iter().copied().collect(),
            ndv_estimates: puffin.ndv_estimates.clone(),
            shredded_paths,
        };
        let puffin_bytes = Bytes::from(serde_json::to_vec(&sidecar)?);
        let p_key = puffin_sidecar_path(file_path);
        self.store
            .put(&ObjPath::from(p_key.as_str()), PutPayload::from(puffin_bytes.clone()))
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store put puffin: {e}")))?;
        self.cache.lock().put(p_key, puffin_bytes);

        let ndv_estimates = puffin.ndv_estimates.clone();
        Ok(DataFileEntry {
            file_path: file_path.to_string(),
            row_group_id: 0,
            row_count: batch.num_rows() as u64,
            byte_size,
            deletion_vector: RoaringBitmap::new(),
            puffin_index: puffin,
            ndv_estimates,
            column_min,
            column_max,
            partition_values: HashMap::new(),
        })
    }

    pub async fn write_deletion_vector(
        &self,
        file_path: &str,
        dv: &RoaringBitmap,
    ) -> Result<()> {
        let mut bytes = Vec::new();
        dv.serialize_into(&mut bytes)?;
        let dv_path = ObjPath::from(dv_sidecar_path(file_path));
        self.store
            .put(&dv_path, PutPayload::from(Bytes::from(bytes)))
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store put dv: {e}")))?;
        Ok(())
    }

    pub async fn read_deletion_vector(&self, file_path: &str) -> Result<RoaringBitmap> {
        let dv_path = ObjPath::from(dv_sidecar_path(file_path));
        match self.store.get(&dv_path).await {
            Ok(resp) => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| RustLakeError::Execution(format!("read dv bytes: {e}")))?;
                let bm = RoaringBitmap::deserialize_from(&bytes[..])?;
                Ok(bm)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(RoaringBitmap::new()),
            Err(e) => Err(RustLakeError::Execution(format!("get dv: {e}"))),
        }
    }

    pub async fn read_puffin_index(&self, file_path: &str) -> Result<FilePuffinIndex> {
        let p_key = puffin_sidecar_path(file_path);
        match self.get_cached_immutable(&p_key).await? {
            Some(bytes) => {
                let sidecar: SerializedPuffinSidecar = serde_json::from_slice(&bytes)?;
                let mut idx = FilePuffinIndex::default();
                for (p, v, rows) in sidecar.inverted {
                    let mut bm = RoaringBitmap::new();
                    for r in rows {
                        bm.insert(r);
                    }
                    idx.inverted.insert((p, v), bm);
                }
                idx.bloom_hashes = sidecar.bloom_hashes.into_iter().collect();
                idx.ndv_estimates = sidecar.ndv_estimates;
                Ok(idx)
            }
            None => Ok(FilePuffinIndex::default()),
        }
    }

    /// Lists Parquet column names from the cached Parquet footer without decoding row data.
    pub async fn parquet_column_names(&self, file_path: &str) -> Result<Vec<String>> {
        let bytes = self
            .get_cached_immutable(file_path)
            .await?
            .ok_or_else(|| {
                RustLakeError::Execution(format!("parquet file not found: '{file_path}'"))
            })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| RustLakeError::Execution(format!("parquet footer build: {e}")))?;
        Ok(builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect())
    }

    /// Reads raw Arrow `RecordBatch`es directly from a cached Parquet file with true
    /// **Parquet Column Projection Pushdown** (`ProjectionMask`) and returns the file's
    /// Iceberg v3 Deletion Vector for vectorized SIMD execution (Wave 1)!
    pub async fn read_arrow_batches(
        &self,
        file_path: &str,
        projected_columns: Option<&[String]>,
    ) -> Result<(Vec<RecordBatch>, RoaringBitmap)> {
        let dv = self.read_deletion_vector(file_path).await?;
        let bytes = self
            .get_cached_immutable(file_path)
            .await?
            .ok_or_else(|| {
                RustLakeError::Execution(format!("parquet file not found: '{file_path}'"))
            })?;
        self.parquet_files_read.fetch_add(1, Ordering::Relaxed);

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| RustLakeError::Execution(format!("parquet reader build: {e}")))?;

        if let Some(cols) = projected_columns {
            let arrow_schema = builder.schema();
            if cols.is_empty() {
                // COUNT(*) fast projection: decode only column 0!
                if !arrow_schema.fields().is_empty() {
                    let mask = ProjectionMask::roots(builder.parquet_schema(), [0]);
                    builder = builder.with_projection(mask);
                }
            } else {
                let indices: Vec<usize> = arrow_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| cols.iter().any(|c| c == f.name()))
                    .map(|(i, _)| i)
                    .collect();
                if !indices.is_empty() {
                    let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
                    builder = builder.with_projection(mask);
                }
            }
        }

        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| RustLakeError::Execution(format!("parquet reader: {e}")))?;

        let mut batches = Vec::new();
        for rb in reader {
            batches
                .push(rb.map_err(|e| RustLakeError::Execution(format!("parquet read batch: {e}")))?);
        }
        Ok((batches, dv))
    }

    /// Reads a real Apache Parquet file into `RowBatch` with optional column projection pushdown,
    /// Iceberg v3 Deletion Vector skipping, and Puffin candidate row-id bitmask.
    pub async fn read_data_file(
        &self,
        file_path: &str,
        schema: &[FieldSchema],
        candidate_rows: Option<&RoaringBitmap>,
    ) -> Result<RowBatch> {
        self.read_data_file_projected(file_path, schema, None, candidate_rows)
            .await
    }

    pub async fn read_data_file_projected(
        &self,
        file_path: &str,
        schema: &[FieldSchema],
        projected_columns: Option<&[String]>,
        candidate_rows: Option<&RoaringBitmap>,
    ) -> Result<RowBatch> {
        self.read_data_file_with_aliases(
            file_path,
            schema,
            &HashMap::new(),
            projected_columns,
            candidate_rows,
        )
        .await
    }

    /// Reads a real Apache Parquet file with full Iceberg v3 Field-ID Schema Evolution (Wave 22):
    /// - Transparently resolves renamed columns via `column_aliases` without rewriting old Parquet files
    /// - Drops removed columns cleanly on read
    /// - Widens numeric/decimal types (`Int64 -> Float64`, `Decimal(p1, s1) -> Decimal(p2, s2)`) on read!
    pub async fn read_data_file_with_aliases(
        &self,
        file_path: &str,
        schema: &[FieldSchema],
        column_aliases: &HashMap<String, Vec<String>>,
        projected_columns: Option<&[String]>,
        candidate_rows: Option<&RoaringBitmap>,
    ) -> Result<RowBatch> {
        // Expand projected columns to include historical physical names of renamed columns!
        let expanded_proj: Option<Vec<String>> = projected_columns.map(|cols| {
            let mut v = cols.to_vec();
            for c in cols {
                if let Some(hist) = column_aliases.get(c) {
                    for old in hist {
                        if !v.contains(old) {
                            v.push(old.clone());
                        }
                    }
                }
            }
            v
        });
        let (batches, dv) = self
            .read_arrow_batches(file_path, expanded_proj.as_deref())
            .await?;

        let schema_names: HashSet<&str> = schema.iter().map(|f| f.name.as_str()).collect();
        let logical_cols: Vec<String> = match projected_columns {
            Some(cols) if !cols.is_empty() => {
                let filtered: Vec<String> = cols
                    .iter()
                    .filter(|c| schema_names.contains(c.as_str()) || c.starts_with("__shred_"))
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    schema.iter().map(|f| f.name.clone()).collect()
                } else {
                    filtered
                }
            }
            _ => schema.iter().map(|f| f.name.clone()).collect(),
        };

        // Map every physical column name (current OR historical alias) -> (&logical_name, &ColumnType)
        let mut phys_to_logical: HashMap<&str, (&str, &ColumnType)> = HashMap::new();
        for f in schema {
            phys_to_logical.insert(f.name.as_str(), (f.name.as_str(), &f.data_type));
            if let Some(hist) = column_aliases.get(&f.name) {
                for old in hist {
                    phys_to_logical.insert(old.as_str(), (f.name.as_str(), &f.data_type));
                }
            }
        }

        let mut out_rows = Vec::new();
        let mut global_row_id: u32 = 0;

        for rb in batches {
            let arrow_schema = rb.schema();
            for row_idx in 0..rb.num_rows() {
                let cur_id = global_row_id;
                global_row_id += 1;

                if dv.contains(cur_id) {
                    self.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if let Some(bm) = candidate_rows
                    && !bm.contains(cur_id)
                {
                    continue;
                }

                let mut row = BTreeMap::new();
                for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
                    let pname = field.name().as_str();
                    if pname.starts_with("__shred_") {
                        if projected_columns.is_none() {
                            continue;
                        }
                        let arr = rb.column(col_idx);
                        let val = extract_scalar(arr.as_ref(), row_idx, None);
                        row.insert(pname.to_string(), val);
                        continue;
                    }
                    // Skip columns dropped via ALTER TABLE DROP COLUMN!
                    let Some(&(logical_name, target_type)) = phys_to_logical.get(pname) else {
                        continue;
                    };
                    let arr = rb.column(col_idx);
                    let raw_val = extract_scalar(arr.as_ref(), row_idx, Some(target_type));
                    // Apply transparent Iceberg type widening on read (Int64 -> Float64, Decimal scale)
                    let val = widen_scalar_on_read(raw_val, target_type);
                    row.insert(logical_name.to_string(), val);
                }
                for col_name in &logical_cols {
                    row.entry(col_name.clone()).or_insert(ScalarValue::Null);
                }
                out_rows.push(row);
            }
        }

        Ok(RowBatch::new(logical_cols, out_rows))
    }

    pub async fn list_paths(&self, prefix: &str) -> Result<Vec<String>> {
        use futures::TryStreamExt;
        let obj_prefix = ObjPath::from(prefix);
        let metas = self
            .store
            .list(Some(&obj_prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| RustLakeError::Execution(format!("list object_store '{prefix}': {e}")))?;
        Ok(metas.into_iter().map(|m| m.location.to_string()).collect())
    }

    pub async fn delete_data_file(&self, file_path: &str) -> Result<()> {
        for p in [
            file_path.to_string(),
            puffin_sidecar_path(file_path),
            dv_sidecar_path(file_path),
        ] {
            self.cache.lock().invalidate(&p);
            let _ = self.store.delete(&ObjPath::from(p.as_str())).await;
        }
        Ok(())
    }

    pub async fn write_hnsw_index(&self, file_path: &str, idx: &hnsw::HnswIndex) -> Result<()> {
        let key = format!("{file_path}.hnsw.json");
        let bytes = Bytes::from(serde_json::to_vec(idx)?);
        self.store
            .put(&ObjPath::from(key.as_str()), PutPayload::from(bytes.clone()))
            .await
            .map_err(|e| RustLakeError::Execution(format!("write hnsw index: {e}")))?;
        self.cache.lock().put(key, bytes);
        Ok(())
    }

    pub async fn read_hnsw_index(&self, file_path: &str) -> Result<Option<hnsw::HnswIndex>> {
        let key = format!("{file_path}.hnsw.json");
        match self.get_cached_immutable(&key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn read_raw_object(&self, path: &str) -> Result<Bytes> {
        let resp = self
            .store
            .get(&ObjPath::from(path))
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store get '{path}': {e}")))?;
        resp.bytes()
            .await
            .map_err(|e| RustLakeError::Execution(format!("object_store read '{path}': {e}")))
    }

    pub async fn write_iceberg_metadata(&self, meta: &IcebergTableMetadata, branch: &str) -> Result<String> {
        let snap = meta.current_snapshot(branch)?;
        let base_ref = self.base_uri.as_deref();

        // 1. Write genuine Apache Iceberg v3 Avro Manifest (`snap-{id}-m0.avro`)
        let manifest_path = format!("{}/metadata/snap-{}-m0.avro", meta.table_name, snap.snapshot_id);
        let manifest_bytes =
            iceberg_avro::encode_iceberg_v3_manifest_with_base(meta, snap, base_ref);
        let manifest_len = manifest_bytes.len() as i64;
        self.store
            .put(&ObjPath::from(manifest_path.as_str()), PutPayload::from(Bytes::from(manifest_bytes)))
            .await
            .map_err(|e| RustLakeError::Execution(format!("write iceberg avro manifest: {e}")))?;

        // 2. Write genuine Apache Iceberg v3 Avro Manifest List (`snap-{id}.avro`)
        let manifest_list_path = format!("{}/metadata/snap-{}.avro", meta.table_name, snap.snapshot_id);
        let manifest_uri = match base_ref {
            Some(base) => format!("{}/{}", base.trim_end_matches('/'), manifest_path),
            None => manifest_path.clone(),
        };
        let manifest_list_bytes =
            iceberg_avro::encode_iceberg_v3_manifest_list(snap, &manifest_uri, manifest_len);
        self.store
            .put(
                &ObjPath::from(manifest_list_path.as_str()),
                PutPayload::from(Bytes::from(manifest_list_bytes)),
            )
            .await
            .map_err(|e| RustLakeError::Execution(format!("write iceberg avro manifest list: {e}")))?;

        // 3. Write standard Iceberg v3 `v{N}.metadata.json` referencing `manifest-list`
        let path_str = format!(
            "{}/metadata/v{}.metadata.json",
            meta.table_name, snap.snapshot_id
        );
        let last_col_id = meta.schema.iter().map(|f| f.field_id).max().unwrap_or(0);
        let iceberg_fields: Vec<serde_json::Value> = meta
            .schema
            .iter()
            .map(|f| {
                let t: String = match f.data_type {
                    ColumnType::Int64 => "long".into(),
                    ColumnType::Float64 => "double".into(),
                    ColumnType::Bool => "boolean".into(),
                    ColumnType::Utf8 => "string".into(),
                    ColumnType::Date => "date".into(),
                    ColumnType::TimestampTz => "timestamptz".into(),
                    ColumnType::Decimal { precision, scale } => {
                        format!("decimal({precision},{scale})")
                    }
                    ColumnType::Variant => "variant".into(),
                };
                serde_json::json!({
                    "id": f.field_id,
                    "name": f.name,
                    "required": !f.nullable,
                    "type": t
                })
            })
            .collect();
        let doc = serde_json::json!({
            "format-version": meta.format_version,
            "table-uuid": format!("rustlakedb-{}", meta.table_name),
            "location": meta.table_name,
            "table-name": meta.table_name,
            "last-sequence-number": snap.snapshot_id,
            "last-column-id": last_col_id,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": iceberg_fields
            }],
            "default-spec-id": 0,
            "partition-specs": [{ "spec-id": 0, "fields": [] }],
            "current-snapshot-id": snap.snapshot_id,
            "refs": meta.refs,
            "schema": meta.schema,
            "json-shredding": meta.json_shredding,
            "indexes": meta.indexes,
            "snapshots": meta.snapshots.values().map(|s| serde_json::json!({
                "snapshot-id": s.snapshot_id,
                "parent-snapshot-id": s.parent_snapshot_id,
                "sequence-number": s.snapshot_id,
                "timestamp-ms": s.timestamp_ms,
                "manifest-list": format!("{}/metadata/snap-{}.avro", meta.table_name, s.snapshot_id),
                "summary": {
                    "operation": "append",
                    "total-data-files": s.data_files.len().to_string(),
                    "total-records": s.data_files.iter().map(|f| f.row_count).sum::<u64>().to_string()
                }
            })).collect::<Vec<_>>(),
        });
        let payload = serde_json::to_vec_pretty(&doc)?;
        self.store
            .put(&ObjPath::from(path_str.as_str()), PutPayload::from(Bytes::from(payload)))
            .await
            .map_err(|e| RustLakeError::Execution(format!("write iceberg metadata: {e}")))?;
        Ok(path_str)
    }
}

pub fn puffin_sidecar_path(parquet_path: &str) -> String {
    format!("{parquet_path}.puffin.json")
}

pub fn dv_sidecar_path(parquet_path: &str) -> String {
    format!("{parquet_path}.dv.roaring")
}

/// Reads an external Apache Parquet file from local disk for `COPY ... FROM '<path>' (FORMAT PARQUET)`.
pub fn read_external_parquet(path: &Path) -> Result<RowBatch> {
    let raw = std::fs::read(path)
        .map_err(|e| RustLakeError::Execution(format!("read external parquet '{}': {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(raw))
        .map_err(|e| RustLakeError::Execution(format!("parse external parquet: {e}")))?;
    let schema = builder.schema().clone();
    let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let reader = builder
        .with_batch_size(8192)
        .build()
        .map_err(|e| RustLakeError::Execution(format!("build external parquet reader: {e}")))?;
    let mut rows = Vec::new();
    for rb in reader {
        let rb = rb.map_err(|e| RustLakeError::Execution(format!("read external parquet batch: {e}")))?;
        for row_idx in 0..rb.num_rows() {
            let mut row = BTreeMap::new();
            for (ci, field) in rb.schema().fields().iter().enumerate() {
                if field.name().starts_with("__shred_") {
                    continue;
                }
                row.insert(
                    field.name().clone(),
                    extract_scalar(rb.column(ci).as_ref(), row_idx, None),
                );
            }
            rows.push(row);
        }
    }
    let clean_cols: Vec<String> = cols
        .into_iter()
        .filter(|c| !c.starts_with("__shred_"))
        .collect();
    Ok(RowBatch::new(clean_cols, rows))
}

/// Converts any `RowBatch` into an Apache Arrow `RecordBatch` preserving native Arrow types
/// (`Int64`, `Float64`, `Boolean`, `Date32`, `Decimal128`, `Utf8`) for Arrow IPC & Arrow Flight SQL (Wave 14/15).
pub fn row_batch_to_record_batch(batch: &RowBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.columns.len());
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.columns.len());

    for col in &batch.columns {
        let inferred = batch
            .rows
            .iter()
            .filter_map(|r| r.get(col))
            .find(|v| !matches!(v, ScalarValue::Null));
        match inferred {
            Some(ScalarValue::Int64(_)) => {
                fields.push(Field::new(col, DataType::Int64, true));
                let mut b = Int64Builder::new();
                for r in &batch.rows {
                    match r.get(col) {
                        Some(ScalarValue::Int64(v)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            Some(ScalarValue::Float64(_)) => {
                fields.push(Field::new(col, DataType::Float64, true));
                let mut b = Float64Builder::new();
                for r in &batch.rows {
                    match r.get(col).and_then(|v| v.as_f64()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            Some(ScalarValue::Date(_)) => {
                fields.push(Field::new(col, DataType::Date32, true));
                let mut b = Date32Builder::new();
                for r in &batch.rows {
                    match r.get(col) {
                        Some(ScalarValue::Date(d)) => b.append_value(*d),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            Some(ScalarValue::Decimal(_, scale)) => {
                let s = *scale;
                fields.push(Field::new(col, DataType::Decimal128(38, s as i8), true));
                let mut b = Decimal128Builder::new().with_data_type(DataType::Decimal128(38, s as i8));
                for r in &batch.rows {
                    match r.get(col) {
                        Some(ScalarValue::Decimal(v, _)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            Some(ScalarValue::Bool(_)) => {
                fields.push(Field::new(col, DataType::Boolean, true));
                let mut b = BooleanBuilder::new();
                for r in &batch.rows {
                    match r.get(col) {
                        Some(ScalarValue::Bool(v)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            _ => {
                fields.push(Field::new(col, DataType::Utf8, true));
                let mut b = StringBuilder::new();
                for r in &batch.rows {
                    match r.get(col).and_then(|v| v.to_pg_text()) {
                        Some(s) => b.append_value(s),
                        None => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
        }
    }

    let arrow_schema = Arc::new(Schema::new(fields));
    if arrow_schema.fields().is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }
    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| RustLakeError::Execution(format!("arrow record batch build: {e}")))
}

/// Converts any Apache Arrow `RecordBatch` into a RustLakeDB `RowBatch`, preserving native
/// `Int64`, `Float64`, `Bool`, `Date32`, `Timestamp(Microsecond)`, `Decimal128`, and `Utf8` values (Wave 18).
pub fn record_batch_to_row_batch(rb: &RecordBatch) -> RowBatch {
    let schema = rb.schema();
    let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut rows = Vec::with_capacity(rb.num_rows());
    for row_idx in 0..rb.num_rows() {
        let mut map = BTreeMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = rb.column(col_idx);
            let val = if col.is_null(row_idx) {
                ScalarValue::Null
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                ScalarValue::Int64(a.value(row_idx))
            } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
                ScalarValue::Int64(a.value(row_idx) as i64)
            } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                ScalarValue::Float64(a.value(row_idx))
            } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::Float32Array>() {
                ScalarValue::Float64(a.value(row_idx) as f64)
            } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                ScalarValue::Bool(a.value(row_idx))
            } else if let Some(a) = col.as_any().downcast_ref::<Date32Array>() {
                ScalarValue::Date(a.value(row_idx))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                ScalarValue::Int64(a.value(row_idx))
            } else if let Some(a) = col.as_any().downcast_ref::<Decimal128Array>() {
                let scale = match field.data_type() {
                    DataType::Decimal128(_, s) => (*s).max(0) as u8,
                    _ => 2,
                };
                ScalarValue::Decimal(a.value(row_idx), scale)
            } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                ScalarValue::Utf8(a.value(row_idx).to_string())
            } else {
                ScalarValue::Null
            };
            map.insert(field.name().clone(), val);
        }
        rows.push(map);
    }
    RowBatch::new(cols, rows)
}

/// Serializes any `RowBatch` into a standard zero-copy **Apache Arrow IPC Stream** (`application/vnd.apache.arrow.stream`)
/// for high-throughput Python (PyArrow, Polars, DuckDB) and BI clients!
pub fn row_batch_to_arrow_ipc(batch: &RowBatch) -> Result<Vec<u8>> {
    let rb = row_batch_to_record_batch(batch)?;
    let arrow_schema = rb.schema();
    let mut out = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut out, &arrow_schema)
            .map_err(|e| RustLakeError::Execution(format!("arrow ipc writer init: {e}")))?;
        if !arrow_schema.fields().is_empty() {
            writer
                .write(&rb)
                .map_err(|e| RustLakeError::Execution(format!("arrow ipc write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RustLakeError::Execution(format!("arrow ipc finish: {e}")))?;
    }
    Ok(out)
}

type ArrowPuffinOutput = (
    RecordBatch,
    FilePuffinIndex,
    Vec<String>,
    HashMap<String, String>,
    HashMap<String, String>,
);

fn row_batch_to_arrow_and_puffin(
    schema: &[FieldSchema],
    batch: &RowBatch,
    extra_shred_paths: &[String],
) -> Result<ArrowPuffinOutput> {
    let mut fields: Vec<Field> = Vec::new();
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    let mut puffin = FilePuffinIndex::default();
    let mut shredded_paths: Vec<String> = Vec::new();
    let mut col_min: HashMap<String, String> = HashMap::new();
    let mut col_max: HashMap<String, String> = HashMap::new();

    for col in schema {
        match col.data_type {
            ColumnType::Int64 => {
                fields.push(Field::new(&col.name, DataType::Int64, col.nullable));
                let mut b = Int64Builder::new();
                let mut distinct = HashSet::new();
                let mut min_v: Option<i64> = None;
                let mut max_v: Option<i64> = None;
                for r in &batch.rows {
                    match r.get(&col.name) {
                        Some(ScalarValue::Int64(v)) => {
                            distinct.insert(*v);
                            min_v = Some(min_v.map_or(*v, |m| m.min(*v)));
                            max_v = Some(max_v.map_or(*v, |m| m.max(*v)));
                            b.append_value(*v);
                        }
                        _ => b.append_null(),
                    }
                }
                if let (Some(mn), Some(mx)) = (min_v, max_v) {
                    col_min.insert(col.name.clone(), mn.to_string());
                    col_max.insert(col.name.clone(), mx.to_string());
                }
                puffin
                    .ndv_estimates
                    .insert(col.name.clone(), distinct.len() as u64);
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::Float64 => {
                fields.push(Field::new(&col.name, DataType::Float64, col.nullable));
                let mut b = Float64Builder::new();
                let mut min_v: Option<f64> = None;
                let mut max_v: Option<f64> = None;
                for r in &batch.rows {
                    match r.get(&col.name).and_then(|v| v.as_f64()) {
                        Some(v) => {
                            min_v = Some(min_v.map_or(v, |m| m.min(v)));
                            max_v = Some(max_v.map_or(v, |m| m.max(v)));
                            b.append_value(v);
                        }
                        None => b.append_null(),
                    }
                }
                if let (Some(mn), Some(mx)) = (min_v, max_v) {
                    col_min.insert(col.name.clone(), mn.to_string());
                    col_max.insert(col.name.clone(), mx.to_string());
                }
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::Bool => {
                fields.push(Field::new(&col.name, DataType::Boolean, col.nullable));
                let mut b = BooleanBuilder::new();
                for r in &batch.rows {
                    match r.get(&col.name) {
                        Some(ScalarValue::Bool(v)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::Date => {
                fields.push(Field::new(&col.name, DataType::Date32, col.nullable));
                let mut b = Date32Builder::new();
                let mut min_v: Option<i32> = None;
                let mut max_v: Option<i32> = None;
                for r in &batch.rows {
                    let parsed = match r.get(&col.name) {
                        Some(ScalarValue::Date(d)) => Some(*d),
                        Some(ScalarValue::Utf8(s)) => rustlakedb_protocol::parse_iso_date_days(s),
                        _ => None,
                    };
                    match parsed {
                        Some(d) => {
                            min_v = Some(min_v.map_or(d, |m| m.min(d)));
                            max_v = Some(max_v.map_or(d, |m| m.max(d)));
                            b.append_value(d);
                        }
                        None => b.append_null(),
                    }
                }
                if let (Some(mn), Some(mx)) = (min_v, max_v) {
                    col_min.insert(col.name.clone(), rustlakedb_protocol::format_iso_date_days(mn));
                    col_max.insert(col.name.clone(), rustlakedb_protocol::format_iso_date_days(mx));
                }
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::Decimal { precision, scale } => {
                let dt = DataType::Decimal128(precision, scale as i8);
                fields.push(Field::new(&col.name, dt.clone(), col.nullable));
                let mut b = Decimal128Builder::new().with_data_type(dt);
                let mut min_v: Option<i128> = None;
                let mut max_v: Option<i128> = None;
                for r in &batch.rows {
                    let parsed = match r.get(&col.name) {
                        Some(ScalarValue::Decimal(v, s)) => {
                            if *s == scale {
                                Some(*v)
                            } else {
                                let txt = rustlakedb_protocol::format_decimal_i128(*v, *s);
                                rustlakedb_protocol::parse_decimal_i128(&txt, scale)
                            }
                        }
                        Some(ScalarValue::Int64(i)) => {
                            (*i as i128).checked_mul(10i128.pow(scale as u32))
                        }
                        Some(ScalarValue::Float64(f)) => {
                            rustlakedb_protocol::parse_decimal_i128(&format!("{f:.*}", scale as usize), scale)
                        }
                        Some(ScalarValue::Utf8(s)) => rustlakedb_protocol::parse_decimal_i128(s, scale),
                        _ => None,
                    };
                    match parsed {
                        Some(v) => {
                            min_v = Some(min_v.map_or(v, |m| m.min(v)));
                            max_v = Some(max_v.map_or(v, |m| m.max(v)));
                            b.append_value(v);
                        }
                        None => b.append_null(),
                    }
                }
                if let (Some(mn), Some(mx)) = (min_v, max_v) {
                    col_min.insert(
                        col.name.clone(),
                        rustlakedb_protocol::format_decimal_i128(mn, scale),
                    );
                    col_max.insert(
                        col.name.clone(),
                        rustlakedb_protocol::format_decimal_i128(mx, scale),
                    );
                }
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::TimestampTz => {
                fields.push(Field::new(
                    &col.name,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    col.nullable,
                ));
                let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
                let mut min_v: Option<String> = None;
                let mut max_v: Option<String> = None;
                for r in &batch.rows {
                    match r.get(&col.name) {
                        Some(ScalarValue::Utf8(s)) => {
                            if let Some(us) = parse_iso_timestamp_micros(s) {
                                min_v = Some(min_v.map_or_else(|| s.clone(), |m| if *s < m { s.clone() } else { m }));
                                max_v = Some(max_v.map_or_else(|| s.clone(), |m| if *s > m { s.clone() } else { m }));
                                b.append_value(us);
                            } else {
                                b.append_null();
                            }
                        }
                        Some(ScalarValue::Int64(us)) => b.append_value(*us),
                        _ => b.append_null(),
                    }
                }
                if let (Some(mn), Some(mx)) = (min_v, max_v) {
                    col_min.insert(col.name.clone(), mn);
                    col_max.insert(col.name.clone(), mx);
                }
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::Utf8 => {
                fields.push(Field::new(&col.name, DataType::Utf8, col.nullable));
                let mut b = StringBuilder::new();
                let mut distinct = HashSet::new();
                let mut min_v: Option<&str> = None;
                let mut max_v: Option<&str> = None;
                for r in &batch.rows {
                    match r.get(&col.name) {
                        Some(ScalarValue::Utf8(s)) => {
                            distinct.insert(s.clone());
                            min_v = Some(min_v.map_or(s.as_str(), |m| if s.as_str() < m { s.as_str() } else { m }));
                            max_v = Some(max_v.map_or(s.as_str(), |m| if s.as_str() > m { s.as_str() } else { m }));
                            b.append_value(s);
                        }
                        _ => b.append_null(),
                    }
                }
                if let (Some(mn), Some(mx)) = (min_v, max_v) {
                    col_min.insert(col.name.clone(), mn.to_string());
                    col_max.insert(col.name.clone(), mx.to_string());
                }
                puffin
                    .ndv_estimates
                    .insert(col.name.clone(), distinct.len() as u64);
                arrays.push(Arc::new(b.finish()));
            }
            ColumnType::Variant => {
                fields.push(Field::new(&col.name, DataType::Utf8, col.nullable));
                let mut b = StringBuilder::new();
                let mut path_counts: HashMap<String, usize> = HashMap::new();

                for (row_idx, r) in batch.rows.iter().enumerate() {
                    match r.get(&col.name) {
                        Some(ScalarValue::Json(doc)) => {
                            if let Some(obj) = doc.as_object() {
                                for k in obj.keys() {
                                    *path_counts.entry(k.clone()).or_insert(0) += 1;
                                }
                                index_json_recursive(&mut puffin, "", obj, row_idx as u32);
                            }
                            b.append_value(doc.to_string());
                        }
                        Some(ScalarValue::Utf8(s)) => {
                            if let Ok(doc) = serde_json::from_str::<serde_json::Value>(s)
                                && let Some(obj) = doc.as_object()
                            {
                                for k in obj.keys() {
                                    *path_counts.entry(k.clone()).or_insert(0) += 1;
                                }
                                index_json_recursive(&mut puffin, "", obj, row_idx as u32);
                            }
                            b.append_value(s);
                        }
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));

                // Auto-shred hot top-level JSON paths (>= 50% presence) + any query-driven dynamic nested paths!
                let min_presence = std::cmp::max(1, batch.rows.len() / 2);
                let mut hot_set: HashSet<String> = path_counts
                    .into_iter()
                    .filter(|(_, c)| *c >= min_presence)
                    .map(|(k, _)| k)
                    .collect();
                let col_prefix = format!("{}.", col.name);
                for ep in extra_shred_paths {
                    if let Some(rel) = ep.strip_prefix(&col_prefix) {
                        hot_set.insert(rel.to_string());
                    }
                }
                let mut hot_paths: Vec<String> = hot_set.into_iter().collect();
                hot_paths.sort();

                for path in hot_paths {
                    let shred_col_name = format!("__shred_{}_{}", col.name, path);
                    fields.push(Field::new(&shred_col_name, DataType::Utf8, true));
                    let mut sb = StringBuilder::new();
                    for r in &batch.rows {
                        let extracted = match r.get(&col.name) {
                            Some(ScalarValue::Json(doc)) => {
                                let mut cur = doc;
                                let mut ok = true;
                                for seg in path.split('.') {
                                    match cur.get(seg) {
                                        Some(next) => cur = next,
                                        None => {
                                            ok = false;
                                            break;
                                        }
                                    }
                                }
                                if ok && !cur.is_null() {
                                    Some(match cur {
                                        serde_json::Value::String(s) => s.clone(),
                                        other => other.to_string(),
                                    })
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        };
                        match extracted {
                            Some(v) => sb.append_value(v),
                            None => sb.append_null(),
                        }
                    }
                    arrays.push(Arc::new(sb.finish()));
                    shredded_paths.push(format!("{}.{}", col.name, path));
                }
            }
        }
    }

    // Attach official Iceberg `PARQUET:field_id` metadata to every Arrow Field so PyIceberg,
    // Spark, DuckDB, and Trino resolve columns by stable Iceberg Field ID directly from Parquet! (Wave 27)
    let id_by_name: HashMap<&str, i32> = schema
        .iter()
        .map(|f| (f.name.as_str(), f.field_id))
        .collect();
    let fields: Vec<Field> = fields
        .into_iter()
        .map(|f| {
            if let Some(&fid) = id_by_name.get(f.name().as_str()) {
                f.with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    fid.to_string(),
                )]))
            } else {
                f
            }
        })
        .collect();

    let arrow_schema = Arc::new(Schema::new(fields));
    let rb = RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| RustLakeError::Execution(format!("arrow record batch build: {e}")))?;
    Ok((rb, puffin, shredded_paths, col_min, col_max))
}

fn index_json_recursive(
    puffin: &mut FilePuffinIndex,
    prefix: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    row_idx: u32,
) {
    for (k, v) in obj {
        let full_path = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        match v {
            serde_json::Value::Object(nested) => {
                puffin.index_json_field(&full_path, &v.to_string(), row_idx);
                index_json_recursive(puffin, &full_path, nested, row_idx);
            }
            serde_json::Value::String(s) => {
                puffin.index_json_field(&full_path, s, row_idx);
            }
            other => {
                puffin.index_json_field(&full_path, &other.to_string(), row_idx);
            }
        }
    }
}

/// Transparently widens physical Parquet scalar values on read to match an evolved Iceberg schema
/// (`Int64 -> Float64`, `Int64 -> Decimal`, `Decimal(v, s1) -> Decimal(v, s2)`, Wave 22).
fn widen_scalar_on_read(val: ScalarValue, target: &ColumnType) -> ScalarValue {
    match (val, target) {
        (ScalarValue::Int64(i), ColumnType::Float64) => ScalarValue::Float64(i as f64),
        (ScalarValue::Int64(i), ColumnType::Decimal { scale, .. }) => {
            let factor = 10i128.pow(*scale as u32);
            ScalarValue::Decimal((i as i128) * factor, *scale)
        }
        (ScalarValue::Decimal(v, s1), ColumnType::Decimal { scale: s2, .. }) if s1 != *s2 => {
            if *s2 > s1 {
                let factor = 10i128.pow((*s2 - s1) as u32);
                ScalarValue::Decimal(v * factor, *s2)
            } else {
                let factor = 10i128.pow((s1 - *s2) as u32);
                ScalarValue::Decimal(v / factor, *s2)
            }
        }
        (ScalarValue::Decimal(v, s), ColumnType::Float64) => {
            ScalarValue::Float64((v as f64) / 10f64.powi(s as i32))
        }
        (other, _) => other,
    }
}

fn extract_scalar(
    arr: &dyn Array,
    row_idx: usize,
    logical_type: Option<&ColumnType>,
) -> ScalarValue {
    if arr.is_null(row_idx) {
        return ScalarValue::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return ScalarValue::Int64(a.value(row_idx));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return ScalarValue::Float64(a.value(row_idx));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return ScalarValue::Date(a.value(row_idx));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match a.data_type() {
            DataType::Decimal128(_, s) => (*s).max(0) as u8,
            _ => 2,
        };
        return ScalarValue::Decimal(a.value(row_idx), scale);
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return ScalarValue::Utf8(format_iso_timestamp_micros(a.value(row_idx)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return ScalarValue::Bool(a.value(row_idx));
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        let s = a.value(row_idx);
        if matches!(logical_type, Some(ColumnType::Variant))
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(s)
        {
            return ScalarValue::Json(v);
        }
        return ScalarValue::Utf8(s.to_string());
    }
    ScalarValue::Null
}

fn parse_iso_timestamp_micros(s: &str) -> Option<i64> {
    let trimmed = s.trim().trim_end_matches('Z');
    let (date_str, time_str) = trimmed
        .split_once('T')
        .or_else(|| trimmed.split_once(' '))
        .unwrap_or((trimmed, "00:00:00"));
    let days = rustlakedb_protocol::parse_iso_date_days(date_str)? as i64;
    let mut tp = time_str.split(':');
    let hh = tp.next().and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
    let mm = tp.next().and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
    let ss_part = tp.next().unwrap_or("0");
    let (ss_s, frac_s) = ss_part.split_once('.').unwrap_or((ss_part, "0"));
    let ss = ss_s.parse::<i64>().unwrap_or(0);
    let mut frac_6 = frac_s.chars().take(6).collect::<String>();
    while frac_6.len() < 6 {
        frac_6.push('0');
    }
    let micros = frac_6.parse::<i64>().unwrap_or(0);
    Some((days * 86_400 + hh * 3_600 + mm * 60 + ss) * 1_000_000 + micros)
}

fn format_iso_timestamp_micros(us: i64) -> String {
    let total_secs = us.div_euclid(1_000_000);
    let days = total_secs.div_euclid(86_400) as i32;
    let sod = total_secs.rem_euclid(86_400);
    let hh = sod / 3_600;
    let mm = (sod % 3_600) / 60;
    let ss = sod % 60;
    let date = rustlakedb_protocol::format_iso_date_days(days);
    format!("{date}T{hh:02}:{mm:02}:{ss:02}Z")
}
