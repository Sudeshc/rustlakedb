// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod columnar;
pub mod rpc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use roaring::RoaringBitmap;
use rustlakedb_catalog::{Catalog, FilePuffinIndex};
use rustlakedb_common::{MemoryPool, Result, RustLakeError, WorkerId};
use rustlakedb_protocol::{
    AggExpr, AggFunc, ArithOp, ColumnExpr, FilterExpr, JoinType, MathFunc, PartitionMetrics,
    Partitioning, PhysicalNode, RowBatch, ScalarValue, SetOpKind, ShuffleBlockLocation,
    ShuffleMode, ShufflePartitionKey, SortKey, StringFunc, TaskCompletionReport, TaskSpec,
    VectorMetric, WindowFunc, WindowSpec,
};
use rustlakedb_shuffle::ShuffleTransport;
use rustlakedb_storage::IcebergObjectStore;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Stateless RustLakeDB Worker executing distributed physical plan stages
/// directly over real Apache Parquet + Puffin + Iceberg v3 Deletion Vectors
/// with bounded `MemoryPool` & NVMe Grace Hash Join spilling (RFC-001, RFC-003).
pub struct WorkerNode {
    pub id: WorkerId,
    catalog: Arc<dyn Catalog>,
    shuffle: Arc<dyn ShuffleTransport>,
    store: Arc<IcebergObjectStore>,
    pub memory_pool: Arc<MemoryPool>,
    spill_dir: PathBuf,
    dead: AtomicBool,
    fail_once: AtomicBool,
    pub tasks_executed: AtomicU64,
    pub vectorized_rows_processed: AtomicU64,
    /// Parquet splits skipped before reading via Dynamic Partition Pruning / Semi-Join Min-Max pushdown (Wave 25).
    pub files_pruned_by_dpp: AtomicU64,
    /// Arrow IPC shuffle blocks read directly as `RecordBatch` without `RowBatch` deserialization (Wave 30).
    pub arrow_shuffle_batches_read: AtomicU64,
    /// Compressed bytes spilled to NVMe by out-of-core Arrow Sort / Grace Hash Join (Wave 34).
    pub arrow_spill_bytes_written: AtomicU64,
    /// Temporary `ARW1` Arrow spill runs/partitions created on NVMe (Wave 34).
    pub arrow_spill_files_created: AtomicU64,
    /// Cooperatively cancelled query IDs checked between split/batch reads (Wave 22).
    pub cancelled_queries: std::sync::RwLock<HashSet<String>>,
}

impl WorkerNode {
    pub fn new(
        id: impl Into<String>,
        catalog: Arc<dyn Catalog>,
        shuffle: Arc<dyn ShuffleTransport>,
        store: Arc<IcebergObjectStore>,
    ) -> Self {
        Self::with_memory_limit(id, catalog, shuffle, store, 256 * 1024 * 1024)
    }

    pub fn with_memory_limit(
        id: impl Into<String>,
        catalog: Arc<dyn Catalog>,
        shuffle: Arc<dyn ShuffleTransport>,
        store: Arc<IcebergObjectStore>,
        memory_limit_bytes: usize,
    ) -> Self {
        let wid = id.into();
        let spill_dir = std::env::temp_dir().join(format!("rustlakedb_spill_{wid}"));
        let _ = std::fs::create_dir_all(&spill_dir);
        Self {
            id: WorkerId::new(wid),
            catalog,
            shuffle,
            store,
            memory_pool: Arc::new(MemoryPool::new(memory_limit_bytes)),
            spill_dir,
            dead: AtomicBool::new(false),
            fail_once: AtomicBool::new(false),
            tasks_executed: AtomicU64::new(0),
            vectorized_rows_processed: AtomicU64::new(0),
            files_pruned_by_dpp: AtomicU64::new(0),
            arrow_shuffle_batches_read: AtomicU64::new(0),
            arrow_spill_bytes_written: AtomicU64::new(0),
            arrow_spill_files_created: AtomicU64::new(0),
            cancelled_queries: std::sync::RwLock::new(HashSet::new()),
        }
    }

    /// Marks a query ID as cancelled so in-flight scans on this worker abort cooperatively (Wave 22).
    pub fn cancel_query(&self, query_id: &str) {
        if let Ok(mut g) = self.cancelled_queries.write() {
            g.insert(query_id.to_string());
        }
    }

    pub fn is_query_cancelled(&self, query_id: &str) -> bool {
        self.cancelled_queries
            .read()
            .map(|g| g.contains(query_id))
            .unwrap_or(false)
    }

    pub fn store(&self) -> &Arc<IcebergObjectStore> {
        &self.store
    }

    pub fn set_dead(&self, is_dead: bool) {
        self.dead.store(is_dead, Ordering::SeqCst);
    }

    pub fn inject_single_failure(&self) {
        self.fail_once.store(true, Ordering::SeqCst);
    }

    pub fn is_healthy(&self) -> bool {
        !self.dead.load(Ordering::SeqCst)
    }

    pub async fn read_shuffle_block(&self, loc: &ShuffleBlockLocation) -> Result<RowBatch> {
        self.shuffle.read_batch(loc).await
    }

    pub async fn read_shuffle_block_raw(&self, loc: &ShuffleBlockLocation) -> Result<bytes::Bytes> {
        self.shuffle.read_raw_block(loc).await
    }

    pub async fn execute_task(&self, task: TaskSpec) -> Result<TaskCompletionReport> {
        if self.dead.load(Ordering::SeqCst) || self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(RustLakeError::WorkerFailed {
                worker_id: self.id.0.clone(),
                reason: "simulated worker crash / spot preemption".into(),
            });
        }

        self.tasks_executed.fetch_add(1, Ordering::Relaxed);
        let result_batch = self.eval_node(&task.plan, &task).await?;

        if let Some(target_part) = &task.output_partitioning {
            let mode = task.output_shuffle_mode.unwrap_or(ShuffleMode::Staged);
            let partitioned = partition_batch(&result_batch, target_part);
            let mut produced_blocks = Vec::with_capacity(partitioned.len());
            let mut metrics = Vec::with_capacity(partitioned.len());

            for (reduce_part, part_batch) in partitioned.into_iter().enumerate() {
                let key = ShufflePartitionKey {
                    query_id: task.attempt_id.query_id,
                    stage_id: task.attempt_id.stage_id,
                    map_partition: task.attempt_id.partition_id,
                    reduce_partition: reduce_part,
                    attempt: task.attempt_id.attempt,
                };
                let loc = self
                    .shuffle
                    .write_batch(&self.id, key, mode, &part_batch)
                    .await?;
                metrics.push(PartitionMetrics {
                    reduce_partition: reduce_part,
                    row_count: loc.row_count,
                    byte_size: loc.byte_size,
                });
                produced_blocks.push(loc);
            }

            Ok(TaskCompletionReport {
                attempt_id: task.attempt_id,
                worker_id: self.id.clone(),
                produced_blocks,
                partition_metrics: metrics,
                final_output: None,
            })
        } else {
            Ok(TaskCompletionReport {
                attempt_id: task.attempt_id,
                worker_id: self.id.clone(),
                produced_blocks: vec![],
                partition_metrics: vec![PartitionMetrics {
                    reduce_partition: task.attempt_id.partition_id,
                    row_count: result_batch.num_rows(),
                    byte_size: result_batch.estimated_bytes(),
                }],
                final_output: Some(result_batch),
            })
        }
    }

    fn eval_node<'a>(
        &'a self,
        node: &'a PhysicalNode,
        task: &'a TaskSpec,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RowBatch>> + Send + 'a>> {
        Box::pin(async move {
            match node {
                PhysicalNode::IcebergScan {
                    table_name,
                    pushed_filter,
                    projection,
                    ..
                } => {
                    let meta = self.catalog.get_table(table_name).await?;
                    let mut all_cols: Vec<String> =
                        meta.schema.iter().map(|f| f.name.clone()).collect();
                    let file_stats: HashMap<&str, &rustlakedb_catalog::DataFileEntry> = meta
                        .snapshots
                        .values()
                        .flat_map(|s| s.data_files.iter())
                        .map(|df| (df.file_path.as_str(), df))
                        .collect();
                    // Wave 31: Compute each file's introduction sequence number (earliest snapshot_id)
                    // and load any active Iceberg v3 Equality Delete files from the latest snapshot.
                    let mut file_first_seq: HashMap<&str, u64> = HashMap::new();
                    for (&sid, snap) in &meta.snapshots {
                        for df in &snap.data_files {
                            file_first_seq.entry(df.file_path.as_str()).or_insert(sid);
                        }
                    }
                    let latest_eq_deletes: Vec<rustlakedb_catalog::EqualityDeleteFile> = meta
                        .snapshots
                        .values()
                        .next_back()
                        .map(|s| s.equality_deletes.clone())
                        .unwrap_or_default();
                    let mut loaded_eq_deletes: Vec<(u64, String, HashSet<String>)> = Vec::new();
                    for eq in &latest_eq_deletes {
                        let keys = self
                            .store
                            .read_equality_delete_keys(&eq.file_path, &eq.equality_column)
                            .await?;
                        loaded_eq_deletes.push((eq.sequence_number, eq.equality_column.clone(), keys));
                    }
                    let mut out_rows = Vec::new();

                    for split in &task.assigned_splits {
                        // Layer 0: Iceberg Hidden-Partition Split Pruning (Wave 14) — O(1) skip before Zone-Maps/Parquet!
                        if let Some(pred) = pushed_filter
                            && let Some(df) = file_stats.get(split.file_path.as_str())
                            && partition_can_skip_file(pred, &df.partition_values)
                        {
                            self.store
                                .files_pruned_by_partition
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        // Layer 1: Zone-Map Min/Max file pruning (Wave 4 Liquid/Hilbert Clustering!)
                        if let Some(pred) = pushed_filter
                            && let Some(df) = file_stats.get(split.file_path.as_str())
                            && zonemap_can_skip_file(pred, &df.column_min, &df.column_max)
                        {
                            self.store
                                .files_pruned_by_zonemap
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        // Layer 2: Puffin Bloom + Inverted Index pruning BEFORE touching Parquet!
                        let candidate_rows: Option<RoaringBitmap> = match pushed_filter {
                            Some(pred) => {
                                let puffin = self.store.read_puffin_index(&split.file_path).await?;
                                match eval_puffin_pruning(&puffin, pred) {
                                    PuffinPruneResult::SkipFile => {
                                        self.store
                                            .files_pruned_by_puffin
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                    PuffinPruneResult::CandidateRows(bm) => Some(bm),
                                    PuffinPruneResult::NoIndex => None,
                                }
                            }
                            None => None,
                        };

                        // Cooperative in-flight query cancellation check between splits (Wave 22)
                        if self.is_query_cancelled(&task.attempt_id.query_id.0.to_string()) {
                            return Err(RustLakeError::Execution(format!(
                                "query '{}' cancelled by user on worker '{}'",
                                task.attempt_id.query_id, self.id
                            )));
                        }

                        // Wave 12 Vectorized Arrow Filtered Scan: evaluate simple column predicates
                        // directly over Arrow RecordBatches and materialize ONLY matching rows!
                        if candidate_rows.is_none()
                            && meta.column_aliases.is_empty()
                            && loaded_eq_deletes.is_empty()
                            && let Some(pred) = pushed_filter
                            && let Some(vec_rows) = self
                                .try_eval_vectorized_filtered_scan(
                                    &split.file_path,
                                    &meta.schema,
                                    projection,
                                    pred,
                                )
                                .await?
                        {
                            out_rows.extend(vec_rows);
                            continue;
                        }

                        let proj_opt = if projection.is_empty() {
                            None
                        } else {
                            Some(projection.as_slice())
                        };
                        let file_batch = self
                            .store
                            .read_data_file_with_aliases(
                                &split.file_path,
                                &meta.schema,
                                &meta.column_aliases,
                                proj_opt,
                                candidate_rows.as_ref(),
                            )
                            .await?;

                        if all_cols.is_empty() {
                            all_cols = file_batch.columns;
                        }
                        let file_seq = file_first_seq
                            .get(split.file_path.as_str())
                            .copied()
                            .unwrap_or(0);
                        for row in file_batch.rows {
                            // Wave 31: Apply Apache Iceberg v3 Equality Deletes whose sequence_number > file_seq!
                            let eq_deleted = loaded_eq_deletes.iter().any(|(eq_seq, eq_col, keys)| {
                                *eq_seq > file_seq
                                    && row
                                        .get(eq_col)
                                        .map(|v| keys.contains(&v.to_display_string()))
                                        .unwrap_or(false)
                            });
                            if eq_deleted {
                                self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            if let Some(pred) = pushed_filter
                                && !eval_filter(&row, pred) {
                                    continue;
                                }
                            out_rows.push(row);
                        }
                    }

                    let cols = if projection.is_empty() {
                        all_cols
                    } else {
                        let schema_set: HashSet<&str> =
                            meta.schema.iter().map(|f| f.name.as_str()).collect();
                        let filtered: Vec<String> = projection
                            .iter()
                            .filter(|c| schema_set.contains(c.as_str()) || c.starts_with("__shred_"))
                            .cloned()
                            .collect();
                        if filtered.is_empty() { all_cols } else { filtered }
                    };
                    Ok(RowBatch::new(cols, out_rows))
                }
                PhysicalNode::Filter { input, predicate } => {
                    let batch = self.eval_node(input, task).await?;
                    let rows = batch
                        .rows
                        .into_iter()
                        .filter(|r| eval_filter(r, predicate))
                        .collect();
                    Ok(RowBatch::new(batch.columns, rows))
                }
                PhysicalNode::Project { input, exprs } => {
                    if let PhysicalNode::IcebergScan {
                        table_name,
                        pushed_filter: None,
                        ..
                    } = input.as_ref()
                        && let Some(vec_proj) = self
                            .try_eval_vectorized_scan_project(table_name, exprs, task)
                            .await?
                    {
                        return Ok(vec_proj);
                    }
                    let batch = self.eval_node(input, task).await?;
                    let out_cols: Vec<String> = exprs.iter().map(|e| e.output_name()).collect();
                    let rows = batch
                        .rows
                        .into_iter()
                        .map(|r| {
                            exprs
                                .iter()
                                .map(|e| (e.output_name(), eval_col_expr(&r, e)))
                                .collect()
                        })
                        .collect();
                    Ok(RowBatch::new(out_cols, rows))
                }
                PhysicalNode::Distinct { input } => {
                    let batch = self.eval_node(input, task).await?;
                    let mut seen = HashSet::new();
                    let mut out = Vec::new();
                    for r in batch.rows {
                        let key = serde_json::to_string(&r).unwrap_or_default();
                        if seen.insert(key) {
                            out.push(r);
                        }
                    }
                    Ok(RowBatch::new(batch.columns, out))
                }
                PhysicalNode::Sort { input, keys } => {
                    // Wave 30 + Wave 34 End-to-End Arrow Shuffle -> Columnar Sort (in-memory or NVMe K-way spill!).
                    if let PhysicalNode::ShuffleRead {
                        upstream_stage_id,
                        output_partitioning,
                        ..
                    } = input.as_ref()
                        && let Some(rb) = self
                            .try_read_shuffle_arrow(*upstream_stage_id, output_partitioning, task)
                            .await?
                    {
                        let mem_bytes = rb.get_array_memory_size();
                        if let Ok(_guard) = self.memory_pool.try_reserve(mem_bytes) {
                            if let Ok(sorted_rb) = columnar::eval_columnar_sort(&rb, keys) {
                                self.vectorized_rows_processed
                                    .fetch_add(sorted_rb.num_rows() as u64, Ordering::Relaxed);
                                return Ok(rustlakedb_storage::record_batch_to_row_batch(&sorted_rb));
                            }
                        } else if let Ok((sorted_rb, spill_stats)) =
                            columnar::eval_columnar_sort_with_spill(
                                &rb,
                                keys,
                                self.memory_pool.limit_bytes(),
                                &self.spill_dir,
                            )
                        {
                            if spill_stats.spill_files_created > 0 {
                                self.memory_pool.record_spill();
                                self.arrow_spill_bytes_written
                                    .fetch_add(spill_stats.spill_bytes_written, Ordering::Relaxed);
                                self.arrow_spill_files_created
                                    .fetch_add(spill_stats.spill_files_created, Ordering::Relaxed);
                            }
                            self.vectorized_rows_processed
                                .fetch_add(sorted_rb.num_rows() as u64, Ordering::Relaxed);
                            return Ok(rustlakedb_storage::record_batch_to_row_batch(&sorted_rb));
                        }
                    }
                    let batch = self.eval_node(input, task).await?;
                    self.eval_sort_with_spill(batch, keys).await
                }
                PhysicalNode::Limit {
                    input,
                    limit,
                    offset,
                } => {
                    let mut batch = self.eval_node(input, task).await?;
                    if *offset > 0 {
                        if *offset >= batch.rows.len() {
                            batch.rows.clear();
                        } else {
                            batch.rows.drain(0..*offset);
                        }
                    }
                    batch.rows.truncate(*limit);
                    Ok(batch)
                }
                PhysicalNode::TopN {
                    input,
                    keys,
                    limit,
                    offset,
                    is_partial,
                } => {
                    if *is_partial
                        && let PhysicalNode::IcebergScan {
                            table_name,
                            projection,
                            pushed_filter: None,
                            ..
                        } = input.as_ref()
                        && let Some(vec_topn) = self
                            .try_eval_vectorized_topn(
                                table_name,
                                projection,
                                keys,
                                limit.saturating_add(*offset),
                                task,
                            )
                            .await?
                    {
                        return Ok(vec_topn);
                    }
                    let mut batch = self.eval_node(input, task).await?;
                    sort_rows(&mut batch.rows, keys);
                    if *is_partial {
                        batch.rows.truncate(limit.saturating_add(*offset));
                    } else {
                        if *offset > 0 {
                            if *offset >= batch.rows.len() {
                                batch.rows.clear();
                            } else {
                                batch.rows.drain(0..*offset);
                            }
                        }
                        batch.rows.truncate(*limit);
                    }
                    Ok(batch)
                }
                PhysicalNode::ShuffleRead {
                    upstream_stage_id,
                    output_partitioning,
                    ..
                } => {
                    let matching: Vec<&ShuffleBlockLocation> = task
                        .upstream_blocks
                        .iter()
                        .filter(|b| {
                            b.key.stage_id == *upstream_stage_id
                                && (matches!(output_partitioning, Partitioning::Single)
                                    || if !task.assigned_reduce_partitions.is_empty() {
                                        task.assigned_reduce_partitions
                                            .contains(&b.key.reduce_partition)
                                    } else {
                                        b.key.reduce_partition == task.attempt_id.partition_id
                                    })
                        })
                        .collect();
                    let mut cols = Vec::new();
                    let mut rows = Vec::new();
                    for loc in matching {
                        let b = self.shuffle.read_batch(loc).await?;
                        if cols.is_empty() && !b.columns.is_empty() {
                            cols = b.columns;
                        }
                        rows.extend(b.rows);
                    }
                    Ok(RowBatch::new(cols, rows))
                }
                PhysicalNode::HashAggregate {
                    input,
                    group_by,
                    aggs,
                    is_partial,
                    grouping_sets,
                } => {
                    if *is_partial {
                        if let PhysicalNode::IcebergScan {
                            table_name,
                            pushed_filter,
                            ..
                        } = input.as_ref()
                            && let Some(vec_batch) = self
                                .try_eval_vectorized_scan_partial_agg(
                                    table_name,
                                    pushed_filter.as_ref(),
                                    group_by,
                                    aggs,
                                    task,
                                )
                                .await?
                        {
                            return Ok(vec_batch);
                        }
                        if let Some(vec_join) = self
                            .try_eval_vectorized_join_partial_agg(input, group_by, aggs, task)
                            .await?
                        {
                            return Ok(vec_join);
                        }
                    }
                    let batch = self.eval_node(input, task).await?;
                    if !*is_partial && !grouping_sets.is_empty() {
                        return Ok(eval_grouping_sets_aggregate(
                            &batch,
                            group_by,
                            aggs,
                            grouping_sets,
                        ));
                    }
                    self.eval_hash_aggregate_with_spill(&batch, group_by, aggs, *is_partial)
                        .await
                }
                PhysicalNode::HashJoin {
                    left,
                    right,
                    left_key,
                    right_key,
                    join_type,
                    ..
                } => {
                    // Wave 30 + Wave 34 End-to-End Arrow Shuffle -> Columnar HashJoin (in-memory or NVMe Grace spill!).
                    if matches!(
                        join_type,
                        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                    ) && let (
                        PhysicalNode::ShuffleRead {
                            upstream_stage_id: ls,
                            output_partitioning: lp,
                            ..
                        },
                        PhysicalNode::ShuffleRead {
                            upstream_stage_id: rs,
                            output_partitioning: rp,
                            ..
                        },
                    ) = (left.as_ref(), right.as_ref())
                        && let (Some(lrb), Some(rrb)) = (
                            self.try_read_shuffle_arrow(*ls, lp, task).await?,
                            self.try_read_shuffle_arrow(*rs, rp, task).await?,
                        )
                    {
                        let mem_bytes = lrb
                            .get_array_memory_size()
                            .saturating_add(rrb.get_array_memory_size());
                        if let Ok(_guard) = self.memory_pool.try_reserve(mem_bytes) {
                            if let Ok(joined_rb) = columnar::eval_columnar_hash_join(
                                &lrb, &rrb, left_key, right_key, *join_type,
                            ) {
                                self.vectorized_rows_processed.fetch_add(
                                    (lrb.num_rows() + rrb.num_rows()) as u64,
                                    Ordering::Relaxed,
                                );
                                return Ok(rustlakedb_storage::record_batch_to_row_batch(&joined_rb));
                            }
                        } else if let Ok((joined_rb, spill_stats)) =
                            columnar::eval_columnar_hash_join_with_spill(
                                &lrb,
                                &rrb,
                                left_key,
                                right_key,
                                *join_type,
                                self.memory_pool.limit_bytes(),
                                &self.spill_dir,
                            )
                        {
                            if spill_stats.spill_files_created > 0 {
                                self.memory_pool.record_spill();
                                self.arrow_spill_bytes_written
                                    .fetch_add(spill_stats.spill_bytes_written, Ordering::Relaxed);
                                self.arrow_spill_files_created
                                    .fetch_add(spill_stats.spill_files_created, Ordering::Relaxed);
                            }
                            self.vectorized_rows_processed.fetch_add(
                                (lrb.num_rows() + rrb.num_rows()) as u64,
                                Ordering::Relaxed,
                            );
                            return Ok(rustlakedb_storage::record_batch_to_row_batch(&joined_rb));
                        }
                    }
                    let left_batch = self.eval_node(left, task).await?;
                    let right_batch = self.eval_node(right, task).await?;
                    self.eval_hash_join_with_spill(
                        &left_batch,
                        &right_batch,
                        left_key,
                        right_key,
                        *join_type,
                    )
                    .await
                }
                PhysicalNode::Window { input, specs } => {
                    // Wave 22 Pure Arrow RecordBatch Window Fast Path: when input is a single-table
                    // IcebergScan with no schema aliases, execute Window directly on Arrow RecordBatches!
                    if let PhysicalNode::IcebergScan {
                        table_name,
                        pushed_filter,
                        splits,
                        ..
                    } = input.as_ref()
                        && let Ok(meta) = self.catalog.get_table(table_name).await
                        && meta.column_aliases.is_empty()
                    {
                        let effective_splits = if !task.assigned_splits.is_empty() {
                            &task.assigned_splits
                        } else {
                            splits
                        };
                        if effective_splits.len() == 1
                            && let Ok((batches, dv)) = self
                                .store
                                .read_arrow_batches(&effective_splits[0].file_path, None)
                                .await
                            && dv.is_empty()
                            && batches.len() == 1
                        {
                            let mut rb = batches.into_iter().next().unwrap();
                            let ok_filter = if let Some(pred) = pushed_filter {
                                match columnar::apply_columnar_filter(&rb, pred) {
                                    Some(f) => {
                                        rb = f;
                                        true
                                    }
                                    None => false,
                                }
                            } else {
                                true
                            };
                            if ok_filter
                                && let Ok(win_rb) = columnar::eval_columnar_window(&rb, specs)
                            {
                                self.vectorized_rows_processed
                                    .fetch_add(win_rb.num_rows() as u64, Ordering::Relaxed);
                                return Ok(rustlakedb_storage::record_batch_to_row_batch(&win_rb));
                            }
                        }
                    }
                    let batch = self.eval_node(input, task).await?;
                    if !batch.rows.is_empty()
                        && let Ok(rb) = rustlakedb_storage::row_batch_to_record_batch(&batch)
                        && let Ok(win_rb) = columnar::eval_columnar_window(&rb, specs)
                    {
                        self.vectorized_rows_processed
                            .fetch_add(win_rb.num_rows() as u64, Ordering::Relaxed);
                        return Ok(rustlakedb_storage::record_batch_to_row_batch(&win_rb));
                    }
                    Ok(eval_window(batch, specs))
                }
                PhysicalNode::UnionAll { inputs, distinct } => {
                    let mut cols = Vec::new();
                    let mut out_rows = Vec::new();
                    let mut seen = HashSet::new();
                    for inp in inputs {
                        let b = self.eval_node(inp, task).await?;
                        if cols.is_empty() && !b.columns.is_empty() {
                            cols = b.columns;
                        }
                        for r in b.rows {
                            if *distinct {
                                let key = serde_json::to_string(&r).unwrap_or_default();
                                if !seen.insert(key) {
                                    continue;
                                }
                            }
                            out_rows.push(r);
                        }
                    }
                    Ok(RowBatch::new(cols, out_rows))
                }
                PhysicalNode::SetOp {
                    op,
                    left,
                    right,
                    distinct,
                } => {
                    let lb = self.eval_node(left, task).await?;
                    let rb = self.eval_node(right, task).await?;
                    let cols = if lb.columns.is_empty() {
                        rb.columns.clone()
                    } else {
                        lb.columns.clone()
                    };
                    // Compare rows positionally so column alias differences between branches work like PostgreSQL!
                    let row_sig = |r: &BTreeMap<String, ScalarValue>, branch_cols: &[String]| -> String {
                        let vals: Vec<ScalarValue> = branch_cols
                            .iter()
                            .map(|c| r.get(c).cloned().unwrap_or(ScalarValue::Null))
                            .collect();
                        serde_json::to_string(&vals).unwrap_or_default()
                    };
                    let mut right_counts: HashMap<String, usize> = HashMap::new();
                    for r in &rb.rows {
                        *right_counts.entry(row_sig(r, &rb.columns)).or_insert(0) += 1;
                    }
                    let mut out_rows = Vec::new();
                    let mut emitted = HashSet::new();
                    match op {
                        SetOpKind::Union => {
                            for (b_rows, b_cols) in [(&lb.rows, &lb.columns), (&rb.rows, &rb.columns)] {
                                for r in b_rows {
                                    let sig = row_sig(r, b_cols);
                                    if *distinct && !emitted.insert(sig) {
                                        continue;
                                    }
                                    out_rows.push(r.clone());
                                }
                            }
                        }
                        SetOpKind::Intersect => {
                            for r in &lb.rows {
                                let sig = row_sig(r, &lb.columns);
                                if let Some(cnt) = right_counts.get_mut(&sig)
                                    && *cnt > 0
                                {
                                    if *distinct {
                                        if emitted.insert(sig) {
                                            out_rows.push(r.clone());
                                        }
                                    } else {
                                        *cnt -= 1;
                                        out_rows.push(r.clone());
                                    }
                                }
                            }
                        }
                        SetOpKind::Except => {
                            for r in &lb.rows {
                                let sig = row_sig(r, &lb.columns);
                                if let Some(cnt) = right_counts.get_mut(&sig)
                                    && *cnt > 0
                                {
                                    if !*distinct {
                                        *cnt -= 1;
                                    }
                                    continue;
                                }
                                if *distinct && !emitted.insert(sig) {
                                    continue;
                                }
                                out_rows.push(r.clone());
                            }
                        }
                    }
                    Ok(RowBatch::new(cols, out_rows))
                }
                PhysicalNode::Exchange { input, .. } => self.eval_node(input, task).await,
            }
        })
    }
}

pub fn eval_col_expr(row: &BTreeMap<String, ScalarValue>, expr: &ColumnExpr) -> ScalarValue {
    match expr {
        ColumnExpr::Column(c) => row.get(c).cloned().unwrap_or(ScalarValue::Null),
        ColumnExpr::JsonPath {
            column,
            path,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            match row.get(column) {
                Some(ScalarValue::Json(doc)) => {
                    let mut cur = doc;
                    for seg in path.split('.') {
                        match cur.get(seg) {
                            Some(next) => cur = next,
                            None => return ScalarValue::Null,
                        }
                    }
                    match cur {
                        serde_json::Value::Null => ScalarValue::Null,
                        serde_json::Value::String(s) => ScalarValue::Utf8(s.clone()),
                        other => ScalarValue::Utf8(other.to_string()),
                    }
                }
                _ => ScalarValue::Null,
            }
        }
        ColumnExpr::CaseWhen {
            branches,
            else_val,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            for (pred, val) in branches {
                if eval_filter(row, pred) {
                    return val.clone();
                }
            }
            else_val.clone()
        }
        ColumnExpr::Coalesce {
            columns,
            default,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            for c in columns {
                if let Some(v) = row.get(c)
                    && !matches!(v, ScalarValue::Null)
                {
                    return v.clone();
                }
            }
            default.clone()
        }
        ColumnExpr::DateTrunc {
            unit,
            column,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            match row.get(column) {
                Some(ScalarValue::Utf8(ts)) => ScalarValue::Utf8(truncate_iso_timestamp(ts, unit)),
                _ => ScalarValue::Null,
            }
        }
        ColumnExpr::Literal { value, .. } => value.clone(),
        ColumnExpr::VectorDistance {
            column,
            query_vec,
            metric,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            let row_vec = match row.get(column) {
                Some(ScalarValue::Json(serde_json::Value::Array(arr))) => arr
                    .iter()
                    .filter_map(|v| v.as_f64())
                    .collect::<Vec<f64>>(),
                Some(ScalarValue::Utf8(s)) => parse_vector_literal(s).unwrap_or_default(),
                _ => Vec::new(),
            };
            if row_vec.len() != query_vec.len() || row_vec.is_empty() {
                return ScalarValue::Null;
            }
            let score = match metric {
                VectorMetric::L2 => row_vec
                    .iter()
                    .zip(query_vec.iter())
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum::<f64>()
                    .sqrt(),
                VectorMetric::Cosine => {
                    let dot: f64 = row_vec.iter().zip(query_vec.iter()).map(|(a, b)| a * b).sum();
                    let na: f64 = row_vec.iter().map(|a| a * a).sum::<f64>().sqrt();
                    let nb: f64 = query_vec.iter().map(|b| b * b).sum::<f64>().sqrt();
                    if na == 0.0 || nb == 0.0 {
                        0.0
                    } else {
                        dot / (na * nb)
                    }
                }
            };
            ScalarValue::Float64(score)
        }
        ColumnExpr::BinaryArith {
            left,
            op,
            right,
            alias,
        } => {
            if let (Some(lv), Some(rv)) = (
                eval_col_expr(row, left).as_f64(),
                eval_col_expr(row, right).as_f64(),
            ) {
                let res = match op {
                    ArithOp::Add => lv + rv,
                    ArithOp::Sub => lv - rv,
                    ArithOp::Mul => lv * rv,
                    ArithOp::Div => {
                        if rv == 0.0 {
                            return ScalarValue::Null;
                        }
                        lv / rv
                    }
                };
                return ScalarValue::Float64(res);
            }
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            ScalarValue::Null
        }
        ColumnExpr::StringExpr { func, args, alias } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            match func {
                StringFunc::Lower => args
                    .first()
                    .and_then(|a| eval_col_expr(row, a).to_pg_text())
                    .map(|s| ScalarValue::Utf8(s.to_lowercase()))
                    .unwrap_or(ScalarValue::Null),
                StringFunc::Upper => args
                    .first()
                    .and_then(|a| eval_col_expr(row, a).to_pg_text())
                    .map(|s| ScalarValue::Utf8(s.to_uppercase()))
                    .unwrap_or(ScalarValue::Null),
                StringFunc::Trim => args
                    .first()
                    .and_then(|a| eval_col_expr(row, a).to_pg_text())
                    .map(|s| ScalarValue::Utf8(s.trim().to_string()))
                    .unwrap_or(ScalarValue::Null),
                StringFunc::Length => args
                    .first()
                    .and_then(|a| eval_col_expr(row, a).to_pg_text())
                    .map(|s| ScalarValue::Int64(s.chars().count() as i64))
                    .unwrap_or(ScalarValue::Null),
                StringFunc::Concat => {
                    let mut out = String::new();
                    for a in args {
                        if let Some(s) = eval_col_expr(row, a).to_pg_text() {
                            out.push_str(&s);
                        }
                    }
                    ScalarValue::Utf8(out)
                }
                StringFunc::Substring => {
                    let Some(s) = args.first().and_then(|a| eval_col_expr(row, a).to_pg_text()) else {
                        return ScalarValue::Null;
                    };
                    let start = args
                        .get(1)
                        .and_then(|a| eval_col_expr(row, a).as_f64())
                        .map(|f| (f as usize).saturating_sub(1))
                        .unwrap_or(0);
                    let chars: Vec<char> = s.chars().collect();
                    let len = args
                        .get(2)
                        .and_then(|a| eval_col_expr(row, a).as_f64())
                        .map(|f| f as usize)
                        .unwrap_or_else(|| chars.len().saturating_sub(start));
                    let sub: String = chars.into_iter().skip(start).take(len).collect();
                    ScalarValue::Utf8(sub)
                }
                StringFunc::ArrayLength => {
                    let Some(val) = args.first().map(|a| eval_col_expr(row, a)) else {
                        return ScalarValue::Null;
                    };
                    let doc = match val {
                        ScalarValue::Json(v) => Some(v),
                        ScalarValue::Utf8(s) => serde_json::from_str::<serde_json::Value>(&s).ok(),
                        _ => None,
                    };
                    match doc {
                        Some(serde_json::Value::Array(arr)) => ScalarValue::Int64(arr.len() as i64),
                        Some(serde_json::Value::Object(obj)) => ScalarValue::Int64(obj.len() as i64),
                        _ => ScalarValue::Null,
                    }
                }
                StringFunc::ArrayContains => {
                    let Some(val) = args.first().map(|a| eval_col_expr(row, a)) else {
                        return ScalarValue::Null;
                    };
                    let needle = args
                        .get(1)
                        .and_then(|a| eval_col_expr(row, a).to_pg_text())
                        .unwrap_or_default();
                    let doc = match val {
                        ScalarValue::Json(v) => Some(v),
                        ScalarValue::Utf8(s) => serde_json::from_str::<serde_json::Value>(&s).ok(),
                        _ => None,
                    };
                    match doc {
                        Some(serde_json::Value::Array(arr)) => {
                            let found = arr.iter().any(|el| match el {
                                serde_json::Value::String(s) => s == &needle,
                                other => format!("{other}") == needle,
                            });
                            ScalarValue::Bool(found)
                        }
                        _ => ScalarValue::Bool(false),
                    }
                }
                StringFunc::MapKeys | StringFunc::MapValues => {
                    let Some(val) = args.first().map(|a| eval_col_expr(row, a)) else {
                        return ScalarValue::Null;
                    };
                    let doc = match val {
                        ScalarValue::Json(v) => Some(v),
                        ScalarValue::Utf8(s) => serde_json::from_str::<serde_json::Value>(&s).ok(),
                        _ => None,
                    };
                    match doc {
                        Some(serde_json::Value::Object(obj)) => {
                            let out: Vec<serde_json::Value> = if matches!(func, StringFunc::MapKeys) {
                                obj.keys().map(|k| serde_json::Value::String(k.clone())).collect()
                            } else {
                                obj.values().cloned().collect()
                            };
                            ScalarValue::Json(serde_json::Value::Array(out))
                        }
                        _ => ScalarValue::Null,
                    }
                }
                StringFunc::ElementAt => {
                    let Some(val) = args.first().map(|a| eval_col_expr(row, a)) else {
                        return ScalarValue::Null;
                    };
                    let Some(idx_sv) = args.get(1).map(|a| eval_col_expr(row, a)) else {
                        return ScalarValue::Null;
                    };
                    let doc = match val {
                        ScalarValue::Json(v) => Some(v),
                        ScalarValue::Utf8(s) => serde_json::from_str::<serde_json::Value>(&s).ok(),
                        _ => None,
                    };
                    let picked = match doc {
                        Some(serde_json::Value::Array(arr)) => {
                            let idx = idx_sv.as_f64().map(|f| f as usize).unwrap_or(0);
                            if idx == 0 {
                                None
                            } else {
                                arr.get(idx - 1).cloned()
                            }
                        }
                        Some(serde_json::Value::Object(obj)) => {
                            idx_sv.to_pg_text().and_then(|k| obj.get(&k).cloned())
                        }
                        _ => None,
                    };
                    match picked {
                        Some(serde_json::Value::String(s)) => ScalarValue::Utf8(s),
                        Some(serde_json::Value::Number(n)) => n
                            .as_i64()
                            .map(ScalarValue::Int64)
                            .or_else(|| n.as_f64().map(ScalarValue::Float64))
                            .unwrap_or(ScalarValue::Null),
                        Some(serde_json::Value::Bool(b)) => ScalarValue::Bool(b),
                        Some(serde_json::Value::Null) | None => ScalarValue::Null,
                        Some(other) => ScalarValue::Json(other),
                    }
                }
            }
        }
        ColumnExpr::MathExpr {
            func,
            arg,
            precision,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            let Some(v) = eval_col_expr(row, arg).as_f64() else {
                return ScalarValue::Null;
            };
            let res = match func {
                MathFunc::Abs => v.abs(),
                MathFunc::Round => {
                    let p = precision.unwrap_or(0);
                    if p == 0 {
                        v.round()
                    } else {
                        let factor = 10f64.powi(p);
                        (v * factor).round() / factor
                    }
                }
            };
            ScalarValue::Float64(res)
        }
        ColumnExpr::Cast {
            expr,
            target_type,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            let v = eval_col_expr(row, expr);
            if matches!(v, ScalarValue::Null) {
                return ScalarValue::Null;
            }
            match target_type.to_uppercase().as_str() {
                "INT" | "INTEGER" | "BIGINT" | "INT8" | "INT64" => match v {
                    ScalarValue::Int64(i) => ScalarValue::Int64(i),
                    ScalarValue::Float64(f) => ScalarValue::Int64(f as i64),
                    ScalarValue::Utf8(s) => s
                        .trim()
                        .parse::<i64>()
                        .map(ScalarValue::Int64)
                        .unwrap_or(ScalarValue::Null),
                    ScalarValue::Bool(b) => ScalarValue::Int64(if b { 1 } else { 0 }),
                    _ => ScalarValue::Null,
                },
                "DOUBLE" | "FLOAT" | "FLOAT8" | "NUMERIC" | "DECIMAL" | "REAL" => v
                    .as_f64()
                    .map(ScalarValue::Float64)
                    .unwrap_or(ScalarValue::Null),
                "TEXT" | "VARCHAR" | "STRING" => v
                    .to_pg_text()
                    .map(ScalarValue::Utf8)
                    .unwrap_or(ScalarValue::Null),
                "BOOL" | "BOOLEAN" => match v {
                    ScalarValue::Bool(b) => ScalarValue::Bool(b),
                    ScalarValue::Utf8(s) => {
                        ScalarValue::Bool(s.eq_ignore_ascii_case("true") || s == "t" || s == "1")
                    }
                    ScalarValue::Int64(i) => ScalarValue::Bool(i != 0),
                    _ => ScalarValue::Null,
                },
                _ => v,
            }
        }
        ColumnExpr::Now { alias } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            let secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            ScalarValue::Utf8(format!("{secs}"))
        }
        ColumnExpr::Bm25Score {
            column,
            query,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            let Some(text) = row.get(column).and_then(|v| v.to_pg_text()) else {
                return ScalarValue::Float64(0.0);
            };
            ScalarValue::Float64(compute_bm25_row_score(&text, query))
        }
        ColumnExpr::CaseExpr {
            branches,
            else_expr,
            alias,
        } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            for (pred, res_expr) in branches {
                if eval_filter(row, pred) {
                    return eval_col_expr(row, res_expr);
                }
            }
            eval_col_expr(row, else_expr)
        }
        ColumnExpr::Extract { field, expr, alias } => {
            if let Some(already) = row.get(alias) {
                return already.clone();
            }
            let val = eval_col_expr(row, expr);
            let date_str = match val {
                ScalarValue::Date(d) => rustlakedb_protocol::format_iso_date_days(d),
                ScalarValue::Utf8(s) => s,
                _ => return ScalarValue::Null,
            };
            let date_part = date_str.split(['T', ' ']).next().unwrap_or("");
            let mut parts = date_part.split('-');
            let y = parts.next().and_then(|s| s.parse::<i64>().ok());
            let m = parts.next().and_then(|s| s.parse::<i64>().ok());
            let d = parts.next().and_then(|s| s.parse::<i64>().ok());
            match field.to_uppercase().as_str() {
                "YEAR" => y.map(ScalarValue::Int64).unwrap_or(ScalarValue::Null),
                "MONTH" => m.map(ScalarValue::Int64).unwrap_or(ScalarValue::Null),
                "DAY" => d.map(ScalarValue::Int64).unwrap_or(ScalarValue::Null),
                _ => ScalarValue::Null,
            }
        }
    }
}

/// Computes BM25-style term-frequency relevance score for full-text search (`BM25_SCORE(col, 'query')`, Wave 8).
fn compute_bm25_row_score(doc: &str, query: &str) -> f64 {
    let doc_tokens: Vec<String> = doc
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect();
    if doc_tokens.is_empty() {
        return 0.0;
    }
    let query_terms: Vec<String> = query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect();
    let dl = doc_tokens.len() as f64;
    let avgdl = 20.0f64;
    let k1 = 1.5f64;
    let b = 0.75f64;
    let mut score = 0.0f64;
    for q in &query_terms {
        let tf = doc_tokens.iter().filter(|t| *t == q).count() as f64;
        if tf > 0.0 {
            let idf = 1.5f64; // positive IDF weight per matched query term
            score += idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avgdl)));
        }
    }
    score
}

fn parse_vector_literal(s: &str) -> Option<Vec<f64>> {
    let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .split(',')
        .map(|p| p.trim().parse::<f64>().ok())
        .collect()
}

fn truncate_iso_timestamp(ts: &str, unit: &str) -> String {
    let t = ts.trim();
    match unit.to_lowercase().as_str() {
        "year" if t.len() >= 4 => format!("{}-01-01T00:00:00Z", &t[..4]),
        "month" if t.len() >= 7 => format!("{}-01T00:00:00Z", &t[..7]),
        "day" if t.len() >= 10 => format!("{}T00:00:00Z", &t[..10]),
        "hour" if t.len() >= 13 => format!("{}:00:00Z", &t[..13]),
        "minute" if t.len() >= 16 => format!("{}:00Z", &t[..16]),
        _ => t.to_string(),
    }
}

fn eval_window(mut batch: RowBatch, specs: &[WindowSpec]) -> RowBatch {
    for spec in specs {
        if !batch.columns.contains(&spec.alias) {
            batch.columns.push(spec.alias.clone());
        }
        // Group row indices by partition_by columns.
        let mut parts: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, row) in batch.rows.iter().enumerate() {
            let pkey = if spec.partition_by.is_empty() {
                String::new()
            } else {
                spec.partition_by
                    .iter()
                    .map(|c| {
                        row.get(c)
                            .and_then(|v| v.to_pg_text())
                            .unwrap_or_else(|| "\x00NULL".into())
                    })
                    .collect::<Vec<_>>()
                    .join("\x1f")
            };
            parts.entry(pkey).or_default().push(idx);
        }

        let mut computed: Vec<(usize, ScalarValue)> = Vec::with_capacity(batch.rows.len());
        for (_, mut indices) in parts {
            if !spec.order_by.is_empty() {
                indices.sort_by(|&a, &b| {
                    let ra = &batch.rows[a];
                    let rb = &batch.rows[b];
                    for k in &spec.order_by {
                        let va = ra.get(&k.column).unwrap_or(&ScalarValue::Null);
                        let vb = rb.get(&k.column).unwrap_or(&ScalarValue::Null);
                        let ord = compare_scalar(va, vb);
                        if ord != CmpOrdering::Equal {
                            return if k.descending { ord.reverse() } else { ord };
                        }
                    }
                    CmpOrdering::Equal
                });
            }

            let part_len = indices.len();
            match &spec.func {
                WindowFunc::RowNumber => {
                    for (pos, &row_idx) in indices.iter().enumerate() {
                        computed.push((row_idx, ScalarValue::Int64((pos + 1) as i64)));
                    }
                }
                WindowFunc::Rank => {
                    let mut rank = 1i64;
                    for (pos, &row_idx) in indices.iter().enumerate() {
                        if pos > 0 && !order_keys_equal(&batch.rows[indices[pos - 1]], &batch.rows[row_idx], &spec.order_by) {
                            rank = (pos + 1) as i64;
                        }
                        computed.push((row_idx, ScalarValue::Int64(rank)));
                    }
                }
                WindowFunc::DenseRank => {
                    let mut rank = 1i64;
                    for (pos, &row_idx) in indices.iter().enumerate() {
                        if pos > 0 && !order_keys_equal(&batch.rows[indices[pos - 1]], &batch.rows[row_idx], &spec.order_by) {
                            rank += 1;
                        }
                        computed.push((row_idx, ScalarValue::Int64(rank)));
                    }
                }
                WindowFunc::Sum(col) => {
                    let has_frame = spec.frame_preceding.is_some() || spec.frame_following.is_some();
                    if has_frame {
                        for (pos, &row_idx) in indices.iter().enumerate() {
                            let lo = match spec.frame_preceding {
                                Some(p) => (pos as i64 - p).clamp(0, part_len as i64 - 1) as usize,
                                None => 0,
                            };
                            let hi = match spec.frame_following {
                                Some(f) => (pos as i64 + f).clamp(0, part_len as i64 - 1) as usize,
                                None => part_len - 1,
                            };
                            let sum: f64 = if lo <= hi {
                                indices[lo..=hi]
                                    .iter()
                                    .filter_map(|&i| batch.rows[i].get(col).and_then(|v| v.as_f64()))
                                    .sum()
                            } else {
                                0.0
                            };
                            computed.push((row_idx, ScalarValue::Float64(sum)));
                        }
                    } else if spec.order_by.is_empty() {
                        let total: f64 = indices
                            .iter()
                            .filter_map(|&i| batch.rows[i].get(col).and_then(|v| v.as_f64()))
                            .sum();
                        for &row_idx in &indices {
                            computed.push((row_idx, ScalarValue::Float64(total)));
                        }
                    } else {
                        let mut running = 0.0f64;
                        for &row_idx in &indices {
                            if let Some(v) = batch.rows[row_idx].get(col).and_then(|v| v.as_f64()) {
                                running += v;
                            }
                            computed.push((row_idx, ScalarValue::Float64(running)));
                        }
                    }
                }
                WindowFunc::Avg(col) => {
                    let has_frame = spec.frame_preceding.is_some() || spec.frame_following.is_some();
                    if has_frame {
                        for (pos, &row_idx) in indices.iter().enumerate() {
                            let lo = match spec.frame_preceding {
                                Some(p) => (pos as i64 - p).clamp(0, part_len as i64 - 1) as usize,
                                None => 0,
                            };
                            let hi = match spec.frame_following {
                                Some(f) => (pos as i64 + f).clamp(0, part_len as i64 - 1) as usize,
                                None => part_len - 1,
                            };
                            let mut sum = 0.0;
                            let mut cnt = 0usize;
                            if lo <= hi {
                                for &i in &indices[lo..=hi] {
                                    if let Some(v) = batch.rows[i].get(col).and_then(|v| v.as_f64()) {
                                        sum += v;
                                        cnt += 1;
                                    }
                                }
                            }
                            let val = if cnt > 0 {
                                ScalarValue::Float64(sum / cnt as f64)
                            } else {
                                ScalarValue::Null
                            };
                            computed.push((row_idx, val));
                        }
                    } else if spec.order_by.is_empty() {
                        let mut sum = 0.0;
                        let mut cnt = 0usize;
                        for &i in &indices {
                            if let Some(v) = batch.rows[i].get(col).and_then(|v| v.as_f64()) {
                                sum += v;
                                cnt += 1;
                            }
                        }
                        let avg = if cnt > 0 { sum / cnt as f64 } else { 0.0 };
                        for &row_idx in &indices {
                            computed.push((row_idx, ScalarValue::Float64(avg)));
                        }
                    } else {
                        let mut sum = 0.0;
                        let mut cnt = 0usize;
                        for &row_idx in &indices {
                            if let Some(v) = batch.rows[row_idx].get(col).and_then(|v| v.as_f64()) {
                                sum += v;
                                cnt += 1;
                            }
                            let avg = if cnt > 0 { sum / cnt as f64 } else { 0.0 };
                            computed.push((row_idx, ScalarValue::Float64(avg)));
                        }
                    }
                }
                WindowFunc::Lag(col, offset) => {
                    for (pos, &row_idx) in indices.iter().enumerate() {
                        let val = if pos >= *offset {
                            batch.rows[indices[pos - *offset]]
                                .get(col)
                                .cloned()
                                .unwrap_or(ScalarValue::Null)
                        } else {
                            ScalarValue::Null
                        };
                        computed.push((row_idx, val));
                    }
                }
                WindowFunc::Lead(col, offset) => {
                    for (pos, &row_idx) in indices.iter().enumerate() {
                        let val = if pos + *offset < part_len {
                            batch.rows[indices[pos + *offset]]
                                .get(col)
                                .cloned()
                                .unwrap_or(ScalarValue::Null)
                        } else {
                            ScalarValue::Null
                        };
                        computed.push((row_idx, val));
                    }
                }
            }
        }

        for (row_idx, val) in computed {
            batch.rows[row_idx].insert(spec.alias.clone(), val);
        }
    }
    batch
}

fn order_keys_equal(
    a: &BTreeMap<String, ScalarValue>,
    b: &BTreeMap<String, ScalarValue>,
    keys: &[SortKey],
) -> bool {
    for k in keys {
        let va = a.get(&k.column).unwrap_or(&ScalarValue::Null);
        let vb = b.get(&k.column).unwrap_or(&ScalarValue::Null);
        if compare_scalar(va, vb) != CmpOrdering::Equal {
            return false;
        }
    }
    true
}

pub fn eval_filter(row: &BTreeMap<String, ScalarValue>, expr: &FilterExpr) -> bool {
    match expr {
        FilterExpr::Eq { column, value } => row.get(column).is_some_and(|v| values_eq(v, value)),
        FilterExpr::NotEq { column, value } => {
            row.get(column).is_some_and(|v| !matches!(v, ScalarValue::Null) && !values_eq(v, value))
        }
        FilterExpr::Gt { column, value } => compare_col(row, column, value) == Some(CmpOrdering::Greater),
        FilterExpr::GtEq { column, value } => matches!(
            compare_col(row, column, value),
            Some(CmpOrdering::Greater | CmpOrdering::Equal)
        ),
        FilterExpr::Lt { column, value } => compare_col(row, column, value) == Some(CmpOrdering::Less),
        FilterExpr::LtEq { column, value } => matches!(
            compare_col(row, column, value),
            Some(CmpOrdering::Less | CmpOrdering::Equal)
        ),
        FilterExpr::ColCmp { left, op, right } => {
            let Some(lv) = row.get(left) else {
                return false;
            };
            let Some(rv) = row.get(right) else {
                return false;
            };
            if matches!(lv, ScalarValue::Null) || matches!(rv, ScalarValue::Null) {
                return false;
            }
            match op {
                rustlakedb_protocol::CmpOp::Eq => values_eq(lv, rv),
                rustlakedb_protocol::CmpOp::NotEq => !values_eq(lv, rv),
                rustlakedb_protocol::CmpOp::Gt => compare_scalar(lv, rv) == CmpOrdering::Greater,
                rustlakedb_protocol::CmpOp::GtEq => matches!(
                    compare_scalar(lv, rv),
                    CmpOrdering::Greater | CmpOrdering::Equal
                ),
                rustlakedb_protocol::CmpOp::Lt => compare_scalar(lv, rv) == CmpOrdering::Less,
                rustlakedb_protocol::CmpOp::LtEq => matches!(
                    compare_scalar(lv, rv),
                    CmpOrdering::Less | CmpOrdering::Equal
                ),
            }
        }
        FilterExpr::IsNull { column } => {
            matches!(row.get(column), None | Some(ScalarValue::Null))
        }
        FilterExpr::IsNotNull { column } => {
            !matches!(row.get(column), None | Some(ScalarValue::Null))
        }
        FilterExpr::InList { column, values } => row
            .get(column)
            .is_some_and(|v| values.iter().any(|cand| values_eq(v, cand))),
        FilterExpr::Like { column, pattern } => match row.get(column) {
            Some(ScalarValue::Utf8(s)) => sql_like_match(s, pattern),
            _ => false,
        },
        FilterExpr::JsonPathEq {
            column,
            json_path,
            value,
        } => match row.get(column) {
            Some(ScalarValue::Json(doc)) => {
                let mut cur = doc;
                for seg in json_path.split('.') {
                    match cur.get(seg) {
                        Some(next) => cur = next,
                        None => return false,
                    }
                }
                match cur {
                    serde_json::Value::String(s) => s == value,
                    serde_json::Value::Bool(b) => (if *b { "true" } else { "false" }) == value,
                    other => other.to_string().as_str() == value.as_str(),
                }
            }
            _ => false,
        },
        FilterExpr::JsonContains { column, subset } => match row.get(column) {
            Some(ScalarValue::Json(doc)) => json_contains(doc, subset),
            _ => false,
        },
        FilterExpr::JsonHasKey { column, key } => match row.get(column) {
            Some(ScalarValue::Json(serde_json::Value::Object(map))) => map.contains_key(key),
            _ => false,
        },
        FilterExpr::And(a, b) => eval_filter(row, a) && eval_filter(row, b),
        FilterExpr::Or(a, b) => eval_filter(row, a) || eval_filter(row, b),
        FilterExpr::Not(inner) => !eval_filter(row, inner),
        FilterExpr::LiteralBool(b) => *b,
    }
}

fn values_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int64(x), ScalarValue::Float64(y)) => (*x as f64) == *y,
        (ScalarValue::Float64(x), ScalarValue::Int64(y)) => *x == (*y as f64),
        (ScalarValue::Date(d), ScalarValue::Utf8(s))
        | (ScalarValue::Utf8(s), ScalarValue::Date(d)) => {
            rustlakedb_protocol::parse_iso_date_days(s) == Some(*d)
        }
        (ScalarValue::Decimal(v, s), other) | (other, ScalarValue::Decimal(v, s)) => {
            match other {
                ScalarValue::Decimal(v2, s2) => {
                    if s == s2 {
                        v == v2
                    } else {
                        ScalarValue::Decimal(*v, *s).as_f64() == ScalarValue::Decimal(*v2, *s2).as_f64()
                    }
                }
                ScalarValue::Int64(i) => *v == (*i as i128) * 10i128.pow(*s as u32),
                ScalarValue::Float64(f) => {
                    rustlakedb_protocol::parse_decimal_i128(&format!("{f:.*}", *s as usize), *s)
                        == Some(*v)
                }
                ScalarValue::Utf8(txt) => {
                    rustlakedb_protocol::parse_decimal_i128(txt, *s) == Some(*v)
                }
                _ => false,
            }
        }
        _ => a == b,
    }
}

fn compare_col(
    row: &BTreeMap<String, ScalarValue>,
    column: &str,
    value: &ScalarValue,
) -> Option<CmpOrdering> {
    let v = row.get(column)?;
    if matches!(v, ScalarValue::Null) || matches!(value, ScalarValue::Null) {
        return None;
    }
    Some(compare_scalar(v, value))
}

fn sql_like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; p.len() + 1]; t.len() + 1];
    dp[0][0] = true;
    for j in 1..=p.len() {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=t.len() {
        for j in 1..=p.len() {
            if p[j - 1] == '%' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if p[j - 1] == '_' || p[j - 1] == t[i - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    dp[t.len()][p.len()]
}

fn sort_rows(rows: &mut [BTreeMap<String, ScalarValue>], keys: &[SortKey]) {
    rows.sort_by(|a, b| {
        for k in keys {
            let va = a.get(&k.column).unwrap_or(&ScalarValue::Null);
            let vb = b.get(&k.column).unwrap_or(&ScalarValue::Null);
            let ord = compare_scalar(va, vb);
            if ord != CmpOrdering::Equal {
                return if k.descending { ord.reverse() } else { ord };
            }
        }
        CmpOrdering::Equal
    });
}

fn compare_scalar(a: &ScalarValue, b: &ScalarValue) -> CmpOrdering {
    match (a, b) {
        (ScalarValue::Null, ScalarValue::Null) => CmpOrdering::Equal,
        (ScalarValue::Null, _) => CmpOrdering::Less,
        (_, ScalarValue::Null) => CmpOrdering::Greater,
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x.cmp(y),
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(CmpOrdering::Equal)
        }
        (ScalarValue::Int64(x), ScalarValue::Float64(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(CmpOrdering::Equal)
        }
        (ScalarValue::Float64(x), ScalarValue::Int64(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(CmpOrdering::Equal)
        }
        (ScalarValue::Date(x), ScalarValue::Date(y)) => x.cmp(y),
        (ScalarValue::Date(x), ScalarValue::Utf8(y)) => {
            rustlakedb_protocol::parse_iso_date_days(y)
                .map(|yd| x.cmp(&yd))
                .unwrap_or_else(|| rustlakedb_protocol::format_iso_date_days(*x).cmp(y))
        }
        (ScalarValue::Utf8(x), ScalarValue::Date(y)) => {
            rustlakedb_protocol::parse_iso_date_days(x)
                .map(|xd| xd.cmp(y))
                .unwrap_or_else(|| x.cmp(&rustlakedb_protocol::format_iso_date_days(*y)))
        }
        (ScalarValue::Decimal(v1, s1), ScalarValue::Decimal(v2, s2)) => {
            if s1 == s2 {
                v1.cmp(v2)
            } else {
                let f1 = ScalarValue::Decimal(*v1, *s1).as_f64().unwrap_or(0.0);
                let f2 = ScalarValue::Decimal(*v2, *s2).as_f64().unwrap_or(0.0);
                f1.partial_cmp(&f2).unwrap_or(CmpOrdering::Equal)
            }
        }
        (ScalarValue::Decimal(_, _), other) => {
            let f1 = a.as_f64().unwrap_or(0.0);
            let f2 = other.as_f64().unwrap_or(0.0);
            f1.partial_cmp(&f2).unwrap_or(CmpOrdering::Equal)
        }
        (other, ScalarValue::Decimal(_, _)) => {
            let f1 = other.as_f64().unwrap_or(0.0);
            let f2 = b.as_f64().unwrap_or(0.0);
            f1.partial_cmp(&f2).unwrap_or(CmpOrdering::Equal)
        }
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x.cmp(y),
        (ScalarValue::Bool(x), ScalarValue::Bool(y)) => x.cmp(y),
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

fn eval_hash_aggregate(
    batch: &RowBatch,
    group_by: &[ColumnExpr],
    aggs: &[AggExpr],
    is_partial: bool,
) -> RowBatch {
    #[derive(Default)]
    struct Acc {
        group_vals: Vec<(String, ScalarValue)>,
        count: i64,
        distinct_sets: HashMap<String, HashSet<String>>,
        sums: HashMap<String, f64>,
        counts_for_avg: HashMap<String, i64>,
        mins: HashMap<String, ScalarValue>,
        maxs: HashMap<String, ScalarValue>,
    }

    let mut groups: HashMap<String, Acc> = HashMap::new();
    for row in &batch.rows {
        let gvals: Vec<(String, ScalarValue)> = group_by
            .iter()
            .map(|g| (g.output_name(), eval_col_expr(row, g)))
            .collect();
        let gkey = serde_json::to_string(&gvals).unwrap_or_default();
        let acc = groups.entry(gkey).or_insert_with(|| Acc {
            group_vals: gvals,
            ..Default::default()
        });

        for agg in aggs {
            match &agg.func {
                AggFunc::Count => {
                    if is_partial {
                        acc.count += 1;
                    } else if let Some(ScalarValue::Int64(c)) = row.get(&agg.alias) {
                        acc.count += *c;
                    } else {
                        acc.count += 1;
                    }
                }
                AggFunc::CountDistinct(col) => {
                    let set = acc.distinct_sets.entry(agg.alias.clone()).or_default();
                    if is_partial {
                        if let Some(v) = row.get(col)
                            && !matches!(v, ScalarValue::Null)
                        {
                            set.insert(serde_json::to_string(v).unwrap_or_default());
                        }
                    } else if let Some(ScalarValue::Json(serde_json::Value::Array(arr))) =
                        row.get(&agg.alias)
                    {
                        for item in arr {
                            if let Some(s) = item.as_str() {
                                set.insert(s.to_string());
                            }
                        }
                    } else if let Some(v) = row.get(col)
                        && !matches!(v, ScalarValue::Null)
                    {
                        set.insert(serde_json::to_string(v).unwrap_or_default());
                    }
                }
                AggFunc::Sum(col) => {
                    let src = if is_partial { col } else { &agg.alias };
                    if let Some(v) = row.get(src).and_then(|s| s.as_f64()) {
                        *acc.sums.entry(agg.alias.clone()).or_insert(0.0) += v;
                    }
                }
                AggFunc::Avg(col) => {
                    let sum_key = format!("__avg_sum_{}", agg.alias);
                    let cnt_key = format!("__avg_cnt_{}", agg.alias);
                    if is_partial {
                        if let Some(v) = row.get(col).and_then(|s| s.as_f64()) {
                            *acc.sums.entry(agg.alias.clone()).or_insert(0.0) += v;
                            *acc.counts_for_avg.entry(agg.alias.clone()).or_insert(0) += 1;
                        }
                    } else {
                        let s = row.get(&sum_key).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let c = match row.get(&cnt_key) {
                            Some(ScalarValue::Int64(n)) => *n,
                            _ => 0,
                        };
                        *acc.sums.entry(agg.alias.clone()).or_insert(0.0) += s;
                        *acc.counts_for_avg.entry(agg.alias.clone()).or_insert(0) += c;
                    }
                }
                AggFunc::Min(col) => {
                    let src = if is_partial { col } else { &agg.alias };
                    if let Some(v) = row.get(src)
                        && !matches!(v, ScalarValue::Null) {
                            acc.mins
                                .entry(agg.alias.clone())
                                .and_modify(|cur| {
                                    if compare_scalar(v, cur) == CmpOrdering::Less {
                                        *cur = v.clone();
                                    }
                                })
                                .or_insert_with(|| v.clone());
                        }
                }
                AggFunc::Max(col) => {
                    let src = if is_partial { col } else { &agg.alias };
                    if let Some(v) = row.get(src)
                        && !matches!(v, ScalarValue::Null) {
                            acc.maxs
                                .entry(agg.alias.clone())
                                .and_modify(|cur| {
                                    if compare_scalar(v, cur) == CmpOrdering::Greater {
                                        *cur = v.clone();
                                    }
                                })
                                .or_insert_with(|| v.clone());
                        }
                }
            }
        }
    }

    let mut out_cols: Vec<String> = group_by.iter().map(|g| g.output_name()).collect();
    for agg in aggs {
        if is_partial && matches!(agg.func, AggFunc::Avg(_)) {
            out_cols.push(format!("__avg_sum_{}", agg.alias));
            out_cols.push(format!("__avg_cnt_{}", agg.alias));
        } else {
            out_cols.push(agg.alias.clone());
        }
    }

    // ANSI SQL / PostgreSQL invariant: global aggregate without GROUP BY on 0 rows
    // MUST still produce 1 row (e.g. `SELECT COUNT(*) FROM empty_table` -> `0`)!
    if groups.is_empty() && group_by.is_empty() && !is_partial && !aggs.is_empty() {
        groups.insert(String::new(), Acc::default());
    }

    let mut out_rows = Vec::with_capacity(groups.len());
    for (_, acc) in groups {
        let mut row = BTreeMap::new();
        for (k, v) in acc.group_vals {
            row.insert(k, v);
        }
        for agg in aggs {
            match &agg.func {
                AggFunc::Count => {
                    row.insert(agg.alias.clone(), ScalarValue::Int64(acc.count));
                }
                AggFunc::CountDistinct(_) => {
                    if is_partial {
                        let vals: Vec<serde_json::Value> = acc
                            .distinct_sets
                            .get(&agg.alias)
                            .map(|s| s.iter().cloned().map(serde_json::Value::String).collect())
                            .unwrap_or_default();
                        row.insert(
                            agg.alias.clone(),
                            ScalarValue::Json(serde_json::Value::Array(vals)),
                        );
                    } else {
                        let n = acc
                            .distinct_sets
                            .get(&agg.alias)
                            .map(|s| s.len() as i64)
                            .unwrap_or(0);
                        row.insert(agg.alias.clone(), ScalarValue::Int64(n));
                    }
                }
                AggFunc::Sum(_) => {
                    let s = acc.sums.get(&agg.alias).copied().unwrap_or(0.0);
                    row.insert(agg.alias.clone(), ScalarValue::Float64(s));
                }
                AggFunc::Avg(_) => {
                    let s = acc.sums.get(&agg.alias).copied().unwrap_or(0.0);
                    let c = acc.counts_for_avg.get(&agg.alias).copied().unwrap_or(0);
                    if is_partial {
                        row.insert(format!("__avg_sum_{}", agg.alias), ScalarValue::Float64(s));
                        row.insert(format!("__avg_cnt_{}", agg.alias), ScalarValue::Int64(c));
                    } else {
                        let avg = if c > 0 { s / (c as f64) } else { 0.0 };
                        row.insert(agg.alias.clone(), ScalarValue::Float64(avg));
                    }
                }
                AggFunc::Min(_) => {
                    row.insert(
                        agg.alias.clone(),
                        acc.mins
                            .get(&agg.alias)
                            .cloned()
                            .unwrap_or(ScalarValue::Null),
                    );
                }
                AggFunc::Max(_) => {
                    row.insert(
                        agg.alias.clone(),
                        acc.maxs
                            .get(&agg.alias)
                            .cloned()
                            .unwrap_or(ScalarValue::Null),
                    );
                }
            }
        }
        out_rows.push(row);
    }

    RowBatch::new(out_cols, out_rows)
}

/// Wave 14 SQL `ROLLUP` / `CUBE` / `GROUPING SETS` Final Aggregator:
/// Evaluates each grouping set subset over the pre-aggregated partial rows and emits
/// subtotal & grand-total rows with `ScalarValue::Null` on omitted dimensions!
fn eval_grouping_sets_aggregate(
    batch: &RowBatch,
    all_group_by: &[ColumnExpr],
    aggs: &[AggExpr],
    grouping_sets: &[Vec<String>],
) -> RowBatch {
    let mut out_cols: Vec<String> = all_group_by.iter().map(|g| g.output_name()).collect();
    for agg in aggs {
        out_cols.push(agg.alias.clone());
    }
    let mut out_rows = Vec::new();
    for subset in grouping_sets {
        let sub_group_by: Vec<ColumnExpr> = all_group_by
            .iter()
            .filter(|g| subset.contains(&g.output_name()))
            .cloned()
            .collect();
        let sub_res = eval_hash_aggregate(batch, &sub_group_by, aggs, false);
        for mut row in sub_res.rows {
            for g in all_group_by {
                let name = g.output_name();
                if !subset.contains(&name) {
                    row.insert(name, ScalarValue::Null);
                }
            }
            out_rows.push(row);
        }
    }
    RowBatch::new(out_cols, out_rows)
}

/// Layer 0 Iceberg Hidden-Partition Split Pruning (Wave 14):
/// Checks whether a file's stamped `partition_values` (`region`, `days(ts)`, `bucket(N, col)`, `truncate(W, col)`)
/// guarantees zero rows can satisfy `pred`, allowing O(1) file skipping before reading Parquet!
fn partition_can_skip_file(
    pred: &FilterExpr,
    part_vals: &HashMap<String, String>,
) -> bool {
    if part_vals.is_empty() {
        return false;
    }
    match pred {
        FilterExpr::And(a, b) => {
            partition_can_skip_file(a, part_vals) || partition_can_skip_file(b, part_vals)
        }
        FilterExpr::Or(a, b) => {
            partition_can_skip_file(a, part_vals) && partition_can_skip_file(b, part_vals)
        }
        FilterExpr::Eq { column, value } => {
            let Some(val_text) = value.to_pg_text() else {
                return false;
            };
            for (spec, file_pval) in part_vals {
                let (src_col, pred_pval) =
                    rustlakedb_catalog::eval_partition_transform(spec, &val_text);
                if src_col == *column && &pred_pval != file_pval {
                    return true;
                }
            }
            false
        }
        FilterExpr::InList { column, values } => {
            let pred_texts: Vec<String> = values.iter().filter_map(|v| v.to_pg_text()).collect();
            if pred_texts.is_empty() {
                return false;
            }
            for (spec, file_pval) in part_vals {
                let src_col = rustlakedb_catalog::partition_spec_source_col(spec);
                if src_col == *column {
                    let any_match = pred_texts.iter().any(|vt| {
                        rustlakedb_catalog::eval_partition_transform(spec, vt).1 == *file_pval
                    });
                    if !any_match {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn eval_hash_join(
    left: &RowBatch,
    right: &RowBatch,
    left_key: &str,
    right_key: &str,
    join_type: JoinType,
) -> RowBatch {
    let mut cols = left.columns.clone();
    for c in &right.columns {
        if !cols.contains(c) {
            cols.push(c.clone());
        }
    }

    // CROSS JOIN: Cartesian product (no join keys required)
    if join_type == JoinType::Cross {
        let mut out_rows = Vec::with_capacity(left.rows.len() * right.rows.len());
        for l in &left.rows {
            for r in &right.rows {
                let mut merged = l.clone();
                for (k, v) in r {
                    if matches!(merged.get(k), None | Some(ScalarValue::Null)) {
                        merged.insert(k.clone(), v.clone());
                    }
                }
                out_rows.push(merged);
            }
        }
        return RowBatch::new(cols, out_rows);
    }

    type BuildRowRef<'a> = (usize, &'a BTreeMap<String, ScalarValue>);
    let mut build_map: HashMap<u64, Vec<BuildRowRef<'_>>> = HashMap::new();
    for (r_idx, r) in right.rows.iter().enumerate() {
        if let Some(k) = r.get(right_key)
            && !matches!(k, ScalarValue::Null)
        {
            build_map.entry(k.hash_key()).or_default().push((r_idx, r));
        }
    }

    let mut right_matched = vec![false; right.rows.len()];
    let mut out_rows = Vec::new();
    for l in &left.rows {
        let mut matched = false;
        if let Some(lk) = l.get(left_key)
            && !matches!(lk, ScalarValue::Null)
            && let Some(matches) = build_map.get(&lk.hash_key())
        {
            for &(r_idx, r) in matches {
                if r.get(right_key).is_some_and(|rk| values_eq(rk, lk)) {
                    let mut merged = l.clone();
                    for (k, v) in r {
                        if matches!(merged.get(k), None | Some(ScalarValue::Null)) {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                    out_rows.push(merged);
                    matched = true;
                    right_matched[r_idx] = true;
                }
            }
        }
        if !matched && matches!(join_type, JoinType::Left | JoinType::Full) {
            let mut merged = l.clone();
            for c in &right.columns {
                merged.entry(c.clone()).or_insert(ScalarValue::Null);
            }
            out_rows.push(merged);
        }
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (r_idx, r) in right.rows.iter().enumerate() {
            if !right_matched[r_idx] {
                let mut merged = BTreeMap::new();
                for c in &left.columns {
                    merged.insert(c.clone(), ScalarValue::Null);
                }
                for (k, v) in r {
                    merged.insert(k.clone(), v.clone());
                }
                out_rows.push(merged);
            }
        }
    }

    RowBatch::new(cols, out_rows)
}

enum PuffinPruneResult {
    NoIndex,
    SkipFile,
    CandidateRows(RoaringBitmap),
}

fn zonemap_can_skip_file(
    pred: &FilterExpr,
    cmin: &HashMap<String, String>,
    cmax: &HashMap<String, String>,
) -> bool {
    match pred {
        FilterExpr::And(a, b) => {
            zonemap_can_skip_file(a, cmin, cmax) || zonemap_can_skip_file(b, cmin, cmax)
        }
        FilterExpr::Eq { column, value } => {
            let (Some(mn), Some(mx)) = (cmin.get(column), cmax.get(column)) else {
                return false;
            };
            compare_zm(value, mn) == Some(CmpOrdering::Less)
                || compare_zm(value, mx) == Some(CmpOrdering::Greater)
        }
        FilterExpr::Gt { column, value } => {
            // col > value is impossible when value >= max
            let Some(mx) = cmax.get(column) else {
                return false;
            };
            matches!(
                compare_zm(value, mx),
                Some(CmpOrdering::Greater | CmpOrdering::Equal)
            )
        }
        FilterExpr::GtEq { column, value } => {
            // col >= value is impossible when value > max
            let Some(mx) = cmax.get(column) else {
                return false;
            };
            compare_zm(value, mx) == Some(CmpOrdering::Greater)
        }
        FilterExpr::Lt { column, value } => {
            // col < value is impossible when value <= min
            let Some(mn) = cmin.get(column) else {
                return false;
            };
            matches!(
                compare_zm(value, mn),
                Some(CmpOrdering::Less | CmpOrdering::Equal)
            )
        }
        FilterExpr::LtEq { column, value } => {
            // col <= value is impossible when value < min
            let Some(mn) = cmin.get(column) else {
                return false;
            };
            compare_zm(value, mn) == Some(CmpOrdering::Less)
        }
        _ => false,
    }
}

fn compare_zm(val: &ScalarValue, bound_str: &str) -> Option<CmpOrdering> {
    match val {
        ScalarValue::Int64(v) => bound_str.parse::<i64>().ok().map(|b| v.cmp(&b)),
        ScalarValue::Float64(v) => bound_str
            .parse::<f64>()
            .ok()
            .and_then(|b| v.partial_cmp(&b)),
        ScalarValue::Utf8(s) => Some(s.as_str().cmp(bound_str)),
        _ => None,
    }
}

fn eval_puffin_pruning(puffin: &FilePuffinIndex, pred: &FilterExpr) -> PuffinPruneResult {
    if puffin.bloom_hashes.is_empty() {
        return PuffinPruneResult::NoIndex;
    }
    match pred {
        FilterExpr::JsonPathEq {
            json_path, value, ..
        } => match puffin.lookup_json_eq(json_path, value) {
            Some(bm) => PuffinPruneResult::CandidateRows(bm.clone()),
            None => PuffinPruneResult::SkipFile,
        },
        FilterExpr::JsonHasKey { key, .. } => match puffin.lookup_json_has_key(key) {
            Some(bm) => PuffinPruneResult::CandidateRows(bm.clone()),
            None => PuffinPruneResult::SkipFile,
        },
        FilterExpr::JsonContains { subset, .. } => {
            if let serde_json::Value::Object(map) = subset {
                let mut combined: Option<RoaringBitmap> = None;
                for (k, v) in map {
                    let val_str = match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => return PuffinPruneResult::NoIndex,
                    };
                    match puffin.lookup_json_eq(k, &val_str) {
                        Some(bm) => {
                            combined = Some(match combined {
                                Some(prev) => prev & bm,
                                None => bm.clone(),
                            });
                        }
                        None => return PuffinPruneResult::SkipFile,
                    }
                }
                match combined {
                    Some(bm) if bm.is_empty() => PuffinPruneResult::SkipFile,
                    Some(bm) => PuffinPruneResult::CandidateRows(bm),
                    None => PuffinPruneResult::NoIndex,
                }
            } else {
                PuffinPruneResult::NoIndex
            }
        }
        _ => PuffinPruneResult::NoIndex,
    }
}

fn json_contains(superset: &serde_json::Value, subset: &serde_json::Value) -> bool {
    match (superset, subset) {
        (serde_json::Value::Object(sup), serde_json::Value::Object(sub)) => sub
            .iter()
            .all(|(k, v)| sup.get(k).is_some_and(|sv| json_contains(sv, v))),
        (serde_json::Value::Array(sup), serde_json::Value::Array(sub)) => sub
            .iter()
            .all(|item| sup.iter().any(|s| json_contains(s, item))),
        _ => superset == subset,
    }
}

impl WorkerNode {
    /// MemoryPool-governed Grace Hash Join (RFC-001):
    /// Attempts in-memory hash build first; if the build side exceeds `memory_pool`,
    /// automatically spills BOTH inputs into Zstd-compressed NVMe hash buckets and joins
    /// bucket-by-bucket within bounded RAM!
    async fn eval_hash_join_with_spill(
        &self,
        left: &RowBatch,
        right: &RowBatch,
        left_key: &str,
        right_key: &str,
        join_type: JoinType,
    ) -> Result<RowBatch> {
        let build_bytes = right.estimated_bytes();
        if let Ok(_reservation) = self.memory_pool.try_reserve(build_bytes) {
            return Ok(eval_hash_join(left, right, left_key, right_key, join_type));
        }

        // Memory budget exceeded -> execute Grace Hash Join with NVMe Zstd bucket spilling!
        self.memory_pool.record_spill();
        const NUM_BUCKETS: usize = 8;
        let run_id = uuid::Uuid::new_v4();

        let left_buckets = partition_batch(
            left,
            &Partitioning::Hash {
                columns: vec![left_key.to_string()],
                partitions: NUM_BUCKETS,
            },
        );
        let right_buckets = partition_batch(
            right,
            &Partitioning::Hash {
                columns: vec![right_key.to_string()],
                partitions: NUM_BUCKETS,
            },
        );

        let mut spill_paths = Vec::with_capacity(NUM_BUCKETS * 2);
        for b in 0..NUM_BUCKETS {
            let lp = self.spill_dir.join(format!("grace_{run_id}_l_{b}.zst"));
            let rp = self.spill_dir.join(format!("grace_{run_id}_r_{b}.zst"));
            let l_comp = zstd::bulk::compress(&serde_json::to_vec(&left_buckets[b])?, 1)
                .map_err(|e| RustLakeError::Execution(format!("spill compress left: {e}")))?;
            let r_comp = zstd::bulk::compress(&serde_json::to_vec(&right_buckets[b])?, 1)
                .map_err(|e| RustLakeError::Execution(format!("spill compress right: {e}")))?;
            tokio::fs::write(&lp, l_comp).await?;
            tokio::fs::write(&rp, r_comp).await?;
            spill_paths.push((lp, rp));
        }

        let mut cols = left.columns.clone();
        for c in &right.columns {
            if !cols.contains(c) {
                cols.push(c.clone());
            }
        }
        let mut out_rows = Vec::new();
        for (lp, rp) in spill_paths {
            let l_raw = zstd::stream::decode_all(&tokio::fs::read(&lp).await?[..])
                .map_err(|e| RustLakeError::Execution(format!("spill read left: {e}")))?;
            let r_raw = zstd::stream::decode_all(&tokio::fs::read(&rp).await?[..])
                .map_err(|e| RustLakeError::Execution(format!("spill read right: {e}")))?;
            let lb: RowBatch = serde_json::from_slice(&l_raw)?;
            let rb: RowBatch = serde_json::from_slice(&r_raw)?;
            let joined = eval_hash_join(&lb, &rb, left_key, right_key, join_type);
            out_rows.extend(joined.rows);
            let _ = tokio::fs::remove_file(lp).await;
            let _ = tokio::fs::remove_file(rp).await;
        }

        Ok(RowBatch::new(cols, out_rows))
    }

    /// Wave 12 MemoryPool-governed External Merge-Sort with NVMe Zstd run spilling!
    async fn eval_sort_with_spill(
        &self,
        mut batch: RowBatch,
        keys: &[SortKey],
    ) -> Result<RowBatch> {
        let est = batch.estimated_bytes();
        if let Ok(_reservation) = self.memory_pool.try_reserve(est) {
            sort_rows(&mut batch.rows, keys);
            return Ok(batch);
        }

        // Exceeded worker memory budget -> spill sorted Zstd chunks to NVMe and k-way merge!
        self.memory_pool.record_spill();
        let run_id = uuid::Uuid::new_v4();
        let chunk_size = std::cmp::max(1, batch.rows.len() / 4);
        let mut run_paths = Vec::new();
        let cols = batch.columns.clone();

        for (idx, chunk) in batch.rows.chunks(chunk_size).enumerate() {
            let mut run_rows = chunk.to_vec();
            sort_rows(&mut run_rows, keys);
            let p = self.spill_dir.join(format!("sort_{run_id}_{idx}.zst"));
            let comp = zstd::bulk::compress(&serde_json::to_vec(&run_rows)?, 1)
                .map_err(|e| RustLakeError::Execution(format!("sort spill compress: {e}")))?;
            tokio::fs::write(&p, comp).await?;
            run_paths.push(p);
        }

        let mut merged_runs: Vec<std::collections::VecDeque<BTreeMap<String, ScalarValue>>> =
            Vec::with_capacity(run_paths.len());
        for p in run_paths {
            let raw = zstd::stream::decode_all(&tokio::fs::read(&p).await?[..])
                .map_err(|e| RustLakeError::Execution(format!("sort spill read: {e}")))?;
            let rows: Vec<BTreeMap<String, ScalarValue>> = serde_json::from_slice(&raw)?;
            merged_runs.push(rows.into());
            let _ = tokio::fs::remove_file(p).await;
        }

        // K-way merge over sorted runs
        let mut out_rows = Vec::with_capacity(batch.rows.len());
        loop {
            let mut best_run: Option<usize> = None;
            for (ri, q) in merged_runs.iter().enumerate() {
                let Some(cand) = q.front() else {
                    continue;
                };
                match best_run {
                    None => best_run = Some(ri),
                    Some(cur_best) => {
                        let cur_row = merged_runs[cur_best].front().unwrap();
                        let mut ord = CmpOrdering::Equal;
                        for k in keys {
                            let va = cand.get(&k.column).unwrap_or(&ScalarValue::Null);
                            let vb = cur_row.get(&k.column).unwrap_or(&ScalarValue::Null);
                            let c = compare_scalar(va, vb);
                            if c != CmpOrdering::Equal {
                                ord = if k.descending { c.reverse() } else { c };
                                break;
                            }
                        }
                        if ord == CmpOrdering::Less {
                            best_run = Some(ri);
                        }
                    }
                }
            }
            match best_run {
                Some(ri) => {
                    if let Some(row) = merged_runs[ri].pop_front() {
                        out_rows.push(row);
                    }
                }
                None => break,
            }
        }

        Ok(RowBatch::new(cols, out_rows))
    }

    /// Wave 12 MemoryPool-governed External Hash Aggregate with NVMe Zstd partition spilling!
    async fn eval_hash_aggregate_with_spill(
        &self,
        batch: &RowBatch,
        group_by: &[ColumnExpr],
        aggs: &[AggExpr],
        is_partial: bool,
    ) -> Result<RowBatch> {
        let est = batch.estimated_bytes();
        if group_by.is_empty() || self.memory_pool.try_reserve(est).is_ok() {
            return Ok(eval_hash_aggregate(batch, group_by, aggs, is_partial));
        }

        // Exceeded memory budget -> hash-partition into NVMe Zstd buckets and aggregate bucket-by-bucket!
        self.memory_pool.record_spill();
        const NUM_BUCKETS: usize = 8;
        let run_id = uuid::Uuid::new_v4();
        let g_cols: Vec<String> = group_by.iter().map(|g| g.output_name()).collect();
        let buckets = partition_batch(
            batch,
            &Partitioning::Hash {
                columns: g_cols,
                partitions: NUM_BUCKETS,
            },
        );

        let mut paths = Vec::with_capacity(NUM_BUCKETS);
        for (idx, b) in buckets.into_iter().enumerate() {
            let p = self.spill_dir.join(format!("agg_{run_id}_{idx}.zst"));
            let comp = zstd::bulk::compress(&serde_json::to_vec(&b)?, 1)
                .map_err(|e| RustLakeError::Execution(format!("agg spill compress: {e}")))?;
            tokio::fs::write(&p, comp).await?;
            paths.push(p);
        }

        let mut out_cols = Vec::new();
        let mut out_rows = Vec::new();
        for p in paths {
            let raw = zstd::stream::decode_all(&tokio::fs::read(&p).await?[..])
                .map_err(|e| RustLakeError::Execution(format!("agg spill read: {e}")))?;
            let b: RowBatch = serde_json::from_slice(&raw)?;
            if b.num_rows() > 0 {
                let agg_b = eval_hash_aggregate(&b, group_by, aggs, is_partial);
                if out_cols.is_empty() {
                    out_cols = agg_b.columns;
                }
                out_rows.extend(agg_b.rows);
            }
            let _ = tokio::fs::remove_file(p).await;
        }
        Ok(RowBatch::new(out_cols, out_rows))
    }

    /// Wave 12 Vectorized Arrow Filtered Scan: evaluates simple column predicates directly over
    /// Arrow `RecordBatch` slices with `ProjectionMask` and materializes ONLY matching rows!
    /// Reads matching upstream `ARW1` shuffle blocks directly into a concatenated Arrow
    /// `RecordBatch` without intermediate `RowBatch` row-map materialization (Wave 30).
    async fn try_read_shuffle_arrow(
        &self,
        upstream_stage_id: u32,
        output_partitioning: &Partitioning,
        task: &TaskSpec,
    ) -> Result<Option<RecordBatch>> {
        let matching: Vec<&ShuffleBlockLocation> = task
            .upstream_blocks
            .iter()
            .filter(|b| {
                b.key.stage_id == upstream_stage_id
                    && (matches!(output_partitioning, Partitioning::Single)
                        || if !task.assigned_reduce_partitions.is_empty() {
                            task.assigned_reduce_partitions
                                .contains(&b.key.reduce_partition)
                        } else {
                            b.key.reduce_partition == task.attempt_id.partition_id
                        })
            })
            .collect();
        if matching.is_empty() {
            return Ok(None);
        }
        let mut rbs = Vec::with_capacity(matching.len());
        for loc in matching {
            if loc.row_count == 0 {
                continue;
            }
            match self.shuffle.read_record_batch(loc).await? {
                Some(rb) if rb.num_rows() > 0 => {
                    self.arrow_shuffle_batches_read
                        .fetch_add(1, Ordering::Relaxed);
                    rbs.push(rb);
                }
                Some(_) => {}
                None => return Ok(None),
            }
        }
        if rbs.is_empty() {
            return Ok(None);
        }
        let first_schema = rbs[0].schema();
        if !rbs.iter().all(|b| b.schema() == first_schema) {
            return Ok(None);
        }
        Ok(Some(columnar::concat_record_batches(&rbs)?))
    }

    async fn try_eval_vectorized_filtered_scan(
        &self,
        file_path: &str,
        schema: &[rustlakedb_catalog::FieldSchema],
        projection: &[String],
        pred: &FilterExpr,
    ) -> Result<Option<Vec<BTreeMap<String, ScalarValue>>>> {
        let mut filter_cols = HashSet::new();
        if !collect_simple_arrow_filter_cols(pred, &mut filter_cols) {
            return Ok(None);
        }
        let schema_types: HashMap<&str, &rustlakedb_catalog::ColumnType> = schema
            .iter()
            .map(|f| (f.name.as_str(), &f.data_type))
            .collect();
        let mut proj_cols: Vec<String> = if projection.is_empty() {
            schema.iter().map(|f| f.name.clone()).collect()
        } else {
            projection
                .iter()
                .filter(|c| schema_types.contains_key(c.as_str()))
                .cloned()
                .collect()
        };
        for fc in &filter_cols {
            if !schema_types.contains_key(fc.as_str()) {
                return Ok(None);
            }
            if !proj_cols.contains(fc) {
                proj_cols.push(fc.clone());
            }
        }

        let (batches, dv) = self
            .store
            .read_arrow_batches(file_path, Some(&proj_cols))
            .await?;
        let has_dv = !dv.is_empty();
        let mut global_row_id = 0u32;
        let mut rows_scanned = 0u64;
        let mut out_rows = Vec::new();

        for rb in &batches {
            let n = rb.num_rows();
            for row_idx in 0..n {
                let cur_id = global_row_id;
                global_row_id += 1;
                if has_dv && dv.contains(cur_id) {
                    self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                rows_scanned += 1;
                if !eval_arrow_filter_row(rb, row_idx, pred) {
                    continue;
                }
                let mut row = BTreeMap::new();
                for (ci, field) in rb.schema().fields().iter().enumerate() {
                    let arr = rb.column(ci);
                    let val = extract_arrow_cell(
                        arr.as_ref(),
                        row_idx,
                        schema_types.get(field.name().as_str()).copied(),
                    );
                    row.insert(field.name().clone(), val);
                }
                out_rows.push(row);
            }
        }

        self.vectorized_rows_processed
            .fetch_add(rows_scanned, Ordering::Relaxed);
        Ok(Some(out_rows))
    }

    /// Wave 14 Vectorized Arrow Scan+Project Fast Path:
    /// Projects columns directly from columnar Arrow `RecordBatch`es with Parquet `ProjectionMask`,
    /// avoiding full-row deserialization of unprojected columns!
    async fn try_eval_vectorized_scan_project(
        &self,
        table_name: &str,
        exprs: &[ColumnExpr],
        task: &TaskSpec,
    ) -> Result<Option<RowBatch>> {
        if task.assigned_splits.is_empty() || exprs.is_empty() {
            return Ok(None);
        }
        // Fast path handles pure column projections and simple expressions over existing columns
        let meta = self.catalog.get_table(table_name).await?;
        let schema_types: HashMap<&str, &rustlakedb_catalog::ColumnType> = meta
            .schema
            .iter()
            .map(|f| (f.name.as_str(), &f.data_type))
            .collect();
        let mut needed: Vec<String> = Vec::new();
        for e in exprs {
            if let ColumnExpr::Column(c) = e {
                if !schema_types.contains_key(c.as_str()) {
                    return Ok(None);
                }
                if !needed.contains(c) {
                    needed.push(c.clone());
                }
            } else {
                return Ok(None);
            }
        }
        let out_cols: Vec<String> = exprs.iter().map(|e| e.output_name()).collect();
        let mut out_rows = Vec::new();
        let mut rows_scanned = 0u64;

        for split in &task.assigned_splits {
            let (batches, dv) = self
                .store
                .read_arrow_batches(&split.file_path, Some(&needed))
                .await?;
            let has_dv = !dv.is_empty();
            let mut global_row_id = 0u32;

            for rb in &batches {
                let n = rb.num_rows();
                for row_idx in 0..n {
                    let cur_id = global_row_id;
                    global_row_id += 1;
                    if has_dv && dv.contains(cur_id) {
                        self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    rows_scanned += 1;
                    let mut row = BTreeMap::new();
                    for (ci, field) in rb.schema().fields().iter().enumerate() {
                        let arr = rb.column(ci);
                        let val = extract_arrow_cell(
                            arr.as_ref(),
                            row_idx,
                            schema_types.get(field.name().as_str()).copied(),
                        );
                        row.insert(field.name().clone(), val);
                    }
                    out_rows.push(row);
                }
            }
        }

        self.vectorized_rows_processed
            .fetch_add(rows_scanned, Ordering::Relaxed);
        Ok(Some(RowBatch::new(out_cols, out_rows)))
    }

    /// Wave 1 Vectorized Arrow Partial-Aggregate & Shredded-JSON Zero-Parse Fast Path:
    /// Scans Parquet splits directly as columnar Arrow `RecordBatch`es with `ProjectionMask`,
    /// resolves `JsonPath` expressions directly to shredded `__shred_<col>_<path>` columns
    /// AND Iceberg `field_id` column aliases zero-copy, evaluates pushed `FilterExpr` via
    /// vectorized `BooleanArray` masks + Zone-Map/Partition split pruning, and aggregates
    /// in-place without allocating per-row `BTreeMap`s (Waves 12–25).
    async fn try_eval_vectorized_scan_partial_agg(
        &self,
        table_name: &str,
        pushed_filter: Option<&FilterExpr>,
        group_by: &[ColumnExpr],
        aggs: &[AggExpr],
        task: &TaskSpec,
    ) -> Result<Option<RowBatch>> {
        if task.assigned_splits.is_empty() {
            return Ok(None);
        }

        // Validate that any pushed filter only references simple Arrow-evaluable columns.
        let mut filter_logical_cols = HashSet::new();
        if let Some(pred) = pushed_filter
            && !collect_simple_arrow_filter_cols(pred, &mut filter_logical_cols)
        {
            return Ok(None);
        }

        let meta_opt = self.catalog.get_table(table_name).await.ok();
        if meta_opt
            .as_ref()
            .and_then(|m| m.snapshots.values().next_back())
            .map(|s| !s.equality_deletes.is_empty())
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let empty_aliases = HashMap::new();
        let aliases = meta_opt
            .as_ref()
            .map(|m| &m.column_aliases)
            .unwrap_or(&empty_aliases);
        let file_stats: HashMap<String, rustlakedb_catalog::DataFileEntry> = meta_opt
            .as_ref()
            .map(|m| {
                m.snapshots
                    .values()
                    .flat_map(|s| s.data_files.iter())
                    .map(|df| (df.file_path.clone(), df.clone()))
                    .collect()
            })
            .unwrap_or_default();

        struct SplitAggPlan {
            file_path: String,
            phys_group_cols: Vec<String>,
            phys_agg_cols: Vec<Option<String>>,
            phys_filter: Option<FilterExpr>,
            proj: Vec<String>,
        }

        // Resolve physical Parquet column names per split from cached Parquet footers (zero-copy Field-ID aliasing!).
        let mut split_plans: Vec<SplitAggPlan> = Vec::with_capacity(task.assigned_splits.len());

        for split in &task.assigned_splits {
            // Check Layer 0 Hidden Partition + Layer 1 Zone-Map pruning BEFORE touching Parquet!
            if let Some(pred) = pushed_filter
                && let Some(df) = file_stats.get(&split.file_path)
            {
                if partition_can_skip_file(pred, &df.partition_values) {
                    self.store
                        .files_pruned_by_partition
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if zonemap_can_skip_file(pred, &df.column_min, &df.column_max) {
                    self.store
                        .files_pruned_by_zonemap
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }

            let file_cols = self.store.parquet_column_names(&split.file_path).await?;
            let col_set: HashSet<&str> = file_cols.iter().map(|s| s.as_str()).collect();

            let mut phys_group_cols = Vec::with_capacity(group_by.len());
            let mut needed = HashSet::new();

            for g in group_by {
                match g {
                    ColumnExpr::Column(c) => {
                        let Some(phys) = resolve_physical_col(c, &col_set, aliases) else {
                            return Ok(None);
                        };
                        phys_group_cols.push(phys.clone());
                        needed.insert(phys);
                    }
                    ColumnExpr::JsonPath { column, path, .. } => {
                        let shred = format!("__shred_{column}_{path}");
                        if !col_set.contains(shred.as_str()) {
                            return Ok(None);
                        }
                        phys_group_cols.push(shred.clone());
                        needed.insert(shred);
                    }
                    _ => return Ok(None),
                }
            }

            let mut phys_agg_cols: Vec<Option<String>> = Vec::with_capacity(aggs.len());
            for a in aggs {
                match &a.func {
                    AggFunc::Count => phys_agg_cols.push(None),
                    AggFunc::CountDistinct(_) => return Ok(None),
                    AggFunc::Sum(c)
                    | AggFunc::Avg(c)
                    | AggFunc::Min(c)
                    | AggFunc::Max(c) => {
                        let Some(phys) = resolve_physical_col(c, &col_set, aliases) else {
                            return Ok(None);
                        };
                        phys_agg_cols.push(Some(phys.clone()));
                        needed.insert(phys);
                    }
                }
            }

            let phys_filter = match pushed_filter {
                Some(pred) => {
                    let Some(rewritten) =
                        rewrite_filter_for_physical_cols(pred, &col_set, aliases, &mut needed)
                    else {
                        return Ok(None);
                    };
                    Some(rewritten)
                }
                None => None,
            };

            let mut proj: Vec<String> = needed.into_iter().collect();
            proj.sort();
            split_plans.push(SplitAggPlan {
                file_path: split.file_path.clone(),
                phys_group_cols,
                phys_agg_cols,
                phys_filter,
                proj,
            });
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum VecGroupKey {
            Null,
            Int(i64),
            FloatBits(u64),
            Bool(bool),
            Str(String),
        }

        impl VecGroupKey {
            fn to_scalar(&self) -> ScalarValue {
                match self {
                    Self::Null => ScalarValue::Null,
                    Self::Int(v) => ScalarValue::Int64(*v),
                    Self::FloatBits(b) => ScalarValue::Float64(f64::from_bits(*b)),
                    Self::Bool(b) => ScalarValue::Bool(*b),
                    Self::Str(s) => ScalarValue::Utf8(s.clone()),
                }
            }
        }

        #[derive(Clone, Default)]
        struct VecAggAcc {
            count: i64,
            sum: f64,
            min_val: Option<ScalarValue>,
            max_val: Option<ScalarValue>,
        }

        enum ColRef<'a> {
            None,
            Int64(&'a Int64Array),
            Float64(&'a Float64Array),
            Utf8(&'a StringArray),
            Bool(&'a BooleanArray),
        }

        fn resolve_col<'a>(rb: &'a RecordBatch, name: &str) -> Result<ColRef<'a>> {
            let idx = rb
                .schema()
                .index_of(name)
                .map_err(|e| RustLakeError::Execution(format!("arrow col '{name}': {e}")))?;
            let arr = rb.column(idx);
            if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                return Ok(ColRef::Int64(a));
            }
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                return Ok(ColRef::Float64(a));
            }
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                return Ok(ColRef::Utf8(a));
            }
            if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
                return Ok(ColRef::Bool(a));
            }
            Ok(ColRef::None)
        }

        #[inline]
        fn read_f64(col: &ColRef<'_>, row: usize) -> Option<f64> {
            match col {
                ColRef::Float64(a) if !a.is_null(row) => Some(a.value(row)),
                ColRef::Int64(a) if !a.is_null(row) => Some(a.value(row) as f64),
                _ => None,
            }
        }

        #[inline]
        fn read_scalar(col: &ColRef<'_>, row: usize) -> Option<ScalarValue> {
            match col {
                ColRef::Int64(a) if !a.is_null(row) => Some(ScalarValue::Int64(a.value(row))),
                ColRef::Float64(a) if !a.is_null(row) => Some(ScalarValue::Float64(a.value(row))),
                ColRef::Utf8(a) if !a.is_null(row) => {
                    Some(ScalarValue::Utf8(a.value(row).to_string()))
                }
                ColRef::Bool(a) if !a.is_null(row) => Some(ScalarValue::Bool(a.value(row))),
                _ => None,
            }
        }

        #[inline]
        fn read_group_key(col: &ColRef<'_>, row: usize) -> VecGroupKey {
            match col {
                ColRef::Int64(a) if !a.is_null(row) => VecGroupKey::Int(a.value(row)),
                ColRef::Float64(a) if !a.is_null(row) => {
                    VecGroupKey::FloatBits(a.value(row).to_bits())
                }
                ColRef::Bool(a) if !a.is_null(row) => VecGroupKey::Bool(a.value(row)),
                ColRef::Utf8(a) if !a.is_null(row) => VecGroupKey::Str(a.value(row).to_string()),
                _ => VecGroupKey::Null,
            }
        }

        #[inline]
        fn update_accs(
            accs: &mut [VecAggAcc],
            aggs: &[AggExpr],
            agg_cols: &[ColRef<'_>],
            row: usize,
        ) {
            for ((acc, agg), col) in accs.iter_mut().zip(aggs.iter()).zip(agg_cols.iter()) {
                match &agg.func {
                    AggFunc::Count => acc.count += 1,
                    AggFunc::Sum(_) => {
                        if let Some(v) = read_f64(col, row) {
                            acc.sum += v;
                        }
                    }
                    AggFunc::Avg(_) => {
                        if let Some(v) = read_f64(col, row) {
                            acc.sum += v;
                            acc.count += 1;
                        }
                    }
                    AggFunc::Min(_) => {
                        if let Some(v) = read_scalar(col, row) {
                            match &mut acc.min_val {
                                Some(cur) if compare_scalar(&v, cur) == CmpOrdering::Less => {
                                    *cur = v;
                                }
                                None => acc.min_val = Some(v),
                                _ => {}
                            }
                        }
                    }
                    AggFunc::Max(_) => {
                        if let Some(v) = read_scalar(col, row) {
                            match &mut acc.max_val {
                                Some(cur) if compare_scalar(&v, cur) == CmpOrdering::Greater => {
                                    *cur = v;
                                }
                                None => acc.max_val = Some(v),
                                _ => {}
                            }
                        }
                    }
                    AggFunc::CountDistinct(_) => {}
                }
            }
        }

        // Single UTF-8 string group-key fast map (zero String allocation on repeated keys!).
        let mut str_groups: HashMap<String, Vec<VecAggAcc>> = HashMap::new();
        let mut null_str_group: Option<Vec<VecAggAcc>> = None;
        let mut used_str_fast_path = false;
        // Multi-column all-UTF-8 zero-allocation composite key fast map (for 1B-row medallion GROUP BY!).
        let mut multi_str_groups: HashMap<String, (Vec<Option<String>>, Vec<VecAggAcc>)> =
            HashMap::new();
        let mut used_multi_str_fast_path = false;
        // General multi-key / typed-key map.
        let mut gen_groups: HashMap<Vec<VecGroupKey>, Vec<VecAggAcc>> = HashMap::new();
        let mut total_rows_processed = 0u64;

        for SplitAggPlan {
            file_path,
            phys_group_cols,
            phys_agg_cols,
            phys_filter,
            proj,
        } in split_plans
        {
            let (batches, dv) = self
                .store
                .read_arrow_batches(&file_path, Some(&proj))
                .await?;
            let has_dv = !dv.is_empty();
            let mut global_row_id = 0u32;

            for rb in &batches {
                let n = rb.num_rows();
                let filter_mask: Option<BooleanArray> = phys_filter
                    .as_ref()
                    .and_then(|pf| columnar::eval_columnar_filter(rb, pf));
                let group_cols: Vec<ColRef<'_>> = phys_group_cols
                    .iter()
                    .map(|c| resolve_col(rb, c))
                    .collect::<Result<Vec<_>>>()?;
                let agg_cols: Vec<ColRef<'_>> = phys_agg_cols
                    .iter()
                    .map(|opt| match opt {
                        Some(c) => resolve_col(rb, c),
                        None => Ok(ColRef::None),
                    })
                    .collect::<Result<Vec<_>>>()?;

                if group_cols.len() == 1
                    && let ColRef::Utf8(str_arr) = group_cols[0]
                    && gen_groups.is_empty()
                {
                    used_str_fast_path = true;
                    for row in 0..n {
                        let cur_id = global_row_id;
                        global_row_id += 1;
                        if has_dv && dv.contains(cur_id) {
                            self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if let Some(ref mask) = filter_mask {
                            if mask.is_null(row) || !mask.value(row) {
                                continue;
                            }
                        } else if let Some(ref pf) = phys_filter
                            && !eval_arrow_filter_row(rb, row, pf)
                        {
                            continue;
                        }
                        total_rows_processed += 1;
                        if str_arr.is_null(row) {
                            let accs = null_str_group
                                .get_or_insert_with(|| vec![VecAggAcc::default(); aggs.len()]);
                            update_accs(accs, aggs, &agg_cols, row);
                        } else {
                            let key_str = str_arr.value(row);
                            if let Some(accs) = str_groups.get_mut(key_str) {
                                update_accs(accs, aggs, &agg_cols, row);
                            } else {
                                let mut accs = vec![VecAggAcc::default(); aggs.len()];
                                update_accs(&mut accs, aggs, &agg_cols, row);
                                str_groups.insert(key_str.to_string(), accs);
                            }
                        }
                    }
                } else if group_cols.len() > 1
                    && group_cols.iter().all(|c| matches!(c, ColRef::Utf8(_)))
                    && gen_groups.is_empty()
                {
                    // Zero-allocation multi-UTF-8 GROUP BY fast path: reuses a single composite
                    // String buffer per row (0 heap allocations on cache hits across 1B+ rows!).
                    used_multi_str_fast_path = true;
                    let str_arrays: Vec<&StringArray> = group_cols
                        .iter()
                        .map(|c| match c {
                            ColRef::Utf8(a) => *a,
                            _ => unreachable!(),
                        })
                        .collect();
                    let mut key_buf = String::with_capacity(64);
                    for row in 0..n {
                        let cur_id = global_row_id;
                        global_row_id += 1;
                        if has_dv && dv.contains(cur_id) {
                            self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if let Some(ref mask) = filter_mask {
                            if mask.is_null(row) || !mask.value(row) {
                                continue;
                            }
                        } else if let Some(ref pf) = phys_filter
                            && !eval_arrow_filter_row(rb, row, pf)
                        {
                            continue;
                        }
                        total_rows_processed += 1;
                        key_buf.clear();
                        for (idx, sa) in str_arrays.iter().enumerate() {
                            if idx > 0 {
                                key_buf.push('\x1f');
                            }
                            if sa.is_null(row) {
                                key_buf.push('\x00');
                            } else {
                                key_buf.push_str(sa.value(row));
                            }
                        }
                        if let Some((_, accs)) = multi_str_groups.get_mut(key_buf.as_str()) {
                            update_accs(accs, aggs, &agg_cols, row);
                        } else {
                            let mut accs = vec![VecAggAcc::default(); aggs.len()];
                            update_accs(&mut accs, aggs, &agg_cols, row);
                            let parts: Vec<Option<String>> = str_arrays
                                .iter()
                                .map(|sa| {
                                    if sa.is_null(row) {
                                        None
                                    } else {
                                        Some(sa.value(row).to_string())
                                    }
                                })
                                .collect();
                            multi_str_groups.insert(key_buf.clone(), (parts, accs));
                        }
                    }
                } else {
                    // Promote any earlier fast-path maps if schemas varied across splits.
                    if used_str_fast_path {
                        for (k, v) in str_groups.drain() {
                            gen_groups.insert(vec![VecGroupKey::Str(k)], v);
                        }
                        if let Some(v) = null_str_group.take() {
                            gen_groups.insert(vec![VecGroupKey::Null], v);
                        }
                        used_str_fast_path = false;
                    }
                    if used_multi_str_fast_path {
                        for (_, (parts, v)) in multi_str_groups.drain() {
                            let gk = parts
                                .into_iter()
                                .map(|p| p.map(VecGroupKey::Str).unwrap_or(VecGroupKey::Null))
                                .collect();
                            gen_groups.insert(gk, v);
                        }
                        used_multi_str_fast_path = false;
                    }
                    let mut key_buf = Vec::with_capacity(group_cols.len());
                    for row in 0..n {
                        let cur_id = global_row_id;
                        global_row_id += 1;
                        if has_dv && dv.contains(cur_id) {
                            self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if let Some(ref mask) = filter_mask {
                            if mask.is_null(row) || !mask.value(row) {
                                continue;
                            }
                        } else if let Some(ref pf) = phys_filter
                            && !eval_arrow_filter_row(rb, row, pf)
                        {
                            continue;
                        }
                        total_rows_processed += 1;
                        key_buf.clear();
                        for gc in &group_cols {
                            key_buf.push(read_group_key(gc, row));
                        }
                        if let Some(accs) = gen_groups.get_mut(&key_buf) {
                            update_accs(accs, aggs, &agg_cols, row);
                        } else {
                            let mut accs = vec![VecAggAcc::default(); aggs.len()];
                            update_accs(&mut accs, aggs, &agg_cols, row);
                            gen_groups.insert(key_buf.clone(), accs);
                        }
                    }
                }
            }
        }

        self.vectorized_rows_processed
            .fetch_add(total_rows_processed, Ordering::Relaxed);

        let group_names: Vec<String> = group_by.iter().map(|g| g.output_name()).collect();
        let mut out_cols = group_names.clone();
        for agg in aggs {
            if matches!(agg.func, AggFunc::Avg(_)) {
                out_cols.push(format!("__avg_sum_{}", agg.alias));
                out_cols.push(format!("__avg_cnt_{}", agg.alias));
            } else {
                out_cols.push(agg.alias.clone());
            }
        }

        let build_row = |gvals: Vec<(String, ScalarValue)>, accs: Vec<VecAggAcc>| {
            let mut row = BTreeMap::new();
            for (k, v) in gvals {
                row.insert(k, v);
            }
            for (agg, acc) in aggs.iter().zip(accs) {
                match &agg.func {
                    AggFunc::Count => {
                        row.insert(agg.alias.clone(), ScalarValue::Int64(acc.count));
                    }
                    AggFunc::Sum(_) => {
                        row.insert(agg.alias.clone(), ScalarValue::Float64(acc.sum));
                    }
                    AggFunc::Avg(_) => {
                        row.insert(
                            format!("__avg_sum_{}", agg.alias),
                            ScalarValue::Float64(acc.sum),
                        );
                        row.insert(
                            format!("__avg_cnt_{}", agg.alias),
                            ScalarValue::Int64(acc.count),
                        );
                    }
                    AggFunc::Min(_) => {
                        row.insert(
                            agg.alias.clone(),
                            acc.min_val.unwrap_or(ScalarValue::Null),
                        );
                    }
                    AggFunc::Max(_) => {
                        row.insert(
                            agg.alias.clone(),
                            acc.max_val.unwrap_or(ScalarValue::Null),
                        );
                    }
                    AggFunc::CountDistinct(_) => {}
                }
            }
            row
        };

        let mut out_rows = Vec::new();
        if used_str_fast_path {
            let gname = group_names[0].clone();
            for (k, accs) in str_groups {
                out_rows.push(build_row(vec![(gname.clone(), ScalarValue::Utf8(k))], accs));
            }
            if let Some(accs) = null_str_group {
                out_rows.push(build_row(vec![(gname, ScalarValue::Null)], accs));
            }
        } else if used_multi_str_fast_path {
            for (_, (parts, accs)) in multi_str_groups {
                let gvals: Vec<(String, ScalarValue)> = group_names
                    .iter()
                    .zip(parts)
                    .map(|(n, p)| {
                        (
                            n.clone(),
                            p.map(ScalarValue::Utf8).unwrap_or(ScalarValue::Null),
                        )
                    })
                    .collect();
                out_rows.push(build_row(gvals, accs));
            }
        } else {
            for (keys, accs) in gen_groups {
                let gvals: Vec<(String, ScalarValue)> = group_names
                    .iter()
                    .zip(keys.iter())
                    .map(|(n, k)| (n.clone(), k.to_scalar()))
                    .collect();
                out_rows.push(build_row(gvals, accs));
            }
        }

        Ok(Some(RowBatch::new(out_cols, out_rows)))
    }

    /// Wave 5 End-to-End Vectorized Arrow Join + Fused Filter + Partial-Aggregate Fast Path:
    /// Eliminates `BTreeMap` row materialization for 1M–100M row probe scans during distributed
    /// Broadcast/Hash Joins (with optional fused `Filter`), probing a compact typed `HashMap<i64, _>`
    /// build table directly from Arrow `RecordBatch` slices!
    async fn try_eval_vectorized_join_partial_agg(
        &self,
        input: &PhysicalNode,
        group_by: &[ColumnExpr],
        aggs: &[AggExpr],
        task: &TaskSpec,
    ) -> Result<Option<RowBatch>> {
        if task.assigned_splits.is_empty() || group_by.len() != 1 {
            return Ok(None);
        }
        let ColumnExpr::Column(group_col) = &group_by[0] else {
            return Ok(None);
        };

        // Match `HashJoin(IcebergScan, right)` OR `Filter(HashJoin(IcebergScan, right))`.
        let (post_filter, left_scan, right_node, left_key, right_key) = match input {
            PhysicalNode::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type: JoinType::Inner,
                ..
            } => (None, left.as_ref(), right.as_ref(), left_key, right_key),
            PhysicalNode::Filter {
                input: inner,
                predicate,
            } => match inner.as_ref() {
                PhysicalNode::HashJoin {
                    left,
                    right,
                    left_key,
                    right_key,
                    join_type: JoinType::Inner,
                    ..
                } => (Some(predicate), left.as_ref(), right.as_ref(), left_key, right_key),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        let PhysicalNode::IcebergScan {
            table_name: left_table_name,
            pushed_filter: scan_filter,
            ..
        } = left_scan
        else {
            return Ok(None);
        };
        if scan_filter.is_some() {
            return Ok(None);
        }

        // Evaluate the build side (`right_node`, e.g., broadcast ShuffleRead of `big_customers`).
        let build_batch = self.eval_node(right_node, task).await?;
        let build_bytes = build_batch.estimated_bytes();
        let Ok(_reservation) = self.memory_pool.try_reserve(build_bytes) else {
            // Respect strict MemoryPool budget -> fall back to NVMe Grace Hash Join spill!
            return Ok(None);
        };

        // Require the group_by column to live on the build side (string) and agg columns on probe side.
        let build_has_group = build_batch.columns.iter().any(|c| c == group_col);
        if !build_has_group {
            return Ok(None);
        }

        // Collect probe-side columns needed: join key + agg columns + any post_filter columns.
        let mut probe_needed: HashSet<String> = HashSet::new();
        probe_needed.insert(left_key.clone());
        let mut agg_cols_names: Vec<Option<String>> = Vec::with_capacity(aggs.len());
        for a in aggs {
            match &a.func {
                AggFunc::Count => agg_cols_names.push(None),
                AggFunc::CountDistinct(_) => return Ok(None),
                AggFunc::Sum(c) | AggFunc::Avg(c) | AggFunc::Min(c) | AggFunc::Max(c) => {
                    agg_cols_names.push(Some(c.clone()));
                    probe_needed.insert(c.clone());
                }
            }
        }
        if let Some(pred) = post_filter {
            if !collect_simple_arrow_filter_cols(pred, &mut probe_needed) {
                return Ok(None);
            }
            // Ensure all filter columns are on the probe side (not build side).
            for c in &probe_needed {
                if c != left_key && build_batch.columns.iter().any(|bc| bc == c) {
                    return Ok(None);
                }
            }
        }

        // Verify first split has all probe columns and Int64 join key.
        let first_cols = self
            .store
            .parquet_column_names(&task.assigned_splits[0].file_path)
            .await?;
        let first_set: HashSet<&str> = first_cols.iter().map(|s| s.as_str()).collect();
        for c in &probe_needed {
            if !first_set.contains(c.as_str()) {
                return Ok(None);
            }
        }

        // Build compact typed `HashMap<i64, Vec<usize>>` mapping join key -> group-index IDs!
        let mut group_names: Vec<String> = Vec::new();
        let mut group_name_to_id: HashMap<String, usize> = HashMap::new();
        let mut int_build_map: HashMap<i64, Vec<usize>> =
            HashMap::with_capacity(build_batch.num_rows());
        let mut has_int_key = false;

        for r in &build_batch.rows {
            let Some(ScalarValue::Int64(k)) = r.get(right_key) else {
                continue;
            };
            has_int_key = true;
            let g_str = match r.get(group_col) {
                Some(ScalarValue::Utf8(s)) => s.as_str(),
                Some(ScalarValue::Null) | None => continue,
                // Non-string group key (e.g. Int64 o_orderkey): bail out so general HashAggregate preserves exact type!
                _ => return Ok(None),
            };
            let gid = if let Some(&id) = group_name_to_id.get(g_str) {
                id
            } else {
                let id = group_names.len();
                group_names.push(g_str.to_string());
                group_name_to_id.insert(g_str.to_string(), id);
                id
            };
            int_build_map.entry(*k).or_default().push(gid);
        }
        if !has_int_key && !build_batch.rows.is_empty() {
            return Ok(None);
        }

        #[derive(Clone, Default)]
        struct JoinAggAcc {
            count: i64,
            sum: f64,
            min_val: Option<f64>,
            max_val: Option<f64>,
            touched: bool,
        }
        let mut group_accs: Vec<Vec<JoinAggAcc>> =
            vec![vec![JoinAggAcc::default(); aggs.len()]; group_names.len()];

        let mut proj: Vec<String> = probe_needed.into_iter().collect();
        proj.sort();

        // Wave 25 Dynamic Partition Pruning (DPP) / Semi-Join Min-Max Pushdown:
        // Compute build-side join-key [min, max] bounds and skip fact-table Parquet splits
        // whose zone-map range cannot overlap any dimension join key BEFORE opening them!
        let build_key_bounds: Option<(i64, i64)> = if int_build_map.is_empty() {
            None
        } else {
            let mut mn = i64::MAX;
            let mut mx = i64::MIN;
            for &k in int_build_map.keys() {
                if k < mn {
                    mn = k;
                }
                if k > mx {
                    mx = k;
                }
            }
            Some((mn, mx))
        };
        let left_file_stats: HashMap<String, rustlakedb_catalog::DataFileEntry> = self
            .catalog
            .get_table(left_table_name)
            .await
            .ok()
            .map(|m| {
                m.snapshots
                    .values()
                    .flat_map(|s| s.data_files.iter())
                    .map(|df| (df.file_path.clone(), df.clone()))
                    .collect()
            })
            .unwrap_or_default();

        let mut rows_scanned = 0u64;
        for split in &task.assigned_splits {
            if int_build_map.is_empty() {
                self.files_pruned_by_dpp.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if let Some((b_min, b_max)) = build_key_bounds
                && let Some(df) = left_file_stats.get(&split.file_path)
            {
                let f_min = df
                    .column_min
                    .get(left_key)
                    .and_then(|s| s.parse::<i64>().ok());
                let f_max = df
                    .column_max
                    .get(left_key)
                    .and_then(|s| s.parse::<i64>().ok());
                if let (Some(fmin), Some(fmax)) = (f_min, f_max)
                    && (fmax < b_min || fmin > b_max)
                {
                    self.files_pruned_by_dpp.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            let (batches, dv) = self
                .store
                .read_arrow_batches(&split.file_path, Some(&proj))
                .await?;
            let has_dv = !dv.is_empty();
            let mut global_row_id = 0u32;

            for rb in &batches {
                let n = rb.num_rows();
                let key_idx = match rb.schema().index_of(left_key) {
                    Ok(i) => i,
                    Err(_) => return Ok(None),
                };
                let Some(key_arr) = rb.column(key_idx).as_any().downcast_ref::<Int64Array>() else {
                    return Ok(None);
                };

                // Pre-resolve agg numeric columns once per RecordBatch!
                enum NumCol<'a> {
                    None,
                    F64(&'a Float64Array),
                    I64(&'a Int64Array),
                }
                let mut num_cols = Vec::with_capacity(aggs.len());
                for opt_name in &agg_cols_names {
                    match opt_name {
                        None => num_cols.push(NumCol::None),
                        Some(cname) => {
                            let idx = rb.schema().index_of(cname).map_err(|e| {
                                RustLakeError::Execution(format!("join agg col '{cname}': {e}"))
                            })?;
                            let arr = rb.column(idx);
                            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                                num_cols.push(NumCol::F64(a));
                            } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                                num_cols.push(NumCol::I64(a));
                            } else {
                                return Ok(None);
                            }
                        }
                    }
                }

                for row in 0..n {
                    let cur_id = global_row_id;
                    global_row_id += 1;
                    if has_dv && dv.contains(cur_id) {
                        self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    rows_scanned += 1;
                    if key_arr.is_null(row) {
                        continue;
                    }
                    if let Some(pred) = post_filter
                        && !eval_arrow_filter_row(rb, row, pred)
                    {
                        continue;
                    }
                    let k = key_arr.value(row);
                    if let Some(matched_gids) = int_build_map.get(&k) {
                        for &gid in matched_gids {
                            let accs = &mut group_accs[gid];
                            for ((acc, agg), ncol) in
                                accs.iter_mut().zip(aggs.iter()).zip(num_cols.iter())
                            {
                                acc.touched = true;
                                match &agg.func {
                                    AggFunc::Count => acc.count += 1,
                                    AggFunc::Sum(_) => {
                                        if let Some(v) = match ncol {
                                            NumCol::F64(a) if !a.is_null(row) => Some(a.value(row)),
                                            NumCol::I64(a) if !a.is_null(row) => {
                                                Some(a.value(row) as f64)
                                            }
                                            _ => None,
                                        } {
                                            acc.sum += v;
                                        }
                                    }
                                    AggFunc::Avg(_) => {
                                        if let Some(v) = match ncol {
                                            NumCol::F64(a) if !a.is_null(row) => Some(a.value(row)),
                                            NumCol::I64(a) if !a.is_null(row) => {
                                                Some(a.value(row) as f64)
                                            }
                                            _ => None,
                                        } {
                                            acc.sum += v;
                                            acc.count += 1;
                                        }
                                    }
                                    AggFunc::Min(_) => {
                                        if let Some(v) = match ncol {
                                            NumCol::F64(a) if !a.is_null(row) => Some(a.value(row)),
                                            NumCol::I64(a) if !a.is_null(row) => {
                                                Some(a.value(row) as f64)
                                            }
                                            _ => None,
                                        } {
                                            acc.min_val =
                                                Some(acc.min_val.map_or(v, |m| m.min(v)));
                                        }
                                    }
                                    AggFunc::Max(_) => {
                                        if let Some(v) = match ncol {
                                            NumCol::F64(a) if !a.is_null(row) => Some(a.value(row)),
                                            NumCol::I64(a) if !a.is_null(row) => {
                                                Some(a.value(row) as f64)
                                            }
                                            _ => None,
                                        } {
                                            acc.max_val =
                                                Some(acc.max_val.map_or(v, |m| m.max(v)));
                                        }
                                    }
                                    AggFunc::CountDistinct(_) => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        self.vectorized_rows_processed
            .fetch_add(rows_scanned, Ordering::Relaxed);

        let mut out_cols = vec![group_col.clone()];
        for agg in aggs {
            if matches!(agg.func, AggFunc::Avg(_)) {
                out_cols.push(format!("__avg_sum_{}", agg.alias));
                out_cols.push(format!("__avg_cnt_{}", agg.alias));
            } else {
                out_cols.push(agg.alias.clone());
            }
        }

        let mut out_rows = Vec::new();
        for (gname, accs) in group_names.into_iter().zip(group_accs) {
            if !accs.iter().any(|a| a.touched) {
                continue;
            }
            let mut row = BTreeMap::new();
            row.insert(group_col.clone(), ScalarValue::Utf8(gname));
            for (agg, acc) in aggs.iter().zip(accs) {
                match &agg.func {
                    AggFunc::Count => {
                        row.insert(agg.alias.clone(), ScalarValue::Int64(acc.count));
                    }
                    AggFunc::Sum(_) => {
                        row.insert(agg.alias.clone(), ScalarValue::Float64(acc.sum));
                    }
                    AggFunc::Avg(_) => {
                        row.insert(
                            format!("__avg_sum_{}", agg.alias),
                            ScalarValue::Float64(acc.sum),
                        );
                        row.insert(
                            format!("__avg_cnt_{}", agg.alias),
                            ScalarValue::Int64(acc.count),
                        );
                    }
                    AggFunc::Min(_) => {
                        row.insert(
                            agg.alias.clone(),
                            acc.min_val
                                .map(ScalarValue::Float64)
                                .unwrap_or(ScalarValue::Null),
                        );
                    }
                    AggFunc::Max(_) => {
                        row.insert(
                            agg.alias.clone(),
                            acc.max_val
                                .map(ScalarValue::Float64)
                                .unwrap_or(ScalarValue::Null),
                        );
                    }
                    AggFunc::CountDistinct(_) => {}
                }
            }
            out_rows.push(row);
        }

        Ok(Some(RowBatch::new(out_cols, out_rows)))
    }

    /// Wave 9 + Wave 12 Multi-Column Vectorized Arrow Top-N Pushdown Fast Path:
    /// Scans Parquet splits directly as columnar Arrow `RecordBatch`es and maintains a bounded
    /// top-K selection over single- or multi-column `ORDER BY k1, k2, ...` Arrow slices!
    async fn try_eval_vectorized_topn(
        &self,
        table_name: &str,
        projection: &[String],
        keys: &[SortKey],
        k: usize,
        task: &TaskSpec,
    ) -> Result<Option<RowBatch>> {
        if task.assigned_splits.is_empty() || keys.is_empty() || k == 0 {
            return Ok(None);
        }
        let meta = self.catalog.get_table(table_name).await?;
        let schema_types: HashMap<&str, &rustlakedb_catalog::ColumnType> = meta
            .schema
            .iter()
            .map(|f| (f.name.as_str(), &f.data_type))
            .collect();
        // Preserve table/projection column order (never sort alphabetically, or `SELECT *` column
        // positions shift for positional clients like PostgreSQL / DBeaver!).
        let mut proj_cols: Vec<String> = if projection.is_empty() {
            meta.schema.iter().map(|f| f.name.clone()).collect()
        } else {
            projection
                .iter()
                .filter(|c| schema_types.contains_key(c.as_str()))
                .cloned()
                .collect()
        };
        for sk in keys {
            if !proj_cols.contains(&sk.column) {
                if !schema_types.contains_key(sk.column.as_str()) {
                    return Ok(None);
                }
                proj_cols.push(sk.column.clone());
            }
        }

        let cmp_key_tuples = |a: &[ScalarValue], b: &[ScalarValue]| -> CmpOrdering {
            for (idx, sk) in keys.iter().enumerate() {
                let ord = compare_scalar(&a[idx], &b[idx]);
                if ord != CmpOrdering::Equal {
                    return if sk.descending { ord.reverse() } else { ord };
                }
            }
            CmpOrdering::Equal
        };

        let mut top_k: Vec<(Vec<ScalarValue>, BTreeMap<String, ScalarValue>)> =
            Vec::with_capacity(k + 1);
        let mut rows_scanned = 0u64;

        for split in &task.assigned_splits {
            let (batches, dv) = self
                .store
                .read_arrow_batches(&split.file_path, Some(&proj_cols))
                .await?;
            let has_dv = !dv.is_empty();
            let mut global_row_id = 0u32;

            for rb in &batches {
                let mut key_col_indices = Vec::with_capacity(keys.len());
                for sk in keys {
                    let Ok(idx) = rb.schema().index_of(&sk.column) else {
                        return Ok(None);
                    };
                    key_col_indices.push(idx);
                }
                let n = rb.num_rows();

                for row_idx in 0..n {
                    let cur_id = global_row_id;
                    global_row_id += 1;
                    if has_dv && dv.contains(cur_id) {
                        self.store.rows_skipped_by_dv.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    rows_scanned += 1;

                    let mut key_vals = Vec::with_capacity(key_col_indices.len());
                    for &ki in &key_col_indices {
                        let arr = rb.column(ki);
                        let sv = if arr.is_null(row_idx) {
                            ScalarValue::Null
                        } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                            ScalarValue::Float64(a.value(row_idx))
                        } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                            ScalarValue::Int64(a.value(row_idx))
                        } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                            ScalarValue::Utf8(a.value(row_idx).to_string())
                        } else if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
                            ScalarValue::Bool(a.value(row_idx))
                        } else {
                            return Ok(None);
                        };
                        key_vals.push(sv);
                    }

                    if top_k.len() == k
                        && cmp_key_tuples(&key_vals, &top_k[k - 1].0) != CmpOrdering::Less
                    {
                        continue;
                    }

                    // Materialize ONLY this winning candidate row!
                    let mut row = BTreeMap::new();
                    for (ci, field) in rb.schema().fields().iter().enumerate() {
                        let arr = rb.column(ci);
                        let val = if arr.is_null(row_idx) {
                            ScalarValue::Null
                        } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                            ScalarValue::Int64(a.value(row_idx))
                        } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                            ScalarValue::Float64(a.value(row_idx))
                        } else if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
                            ScalarValue::Bool(a.value(row_idx))
                        } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                            let s = a.value(row_idx);
                            if matches!(
                                schema_types.get(field.name().as_str()),
                                Some(rustlakedb_catalog::ColumnType::Variant)
                            ) {
                                serde_json::from_str::<serde_json::Value>(s)
                                    .map(ScalarValue::Json)
                                    .unwrap_or_else(|_| ScalarValue::Utf8(s.to_string()))
                            } else {
                                ScalarValue::Utf8(s.to_string())
                            }
                        } else {
                            ScalarValue::Null
                        };
                        row.insert(field.name().clone(), val);
                    }

                    top_k.push((key_vals, row));
                    top_k.sort_by(|a, b| cmp_key_tuples(&a.0, &b.0));
                    if top_k.len() > k {
                        top_k.truncate(k);
                    }
                }
            }
        }

        self.vectorized_rows_processed
            .fetch_add(rows_scanned, Ordering::Relaxed);

        let rows: Vec<BTreeMap<String, ScalarValue>> = top_k.into_iter().map(|(_, r)| r).collect();
        Ok(Some(RowBatch::new(proj_cols, rows)))
    }
}

fn extract_arrow_cell(
    arr: &dyn Array,
    row_idx: usize,
    logical_type: Option<&rustlakedb_catalog::ColumnType>,
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
            arrow_schema::DataType::Decimal128(_, s) => (*s).max(0) as u8,
            _ => 2,
        };
        return ScalarValue::Decimal(a.value(row_idx), scale);
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let secs = a.value(row_idx).div_euclid(1_000_000);
        let days = secs.div_euclid(86_400) as i32;
        let sod = secs.rem_euclid(86_400);
        return ScalarValue::Utf8(format!(
            "{}T{:02}:{:02}:{:02}Z",
            rustlakedb_protocol::format_iso_date_days(days),
            sod / 3600,
            (sod % 3600) / 60,
            sod % 60
        ));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return ScalarValue::Bool(a.value(row_idx));
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        let s = a.value(row_idx);
        if matches!(logical_type, Some(rustlakedb_catalog::ColumnType::Variant)) {
            return serde_json::from_str::<serde_json::Value>(s)
                .map(ScalarValue::Json)
                .unwrap_or_else(|_| ScalarValue::Utf8(s.to_string()));
        }
        return ScalarValue::Utf8(s.to_string());
    }
    ScalarValue::Null
}

/// Resolves a logical Iceberg column name to its physical Parquet column name in a specific file
/// (zero-copy Field-ID schema evolution alias resolution, Wave 25).
fn resolve_physical_col(
    logical: &str,
    col_set: &HashSet<&str>,
    aliases: &HashMap<String, Vec<String>>,
) -> Option<String> {
    if col_set.contains(logical) {
        return Some(logical.to_string());
    }
    if let Some(hist) = aliases.get(logical) {
        for old in hist.iter().rev() {
            if col_set.contains(old.as_str()) {
                return Some(old.clone());
            }
        }
    }
    None
}

/// Rewrites a simple `FilterExpr` to reference physical Parquet column names for zero-copy
/// post-evolution Arrow scanning (Wave 25).
fn rewrite_filter_for_physical_cols(
    expr: &FilterExpr,
    col_set: &HashSet<&str>,
    aliases: &HashMap<String, Vec<String>>,
    needed: &mut HashSet<String>,
) -> Option<FilterExpr> {
    let mut map_col = |c: &str| -> Option<String> {
        let phys = resolve_physical_col(c, col_set, aliases)?;
        needed.insert(phys.clone());
        Some(phys)
    };
    match expr {
        FilterExpr::Eq { column, value } => Some(FilterExpr::Eq {
            column: map_col(column)?,
            value: value.clone(),
        }),
        FilterExpr::NotEq { column, value } => Some(FilterExpr::NotEq {
            column: map_col(column)?,
            value: value.clone(),
        }),
        FilterExpr::Gt { column, value } => Some(FilterExpr::Gt {
            column: map_col(column)?,
            value: value.clone(),
        }),
        FilterExpr::GtEq { column, value } => Some(FilterExpr::GtEq {
            column: map_col(column)?,
            value: value.clone(),
        }),
        FilterExpr::Lt { column, value } => Some(FilterExpr::Lt {
            column: map_col(column)?,
            value: value.clone(),
        }),
        FilterExpr::LtEq { column, value } => Some(FilterExpr::LtEq {
            column: map_col(column)?,
            value: value.clone(),
        }),
        FilterExpr::InList { column, values } => Some(FilterExpr::InList {
            column: map_col(column)?,
            values: values.clone(),
        }),
        FilterExpr::IsNull { column } => Some(FilterExpr::IsNull {
            column: map_col(column)?,
        }),
        FilterExpr::IsNotNull { column } => Some(FilterExpr::IsNotNull {
            column: map_col(column)?,
        }),
        FilterExpr::And(a, b) => Some(FilterExpr::And(
            Box::new(rewrite_filter_for_physical_cols(a, col_set, aliases, needed)?),
            Box::new(rewrite_filter_for_physical_cols(b, col_set, aliases, needed)?),
        )),
        FilterExpr::Or(a, b) => Some(FilterExpr::Or(
            Box::new(rewrite_filter_for_physical_cols(a, col_set, aliases, needed)?),
            Box::new(rewrite_filter_for_physical_cols(b, col_set, aliases, needed)?),
        )),
        FilterExpr::Not(inner) => Some(FilterExpr::Not(Box::new(
            rewrite_filter_for_physical_cols(inner, col_set, aliases, needed)?,
        ))),
        _ => None,
    }
}

fn collect_simple_arrow_filter_cols(expr: &FilterExpr, out: &mut HashSet<String>) -> bool {
    match expr {
        FilterExpr::Eq { column, .. }
        | FilterExpr::NotEq { column, .. }
        | FilterExpr::Gt { column, .. }
        | FilterExpr::GtEq { column, .. }
        | FilterExpr::Lt { column, .. }
        | FilterExpr::LtEq { column, .. }
        | FilterExpr::InList { column, .. }
        | FilterExpr::IsNull { column }
        | FilterExpr::IsNotNull { column } => {
            out.insert(column.clone());
            true
        }
        FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            collect_simple_arrow_filter_cols(a, out) && collect_simple_arrow_filter_cols(b, out)
        }
        FilterExpr::Not(inner) => collect_simple_arrow_filter_cols(inner, out),
        _ => false,
    }
}

fn eval_arrow_filter_row(rb: &RecordBatch, row: usize, expr: &FilterExpr) -> bool {
    let get_val = |col: &str| -> ScalarValue {
        let Ok(idx) = rb.schema().index_of(col) else {
            return ScalarValue::Null;
        };
        let arr = rb.column(idx);
        if arr.is_null(row) {
            return ScalarValue::Null;
        }
        if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            return ScalarValue::Int64(a.value(row));
        }
        if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            return ScalarValue::Float64(a.value(row));
        }
        if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
            return ScalarValue::Date(a.value(row));
        }
        if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
            let scale = match a.data_type() {
                arrow_schema::DataType::Decimal128(_, s) => (*s).max(0) as u8,
                _ => 2,
            };
            return ScalarValue::Decimal(a.value(row), scale);
        }
        if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            let secs = a.value(row).div_euclid(1_000_000);
            let days = secs.div_euclid(86_400) as i32;
            let sod = secs.rem_euclid(86_400);
            return ScalarValue::Utf8(format!(
                "{}T{:02}:{:02}:{:02}Z",
                rustlakedb_protocol::format_iso_date_days(days),
                sod / 3600,
                (sod % 3600) / 60,
                sod % 60
            ));
        }
        if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
            return ScalarValue::Utf8(a.value(row).to_string());
        }
        if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
            return ScalarValue::Bool(a.value(row));
        }
        ScalarValue::Null
    };

    match expr {
        FilterExpr::And(a, b) => {
            eval_arrow_filter_row(rb, row, a) && eval_arrow_filter_row(rb, row, b)
        }
        FilterExpr::Or(a, b) => {
            eval_arrow_filter_row(rb, row, a) || eval_arrow_filter_row(rb, row, b)
        }
        FilterExpr::Not(i) => !eval_arrow_filter_row(rb, row, i),
        FilterExpr::Eq { column, value } => values_eq(&get_val(column), value),
        FilterExpr::NotEq { column, value } => {
            let v = get_val(column);
            !matches!(v, ScalarValue::Null) && !values_eq(&v, value)
        }
        FilterExpr::Gt { column, value } => {
            let v = get_val(column);
            !matches!(v, ScalarValue::Null) && compare_scalar(&v, value) == CmpOrdering::Greater
        }
        FilterExpr::GtEq { column, value } => {
            let v = get_val(column);
            !matches!(v, ScalarValue::Null)
                && matches!(
                    compare_scalar(&v, value),
                    CmpOrdering::Greater | CmpOrdering::Equal
                )
        }
        FilterExpr::Lt { column, value } => {
            let v = get_val(column);
            !matches!(v, ScalarValue::Null) && compare_scalar(&v, value) == CmpOrdering::Less
        }
        FilterExpr::LtEq { column, value } => {
            let v = get_val(column);
            !matches!(v, ScalarValue::Null)
                && matches!(
                    compare_scalar(&v, value),
                    CmpOrdering::Less | CmpOrdering::Equal
                )
        }
        FilterExpr::IsNull { column } => matches!(get_val(column), ScalarValue::Null),
        FilterExpr::IsNotNull { column } => !matches!(get_val(column), ScalarValue::Null),
        FilterExpr::InList { column, values } => {
            let v = get_val(column);
            values.iter().any(|c| values_eq(&v, c))
        }
        _ => false,
    }
}

fn partition_batch(batch: &RowBatch, partitioning: &Partitioning) -> Vec<RowBatch> {
    let n = partitioning.partition_count();
    let mut buckets: Vec<Vec<BTreeMap<String, ScalarValue>>> = vec![Vec::new(); n];

    match partitioning {
        Partitioning::Single => {
            buckets[0] = batch.rows.clone();
        }
        Partitioning::Hash { columns, .. } => {
            for row in &batch.rows {
                let mut h: u64 = 0;
                for c in columns {
                    let v = row.get(c).cloned().unwrap_or(ScalarValue::Null);
                    h = h.wrapping_mul(31).wrapping_add(v.hash_key());
                }
                let idx = (h as usize) % n;
                buckets[idx].push(row.clone());
            }
        }
        Partitioning::RoundRobin(_) | Partitioning::Unknown(_) => {
            for (i, row) in batch.rows.iter().enumerate() {
                buckets[i % n].push(row.clone());
            }
        }
    }

    buckets
        .into_iter()
        .map(|rows| RowBatch::new(batch.columns.clone(), rows))
        .collect()
}
