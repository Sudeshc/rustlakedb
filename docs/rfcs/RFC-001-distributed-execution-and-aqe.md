# RFC-001: Distributed Physical Plan, Stage DAG, Hybrid Shuffle & AQE

- **Status:** Accepted (Phase 1–2 Specification)
- **Crates:** `rustlakedb-protocol`, `rustlakedb-planner`, `rustlakedb-coordinator`, `rustlakedb-worker`, `rustlakedb-shuffle`

---

## 1. Goals & Non-Goals

### Goals
1. **Distributed-First from Commit 1:** Every physical operator declares partitioning/distribution contracts so the planner inserts `Exchange` boundaries automatically.
2. **Dual-Mode Execution:**
   - **Sub-second Interactive Path:** Pipelined Arrow Flight streaming + adaptive single-worker DAG collapsing when input fits in local cache.
   - **Massive Spark-Scale Path:** Staged fault-tolerant shuffle (NVMe or object-store backed), bounded memory with disk spilling, task-level retry on worker death, and runtime Adaptive Query Execution (AQE).
3. **Zero-Friction Local Testing (`local[N]`):** Run 1 Coordinator + *N* Workers in-process using the exact same stage planner, shuffle traits, and task-attempt state machine as a 100-node Kubernetes cluster.

---

## 2. Physical Distribution Contracts & `EnsureRequirements`

Every operator implements:
- `output_partitioning() -> Partitioning`:
  - `Single`
  - `RoundRobin(usize)`
  - `Hash { exprs: Vec<Expr>, partitions: usize }`
  - `Range { sort_exprs: Vec<SortExpr>, partitions: usize }`
  - `Unknown(usize)`
- `required_input_distribution() -> Vec<Distribution>`:
  - `Unspecified`
  - `SinglePartition`
  - `HashPartitioned(Vec<Expr>)`
  - `Broadcast`

### `EnsureRequirements` Rewrite Pass
Bottom-up pass over the physical plan:
1. For each child input `i`, check if `child.output_partitioning()` satisfies `parent.required_input_distribution()[i]`.
2. If not satisfied, insert an `ExchangeExec` specifying the target `Partitioning` and `ShuffleMode` (`Pipelined` vs. `Staged`).
3. Coalesce back-to-back redundant exchanges and elide exchanges when `EquivalenceProperties` prove the data is already co-partitioned (e.g., Iceberg bucket-partitioned tables joined on bucket key -> **zero-shuffle bucket join**).

---

## 3. Stage DAG Cutting & Locality-Aware Scheduling

The `StagePlanner` cuts the physical plan at every `ExchangeExec` boundary:
- Each cut produces a `QueryStage`:
  - `stage_id: u32`
  - `input_stages: Vec<u32>`
  - `output_partitioning: Partitioning`
  - `shuffle_mode: ShuffleMode`
  - `plan_bytes: Vec<u8>` (serialized physical sub-plan)
  - `tasks: Vec<TaskSpec>` (1 task per Iceberg split group or upstream shuffle partition)

### Locality-Aware Split Assignment (Rendezvous Hashing)
For leaf scan stages over Iceberg Parquet files:
- Each split `(file_path, row_group_id)` is scored against every healthy worker `w` via `xxh3_64(w.worker_id || file_path || row_group_id)`.
- The split is assigned to the highest-scoring worker (with load-cap spillover if a worker exceeds `1.25x` mean cluster load).
- Result: Repeated queries hit warm local NVMe Parquet footers, Puffin indexes, and column pages (>90% cache hit rate), and scaling workers shifts only `1/N` of cached files.

### Adaptive Single-Node Collapsing
Before dispatching a multi-stage DAG, if:
- Estimated total scan bytes `< 128 MB`, AND
- All splits fit within one worker's memory budget,
the Coordinator collapses all stages into a single `local_in_process` stage on the worker owning the majority of cached splits—eliminating all network hops and delivering `<100 ms` latency.

---

## 4. Hybrid Shuffle Protocol (`ShuffleTransport`)

All shuffles go through one trait:
```rust
#[async_trait]
pub trait ShuffleTransport: Send + Sync {
    async fn write_partition(
        &self,
        key: ShufflePartitionKey, // (query_id, stage_id, map_partition, reduce_partition, attempt_id)
        payload: Bytes,
    ) -> Result<ShuffleBlockLocation>;

    async fn read_partition(
        &self,
        location: &ShuffleBlockLocation,
    ) -> Result<Bytes>;

    async fn cleanup_query(&self, query_id: QueryId) -> Result<()>;
}
```

### Mode A: Pipelined Shuffle (Interactive)
- Map tasks stream Arrow `RecordBatch` frames directly to target Reduce tasks over **Arrow Flight `DoPut` / `DoGet`** gRPC streams with bounded backpressure queues.
- Zero disk writes; failure triggers fast whole-query retry.

### Mode B: Staged Shuffle (Spark-Scale Fault Tolerance)
- Map tasks hash-partition rows into `R` buckets, compress with Zstd Arrow IPC, and write to **local NVMe** (or **remote S3/GCS/S3 Express** in disaggregated spot-worker mode).
- Upon task completion, the worker reports a `TaskCompletionReport` containing `Vec<ShuffleBlockLocation>` and per-partition `(row_count, byte_size)` telemetry to the Coordinator.
- Reduce tasks fetch their partition `r` from all completed map locations in parallel over Arrow Flight `DoGet` (or directly from S3 in disaggregated mode).

### Deterministic Task Attempts & Fault Recovery
Every task execution carries `TaskAttemptId { query_id, stage_id, partition_id, attempt: u32 }`:
- **Reduce task crash:** Coordinator increments `attempt` and re-schedules only that single reduce partition against existing map shuffle outputs.
- **Map worker crash (NVMe shuffle lost):** Reduce task reports `FetchFailed(lost_worker_id, map_partition)`; Coordinator invalidates shuffle locations from `lost_worker_id`, re-runs only those map tasks on healthy workers, and resumes the reduce task.
- **Idempotent Iceberg Commits:** Staging Parquet paths embed `(query_id, attempt_id)`. Speculative or retried attempts cannot corrupt committed snapshots because only the winning attempt's file list is included in the Coordinator's atomic Iceberg commit.

---

## 5. Adaptive Query Execution (AQE) Runtime Loop

In Staged mode, the Coordinator pauses briefly at each completed shuffle barrier, aggregates exact runtime `(row_count, byte_size)` across all map tasks for every partition `0..R`, and applies three re-optimization rules before launching downstream stages:

1. **Dynamic Broadcast Join Switch:** If either side of a pending `PartitionedHashJoinExec` materialized `< 64 MB` after filters, rewrite the join into `BroadcastHashJoinExec` and cancel redundant shuffle reads on the probe side.
2. **Automatic Skew Join Splitting:** If any partition `p` has `bytes(p) > max(256 MB, 5 * median_bytes)`, split partition `p`'s map blocks into `K` sub-tasks and replicate the matching build-side partition `p` across all `K` sub-tasks.
3. **Dynamic Partition Coalescing:** Merge contiguous small post-filter partitions whose combined size is `< 64 MB` into single reduce tasks to eliminate empty/tiny task overhead.
