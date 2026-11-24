# RFC-002: Built-in `openraft` Iceberg v3 REST Catalog, Multi-Table ACID & Autonomous Maintainer

- **Status:** Accepted (Phase 1–3 Specification)
- **Crates:** `rustlakedb-catalog`, `rustlakedb-coordinator`, `rustlakedb-storage`

---

## 1. Philosophy: Iceberg Standard Format, Delta-Like Zero-External-Metastore UX

Traditional Iceberg deployments require standing up an external Hive Metastore, AWS Glue, or a separate Polaris/PostgreSQL cluster before running your first query. **RustLakeDB embeds a full, HA, Apache Iceberg v3 REST Catalog directly inside its `openraft` Coordinator quorum**:
- **Zero external dependencies:** Deploy a 3-node coordinator quorum on AWS, GCP, Azure, or air-gapped on-prem—no JVM, no ZooKeeper, no external RDS required.
- **Open ecosystem interoperability:** Exposes the standard Apache Iceberg REST Catalog HTTP API (`/v1/config`, `/v1/namespaces`, `/v1/namespaces/{ns}/tables/{tbl}`, `/v1/transactions/commit`, `/v1/.../indexes`), so Spark, DuckDB, Trino, Flink, and PyIceberg can read/write RustLakeDB tables natively.

---

## 2. Consensus State Machine (`openraft`)

The Raft log stores **strictly lightweight metadata** (never table data bytes):
1. **Namespace & Table Registry:** `NamespaceId -> TableId -> TableMetadataPointer { metadata_location, current_snapshot_id, format_version: 3, refs (branches/tags) }`.
2. **Global Multi-Table Transaction Ledger:** `GlobalTxnId -> { status: Prepared | Committed | Aborted, table_commits: Vec<TableCommitEntry>, commit_ts }`.
3. **Secondary Index & JSON Shredding Registry:** Tracks Puffin index definitions (`Inverted`, `Bloom`, `ThetaSketch`) and evolving JSON shredding schemas per table.
4. **Security & Governance Metadata:** Roles, RBAC grants, Row-Level Security (RLS) predicate expressions, and column/JSON-path masking policies.
5. **Cluster Membership & Worker Leases:** Active workers, NVMe cache capacities, and heartbeats.

Raft snapshots compact periodically to local disk + object storage backup for rapid disaster recovery.

---

## 3. Single-Table & Multi-Table ACID Commit Protocol

### Single-Table Commit (Optimistic Concurrency Control)
1. Workers write immutable Parquet data files, Iceberg v3 Deletion Vectors (Puffin Roaring Bitmaps), and Puffin secondary indexes to object storage under uncommitted paths.
2. Coordinator receives `CommitProposal { table_id, base_snapshot_id, added_files, deleted_files, added_dvs, read_filter_predicate, is_compaction }`.
3. Under the Raft leader's table commit serializer:
   - If `base_snapshot_id == current_snapshot_id`, fast-path commit!
   - If concurrent snapshots `(base+1 .. current)` intervened, run **Iceberg Serializable / Snapshot Isolation OCC validation**:
     - Append vs. Append: auto-rebase cleanly.
     - Append vs. Update/Delete: check if newly appended files' min/max stats overlap `read_filter_predicate`; if disjoint, rebase cleanly.
     - Compaction (`replace`) vs. Update/Delete: verify no replaced data file was deleted/DV-modified; drop conflicting files or retry cleanly.
4. Write new Iceberg `metadata.json` + manifest list to object storage and commit the pointer atomically via `openraft`.

### Multi-Table ACID Transactions (`BEGIN; ...; COMMIT;`)
1. Client issues `BEGIN` -> Coordinator assigns `read_watermark_txn_id`. All reads inside the transaction resolve each table's snapshot as of `read_watermark_txn_id` (consistent cross-table Snapshot Isolation).
2. Writes across tables `T1, T2, ..., Tk` stage their Parquet/DV files to object storage and send a unified `MultiTableCommitProposal` to the Coordinator.
3. Coordinator validates OCC rules across all `k` tables atomically inside a single `openraft` log entry (`CommitMultiTableTxn`).
4. Once quorum commits, all `k` table pointers advance atomically in one Raft state transition. External Iceberg REST clients observe the newly written `metadata.json` files immediately (with idempotent leader-crash recovery ensuring every committed metadata file is flushed to object storage).

---

## 4. Git-Style Zero-Copy Branching & Time Travel

Powered natively by Iceberg v3 `refs` (branches and tags):
```sql
CREATE BRANCH staging FROM main;
USE BRANCH staging;
DELETE FROM events WHERE env = 'test';
-- Validate staging without affecting production readers
MERGE BRANCH staging INTO main; -- Fast-forward or OCC-validated merge
SELECT * FROM events AT TIMESTAMP '2026-11-24 10:00:00Z';
SELECT * FROM events AT VERSION 1042;
```

---

## 5. Autonomous Background Maintainer (`--mode maintainer`)

A dedicated stateless/leader-elected Rust control loop continuously inspects catalog metrics and dispatches distributed maintenance jobs across the worker pool:
1. **Small-File Compaction:** Bin-packs Parquet files `< 64 MB` into target `256 MB` files.
2. **Incremental Hilbert / Liquid Clustering:** Clusters data files along declared `CLUSTER BY (col1, col2, json_path)` dimensions without rigid directory partitioning.
3. **Deletion Vector Materialization:** Rewrites Parquet files once v3 Deletion Vector density exceeds 15%.
4. **Dynamic JSON Re-Shredding (`RFC-003`):** Promotes newly hot JSON paths into typed Parquet sub-columns during compaction and rebuilds file-scoped Puffin indexes in the same pass.
5. **Snapshot Expiration & Orphan GC (`VACUUM`):** Expires snapshots beyond retention SLA and safely sweeps uncommitted staging files older than `max_txn_ttl`.
