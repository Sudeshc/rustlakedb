# Changelog

All notable changes to **RustLakeDB** are documented in this file.

## [2.0.0] - 2026-11-24 — Enterprise Lakehouse Platform Release (Waves 1–10)

### Wave 10 — Security, Cloud Credential Vending, SQL UDFs & Release Engineering
- **Real TLS 1.3 PostgreSQL Wire Protocol (`rustls` / `tokio-rustls`):** Handles PG `SSLRequest -> 'S'` upgrade natively over `RUSTLAKEDB_TLS_CERT` + `RUSTLAKEDB_TLS_KEY` PEM credentials, combined with RFC 5802 SCRAM-SHA-256 authentication.
- **Iceberg v3 REST Catalog Temporary STS Credential Vending:** Added `GET /v1/namespaces/{namespace}/tables/{table}/credentials` returning scoped temporary `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, and `expiration-time`.
- **SQL Scalar UDFs:** Added `CREATE [OR REPLACE] FUNCTION <name>(<param> <type>) RETURNS <type> AS '<expr>'` with inline query expansion across distributed stages.
- **CI/CD & Packaging:** Added GitHub Actions CI (`.github/workflows/ci.yml`) and bumped workspace, Docker, and Kubernetes Helm Chart to `v2.0.0`.

### Wave 9 — Vectorized Arrow Top-N, dbt DDL, Auto-Loader & Elastic Cluster Hardening
- **Vectorized Arrow Top-N Pushdown (`try_eval_vectorized_topn`):** Evaluates `ORDER BY <col> LIMIT N` directly over Arrow `Float64Array`/`Int64Array`/`StringArray` slices without full row-map materialization.
- **Full `dbt`-Ready DDL & Multi-Table CBO:** Added `CREATE/DROP SCHEMA [IF NOT EXISTS | IF EXISTS]`, `CREATE TABLE IF NOT EXISTS`, `DROP TABLE/VIEW IF EXISTS`, `ALTER TABLE RENAME TO`, `TRUNCATE TABLE` (preserving Iceberg Time Travel snapshots), `NOT IN (SELECT ...)` / `NOT EXISTS (SELECT ...)`, and 3+ table CBO inner-join size ordering.
- **Cloud / Landing-Prefix Auto-Loader Daemon (`--mode autoloader`):** Exactly-once checkpointed ingestion (`_autoloader/autoloader_<table>_checkpoint.json`) for NDJSON (with `<table_name>_dlq` routing), CSV, and Parquet files.
- **Elastic Worker Hardening:** Added Dynamic Worker Heartbeat TTL eviction (`evict_stale_workers`) and Speculative Straggler Execution (`speculative_threshold_ms`) racing slow tasks on healthy peer workers.

### Wave 8 — Distributed Top-N Pushdown, Incremental MV Refresh, HNSW & BM25
- **Distributed Top-N Pushdown (`PhysicalNode::TopN`):** Pushes bounded local Top-N heaps before `Exchange(Single)`, slashing 1M-row Top-5 shuffle volume to <1 KB.
- **True Incremental Materialized View Refresh:** Tracks source Iceberg snapshot watermarks to append only newly added delta Parquet files (`REFRESH MATERIALIZED VIEW INCREMENTAL`).
- **HNSW Vector Index Sidecars & BM25 Full-Text Search:** Added `CREATE INDEX ... USING HNSW` proximity graph sidecars and `BM25_SCORE(col, 'query')`.

### Wave 7 — Dynamic Worker Discovery, Persistent Raft WAL & JWT Auth
- **Dynamic Worker Self-Registration & Heartbeat:** `--coordinator-url` + `POST /v1/workers/register` & `/v1/workers/heartbeat`.
- **Crash-Safe Persistent Raft WAL & 3-Coordinator HA Docker Compose:** `raft_wal_node_<id>.json`, `--raft-node-id`/`--raft-bind`/`--raft-peers`, and `docker/docker-compose.ha.yml`.
- **HMAC-SHA256 JWT Bearer Authentication:** Control Plane enforcement via `RUSTLAKEDB_JWT_SECRET`.

### Wave 6 — SQL Completeness & Multi-Table ACID Transactions
- **Rich SQL:** `CTAS`, `INSERT INTO ... SELECT`, Arithmetic (`+,-,*,/`), String/Math/Cast functions (`LOWER`, `UPPER`, `TRIM`, `LENGTH`, `CONCAT`, `SUBSTRING`, `ROUND`, `ABS`, `CAST`, `NOW()`), `RIGHT`/`FULL`/`CROSS` Joins, `IN (SELECT ...)` and `EXISTS (SELECT ...)` subqueries.
- **Multi-Statement Multi-Table ACID Transactions:** Full `BEGIN` / `COMMIT` / `ROLLBACK` buffering atomic multi-table Iceberg OCC proposals.

### Wave 5 — Vectorized Arrow Joins, Binary Arrow IPC Shuffle, Avro Manifests, Raft Quorum & SCRAM-SHA-256
- **Vectorized Arrow Join + Fused Filter + Binary Arrow IPC Zstd Shuffle (`ARW1`):** Crushed live 1M×20K distributed JOIN latency from `463 ms` down to **`66–72 ms`**.
- **Genuine Apache Avro 1.11 OCF (`Obj\x01`) Iceberg v3 Manifests & Manifest Lists.**
- **Multi-Node Raft Consensus HA Catalog Quorum** with leader election, majority commit, follower forwarding, and leader-kill failover.
- **RFC 5802 PostgreSQL 16 SCRAM-SHA-256** authentication.

### Waves 1–4 — Core Vectorized Engine, Ingestion, Governance, Control Plane & Lakehouse Intelligence
- **Wave 1:** Bounded RAM+NVMe LRU `BoundedObjectCache`, Parquet `ProjectionMask`, Vectorized Arrow Partial Aggregation + Zero-Parse Shredded JSON, sub-second worker failover (`412 ms`).
- **Wave 2:** Bulk `COPY` (CSV/JSON/Parquet + DLQ), HTTP NDJSON ingest, ACID CDC `MERGE INTO`, `CASE WHEN`, `COALESCE`, `DATE_TRUNC`, `UNION`/`UNION ALL`, Window Functions.
- **Wave 3:** RBAC (`CREATE ROLE`, `GRANT`, `REVOKE`), Row-Level Security (`CREATE POLICY`), Dynamic Column & JSON-path Masking, Virtual & Materialized Views, `EXPLAIN ANALYZE`, `system.query_history`, `system.audit_log`, Web SQL Workbench UI, Prometheus `/metrics`, Zero-Copy Arrow IPC SQL endpoint.
- **Wave 4:** Liquid Multi-Column Clustering (`CLUSTER BY`, `OPTIMIZE TABLE`), per-file Zone-Map pruning, recursive nested JSON Puffin indexing & dynamic re-shredding, Hybrid Vector SQL (`VECTOR_COSINE`, `VECTOR_L2`, `<->`), WLM `SHOW QUERIES`, Kubernetes Helm Chart.

## [1.0.0] - 2026-11-24 — Initial Production Release
- Distributed Coordinator/Worker architecture over Apache Iceberg v3 Parquet + Puffin indexes + Deletion Vectors, PostgreSQL v3 Wire Protocol (DBeaver/psql), Iceberg v3 REST Catalog, AQE coalescing & skew handling, Autonomous Maintainer daemon.
