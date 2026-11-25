# Changelog

All notable changes to **RustLakeDB** are documented in this file.

## [7.0.0] - 2026-11-24 — Flagship Lakehouse Release: 18.9× Vectorized Engine Boost, Strict Schema Enforcement, Time Travel Rollback, Centralized Governance & Lineage, Live PyIceberg Direct Scan, Live RustFS S3 & Zero-Downtime Rolling Upgrades (Waves 24–28)

### Wave 28 — Turnkey Pure-Rust Production Stack & Zero-Downtime Rolling-Upgrade Chaos
- **Zero-Downtime 3-Node Raft Rolling-Upgrade Chaos Test:** Added `RaftCatalogNode::resume()` restarting the election/heartbeat loop on rejoined followers and proved continuous Raft leadership, writes, and tag replication across a rolling node-by-node upgrade of all 3 nodes with zero data loss.
- **Turnkey Production Stack (`docker/docker-compose.prod.yml`):** Single-command pure-Rust lakehouse stack launching **RustFS S3 (`s3://warehouse`) + 2 Distributed RustLakeDB Workers + Coordinator + Prometheus (with alert rules) + Grafana (auto-provisioned dashboard)** — verified live end-to-end (`29 ms` distributed S3 query, `22 ms` cloud `COPY TO s3://`, `93 ms` `COPY FROM s3://`).

### Wave 27 — Live `PyIceberg` Direct Parquet Scans (`table.scan().to_arrow()`) & Live `psycopg3`/dbt Interop
- **Full-URI Iceberg v3 Avro Manifests + `PARQUET:field_id`:** Embedded official Iceberg `PARQUET:field_id` metadata on every Parquet column, added Iceberg v3 `partition` (`r102`), `schema`, and `partition-spec` metadata to Avro OCF manifests, and aligned full `file://`/`s3://` URIs across Avro Manifest Lists and REST Catalog `load_table` responses.
- **Verified Live:** Official **`pyiceberg` `table.scan().to_arrow()`** directly reads RustLakeDB Zstd Parquet files without SQL proxying, and live **`psycopg3` / `dbt`-style** `CREATE VIEW` → `CTAS` + `system.lineage` workflows pass over PGWire (`:5432`).

### Wave 26 — Cloud `COPY TO`/`COPY FROM`, Real `ANALYZE TABLE` CBO Statistics & Strict Memory-Bounded NVMe Spill
- **SQL `COPY TO` & Cloud `COPY FROM`:** Export query results as Parquet, CSV, or NDJSON to local paths or `s3://`/`gs://`/`az://`, and bulk-load from cloud object stores with automatic `system.lineage` recording.
- **Real `ANALYZE TABLE` Statistics in CBO:** Computes and persists column `ndv`, `null_count`, `min_value`, `max_value`, and `total_rows` in catalog metadata and surfaces them in `EXPLAIN` CBO plans.
- **Memory-Bounded NVMe Spill Proof:** Verified External Merge Sort and NVMe Grace Hash Join under a strict `8 KB` per-worker RAM ceiling with zero OOM.

### Wave 25 — `7.3×–18.9×` Vectorized Arrow Engine Boost & Dynamic Partition Pruning (DPP)
- **Zero-Copy Arrow Field-ID Aliasing:** Resolved `column_aliases` directly inside vectorized Arrow scan + aggregate loops, dropping post-`RENAME COLUMN` 10M-row aggregations from **`7,515 ms` → `398 ms`** (**18.9× faster**, **25.1M rows/s**).
- **Fused Arrow `BooleanArray` Filter + HashAggregate + Zone-Map Skipping:** Removed the `pushed_filter: None` restriction and evaluated filters once per `RecordBatch` via bit-packed `BooleanArray` masks, dropping 10M-row filtered `AVG`/`GROUP BY` from **`2,705 ms` → `371 ms`** (**7.29× faster**, **27.0M rows/s**).
- **Dynamic Partition Pruning (DPP):** Added semi-join build-side `[min, max]` key pushdown skipping non-overlapping fact-table Parquet splits before reading them (`WorkerNode::files_pruned_by_dpp`).

### Wave 24 — Strict Schema Enforcement, Timestamp Time Travel, Atomic `RESTORE TABLE` & Centralized Governance
- **Write-Time Schema Enforcement:** Enforces column `DEFAULT` values, unknown-column rejection, `NOT NULL` (`23502`), and column+table `CHECK (...)` (`23514`) constraints across `INSERT`, `INSERT ... SELECT`, and `COPY FROM`.
- **Timestamp Time Travel & Atomic Rollback:** Added `FOR SYSTEM_TIME AS OF TIMESTAMP '...'`, `RESTORE TABLE <table> TO VERSION <id> | TO TIMESTAMP '...'`, and `SHOW SNAPSHOTS ON <table>`.
- **Centralized Governance, Tagging & Automatic Data Lineage:** Added `ALTER TABLE ... SET TAG ('k'='v')`, `SHOW TAGS`, automatic lineage capture on `CTAS`/`INSERT SELECT`/`COPY`, virtual system tables (`system.tables`, `system.snapshots`, `system.tags`, `system.lineage`), and REST APIs (`/v1/governance/lineage`, `/v1/governance/tags`, `/v1/governance/audit`, `/v1/tables/{t}/snapshots`).

## [6.0.0] - 2026-11-24 — Battle-Tested Production Lakehouse Release: Zero-Rewrite Field-ID Evolution, Live Redpanda/PyIceberg/ADBC Verification, Cooperative Cancel, OIDC Poller, Observability Pack & Jepsen Chaos Hardening (Waves 22–23)

### Wave 23 — Jepsen-Style Chaos & Soak Hardening + Production Adoption Pack
- **50-Writer Concurrent OCC Commit Storm:** Proved strict linearizability across 50 simultaneous mutating OCC commits (1 winner, 49 conflict rejections) and 25 concurrent fast-forward append commits (zero lost updates).
- **3-Node Raft Split-Brain Leader Fencing & Majority Failover:** Verified killed/partitioned old leaders are fenced from split-brain writes while the surviving majority automatically elects a new leader at a strictly higher Raft term and replicates schema evolution.
- **Mid-Shuffle Worker Crash Soak & Production Adoption Pack:** Added 10-iteration alternating worker-fault distributed join soak test, plus prebuilt Grafana Dashboard ([deploy/observability/grafana-dashboard.json](deploy/observability/grafana-dashboard.json)), Prometheus alert rules ([deploy/observability/prometheus-alerts.yaml](deploy/observability/prometheus-alerts.yaml)), `dbt-postgres` quickstart ([examples/dbt_quickstart/](examples/dbt_quickstart/)), and Cloud Production Playbook ([docs/PRODUCTION_DEPLOYMENT.md](docs/PRODUCTION_DEPLOYMENT.md)).

### Wave 22 — Zero-Rewrite Iceberg Field-ID Schema Evolution, Pure Arrow Window, Cooperative Cancellation, Live OIDC Poller & Live Ecosystem Verification
- **True Zero-Rewrite Iceberg Field-ID Schema Evolution:** Added `ALTER TABLE RENAME COLUMN`, `ALTER TABLE DROP COLUMN [IF EXISTS]`, and `ALTER TABLE ALTER COLUMN ... TYPE` (`Int64 -> Float64`, `Decimal` scale widening) resolving historical Parquet files transparently via stable Iceberg `field_id` + `column_aliases` with zero Parquet rewrites.
- **Pure Arrow `RecordBatch` Window Fast Path:** Wired `columnar::eval_columnar_window` directly into `WorkerNode::eval_node` for vectorized window execution across single-node and distributed stages.
- **Cooperative In-Flight Query Cancellation & Live OIDC JWKS Poller:** Propagated query cancellation checks across `Coordinator::execute_dag` stages and `WorkerNode` split scans, and wired `RUSTLAKEDB_OIDC_JWKS_URL` live HTTP background key rotation in `rustlakedb-cli`.
- **Live Third-Party Ecosystem Verification & 10M-Row Release Benchmark:** Verified end-to-end against **live Redpanda (`redpandadata/redpanda`, adding Snappy `codec=2` xerial/raw RecordBatch v2 support!)**, **live `adbc-driver-flightsql` (`:8815`, 999,984 rows in `310.9 ms`)**, and **live `pyiceberg` (`:8181`, deterministic RFC 4122 `table-uuid` + create/insert/evolve/reload/drop round-trip)**; benchmarked **10,000,000 rows** (40 Zstd Parquet splits, `38.72 MB`): `491 ms` full-scan `GROUP BY` (**20.4M rows/s**), `479 ms` distributed Top-N (**20.9M rows/s**), `1,605 ms` 10M×20K hash join, and `2 ms` zero-rewrite `RENAME COLUMN`!

## [5.0.0] - 2026-11-24 — Tier-1 Lakehouse Parity Release: Flight SQL, Bi-Directional REST Catalog, Kafka RecordBatch v2, Full 22-Query Analytics + CBO, Columnar Arrow Pipeline & OIDC/JWKS (Waves 17–21)

### Wave 21 — Enterprise OIDC/JWKS `RS256` Auth, Active Query Cancellation, Upgraded Web Console & Helm v5
- **OIDC / JWKS (`RS256`) Authentication:** Added RFC 7517 JWKS RSA public key loading (`GovernanceState::set_jwks_json`) and cryptographic `RSA_PKCS1_2048_8192_SHA256` JWT signature + `exp` validation via `ring` for Okta, Auth0, Entra ID, and AWS Cognito integration.
- **Active Query Cancellation & Live Cluster Topology UI:** Added `CANCEL QUERY '<id>'`, `GET /v1/queries/active`, `POST /v1/queries/{id}/cancel`, and upgraded the embedded Web Admin Console (`GET /`) with live worker health badges and 1-click query cancellation.
- **Kubernetes Helm Chart `v5.0.0`:** Added Arrow Flight SQL (`8815`), optional cluster-internal mTLS secret mounts, OIDC JWKS env wiring, worker self-registration, and `autoscaling/v2` HPA.

### Wave 20 — End-to-End Native Apache Arrow `RecordBatch` Columnar Execution Pipeline
- **Native Arrow Columnar Engine (`rustlakedb_worker::columnar`):** Evaluates `BinaryArith`, `CaseExpr`, `StringExpr`, `MathExpr`, `Extract`, `Filter` (`arrow_select::filter_record_batch`), `HashAggregate` + `ROLLUP`/`CUBE`/`GROUPING SETS`, and `Window` (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `LAG`, `LEAD`) directly over Arrow `RecordBatch` arrays without intermediate `RowBatch` row-maps.

### Wave 19 — Full 22-Query Analytical Decision-Support Suite & CBO Optimizer
- **SQL Completeness:** Added `DATE +/- INTERVAL 'N' DAY|MONTH|YEAR` compile-time folding, `EXTRACT(YEAR|MONTH|DAY FROM ...)`, `BETWEEN`/`NOT BETWEEN`, general `CASE WHEN ... THEN <expr> ELSE <expr> END`, derived expressions inside aggregates (`SUM(a * (1 - b))`, `SUM(CASE WHEN ... END)`), column-to-column filter comparisons (`l_commitdate < l_receiptdate`), inline aggregate rewriting in `HAVING`, and scalar subquery substitution (`< (SELECT AVG(...))`).
- **CBO Enhancements:** Added **Predicate Pushdown Through Joins** (`push_filter_through_joins`) onto `IcebergScan` leaves and **Dependency-Aware Multi-Table CBO Join Reordering** for arbitrary 5+ table snowflake/star joins.

### Wave 18 — Full Apache Arrow Flight SQL (`arrow-flight::sql`) & Multi-Arch Releases
- **Full Arrow Flight SQL Protocol (`:8815`):** Implemented `CommandStatementQuery`, `CommandStatementUpdate`, `CommandStatementIngest` (`DoPut` bulk zero-copy Arrow ingestion), `CommandGetCatalogs`, `CommandGetDbSchemas`, `CommandGetTables`, `CommandGetTableTypes`, `CommandGetSqlInfo`, and `PreparedStatement` lifecycle (`CreatePreparedStatement`/`ClosePreparedStatement`), verified with the official `FlightSqlServiceClient`.
- **Multi-Arch Release Pipeline:** Upgraded `.github/workflows/release.yml` to build native binaries for `linux-x86_64`, `linux-aarch64`, `macos-aarch64`, and `windows-x86_64`, plus multi-arch `linux/amd64` + `linux/arm64` GHCR container manifests.

### Wave 17 — Bi-Directional Iceberg v3 REST Catalog & Production Kafka/Redpanda RecordBatch v2 CDC
- **Bi-Directional Iceberg v3 REST Catalog Read/Write Spec:** Added `POST /v1/oauth/tokens`, `POST/GET/DELETE /v1/namespaces`, `POST /v1/namespaces/{ns}/tables`, `HEAD /v1/namespaces/{ns}/tables/{table}`, and `POST /v1/namespaces/{ns}/tables/{table}` OCC table commits (`assert-ref-snapshot-id`, `add-schema`, `add-snapshot`, `remove-snapshots`) with `409 Conflict` enforcement.
- **Kafka / Redpanda RecordBatch v2 (`magic=2`):** Upgraded `kafka_ingest.rs` to **Metadata v1**, **ListOffsets v1**, and **Fetch v4** decoding Kafka RecordBatch v2 (`CRC32C`, zigzag varints, Zstd & uncompressed) with automatic legacy Fetch v0 fallback.

## [4.0.0] - 2026-11-24 — Frontier Lakehouse Release: Arrow Flight gRPC, Native Temporal/Decimal, Hidden Partitioning, Kafka CDC, Cluster mTLS & AWS STS SigV4 (Waves 14–16)

### Wave 16 — Cluster-Internal mTLS, Real Cloud AWS STS `AssumeRole` SigV4 & Cross-Engine Interop
- **Cluster-Internal Mutual TLS (mTLS):** Added `build_mtls_server_acceptor` (`WebPkiClientVerifier` over `rustls`), `WorkerHttpServer::serve_mtls`, and `HttpWorkerClient::new_mtls` (auto-enabled via `RUSTLAKEDB_MTLS_CA`, `RUSTLAKEDB_MTLS_CERT`, `RUSTLAKEDB_MTLS_KEY`) so Coordinator ↔ Worker RPC rejects any peer without a valid cluster-CA-signed client certificate.
- **Real Cloud AWS STS `AssumeRole` SigV4 Credential Vending:** Implemented pure-Rust `AWS4-HMAC-SHA256` SigV4 signing (`sign_aws_sigv4` + `assume_role_sigv4` using `ring`) and XML `<Credentials>` parsing on `/v1/namespaces/{ns}/tables/{table}/credentials` whenever `RUSTLAKEDB_STS_ROLE_ARN` is configured.
- **Live Cross-Engine Iceberg v3 Interop Verification:** Independently verified Iceberg v3 `v*.metadata.json`, Avro OCF manifests, and Zstd Parquet files with native Arrow types via external Apache Arrow / DuckDB / PyArrow readers.

### Wave 15 — Apache Arrow Flight gRPC Server & Native Pure-Rust Kafka/Redpanda CDC Consumer
- **Apache Arrow Flight gRPC Server (`--flight-addr 0.0.0.0:8815`):** Full `arrow-flight` + `tonic` gRPC service implementing `Handshake`, `ListFlights`, `GetFlightInfo`, `GetSchema`, and zero-copy `DoGet` streaming columnar Arrow `RecordBatch` frames for DuckDB, Polars, PyArrow, Spark, and BI clients.
- **Native Pure-Rust Kafka / Redpanda Streaming CDC Consumer (`--mode kafka-ingest`):** Speaks the Kafka binary wire protocol (`Fetch v0` over raw TCP) directly to Kafka/Redpanda brokers, parses Debezium JSON payloads, checkpoints durable offsets to `_kafka/*.json`, and feeds atomic exactly-once `Coordinator::ingest_cdc_events`.
- **Automated GitHub Releases & GHCR Publishing (`.github/workflows/release.yml`):** Automatically builds Linux x86_64 release tarballs + SHA256 checksums, pushes `ghcr.io/sudeshc/rustlakedb:<tag>` & `latest`, and publishes GitHub Releases on every `v*` tag push.

### Wave 14 — Native Temporal/Decimal Arrow Types, Write-Time Hidden Partitioning & `ROLLUP`/`CUBE`/`GROUPING SETS`
- **Native Arrow `Date32`, `Timestamp(Microsecond, UTC)` & `Decimal128(p, s)`:** End-to-end native physical encoding across `catalog`, `protocol`, `storage`, `shuffle`, `planner`, `coordinator`, `worker`, and PostgreSQL OIDs (`1082`, `1114`, `1700`), plus a vectorized Arrow Scan+Project fast path (`try_eval_vectorized_scan_project`).
- **Write-Time Iceberg Hidden Partition Routing & Layer 0 Split Pruning:** Routes writes into Iceberg partition folders for identity, `days(ts)`, `bucket(N, col)`, and `truncate(W, col)` transforms, and prunes non-matching partition splits at Layer 0 (`files_pruned_by_partition`) before opening Parquet footers.
- **OLAP Grouping Extensions:** Added `GROUP BY ROLLUP(...)`, `GROUP BY CUBE(...)`, and `GROUP BY GROUPING SETS (...)`.

## [3.0.0] - 2026-11-24 — Hyperscale Cloud, Vectorized Spill, Streaming CDC & TPC-H Release (Waves 11–13)

### Wave 13 — Debezium CDC Streaming, OpenTelemetry Tracing & TPC-H Analytical Suite
- **Debezium-Compatible Streaming CDC (`POST /v1/cdc/{table}`):** Processes `op: "c" | "u" | "d"` event batches with **exactly-once offset watermarks**, atomic Iceberg v3 Deletion Vectors for updates/deletes, and single-snapshot OCC commits.
- **OpenTelemetry-Compatible Distributed Tracing (`GET /v1/traces`):** Propagates W3C `traceparent` headers through Coordinator execution and exports structured OTLP-compatible spans.
- **Automated TPC-H Analytical Benchmark Suite:** Executes canonical TPC-H multi-table queries (Q1, Q3, Q5, Q6, Q10, Q12) over distributed Zstd Parquet splits (`.\scripts\run-tests.ps1 -Suite tpch`).

### Wave 12 — Deep Vectorized Arrow Engine, External NVMe Spilling & Rich Analytics SQL
- **Vectorized Arrow Filtered Scan & Multi-Column Vectorized Top-N:** Evaluates predicates and arbitrary multi-key `ORDER BY k1, k2, ... LIMIT N` directly over columnar Arrow slices without materializing rejected rows.
- **MemoryPool-Governed External NVMe Spill for Sort & Hash-Aggregate:** Extends Grace Hash Join spilling with Zstd-compressed External Merge-Sort and External Hash-Aggregate spilling under tight RAM budgets.
- **Rich SQL & Iceberg Partitioning:** Added distributed `COUNT(DISTINCT col)`, `INTERSECT` & `EXCEPT`, correlated `EXISTS`/`NOT EXISTS` decorrelation, `CREATE TABLE ... PARTITION BY (...)`, statistics-driven NDV CBO notes, and PGWire `$1`/`$2` bind parameter verification.

### Wave 11 — Zero-Touch HA Failover, Raft WAL Snapshotting, Cloud S3 CLI & Iceberg v3 Interop
- **Zero-Touch Multi-Coordinator HA Failover (`--coordinator-urls`):** Workers broadcast heartbeats concurrently to all HA coordinators so killing the active Raft Leader requires zero manual worker re-registration.
- **Automatic Raft WAL Log Compaction & Snapshotting:** Compacts applied WAL entries (`snapshot_index`/`snapshot_term`) automatically and recovers cleanly across cold restarts.
- **Cloud S3/GCS/Azure Built Into CLI (`--storage-uri`):** Enables `rustlakedb-storage/cloud` natively in the `rustlakedb` binary for live `s3://` (AWS/MinIO), `gs://`, and `az://` warehouses.
- **Independent Iceberg v3 Avro + Parquet Interop Decoder:** Full binary Avro OCF record decoders verifying `metadata.json` -> Manifest List -> Manifest -> Zstd Parquet round-trips.

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
