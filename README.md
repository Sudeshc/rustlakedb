# RustLakeDB

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-100%25-orange.svg)](Cargo.toml)
[![Version](https://img.shields.io/badge/Version-6.0.0-brightgreen.svg)](CHANGELOG.md)

**RustLakeDB** is a 100% Rust, cloud-agnostic, distributed SQL and lakehouse database engine built natively over **Apache Iceberg v3** (Zstd Parquet, Shredded `VARIANT` JSON, Puffin Secondary Indexes, Roaring Bitmap Deletion Vectors, and Apache Avro OCF Manifests).

It speaks the **PostgreSQL v3 Wire Protocol** (DBeaver, `psql`, JDBC, BI) over **TLS 1.3** + **SCRAM-SHA-256** AND **Full Apache Arrow Flight SQL** (`:8815` for ADBC, JDBC Flight SQL, Polars, PyArrow, DuckDB, Tableau, Power BI), executes parallel distributed queries across stateless worker clusters secured with **Mutual TLS (mTLS)** using a **Native Arrow `RecordBatch` Columnar Engine**, and includes a **Multi-Node Raft HA Bi-Directional Iceberg v3 REST Catalog** with real **AWS STS `AssumeRole` SigV4** credential vending and **OIDC/JWKS (`RS256`)** auth.

---

## Key Capabilities (`v6.0.0`)

- **Native Arrow Columnar Pipeline & Full Arrow Flight SQL (Live ADBC Verified):** Parallel Spark-style DAG scheduler with Adaptive Query Execution (AQE), Rendezvous split placement, speculative straggler hedging, cooperative in-flight query cancellation, Binary Arrow IPC Zstd shuffle (`ARW1`), **Full Apache Arrow Flight SQL (`--flight-addr 0.0.0.0:8815`, verified live with `adbc-driver-flightsql`, PyArrow, and bulk `DoPut` ingestion)**, NVMe spilling, and a **Native Arrow `RecordBatch` Columnar Engine** (`Filter`, `Project`, `HashAggregate`, `ROLLUP`/`CUBE`, `Window`, Joins, Top-N).
- **Bi-Directional Iceberg v3 REST Catalog (Live PyIceberg Verified) & Zero-Rewrite Field-ID Evolution:** Full OpenAPI read/write Iceberg v3 REST Catalog (`OAuth2`, `namespaces`, `tables`, RFC 4122 `table-uuid`, OCC commits), **zero-rewrite `RENAME COLUMN`, `DROP COLUMN`, and type widening** via stable Iceberg `field_id`, live `s3://` (AWS/RustFS/MinIO), `gs://`, and `az://` storage, native Arrow `Date32`, `Timestamp(us, UTC)` & `Decimal128(p, s)`, hidden partitioning + Layer 0 pruning, Puffin JSON indexes, Deletion Vectors, Apache Avro 1.11 OCF manifests (verified live with `fastavro` + DuckDB), Liquid Clustering, Time Travel, and Branching.
- **Built-in Multi-Node Raft HA (Jepsen Chaos Verified), Cluster mTLS, Live OIDC/JWKS & AWS STS SigV4:** Crash-safe persistent Raft WAL quorum with automatic log compaction, split-brain leader fencing, zero-touch multi-coordinator worker failover (`--coordinator-urls`), **Cluster-Internal Mutual TLS (mTLS)**, **live `RUSTLAKEDB_OIDC_JWKS_URL` `RS256` auto-refresh poller** + HS256 JWT, and real **AWS STS `AssumeRole` SigV4** credential vending.
- **Full 22-Query Analytical SQL, CBO & Live Redpanda RecordBatch v2 CDC:** Full 22-query analytical decision-support SQL (`DATE +/- INTERVAL`, `EXTRACT`, `BETWEEN`, `CASE` inside aggregates, col-vs-col filters, inline `HAVING` aggs, scalar/IN/EXISTS subqueries, `ROLLUP`/`CUBE`/`GROUPING SETS`, Window Functions, Views & Incremental MVs, multi-table ACID `BEGIN`/`COMMIT`/`ROLLBACK`, HNSW Vector + BM25 search), **Predicate Pushdown Through Joins + Dependency-Aware Multi-Table CBO Join Reordering**, and **pure-Rust Kafka/Redpanda RecordBatch v2 (`magic=2`, uncompressed/Snappy/Zstd, verified live against `redpandadata/redpanda`)** CDC consumer.
- **Enterprise Operations, Observability Pack & Multi-Arch Releases:** Web Admin Console (`:8181`) with Cluster Topology & `CANCEL QUERY`, prebuilt **Grafana Dashboard** + **Prometheus Alert Rules** ([deploy/observability/](deploy/observability/)), **`dbt-postgres` Quickstart** ([examples/dbt_quickstart/](examples/dbt_quickstart/)), **Production Deployment Playbook** ([docs/PRODUCTION_DEPLOYMENT.md](docs/PRODUCTION_DEPLOYMENT.md)), Kubernetes Helm Chart (`autoscaling/v2` HPA + Flight SQL + mTLS + OIDC), and automated multi-arch GitHub Releases (`linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `windows-x86_64`) + multi-arch GHCR images (`ghcr.io/sudeshc/rustlakedb`).

---

## Verified 10M-Row Benchmarks & Live Ecosystem Interop (`v6.0.0`)

Measured on `rustlakedb:6.0.0` (`--mode gateway --workers 4`, 40 Zstd Parquet + Puffin splits, `38.72 MB` compressed on disk, **10,000,000** `big_orders` × **20,000** `big_customers`):

| Benchmark Query (`10,000,000` rows) | Engine Latency | Throughput | Notes |
|---|---:|---:|---|
| Full Scan + `GROUP BY region` (`COUNT`, `SUM`, warm) | **491 ms** | **20.4M rows/s** | Native Arrow `RecordBatch` columnar path (`498 ms` cold) |
| Distributed Top-N (`ORDER BY amount DESC LIMIT 10`) | **479 ms** | **20.9M rows/s** | Vectorized Arrow Top-N pushdown |
| 10M × 20K Hash Join + `GROUP BY tier` (`COUNT`, `SUM`) | **1,605 ms** | **6.2M rows/s** | Vectorized Arrow hash-join + aggregate |
| Zero-Rewrite `ALTER TABLE ... RENAME COLUMN` | **2 ms** | — | Metadata-only Iceberg `field_id` evolution (`0` Parquet rewrites) |

**Live External Client Verification (`v6.0.0` 5-container Docker cluster):**
- **`adbc-driver-flightsql` (`grpc://:8815`):** Queried `999,984` rows with distributed `GROUP BY` in `310.9 ms` returning native PyArrow `Table`.
- **`pyiceberg` (`http://:8181`):** Full round-trip `list_namespaces()`, `load_table()` (RFC 4122 `table-uuid`), `create_table()`, SQL `INSERT` + zero-rewrite `RENAME COLUMN`, `load_table()` verifying evolved schema, and `drop_table()`.
- **`redpandadata/redpanda:latest`:** Live `Metadata v1`, `ListOffsets v1`, and `Fetch v4` Snappy (`codec=2`) RecordBatch v2 Debezium `c/u/d` CDC ingestion into ACID Iceberg v3 state.

---

## Documentation

- **Architecture & ADRs:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **RFC-001 (Distributed Execution, Hybrid Shuffle & AQE):** [docs/rfcs/RFC-001-distributed-execution-and-aqe.md](docs/rfcs/RFC-001-distributed-execution-and-aqe.md)
- **RFC-002 (Built-in Raft Iceberg v3 Catalog & ACID):** [docs/rfcs/RFC-002-openraft-iceberg-v3-catalog.md](docs/rfcs/RFC-002-openraft-iceberg-v3-catalog.md)
- **RFC-003 (Three-Layer JSON `VARIANT` Shredding & Puffin Indexes):** [docs/rfcs/RFC-003-json-variant-shredding-and-puffin-indexes.md](docs/rfcs/RFC-003-json-variant-shredding-and-puffin-indexes.md)
- **RFC-004 (Cost-Based Join Reorderer):** [docs/rfcs/RFC-004-cost-based-join-reorderer.md](docs/rfcs/RFC-004-cost-based-join-reorderer.md)
- **Release History:** [CHANGELOG.md](CHANGELOG.md)

---

## Quick Start

```bash
# Run a 3-worker local distributed demo over real Parquet + Puffin storage
cargo run --bin rustlakedb -- --mode local --workers 3

# Launch a live PostgreSQL v3 Wire Protocol gateway (127.0.0.1:5432) + Control Plane UI (127.0.0.1:8181)
cargo run --bin rustlakedb -- --mode gateway --workers 3 --pg-addr 127.0.0.1:5432 --rest-addr 127.0.0.1:8181
```

### Multi-Container Distributed Docker Cluster

```powershell
# Launch 3 independent workers + Coordinator + Autonomous Maintainer & benchmark 1,000,000 rows
.\scripts\run-large-scale-docker-bench.ps1 -Rebuild -Rows 1000000 -Splits 24

# Launch a 3-Coordinator HA Raft Quorum + dynamically self-registering workers
docker compose -p rustlakedb-ha -f docker/docker-compose.ha.yml up -d
```

### Automated Test Suite (82 Tests Across Waves 1–23, Full 22-Query Analytics & Jepsen Chaos)

```powershell
# Run strict Clippy + all 82 regression tests
cargo clippy --all-targets -- -D warnings
.\scripts\run-tests.ps1
```

---

## Copyright & License

Copyright (C) 2026 **Sudeshc**

RustLakeDB is free software licensed under the **GNU General Public License v3.0 or later** (`GPL-3.0-or-later`). See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT) for full terms.
