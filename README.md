# RustLakeDB

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-100%25-orange.svg)](Cargo.toml)
[![Version](https://img.shields.io/badge/Version-7.0.0-brightgreen.svg)](CHANGELOG.md)

**RustLakeDB** is a 100% Rust, cloud-agnostic, distributed SQL and lakehouse database engine built natively over **Apache Iceberg v3** (Zstd Parquet, Shredded `VARIANT` JSON, Puffin Secondary Indexes, Roaring Bitmap Deletion Vectors, and Apache Avro OCF Manifests).

It speaks the **PostgreSQL v3 Wire Protocol** (DBeaver, `psql`, JDBC, BI) over **TLS 1.3** + **SCRAM-SHA-256** AND **Full Apache Arrow Flight SQL** (`:8815` for ADBC, JDBC Flight SQL, Polars, PyArrow, DuckDB, Tableau, Power BI), executes parallel distributed queries across stateless worker clusters secured with **Mutual TLS (mTLS)** using a **Native Arrow `RecordBatch` Columnar Engine**, and includes a **Multi-Node Raft HA Bi-Directional Iceberg v3 REST Catalog** with real **AWS STS `AssumeRole` SigV4** credential vending and **OIDC/JWKS (`RS256`)** auth.

---

## Key Capabilities (`v7.0.0`)

- **Native Arrow Columnar Engine (`27M rows/s`), DPP & Full Arrow Flight SQL:** Parallel Spark-style DAG scheduler with Adaptive Query Execution (AQE), Rendezvous split placement, speculative hedging, cooperative in-flight cancellation, Binary Arrow IPC Zstd shuffle (`ARW1`), **Zero-Copy Arrow Field-ID Aliasing (`18.9×` faster post-evolution scans)**, **Fused Arrow `BooleanArray` Filter + HashAggregate (`7.3×` faster filtered aggregations)**, **Dynamic Partition Pruning (DPP)** on star-schema joins, **Full Apache Arrow Flight SQL (`:8815`, live `adbc-driver-flightsql` verified)**, memory-bounded NVMe spilling (External Merge Sort & Grace Hash Join), and real **`ANALYZE TABLE`** column statistics fed into the CBO.
- **Bi-Directional Iceberg v3 REST Catalog (Live `PyIceberg` `scan().to_arrow()` Verified), Schema Enforcement & Time Travel:** Strict write-time **`NOT NULL` (`23502`), `DEFAULT`, and `CHECK (...)` (`23514`)** schema enforcement, **zero-rewrite Field-ID `RENAME COLUMN`, `DROP COLUMN`, `ADD COLUMN [DEFAULT]`, and type widening**, **Timestamp & Snapshot Time Travel (`FOR SYSTEM_TIME AS OF TIMESTAMP '...'`)**, atomic **`RESTORE TABLE ... TO VERSION`** rollback, `SHOW SNAPSHOTS`, `PARQUET:field_id` + full-URI Iceberg v3 Avro OCF manifests (**live `pyiceberg` `table.scan().to_arrow()` verified**), and live **`s3://` (RustFS / AWS S3)** `COPY TO`/`COPY FROM` (`Parquet`, `CSV`, `JSON`).
- **Centralized Governance, Multi-Node Raft HA (Zero-Downtime Rolling-Upgrade Verified) & Enterprise Security:** Built-in persistent Raft HA quorum with automatic log compaction, split-brain leader fencing, **zero-downtime 3-node rolling-upgrade failover**, **Catalog Tagging (`ALTER TABLE ... SET TAG`, `SHOW TAGS`)**, **Automatic Data Lineage (`system.lineage`)**, virtual governance tables (`system.tables`, `system.snapshots`, `system.tags`, `system.lineage`, `system.audit_log`, `system.query_history`) + `/v1/governance/*` REST APIs, **RBAC + RLS + Dynamic Column/JSON-Path Masking**, **Cluster mTLS**, **live OIDC/JWKS (`RS256`) poller**, and real **AWS STS `AssumeRole` SigV4** credential vending.
- **Full 22-Query Analytical SQL, `psycopg3`/`dbt` PGWire & Live Redpanda CDC:** Full 22-query analytical decision-support SQL, **Predicate Pushdown Through Joins + Dependency-Aware Multi-Table CBO Join Reordering**, live **`psycopg3` / `dbt-postgres`** PGWire compatibility, and **pure-Rust Kafka/Redpanda RecordBatch v2 (`magic=2`, uncompressed/Snappy/Zstd, verified live against `redpandadata/redpanda`)** CDC consumer.
- **Turnkey Pure-Rust Production Stack & Multi-Arch Releases:** One-command **`docker/docker-compose.prod.yml`** launching **RustFS S3 + Distributed RustLakeDB + Prometheus (with alerts) + Grafana (auto-provisioned dashboard)**, Kubernetes Helm Chart `v7.0.0` (`autoscaling/v2` HPA), **`dbt-postgres` Quickstart** ([examples/dbt_quickstart/](examples/dbt_quickstart/)), **Production Playbook** ([docs/PRODUCTION_DEPLOYMENT.md](docs/PRODUCTION_DEPLOYMENT.md)), and automated multi-arch GitHub Releases (`linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `windows-x86_64`) + multi-arch GHCR images (`ghcr.io/sudeshc/rustlakedb`).

---

## Verified 10M-Row Benchmarks & Live Ecosystem Interop (`v7.0.0`)

Measured on `rustlakedb:7.0.0` (`--mode gateway --workers 4`, 40 Zstd Parquet + Puffin splits, `38.74 MB` compressed on disk, **10,000,000** `big_orders` × **20,000** `big_customers`):

| Benchmark Query (`10,000,000` rows) | `v6.0.0` Engine | **`v7.0.0` Engine** | Throughput | Speedup |
|---|---:|---:|---:|---:|
| Full Scan + `GROUP BY region` (`COUNT`, `SUM`) | `498 ms` | **`441 ms`** | **22.7M rows/s** | **1.13×** |
| Filtered `WHERE amount > 450` + `GROUP BY` + `AVG` | `2,705 ms` | **`371 ms`** | **27.0M rows/s** | **7.29×** |
| Distributed Top-N (`ORDER BY amount DESC LIMIT 10`) | `479 ms` | **`549 ms`** | **18.2M rows/s** | — |
| 10M × 20K Hash Join + `GROUP BY tier` (`COUNT`, `SUM`) | `1,605 ms` | **`1,518 ms`** | **6.6M rows/s** | **1.06×** |
| Zero-Rewrite `ALTER TABLE ... RENAME COLUMN` | `2 ms` | **`2 ms`** | — | `0` rewrites |
| Post-Evolution `GROUP BY` on renamed `gross_amount` | `7,515 ms` | **`398 ms`** | **25.1M rows/s** | **18.9×** |

**Live External Client & Cloud Storage Verification (`v7.0.0` Docker stacks):**
- **`pyiceberg` `table.scan().to_arrow()` (`http://:8181`):** Direct engine-independent Parquet reads via RustLakeDB's Iceberg v3 REST Catalog, Avro OCF Manifest List (`snap-*.avro`), Avro OCF Manifest (`*-m0.avro`), and `PARQUET:field_id` metadata — plus full `create_table` / `RENAME COLUMN` / `drop_table` round-trip.
- **Live RustFS S3 (`s3://warehouse` via `docker/docker-compose.prod.yml`):** 2 distributed workers + coordinator reading/writing Iceberg v3 tables on live S3 (`29 ms`), plus cloud `COPY TO 's3://warehouse/...'` (`22 ms`) and `COPY FROM 's3://warehouse/...'` (`93 ms`) + Prometheus `/metrics`.
- **`psycopg3` / `dbt` PGWire (`:5432`):** Live `CREATE VIEW` → `CTAS` across `999,984` rows with automatic `system.lineage` capture.
- **`adbc-driver-flightsql` (`grpc://:8815`):** Queried `999,984` rows with distributed `GROUP BY` in `310.9 ms` returning native PyArrow `Table`.
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

### Automated Test Suite (90 Tests Across Waves 1–28, Full 22-Query Analytics & Rolling Chaos)

```powershell
# Run strict Clippy + all 90 regression tests
cargo clippy --all-targets -- -D warnings
.\scripts\run-tests.ps1
```

---

## Copyright & License

Copyright (C) 2026 **Sudeshc**

RustLakeDB is free software licensed under the **GNU General Public License v3.0 or later** (`GPL-3.0-or-later`). See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT) for full terms.
