# RustLakeDB

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-100%25-orange.svg)](Cargo.toml)
[![Version](https://img.shields.io/badge/Version-5.0.0-brightgreen.svg)](CHANGELOG.md)

**RustLakeDB** is a 100% Rust, cloud-agnostic, distributed SQL and lakehouse database engine built natively over **Apache Iceberg v3** (Zstd Parquet, Shredded `VARIANT` JSON, Puffin Secondary Indexes, Roaring Bitmap Deletion Vectors, and Apache Avro OCF Manifests).

It speaks the **PostgreSQL v3 Wire Protocol** (DBeaver, `psql`, JDBC, BI) over **TLS 1.3** + **SCRAM-SHA-256** AND **Full Apache Arrow Flight SQL** (`:8815` for ADBC, JDBC Flight SQL, Polars, PyArrow, DuckDB, Tableau, Power BI), executes parallel distributed queries across stateless worker clusters secured with **Mutual TLS (mTLS)** using a **Native Arrow `RecordBatch` Columnar Engine**, and includes a **Multi-Node Raft HA Bi-Directional Iceberg v3 REST Catalog** with real **AWS STS `AssumeRole` SigV4** credential vending and **OIDC/JWKS (`RS256`)** auth.

---

## Key Capabilities (`v5.0.0`)

- **Native Arrow Columnar Pipeline & Full Arrow Flight SQL:** Parallel Spark-style DAG scheduler with Adaptive Query Execution (AQE), Rendezvous split placement, speculative straggler hedging, Binary Arrow IPC Zstd shuffle (`ARW1`), **Full Apache Arrow Flight SQL (`--flight-addr 0.0.0.0:8815`, including `DoPut` bulk Arrow ingestion & Prepared Statements)**, NVMe spilling (Grace Hash Join, External Merge-Sort, External Hash-Aggregate), and an end-to-end **Native Arrow `RecordBatch` Columnar Pipeline** (`Filter`, `Project`, `HashAggregate`, `ROLLUP`/`CUBE`, `Window`, Joins, Top-N).
- **Bi-Directional Iceberg v3 REST Catalog, Native Types & Hidden Partitioning:** Full OpenAPI read/write Iceberg v3 REST Catalog (`OAuth2`, `namespaces`, `tables`, OCC commits with `409 Conflict`), live `s3://` (AWS/RustFS/MinIO), `gs://`, and `az://` object-store support (`--storage-uri`), native Arrow `Date32`, `Timestamp(Microsecond, UTC)` & `Decimal128(p, s)`, write-time hidden-partition routing (`identity`, `days`, `bucket`, `truncate`) with Layer 0 pruning, recursive JSON Puffin indexes, Roaring Bitmap Deletion Vectors, Apache Avro 1.11 OCF manifests, Liquid Clustering, Time Travel, Branching, and Autonomous Maintainer (`COMPACT`/`VACUUM`).
- **Built-in Multi-Node Raft HA, Cluster mTLS, OIDC/JWKS & Real AWS STS SigV4:** Crash-safe persistent Raft WAL quorum with automatic log compaction & snapshotting, zero-touch multi-coordinator worker failover (`--coordinator-urls`), **Cluster-Internal Mutual TLS (mTLS)** (`RUSTLAKEDB_MTLS_CA`/`CERT`/`KEY`), **OIDC/JWKS `RS256`** + HS256 JWT authentication, and real **AWS STS `AssumeRole` SigV4** (`AWS4-HMAC-SHA256`) credential vending.
- **Full 22-Query Analytical SQL, CBO & Kafka/Redpanda RecordBatch v2 CDC:** Full 22-query analytical decision-support SQL (`DATE +/- INTERVAL`, `EXTRACT`, `BETWEEN`, `CASE` inside aggregates, column-to-column filters, `HAVING` inline aggregates, scalar/IN/EXISTS subqueries, `ROLLUP`/`CUBE`/`GROUPING SETS`, Window Functions, Views & Incremental MVs, multi-table ACID `BEGIN`/`COMMIT`/`ROLLBACK`, HNSW Vector + BM25 search), **Predicate Pushdown Through Joins + Dependency-Aware Multi-Table CBO Join Reordering**, **Kafka/Redpanda RecordBatch v2 (`magic=2`, Zstd, Metadata v1, ListOffsets v1, Fetch v4)** CDC consumer (`--mode kafka-ingest`), Debezium HTTP CDC, `COPY` + DLQ, and Auto-Loader.
- **Enterprise Control Plane, Multi-Arch Releases & Kubernetes:** Live Web Admin Console (`:8181`) with Cluster Topology badges, Interactive SQL Workbench & `CANCEL QUERY` / `POST /v1/queries/{id}/cancel`, Prometheus `/metrics`, OTLP `/v1/traces`, Kubernetes Helm Chart with `autoscaling/v2` HPA + Flight SQL + mTLS + OIDC, and automated multi-arch GitHub Releases (`linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `windows-x86_64`) + multi-arch GHCR images (`ghcr.io/sudeshc/rustlakedb`).

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

### Automated Test Suite (77 Tests Across Waves 1–21 & Full 22-Query Analytics)

```powershell
# Run strict Clippy + all 77 regression tests
cargo clippy --all-targets -- -D warnings
.\scripts\run-tests.ps1
```

---

## Copyright & License

Copyright (C) 2026 **Sudeshc**

RustLakeDB is free software licensed under the **GNU General Public License v3.0 or later** (`GPL-3.0-or-later`). See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT) for full terms.
