# RustLakeDB

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-100%25-orange.svg)](Cargo.toml)
[![Version](https://img.shields.io/badge/Version-2.0.0-brightgreen.svg)](CHANGELOG.md)

**RustLakeDB** is a 100% Rust, cloud-agnostic, distributed SQL and lakehouse database engine built natively over **Apache Iceberg v3** (Zstd Parquet, Shredded `VARIANT` JSON, Puffin Secondary Indexes, Roaring Bitmap Deletion Vectors, and Apache Avro OCF Manifests).

It speaks the **PostgreSQL v3 Wire Protocol** (compatible with DBeaver, `psql`, JDBC, and BI clients) over **TLS 1.3** and **SCRAM-SHA-256**, executes parallel distributed queries across stateless worker clusters with **Vectorized Arrow** operators, and includes an embedded **Multi-Node Raft HA Iceberg v3 REST Catalog** and Web Control Plane.

---

## Key Capabilities (`v2.0.0`)

- **Vectorized Distributed Execution:** Parallel Spark-style DAG scheduler with Adaptive Query Execution (AQE partition coalescing & skew-join splitting), Rendezvous split placement, speculative straggler hedging, Binary Arrow IPC Zstd shuffle (`ARW1`), Grace Hash Join NVMe spill, Vectorized Arrow Partial Aggregation, Zero-Parse Shredded JSON aggregation, Vectorized Arrow Joins, and Vectorized Arrow Top-N Pushdown.
- **Apache Iceberg v3 Native Storage:** Zstd Parquet with `ProjectionMask` + Zone-Map min/max pruning, recursive JSON Puffin Bloom/Inverted indexes, Roaring Bitmap Deletion Vectors (`UPDATE`, `DELETE`, `MERGE INTO`), genuine Apache Avro 1.11 OCF manifests & manifest lists, Liquid Multi-Column Clustering (`CLUSTER BY`, `OPTIMIZE TABLE`), Time Travel (`AT VERSION`), Git-style table branching (`CREATE BRANCH` / `MERGE BRANCH`), and an Autonomous Maintainer daemon (`COMPACT` / `VACUUM`).
- **Built-in Multi-Node Raft HA & Iceberg v3 REST Catalog:** Crash-safe persistent Raft WAL quorum (`--raft-node-id`), leader election & failover, Temporary STS Credential Vending (`/v1/namespaces/{ns}/tables/{table}/credentials`), and dynamic worker self-registration with TTL eviction.
- **Rich SQL, AI & Lakehouse Ingestion:** Full `dbt`-ready DDL (`CREATE/DROP SCHEMA`, `IF NOT EXISTS`/`IF EXISTS`, `ALTER TABLE RENAME TO`, `TRUNCATE TABLE`, `CTAS`, `INSERT ... SELECT`), multi-table ACID `BEGIN`/`COMMIT`/`ROLLBACK`, Views & True Incremental Materialized Views, Window Functions, `UNION`/`UNION ALL`, `IN`/`NOT IN`/`EXISTS`/`NOT EXISTS` subqueries, 3+ table CBO join ordering, SQL Scalar UDFs (`CREATE FUNCTION`), HNSW Vector Search (`VECTOR_COSINE`, `VECTOR_L2`, `<->`), `BM25_SCORE` full-text search, `COPY` (CSV/NDJSON/Parquet + DLQ), HTTP NDJSON ingest, and an exactly-once Auto-Loader daemon (`--mode autoloader`).
- **Enterprise Governance, Security & Control Plane:** TLS 1.3 PGWire (`SSLRequest -> 'S'`), RFC 5802 SCRAM-SHA-256, HMAC-SHA256 JWT Bearer Auth, RBAC (`CREATE ROLE`, `GRANT`, `REVOKE`), Row-Level Security (`CREATE POLICY`), Dynamic Column & JSON-Path Masking, `EXPLAIN ANALYZE`, `system.query_history`, `system.audit_log`, embedded Web SQL Workbench UI, Prometheus `/metrics`, Zero-Copy Arrow IPC SQL endpoint (`POST /v1/sql/arrow`), Docker Compose, and Kubernetes Helm Chart.

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

### Automated Test Suite (54 Tests Across Waves 1–10)

```powershell
# Run strict Clippy + all 54 regression tests
cargo clippy --all-targets -- -D warnings
.\scripts\run-tests.ps1
```

---

## Copyright & License

Copyright (C) 2026 **Sudeshc**

RustLakeDB is free software licensed under the **GNU General Public License v3.0 or later** (`GPL-3.0-or-later`). See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT) for full terms.
