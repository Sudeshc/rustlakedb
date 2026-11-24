# RustLakeDB — Master Architecture & Decision Record

**RustLakeDB** is a 100% Rust, cloud-agnostic, distributed SQL and lakehouse database engine operating natively over **Apache Iceberg v3** storage. It combines **Postgres-compatible ANSI SQL**, **ultra-fast sub-second interactive execution**, **massive Spark-style fault-tolerant distributed joins/aggregations/ETLs**, **built-in zero-external-metastore metadata management**, and **first-class JSON querying & secondary indexing**.

---

## Core Architecture (4 Composable Planes, 1 Static Binary)

1. **Stateless Frontend & Optimizer Plane (`rustlakedb-gateway`, `rustlakedb-planner`)**
   - Multi-protocol ingress: PostgreSQL wire protocol (`pgwire` v3), Arrow Flight SQL, and HTTP/REST.
   - ANSI SQL:2016 + Postgres dialect (`pg_catalog`, CTEs, window functions, `LATERAL`, `MERGE`, `UPDATE`/`DELETE`, JSONB operators).
   - Custom **Cost-Based Join Reorderer (CBO)** (`RFC-004`) + **Adaptive Query Execution (AQE)** (`RFC-001`).
2. **Control & Metadata Plane (`rustlakedb-coordinator`, `rustlakedb-catalog`)**
   - Embedded **`openraft` consensus quorum** — zero ZooKeeper, zero Hive Metastore, zero JVM.
   - Built-in **Apache Iceberg v3 REST Catalog** (`RFC-002`) with multi-table ACID transactions, Git-style zero-copy branching/tagging, schema evolution, RBAC, and JSON-path column masking.
   - Locality-aware distributed DAG scheduler using **Rendezvous Consistent Hashing**.
3. **Elastic Distributed Compute Plane (`rustlakedb-worker`, `rustlakedb-shuffle`)**
   - Stateless auto-scaling Rust workers executing vectorized columnar plans with strict `MemoryPool` disk spilling.
   - **Dual-Mode Hybrid Shuffle (`RFC-001`)**: pipelined Arrow Flight gRPC streams for sub-second interactive queries; fault-tolerant NVMe/S3 staged shuffles with task-level retries and runtime AQE skew splitting for multi-TB Spark-scale jobs.
   - Local hybrid RAM + NVMe caching (`foyer`) for Parquet footers, Iceberg manifests, and Puffin indexes.
4. **Cloud-Agnostic Iceberg v3 Storage & JSON Engine (`rustlakedb-storage`)**
   - Unified async object storage over AWS S3 (incl. S3 Express One Zone), GCP GCS, Azure Blob/ADLS Gen2, and on-prem MinIO/Ceph/POSIX.
   - **Three-Layer JSON Engine (`RFC-003`)**: Iceberg v3 `VARIANT` binary + automatic Parquet sub-column shredding + compaction-aware Puffin FST/Roaring inverted & split-block bloom indexes.
   - Autonomous background maintainer for compaction, Hilbert/Liquid clustering, dynamic JSON re-shredding, Deletion Vector cleanup, and `VACUUM`.

---

## Technical RFC Index

| RFC | Title | File |
| :--- | :--- | :--- |
| **RFC-001** | Distributed Physical Plan, Stage DAG, Hybrid Shuffle & AQE | [RFC-001](rfcs/RFC-001-distributed-execution-and-aqe.md) |
| **RFC-002** | Built-in `openraft` Iceberg v3 REST Catalog, Multi-Table ACID & Autonomous Maintainer | [RFC-002](rfcs/RFC-002-openraft-iceberg-v3-catalog.md) |
| **RFC-003** | Three-Layer JSON Engine: `VARIANT` Auto-Shredding & Puffin Secondary Indexes | [RFC-003](rfcs/RFC-003-json-variant-shredding-and-puffin-indexes.md) |
| **RFC-004** | Cost-Based Join Reorderer (DPccp / Cascades-Lite) with Puffin Sketches | [RFC-004](rfcs/RFC-004-cost-based-join-reorderer.md) |

---

## Architectural Decision Log (ADR)

| ID | Date | Decision | Status |
| :--- | :--- | :--- | :--- |
| **ADR-001** | 2026-11-24 | 100% Rust implementation compiled into a single static binary with composable roles (`standalone`, `local`, `coordinator`, `worker`, `gateway`, `maintainer`). | Accepted |
| **ADR-002** | 2026-11-24 | Native Apache Iceberg v3 table format with built-in `openraft` REST Catalog (Delta-Lake-like zero-external-metastore deployment UX). | Accepted |
| **ADR-003** | 2026-11-24 | Distributed-first architecture from Commit 1, with an in-process `local[N]` multi-worker cluster mode executing identical RPC/plan/shuffle code paths. | Accepted |
| **ADR-004** | 2026-11-24 | Dual-mode hybrid shuffle (Arrow Flight pipelined streaming + fault-tolerant NVMe/S3 staged shuffle) with runtime AQE. | Accepted |
| **ADR-005** | 2026-11-24 | Three-layer JSON architecture: Iceberg v3 `VARIANT` + automatic Parquet shredding + compaction-aware Puffin inverted & bloom indexes. | Accepted |
| **ADR-006** | 2026-11-24 | Full standard SQL + multi-protocol frontend (`pgwire` v3, Arrow Flight SQL, REST). | Accepted |
| **ADR-007** | 2026-11-24 | Pragmatic Reuse vs. Build-New: reuse `arrow-rs`, `parquet_variant`, `object_store`, `foyer`, `openraft`, `pgwire`, and DataFusion single-node operator/spilling crates; build new CBO, Distributed AQE Scheduler/Shuffle, JSON Shredder/Puffin Index Engine, and Built-in Iceberg v3 Catalog/Maintainer. | Accepted |
| **ADR-008** | 2026-11-24 | Project named **RustLakeDB** (`rustlakedb` CLI & crate namespace) after verifying no existing conflicting database or crate on crates.io/GitHub. | Accepted |
