# RustLakeDB Benchmarks & Methodology

Reproduce all numbers below with a single command:

```powershell
.\scripts\run-tpch-bench.ps1 -Rows 10000000 -Splits 40 -Workers 4
```

## 10,000,000-Row Analytical Suite (`v6.0.0` vs `v7.0.0` / `v8.0.0`)

- **Dataset:** `10,000,000` rows in `big_orders` (`40` Zstd Parquet + Puffin splits) × `20,000` rows in `big_customers` (`4` splits), `38.74 MB` compressed on disk.
- **Engine Mode:** `--mode gateway --workers 4` inside `rustlakedb` release container.

| Query | `v6.0.0` Engine | `v7.0.0+` Engine | Throughput | Speedup |
|---|---:|---:|---:|---:|
| **Q1:** Full Scan + `GROUP BY region` (`COUNT`, `SUM`) | `498 ms` | **`441 ms`** | **22.7M rows/s** | **1.13×** |
| **Q2:** Filtered `WHERE amount > 450` + `GROUP BY` + `AVG` | `2,705 ms` | **`371 ms`** | **27.0M rows/s** | **7.29×** |
| **Q3:** Distributed Top-N (`ORDER BY amount DESC LIMIT 10`) | `479 ms` | **`549 ms`** | **18.2M rows/s** | — |
| **Q4:** `10M × 20K` Hash Join + `GROUP BY tier` | `1,605 ms` | **`1,518 ms`** | **6.6M rows/s** | **1.06×** |
| **Q5:** Zero-Rewrite `ALTER TABLE ... RENAME COLUMN` | `2 ms` | **`2 ms`** | — | `0` rewrites |
| **Q6:** Post-Evolution `GROUP BY` on renamed `gross_amount` | `7,515 ms` | **`398 ms`** | **25.1M rows/s** | **18.9×** |

## 22-Query Analytical Decision-Support Suite (TPC-H Q1–Q22)

All 22 analytical decision-support queries (`Q1`–`Q22`) execute end-to-end across our distributed DAG engine with CBO predicate pushdown, dependency-aware multi-table join reordering, and vectorized Arrow execution (`crates/rustlakedb-testkit/tests/wave19_tpch_22_and_cbo.rs`).

## Live External Ecosystem Latencies

| Integration | Operation | Verified Result |
|---|---|---|
| **ADBC Arrow Flight SQL** (`:8815`) | `999,984`-row `GROUP BY` -> PyArrow `Table` | **`310.9 ms`** |
| **RustFS S3** (`s3://warehouse`) | Distributed 2-worker S3 `GROUP BY` | **`29 ms`** |
| **Cloud `COPY TO` / `COPY FROM`** (`s3://`) | Parquet export / import round-trip | **`22 ms` / `93 ms`** |
| **PyIceberg** (`table.scan().to_arrow()`) | Direct Parquet read via Avro Manifests + `PARQUET:field_id` | **100% exact** |
| **Redpanda CDC** (`Fetch v4` Snappy) | Live Debezium `c/u/d` -> ACID Iceberg v3 | **100% exact** |
