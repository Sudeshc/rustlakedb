# RFC-003: Three-Layer JSON Engine — `VARIANT` Auto-Shredding & Puffin Secondary Indexes

- **Status:** Accepted (Phase 3 Specification; Metadata Hooks in Phase 1)
- **Crates:** `rustlakedb-storage`, `rustlakedb-planner`, `rustlakedb-catalog`

---

## 1. Problem Statement

Semi-structured JSON (`JSON` / `JSONB` / `VARIANT`) in cloud data lakes historically suffers from two failures:
1. **Scanning opaque JSON strings** burns 80%+ CPU parsing text and skips Parquet columnar compression and min/max pruning.
2. **Unclustered JSON filters, nested array containment (`@>`), and full-text lookups** cannot be pruned by Parquet min/max stats, forcing full-table S3 scans.

RustLakeDB solves both using a **Three-Layer JSON Architecture** built 100% on open **Apache Iceberg v3 + Parquet + Puffin** standards.

---

## 2. Layer 1: Iceberg v3 `VARIANT` + Automatic Parquet Shredding

### Storage Layout
Every `JSONB` / `VARIANT` column maps to an Iceberg v3 `VARIANT` column stored in Parquet via `parquet_variant`:
- **Shredded Typed Sub-Columns (`typed_value`):** Frequently occurring JSON paths are automatically extracted into strongly-typed, dictionary/RLE-compressed Parquet leaf columns (e.g., `payload.user.id: INT64`, `payload.event_type: UTF8`, `payload.amount: DECIMAL(18,2)`).
- **Binary Variant Remainder (`metadata` + `value`):** Rare keys, deeply nested ad-hoc blobs, and type-drifted values (e.g., a row where `amount` arrived as `"unknown"`) are stored losslessly in the compact self-describing binary `VARIANT` blob.

### Automatic Path Inference & Dynamic Re-Shredding
1. **Write-Time Sampling:** On initial batch writes (`INSERT` / `COPY`), the writer samples the first *K* rows (e.g., 4,096 rows) and promotes paths appearing in `>= 5%` of rows with a dominant scalar type (up to a configurable cap, default 128 shredded paths/column) into the file's `ShreddedSchemaBuilder`.
2. **Query-Driven Compaction Re-Shredding:** Gateways record lightweight counters of queried JSON paths (`payload.device.os`, `payload.geo.country`). During background compaction (`RFC-002`), the Maintainer unions write-frequency + query-heat to promote hot paths into typed Parquet columns and demote cold paths back into the binary remainder.
3. **Zero-Parse Execution:** DataFusion struct-field pushdown reads *only* the targeted shredded Parquet column with full row-group min/max pruning—falling back to binary `VARIANT` decoding only for rows where `typed_value` is null and `value` is non-null.

---

## 3. Layer 2: Compaction-Aware Puffin Secondary Indexes

To accelerate arbitrary point lookups, JSON array containment, and text search across non-clustered data, RustLakeDB writes **file-scoped secondary index blobs inside Iceberg Puffin files** (registered under `/tables/{tbl}/indexes` in the built-in catalog).

### Index Types

| Index Type | Internal Data Structure (Rust) | Target Query Patterns |
| :--- | :--- | :--- |
| **`JSON_INVERTED`** | `fst::Map` (compressed sorted `(path_id, value/token)` dictionary) -> `RoaringBitmap` of local Parquet row offsets | `doc->>'status' = 'failed'`, `doc @> '{"tags": ["vip"]}'`, `doc ? 'error_code'`, full-text `doc->>'msg' @@ 'timeout'` |
| **`JSON_BLOOM`** | Split-Block xxHash3 Bloom Filter (~10 bits/key, SIMD probe) per configured JSON path | High-cardinality lookups: `doc->>'trace_id' = '...'`, `doc->>'order_uuid' = '...'` |
| **`THETA_SKETCH`** | Compact distinct-count sketch + equi-depth histogram per shredded path | Feeds accurate cardinality/NDV estimates into the CBO (`RFC-004`) |

### Multi-Stage Query Pruning Pipeline
When evaluating a JSON predicate on an Iceberg table, the worker executes a strict cheap-to-expensive funnel:
1. **Iceberg Manifest Pruning:** Partition + column-level min/max stats (RAM).
2. **Puffin Bloom Filter Check:** 1–4 KB probe per candidate Parquet file -> eliminates ~99% of non-matching files before touching Parquet data.
3. **Puffin Inverted Index Intersect:** Lookup `(path, value)` in `fst::Map`, intersect `RoaringBitmap`s across `AND`/`OR` predicates -> yields exact matching `RowSelection` bitmask! If empty, skip Parquet file completely.
4. **Late-Materialized Parquet Scan:** Pass the `RowSelection` mask directly into `parquet-rs` `ArrowReaderBuilder::with_row_selection(...)` so only matching Parquet pages are fetched and decoded.

### Compaction-Aware Lifecycle
Because every Puffin index blob is immutably bound to one Parquet `file_id`, there is **zero index drift**:
- New inserts generate a Puffin file alongside each new Parquet file.
- Background compaction merges `N` Parquet files into `1` compacted Parquet file and builds the merged Puffin index in the exact same streaming pass, committing both atomically in one Iceberg snapshot.

---

## 4. Layer 3: Full SQL & Postgres JSONB Surface

Users manage JSON indexes with standard declarative DDL:
```sql
CREATE TABLE events (
    ts TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
) WITH (
    json_auto_shred = true,
    json_max_shredded_paths = 128
);

CREATE INDEX idx_events_payload_inv ON events USING INVERTED (payload);
CREATE INDEX idx_events_trace_bloom ON events USING BLOOM ((payload->>'trace_id'));
```

Supported query operators automatically rewritten to shredded column scans + Puffin index pushdown:
- **Postgres JSONB:** `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `jsonb_array_elements`, `jsonb_path_query`.
- **ANSI SQL:2016 JSON:** `JSON_VALUE`, `JSON_QUERY`, `JSON_EXISTS`, `JSON_TABLE`.
