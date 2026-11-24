# RFC-004: Cost-Based Join Reorderer (DPccp / Cascades-Lite) with Puffin Sketches

- **Status:** Accepted (Phase 2 Specification; Optimizer Hook in Phase 1)
- **Crates:** `rustlakedb-planner`, `rustlakedb-catalog`

---

## 1. Why Build a Custom CBO Pass?

Rule-based optimizers (including DataFusion's default optimizer) excel at predicate pushdown, projection pruning, and constant folding, but struggle on **complex 5–15 table joins** (TPC-DS, star/snowflake schemas, multi-event JSON joins) where the wrong join order or broadcast choice causes 100x shuffle amplification.

RustLakeDB plugs a dedicated **Cost-Based Join Reorderer & Physical Strategy Selector** directly into the logical-to-physical planning pipeline, backed by real Iceberg + Puffin statistics.

---

## 2. Statistics Collection (Zero Manual `ANALYZE` Needed)

Stats are maintained automatically at write and compaction time inside Iceberg manifests and Puffin files:
- **Table/File Level:** Exact row count, uncompressed/compressed byte sizes (from Iceberg manifest summaries).
- **Column & Shredded JSON Path Level:** Null count, min/max bounds, **NDV (Number of Distinct Values via Puffin Theta Sketches)**, and lightweight Top-K heavy hitters (crucial for pre-detecting join skew before execution!).

---

## 3. Join Enumeration Algorithm (DPccp + Greedy Fallback)

1. **Hypergraph Extraction:** Collapse contiguous `INNER` / `LEFT` / `SEMI` / `ANTI` join trees into a join hypergraph preserving outer-join reordering constraints (Moerkotte & Neumann eligibility rules).
2. **Exact Enumeration (`N <= 12` relations):** Run **DPccp** (Dynamic Programming over Connected Subgraphs) to evaluate all valid bushy and left-deep join trees without considering Cartesian products unless forced.
3. **Large Join Graphs (`N > 12` relations):** Seamlessly fall back to **MinSel / Greedy Bushy Join Ordering** to keep planning latency `< 20 ms`.

---

## 4. Distributed Cost Model

Each candidate join plan is scored by an explicit distributed cost vector:
$$\text{Cost}(P) = w_{\text{cpu}} \cdot \text{Rows}(P) + w_{\text{io}} \cdot \text{ScanBytes}(P) + w_{\text{net}} \cdot \text{ShuffleBytes}(P) + w_{\text{mem}} \cdot \text{BuildBytes}(P)$$

### Physical Join Strategy Selection
For each join node `(L ⋈ R)` inside the optimal tree:
- **Zero-Shuffle Co-Partitioned Join:** If both sides are already `HashPartitioned` on the join key with matching partition counts (e.g., Iceberg bucketed tables or upstream aligned shuffle), cost of network shuffle = `0`.
- **Broadcast Hash Join:** Selected when `min(est_bytes(L), est_bytes(R)) <= broadcast_threshold` (default `64 MB`); network cost = `workers * build_bytes`.
- **Partitioned Grace Hash Join:** Default for large equi-joins; network cost = `bytes(L) + bytes(R)`, with NVMe spill fallback when build side exceeds per-task `MemoryPool`.
- **Runtime Safety Net:** Every static CBO estimate is verified at runtime by **AQE (`RFC-001`)** at staged shuffle barriers, automatically correcting bad estimates via runtime broadcast switches or skew splitting.
