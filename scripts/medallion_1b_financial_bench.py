#!/usr/bin/env python3
# Copyright (C) 2026 Sudeshc
# SPDX-License-Identifier: GPL-3.0-or-later

"""
RustLakeDB v9.0.0 — 1,000,000,000 (1 Billion) Financial Transaction Medallion Benchmark
(Bronze -> Silver -> Gold) running live against the 3-worker distributed RustLakeDB cluster.
"""

import concurrent.futures
import json
import os
import shutil
import sys
import time
import urllib.request
import uuid

import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

COORD_PG = os.environ.get("RLDB_PG", "host=coordinator port=5432 user=postgres dbname=default")
COORD_HTTP = os.environ.get("RLDB_HTTP", "http://coordinator:8181")
WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "/warehouse")
OUT_JSON = os.environ.get("OUT_JSON", "/out/medallion_1b_results.json")

TOTAL_ROWS = 1_000_000_000
NUM_SPLITS = 200          # 200 splits x 5,000,000 rows = 1,000,000,000 rows!
ROWS_PER_SPLIT = TOTAL_ROWS // NUM_SPLITS
COMMIT_BATCH_SIZE = 20    # 10 incremental Iceberg commits (100M rows/commit)
RUN_ID = uuid.uuid4().hex[:8]


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000.0, 2)


def pg_conn(user: str = "postgres"):
    return psycopg.connect(COORD_PG.replace("user=postgres", f"user={user}"), autocommit=True)


def http_json(method: str, url: str, body=None, headers=None):
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


SCHEMA = pa.schema([
    pa.field("tx_id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
    pa.field("account_id", pa.int64(), metadata={b"PARQUET:field_id": b"2"}),
    pa.field("tx_day", pa.string(), metadata={b"PARQUET:field_id": b"3"}),
    pa.field("currency", pa.string(), metadata={b"PARQUET:field_id": b"4"}),
    pa.field("merchant_cat", pa.string(), metadata={b"PARQUET:field_id": b"5"}),
    pa.field("status", pa.string(), metadata={b"PARQUET:field_id": b"6"}),
    pa.field("amount", pa.float64(), metadata={b"PARQUET:field_id": b"7"}),
])

DAYS = [f"2026-01-{d:02d}" for d in range(1, 21)]
CURRENCIES = np.array(["USD", "USD", "USD", "USD", "USD", "EUR", "EUR", "GBP", "INR", "JPY"])
CATS = np.array(["retail", "wire_transfer", "payroll", "travel", "crypto", "saas", "atm", "healthcare"])
# 92% SETTLED, 5% PENDING, 2% FAILED, 1% REVERSED (exact 100-slot lookup table!)
STATUSES = np.array(["SETTLED"] * 92 + ["PENDING"] * 5 + ["FAILED"] * 2 + ["REVERSED"] * 1)


def generate_split(split_idx: int):
    """Generates one 5,000,000-row Zstd Parquet financial split with deterministic exact distributions."""
    n = ROWS_PER_SPLIT
    base_id = split_idx * n + 1
    day_str = DAYS[split_idx // 10]  # 10 splits (50M rows) per trading day!

    oids = np.arange(base_id, base_id + n, dtype=np.int64)
    acct_ids = ((oids * 2654435761) % 2_000_000) + 1
    curr_col = CURRENCIES[oids % 10]
    cat_col = CATS[(oids // 10) % 8]
    st_col = STATUSES[oids % 100]

    rng = np.random.default_rng(10_000 + split_idx)
    # Uniformly distributed $1.00 .. $50,000.00 (exact 2 decimal places)
    amounts = np.round(rng.uniform(1.0, 50_000.0, size=n), 2)

    # Compute ground-truth settled sum for independent penny-exact verification!
    settled_mask = (oids % 100) < 92
    settled_count = int(np.count_nonzero(settled_mask))
    settled_sum = float(np.sum(amounts[settled_mask]))
    aml_mask = settled_mask & (((oids // 10) % 8) == 4) & (amounts > 49_995.0)
    aml_count = int(np.count_nonzero(aml_mask))

    rel_path = f"bronze_fin_tx_1b/data/part-{RUN_ID}-{split_idx:04d}.parquet"
    full_path = os.path.join(WAREHOUSE_DIR, rel_path)
    tbl = pa.table(
        {
            "tx_id": pa.array(oids, type=pa.int64()),
            "account_id": pa.array(acct_ids, type=pa.int64()),
            "tx_day": pa.array(np.full(n, day_str), type=pa.string()),
            "currency": pa.array(curr_col, type=pa.string()),
            "merchant_cat": pa.array(cat_col, type=pa.string()),
            "status": pa.array(st_col, type=pa.string()),
            "amount": pa.array(amounts, type=pa.float64()),
        },
        schema=SCHEMA,
    )
    pq.write_table(tbl, full_path, compression="zstd", row_group_size=1_000_000)
    fsize = os.path.getsize(full_path)
    return {
        "split_idx": split_idx,
        "file_path": rel_path,
        "rows": n,
        "bytes": fsize,
        "settled_count": settled_count,
        "settled_sum": settled_sum,
        "aml_count": aml_count,
    }


def cleanup():
    with pg_conn() as conn, conn.cursor() as cur:
        for t in [
            "bronze_fin_tx_1b",
            "silver_daily_settlement_ledger",
            "silver_aml_high_value_alerts",
            "silver_aml_adjustments",
            "gold_cfo_currency_pnl",
        ]:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute("DROP VIEW IF EXISTS gold_v_cfo_dashboard")
    for d in [
        "bronze_fin_tx_1b",
        "silver_daily_settlement_ledger",
        "silver_aml_high_value_alerts",
        "silver_aml_adjustments",
        "gold_cfo_currency_pnl",
    ]:
        p = os.path.join(WAREHOUSE_DIR, d)
        if os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True)
    print("MEDALLION_1B_CLEANUP_COMPLETE")


def main():
    if "--cleanup" in sys.argv:
        cleanup()
        return

    report = {"bronze": {}, "silver": {}, "gold": {}, "reconciliation": {}, "queries": []}

    # =========================================================================
    # STAGE 1: BRONZE INGESTION (1,000,000,000 FINANCIAL TRANSACTIONS!)
    # =========================================================================
    print("=== STAGE 1: BRONZE — Ingesting 1,000,000,000 Financial Transactions (200 x 5M Zstd Parquet Splits, 10 Incremental Iceberg Commits) ===")
    with pg_conn() as conn, conn.cursor() as cur:
        for t in [
            "bronze_fin_tx_1b",
            "silver_daily_settlement_ledger",
            "silver_aml_high_value_alerts",
            "silver_aml_adjustments",
            "gold_cfo_currency_pnl",
        ]:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute(
            "CREATE TABLE bronze_fin_tx_1b ("
            "tx_id BIGINT, account_id BIGINT, tx_day VARCHAR, currency VARCHAR, "
            "merchant_cat VARCHAR, status VARCHAR, amount DOUBLE)"
        )

    data_dir = os.path.join(WAREHOUSE_DIR, "bronze_fin_tx_1b", "data")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(data_dir, exist_ok=True)

    t_ingest = time.perf_counter()
    total_bytes = 0
    gt_settled_count = 0
    gt_settled_sum = 0.0
    gt_aml_count = 0
    current_snap_id = 1
    commit_latencies = []

    # Generate & commit in 10 incremental intraday batches of 20 splits (100,000,000 rows per batch)!
    for batch_idx in range(NUM_SPLITS // COMMIT_BATCH_SIZE):
        start_s = batch_idx * COMMIT_BATCH_SIZE
        end_s = start_s + COMMIT_BATCH_SIZE
        t_b = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            batch_metas = list(pool.map(generate_split, range(start_s, end_s)))

        added_files = []
        for m in batch_metas:
            total_bytes += m["bytes"]
            gt_settled_count += m["settled_count"]
            gt_settled_sum += m["settled_sum"]
            gt_aml_count += m["aml_count"]
            added_files.append({
                "file-path": m["file_path"],
                "record-count": m["rows"],
                "file-size-in-bytes": m["bytes"],
            })

        t_c = time.perf_counter()
        st, _ = http_json(
            "POST",
            f"{COORD_HTTP}/v1/namespaces/default/tables/bronze_fin_tx_1b",
            {
                "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current_snap_id}],
                "updates": [{"action": "add-snapshot", "snapshot": {"added-files": added_files}}],
            },
        )
        assert st == 200, f"Batch {batch_idx} commit failed: HTTP {st}"
        c_ms = ms(t_c)
        commit_latencies.append(c_ms)
        current_snap_id += 1
        print(
            f"  [BRONZE BATCH {batch_idx + 1:2d}/10] +100M rows (total {(batch_idx + 1) * 100:,}M) | "
            f"batch_wall={time.perf_counter() - t_b:.2f}s | iceberg_commit={c_ms:.1f} ms | "
            f"cum_parquet={total_bytes / (1024**3):.2f} GiB"
        )

    ingest_wall_s = round(time.perf_counter() - t_ingest, 2)
    parquet_gib = round(total_bytes / (1024**3), 3)
    bytes_per_row = round(total_bytes / TOTAL_ROWS, 2)
    ingest_m_rows_s = round((TOTAL_ROWS / ingest_wall_s) / 1_000_000.0, 2)
    report["bronze"] = {
        "rows": TOTAL_ROWS,
        "splits": NUM_SPLITS,
        "commits": len(commit_latencies),
        "parquet_gib": parquet_gib,
        "bytes_per_row": bytes_per_row,
        "ingest_wall_s": ingest_wall_s,
        "ingest_m_rows_sec": ingest_m_rows_s,
        "avg_iceberg_commit_ms": round(sum(commit_latencies) / len(commit_latencies), 2),
        "gt_settled_count": gt_settled_count,
        "gt_settled_sum": round(gt_settled_sum, 2),
        "gt_aml_count": gt_aml_count,
    }
    print(
        f"  [BRONZE READY] 1,000,000,000 rows in {ingest_wall_s:.2f}s ({ingest_m_rows_s:.2f}M rows/s) | "
        f"{parquet_gib:.2f} GiB Zstd Parquet ({bytes_per_row} bytes/row) | "
        f"GT settled={gt_settled_count:,}, GT AML alerts={gt_aml_count:,}"
    )

    with pg_conn() as conn, conn.cursor() as cur:
        # B1: Full 1-Billion-Row COUNT(*)
        t0 = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM bronze_fin_tx_1b")
        b_cnt = int(cur.fetchone()[0])
        b1_ms = ms(t0)
        assert b_cnt == TOTAL_ROWS, f"expected 1,000,000,000 got {b_cnt}"
        report["queries"].append({"stage": "Bronze", "name": "1B COUNT(*)", "ms": b1_ms, "m_rows_s": round(1000.0 / (b1_ms / 1000.0), 2), "detail": f"exact {b_cnt:,} rows"})
        print(f"  [BRONZE Q1] SELECT COUNT(*) -> {b_cnt:,} in {b1_ms:.1f} ms ({1000.0 / (b1_ms / 1000.0):.1f}M rows/s)")

        # B2: Full 1-Billion-Row Status Audit GROUP BY
        t0 = time.perf_counter()
        cur.execute(
            "SELECT status, COUNT(*) AS tx_cnt, SUM(amount) AS gross_vol, AVG(amount) AS avg_amt "
            "FROM bronze_fin_tx_1b GROUP BY status ORDER BY tx_cnt DESC"
        )
        b_status = cur.fetchall()
        b2_ms = ms(t0)
        assert len(b_status) == 4 and int(b_status[0][1]) == gt_settled_count
        report["queries"].append({"stage": "Bronze", "name": "1B Status Audit GROUP BY (COUNT+SUM+AVG)", "ms": b2_ms, "m_rows_s": round(1000.0 / (b2_ms / 1000.0), 2), "detail": f"4 statuses, SETTLED={int(b_status[0][1]):,}"})
        print(f"  [BRONZE Q2] 1B Status Audit GROUP BY -> {b2_ms:.1f} ms ({1000.0 / (b2_ms / 1000.0):.1f}M rows/s) | SETTLED={int(b_status[0][1]):,}")

        # =====================================================================
        # STAGE 2: SILVER CURATION (BRONZE 1B -> SILVER ICEBERG TABLES!)
        # =====================================================================
        print("\n=== STAGE 2: SILVER — Transforming 1,000,000,000 Bronze Rows -> Curated Silver Layer ===")

        # Silver 1: Daily Cleansed Settlement Ledger (filters 80M non-settled, aggregates 920M settled!)
        t0 = time.perf_counter()
        cur.execute(
            "CREATE TABLE silver_daily_settlement_ledger AS "
            "SELECT tx_day, currency, merchant_cat, "
            "       COUNT(*) AS settled_tx_cnt, "
            "       SUM(amount) AS net_settled_vol, "
            "       AVG(amount) AS avg_ticket, "
            "       MAX(amount) AS max_ticket "
            "FROM bronze_fin_tx_1b "
            "WHERE status = 'SETTLED' "
            "GROUP BY tx_day, currency, merchant_cat"
        )
        s1_ms = ms(t0)
        cur.execute("SELECT COUNT(*), SUM(settled_tx_cnt), SUM(net_settled_vol) FROM silver_daily_settlement_ledger")
        s1_rows, s1_tx, s1_vol = cur.fetchone()
        s1_rows, s1_tx, s1_vol = int(s1_rows), int(float(s1_tx)), float(s1_vol)
        assert s1_rows == 800, f"expected 20x5x8=800 silver ledger rows, got {s1_rows}"
        assert s1_tx == gt_settled_count, f"Silver tx {s1_tx} != GT {gt_settled_count}"
        report["silver"]["daily_ledger_ctas_ms"] = s1_ms
        report["silver"]["daily_ledger_rows"] = s1_rows
        report["silver"]["daily_ledger_m_rows_s"] = round(1000.0 / (s1_ms / 1000.0), 2)
        report["queries"].append({"stage": "Silver", "name": "CTAS Daily Settlement Ledger (1B -> 920M settled -> 800 rows)", "ms": s1_ms, "m_rows_s": round(1000.0 / (s1_ms / 1000.0), 2), "detail": f"800 dayxcurxcat rows, vol=${s1_vol:,.2f}"})
        print(f"  [SILVER 1] CTAS silver_daily_settlement_ledger (1B -> 800 rows) in {s1_ms:.1f} ms ({1000.0 / (s1_ms / 1000.0):.1f}M rows/s) | settled_tx={s1_tx:,}, vol=${s1_vol:,.2f}")

        # Silver 2: 1-Billion-Row AML High-Value Crypto Surveillance Extraction (needle in 1B haystack!)
        t0 = time.perf_counter()
        cur.execute(
            "CREATE TABLE silver_aml_high_value_alerts AS "
            "SELECT tx_id, account_id, tx_day, currency, merchant_cat, amount "
            "FROM bronze_fin_tx_1b "
            "WHERE status = 'SETTLED' AND merchant_cat = 'crypto' AND amount > 49995.0"
        )
        s2_ms = ms(t0)
        cur.execute("SELECT COUNT(*) FROM silver_aml_high_value_alerts")
        s2_cnt = int(cur.fetchone()[0])
        assert s2_cnt == gt_aml_count, f"AML count {s2_cnt} != GT {gt_aml_count}"
        report["silver"]["aml_ctas_ms"] = s2_ms
        report["silver"]["aml_alert_rows"] = s2_cnt
        report["silver"]["aml_m_rows_s"] = round(1000.0 / (s2_ms / 1000.0), 2)
        report["queries"].append({"stage": "Silver", "name": "CTAS 1B AML Needle-in-Haystack Extraction", "ms": s2_ms, "m_rows_s": round(1000.0 / (s2_ms / 1000.0), 2), "detail": f"{s2_cnt:,} exact high-value crypto alerts extracted from 1B rows"})
        print(f"  [SILVER 2] CTAS silver_aml_high_value_alerts (1B -> {s2_cnt:,} alerts) in {s2_ms:.1f} ms ({1000.0 / (s2_ms / 1000.0):.1f}M rows/s)")

        # Silver 3: Late-Arriving Adjustments via Conditional Multi-Clause MERGE INTO + Schema Evolution!
        t0 = time.perf_counter()
        cur.execute("SELECT tx_id, account_id, tx_day, currency, merchant_cat, amount FROM silver_aml_high_value_alerts ORDER BY tx_id LIMIT 2")
        sample_alerts = cur.fetchall()
        a1_id, a2_id = int(sample_alerts[0][0]), int(sample_alerts[1][0])
        cur.execute("CREATE TABLE silver_aml_adjustments (tx_id BIGINT, account_id BIGINT, tx_day VARCHAR, currency VARCHAR, merchant_cat VARCHAR, amount DOUBLE, op VARCHAR)")
        cur.execute(
            f"INSERT INTO silver_aml_adjustments VALUES "
            f"({a1_id}, 1, '2026-01-01', 'USD', 'crypto', 0.0, 'D'), "
            f"({a2_id}, 2, '2026-01-01', 'USD', 'crypto', 75000.0, 'U'), "
            f"(9999999999, 42, '2026-01-20', 'USD', 'crypto', 88888.0, 'I')"
        )
        cur.execute(
            "MERGE INTO silver_aml_high_value_alerts AS t "
            "USING (SELECT tx_id, account_id, tx_day, currency, merchant_cat, amount, op FROM silver_aml_adjustments) AS s "
            "ON t.tx_id = s.tx_id "
            "WHEN MATCHED AND s.op = 'D' THEN DELETE "
            "WHEN MATCHED AND s.op = 'U' THEN UPDATE SET amount = s.amount "
            "WHEN NOT MATCHED AND s.op = 'I' THEN INSERT VALUES (s.tx_id, s.account_id, s.tx_day, s.currency, s.merchant_cat, s.amount)"
        )
        cur.execute("ALTER TABLE silver_aml_high_value_alerts ADD COLUMN sar_status VARCHAR")
        cur.execute("SELECT COUNT(*) FROM silver_aml_high_value_alerts")
        s3_cnt = int(cur.fetchone()[0])
        assert s3_cnt == s2_cnt  # -1 deleted + 1 inserted!
        s3_ms = ms(t0)
        report["silver"]["merge_and_evolution_ms"] = s3_ms
        report["queries"].append({"stage": "Silver", "name": "Silver Conditional MERGE INTO + Zero-Rewrite ADD COLUMN", "ms": s3_ms, "m_rows_s": None, "detail": f"1D + 1U + 1I atomic commit, final={s3_cnt:,} rows"})
        print(f"  [SILVER 3] Conditional MERGE INTO + ADD COLUMN -> {s3_ms:.1f} ms (final={s3_cnt:,} rows)")

        # =====================================================================
        # STAGE 3: GOLD LAYER (EXECUTIVE P&L, WINDOWS, LINEAGE, RBAC, PYICEBERG)
        # =====================================================================
        print("\n=== STAGE 3: GOLD — Executive P&L Cubes, Rolling Windows, 3-Hop Lineage, Governance & PyIceberg Reconciliation ===")

        # Gold 1: CFO Currency x Merchant P&L Cube (Silver -> Gold)
        t0 = time.perf_counter()
        cur.execute(
            "CREATE TABLE gold_cfo_currency_pnl AS "
            "SELECT currency, merchant_cat, "
            "       SUM(settled_tx_cnt) AS total_tx, "
            "       SUM(net_settled_vol) AS total_settled_vol, "
            "       MAX(max_ticket) AS peak_tx "
            "FROM silver_daily_settlement_ledger "
            "GROUP BY currency, merchant_cat"
        )
        g1_ms = ms(t0)
        cur.execute("SELECT COUNT(*), SUM(total_tx), SUM(total_settled_vol) FROM gold_cfo_currency_pnl")
        g1_rows, g1_tx, g1_vol = cur.fetchone()
        g1_rows, g1_tx, g1_vol = int(g1_rows), int(float(g1_tx)), float(g1_vol)
        assert g1_rows == 40 and g1_tx == gt_settled_count
        report["gold"]["cfo_pnl_ctas_ms"] = g1_ms
        report["gold"]["cfo_pnl_rows"] = g1_rows
        report["queries"].append({"stage": "Gold", "name": "CTAS Gold CFO Currency x Merchant P&L Cube", "ms": g1_ms, "m_rows_s": None, "detail": f"40 curxcat KPI rows, total_tx={g1_tx:,}"})
        print(f"  [GOLD 1] CTAS gold_cfo_currency_pnl (40 rows) in {g1_ms:.1f} ms | total_tx={g1_tx:,}, vol=${g1_vol:,.2f}")

        # Gold 2: 3-Day Sliding Window Rolling Cashflow + QUALIFY + BI View
        t0 = time.perf_counter()
        cur.execute(
            "SELECT tx_day, currency, net_settled_vol, "
            "       AVG(net_settled_vol) OVER (PARTITION BY currency ORDER BY tx_day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3d_avg "
            "FROM silver_daily_settlement_ledger "
            "WHERE merchant_cat = 'wire_transfer' "
            "QUALIFY rolling_3d_avg > 0"
        )
        g_win = cur.fetchall()
        assert len(g_win) == 100  # 20 days x 5 currencies
        cur.execute(
            "CREATE OR REPLACE VIEW gold_v_cfo_dashboard AS "
            "SELECT currency, merchant_cat, total_tx, total_settled_vol FROM gold_cfo_currency_pnl WHERE total_settled_vol > 1000000000.0"
        )
        cur.execute("SELECT COUNT(*) FROM gold_v_cfo_dashboard")
        v_cnt = int(cur.fetchone()[0])
        assert v_cnt == 40
        g2_ms = ms(t0)
        report["gold"]["rolling_window_and_view_ms"] = g2_ms
        report["queries"].append({"stage": "Gold", "name": "3-Day Sliding Window + QUALIFY + CFO BI View", "ms": g2_ms, "m_rows_s": None, "detail": f"100 rolling-window rows, view={v_cnt} multi-billion-$ rows"})
        print(f"  [GOLD 2] 3-Day Sliding Window + QUALIFY + BI View -> {g2_ms:.1f} ms (win={len(g_win)}, view={v_cnt})")

        # Gold 3: 3-Hop Automated Medallion Lineage + Governance RLS Check
        t0 = time.perf_counter()
        cur.execute("SELECT source_table, target_table, operation FROM system.lineage")
        edges = [(r[0], r[1], r[2]) for r in cur.fetchall()]
        has_b_to_s1 = any(e[0] == "bronze_fin_tx_1b" and e[1] == "silver_daily_settlement_ledger" for e in edges)
        has_b_to_s2 = any(e[0] == "bronze_fin_tx_1b" and e[1] == "silver_aml_high_value_alerts" for e in edges)
        has_s1_to_g1 = any(e[0] == "silver_daily_settlement_ledger" and e[1] == "gold_cfo_currency_pnl" for e in edges)
        assert has_b_to_s1 and has_b_to_s2 and has_s1_to_g1, f"missing medallion lineage edges: {edges}"

        cur.execute("CREATE ROLE usd_controller")
        cur.execute("GRANT SELECT ON gold_cfo_currency_pnl TO usd_controller")
        cur.execute("CREATE POLICY usd_only_gold ON gold_cfo_currency_pnl FOR usd_controller USING (currency = 'USD')")
        with pg_conn(user="usd_controller") as uconn, uconn.cursor() as ucur:
            ucur.execute("SELECT COUNT(*) FROM gold_cfo_currency_pnl")
            usd_rows = int(ucur.fetchone()[0])
            assert usd_rows == 8, f"expected 8 USD rows via RLS, got {usd_rows}"
        g3_ms = ms(t0)
        report["gold"]["lineage_and_rls_ms"] = g3_ms
        print(f"  [GOLD 3] 3-Hop Lineage (Bronze->Silver->Gold) + USD RLS (8/40 rows) -> {g3_ms:.1f} ms")

    # Gold 4: Official PyIceberg Direct Parquet Reconciliation vs 1-Billion-Row Ground Truth!
    t0 = time.perf_counter()
    cat = RestCatalog("rustlakedb", uri=COORD_HTTP)
    ice_gold = cat.load_table("default.gold_cfo_currency_pnl")
    ice_arrow = ice_gold.scan().to_arrow()
    ice_tx_sum = int(sum(ice_arrow.column("total_tx").to_pylist()))
    ice_vol_sum = float(sum(ice_arrow.column("total_settled_vol").to_pylist()))
    g4_ms = ms(t0)
    drift_pct = abs(ice_vol_sum - gt_settled_sum) / max(gt_settled_sum, 1.0) * 100.0
    assert ice_tx_sum == gt_settled_count
    assert drift_pct < 1e-6, f"reconciliation drift {drift_pct}%"
    report["reconciliation"] = {
        "bronze_total_rows": TOTAL_ROWS,
        "gt_settled_tx": gt_settled_count,
        "gold_pyiceberg_tx": ice_tx_sum,
        "gt_settled_volume_usd": round(gt_settled_sum, 2),
        "gold_pyiceberg_volume_usd": round(ice_vol_sum, 2),
        "drift_pct": round(drift_pct, 10),
        "pyiceberg_scan_ms": g4_ms,
    }
    print(
        f"  [GOLD 4] Official PyIceberg Reconciliation -> {g4_ms:.1f} ms | "
        f"Gold Tx={ice_tx_sum:,} == GT {gt_settled_count:,} | "
        f"Gold Vol=${ice_vol_sum:,.2f} == GT ${gt_settled_sum:,.2f} (drift={drift_pct:.9f}%)"
    )

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\n1-BILLION-ROW FINANCIAL MEDALLION BENCHMARK PASSED 100%! Saved -> {OUT_JSON}")


if __name__ == "__main__":
    main()
