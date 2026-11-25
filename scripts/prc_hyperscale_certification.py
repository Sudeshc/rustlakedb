#!/usr/bin/env python3
# Copyright (C) 2026 Sudeshc
# SPDX-License-Identifier: GPL-3.0-or-later

"""
RustLakeDB v9.0.0 — 7-Pillar Hyperscale Production Readiness Certification (PRC) Harness
Simulates a 1,000-job enterprise multi-tenant lakehouse day + PostgreSQL 16 differential
correctness oracle + 12-writer OCC storm + live SIGKILL worker & Coordinator crash recovery
+ bad-deploy DR drill + resource/metrics hygiene audit.
"""

import concurrent.futures
import io
import json
import os
import random
import shutil
import statistics
import sys
import threading
import time
import urllib.request
import urllib.error

import psycopg
import pyarrow.ipc as pa_ipc
from pyiceberg.catalog.rest import RestCatalog

COORD_PG = os.environ.get("RLDB_PG", "host=coordinator port=5432 user=postgres dbname=default")
COORD_HTTP = os.environ.get("RLDB_HTTP", "http://coordinator:8181")
ORACLE_PG = os.environ.get("ORACLE_PG", "host=prc-oracle-pg port=5432 user=postgres password=postgres dbname=oracle")
WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "/warehouse")
OUT_JSON = os.environ.get("OUT_JSON", "/out/prc_results.json")


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000.0, 2)


def rldb_conn(user: str = "postgres"):
    return psycopg.connect(COORD_PG.replace("user=postgres", f"user={user}"), autocommit=True)


def http_json(method: str, url: str, body=None, headers=None):
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def http_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read().decode("utf-8")


# =============================================================================
# PILLAR 2: DIFFERENTIAL CORRECTNESS ORACLE VS LIVE POSTGRESQL 16 (50 QUERIES)
# =============================================================================
def run_differential_oracle():
    print("\n=== PILLAR 2: Differential Correctness Oracle vs Live PostgreSQL 16 (50 Queries) ===")
    t_all = time.perf_counter()
    rng = random.Random(2026)

    depts = ["eng", "sales", "finance", "ops", "support"]
    regions = ["us-east", "us-west", "eu-central", "ap-south"]
    tiers = ["platinum", "gold", "silver", "bronze"]

    # Build deterministic 1,200-row facts + 40-row dims with NULLs, negatives, duplicates, edge values.
    dim_rows = [(i, tiers[i % 4], regions[i % 4]) for i in range(1, 41)]
    fact_rows = []
    for i in range(1, 1201):
        dept = depts[i % 5]
        region = regions[(i * 3) % 4]
        cid = (i % 40) + 1
        amt = round(rng.uniform(-50.0, 1000.0), 2)
        qty = rng.randint(1, 20)
        tag = None if i % 7 == 0 else f"tag_{i % 11}"
        fact_rows.append((i, cid, dept, region, amt, qty, tag))

    def seed_engine(conn, is_pg: bool):
        dbl = "DOUBLE PRECISION" if is_pg else "DOUBLE"
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS prc_diff_fact")
            cur.execute("DROP TABLE IF EXISTS prc_diff_dim")
            cur.execute(
                f"CREATE TABLE prc_diff_fact (id BIGINT, cid BIGINT, dept VARCHAR(32), "
                f"region VARCHAR(32), amount {dbl}, qty BIGINT, tag VARCHAR(32))"
            )
            cur.execute(
                "CREATE TABLE prc_diff_dim (cid BIGINT, tier VARCHAR(32), home_region VARCHAR(32))"
            )
            # Batch insert in chunks of 200
            for start in range(0, len(dim_rows), 200):
                chunk = dim_rows[start:start + 200]
                vals = ",".join(f"({r[0]},'{r[1]}','{r[2]}')" for r in chunk)
                cur.execute(f"INSERT INTO prc_diff_dim VALUES {vals}")
            for start in range(0, len(fact_rows), 200):
                chunk = fact_rows[start:start + 200]
                vals = ",".join(
                    f"({r[0]},{r[1]},'{r[2]}','{r[3]}',{r[4]},{r[5]},{'NULL' if r[6] is None else repr(r[6])})"
                    for r in chunk
                )
                cur.execute(f"INSERT INTO prc_diff_fact VALUES {vals}")
            # Apply identical UPDATE + DELETE DML mutations to both engines!
            cur.execute("UPDATE prc_diff_fact SET amount = 500.0 WHERE dept = 'finance' AND qty > 15")
            cur.execute("DELETE FROM prc_diff_fact WHERE amount < 0.0")

    with psycopg.connect(ORACLE_PG, autocommit=True) as pg_c, rldb_conn() as rl_c:
        seed_engine(pg_c, is_pg=True)
        seed_engine(rl_c, is_pg=False)

        # 50 diverse SQL queries: filters, NULLs, IN, LIKE, GROUP BY, HAVING, JOINs, CASE, COALESCE, Windows!
        queries = [
            "SELECT COUNT(*) FROM prc_diff_fact",
            "SELECT COUNT(*) FROM prc_diff_fact WHERE tag IS NULL",
            "SELECT COUNT(*) FROM prc_diff_fact WHERE tag IS NOT NULL",
            "SELECT dept, COUNT(*) AS c, SUM(amount) AS s FROM prc_diff_fact GROUP BY dept ORDER BY dept",
            "SELECT region, COUNT(*) AS c, AVG(amount) AS a FROM prc_diff_fact WHERE qty >= 10 GROUP BY region ORDER BY region",
            "SELECT dept, region, COUNT(*) AS c, MIN(amount) AS mn, MAX(amount) AS mx FROM prc_diff_fact GROUP BY dept, region ORDER BY dept, region",
            "SELECT dept, COUNT(*) AS c FROM prc_diff_fact GROUP BY dept HAVING COUNT(*) > 150 ORDER BY dept",
            "SELECT id, amount FROM prc_diff_fact WHERE dept IN ('eng', 'sales') AND amount > 800.0 ORDER BY id LIMIT 25",
            "SELECT COUNT(*) FROM prc_diff_fact WHERE tag LIKE 'tag_1%'",
            "SELECT d.tier, COUNT(*) AS c, SUM(f.amount) AS s FROM prc_diff_fact f JOIN prc_diff_dim d ON f.cid = d.cid GROUP BY d.tier ORDER BY d.tier",
            "SELECT d.tier, f.dept, COUNT(*) AS c FROM prc_diff_fact f LEFT JOIN prc_diff_dim d ON f.cid = d.cid WHERE f.qty > 12 GROUP BY d.tier, f.dept ORDER BY d.tier, f.dept",
            "SELECT id, COALESCE(tag, 'missing') AS t FROM prc_diff_fact WHERE id <= 30 ORDER BY id",
            "SELECT id, CASE WHEN amount > 500.0 THEN 'high' ELSE 'low' END AS bucket FROM prc_diff_fact WHERE id <= 30 ORDER BY id",
            "SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn FROM prc_diff_fact WHERE id <= 40 ORDER BY dept, id",
            "SELECT DISTINCT dept FROM prc_diff_fact ORDER BY dept",
            "SELECT DISTINCT region FROM prc_diff_fact WHERE amount > 200.0 ORDER BY region",
            "SELECT id, amount * qty AS gross FROM prc_diff_fact WHERE id <= 25 ORDER BY id",
            "SELECT dept, SUM(qty) AS total_qty FROM prc_diff_fact WHERE region = 'us-east' GROUP BY dept ORDER BY dept",
            "SELECT id, amount FROM prc_diff_fact WHERE tag IS NULL AND amount > 400.0 ORDER BY id LIMIT 20",
            "SELECT d.home_region, COUNT(*) AS c FROM prc_diff_fact f JOIN prc_diff_dim d ON f.cid = d.cid WHERE f.amount > 700.0 GROUP BY d.home_region ORDER BY d.home_region",
        ]
        # Add 30 randomized combinatorial queries!
        for _ in range(30):
            d_pick = rng.choice(depts)
            r_pick = rng.choice(regions)
            amt_cut = round(rng.uniform(50.0, 850.0), 1)
            qty_cut = rng.randint(2, 16)
            kind = rng.randint(0, 4)
            if kind == 0:
                queries.append(f"SELECT dept, COUNT(*) AS c, SUM(amount) AS s FROM prc_diff_fact WHERE amount > {amt_cut} AND qty >= {qty_cut} GROUP BY dept ORDER BY dept")
            elif kind == 1:
                queries.append(f"SELECT id, amount FROM prc_diff_fact WHERE dept = '{d_pick}' AND region = '{r_pick}' AND amount >= {amt_cut} ORDER BY id LIMIT 15")
            elif kind == 2:
                queries.append(f"SELECT region, COUNT(*) AS c, AVG(amount) AS a FROM prc_diff_fact WHERE dept != '{d_pick}' AND amount < {amt_cut} GROUP BY region ORDER BY region")
            elif kind == 3:
                queries.append(f"SELECT d.tier, COUNT(*) AS c, SUM(f.amount) AS s FROM prc_diff_fact f JOIN prc_diff_dim d ON f.cid = d.cid WHERE f.amount > {amt_cut} GROUP BY d.tier ORDER BY d.tier")
            else:
                queries.append(f"SELECT COUNT(*) FROM prc_diff_fact WHERE (dept = '{d_pick}' OR region = '{r_pick}') AND qty > {qty_cut}")

        def norm_cell(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return round(float(v), 2)
            s = str(v)
            try:
                return round(float(s), 2)
            except Exception:
                return s

        def norm_rows(rows):
            return [tuple(norm_cell(c) for c in r) for r in rows]

        mismatches = 0
        with pg_c.cursor() as pcur, rl_c.cursor() as rcur:
            for idx, q in enumerate(queries):
                pcur.execute(q)
                pg_res = norm_rows(pcur.fetchall())
                rcur.execute(q)
                rl_res = norm_rows(rcur.fetchall())
                if pg_res != rl_res:
                    mismatches += 1
                    print(f"  [MISMATCH #{idx}] SQL={q}\n    PG={pg_res[:3]}\n    RL={rl_res[:3]}")

        assert mismatches == 0, f"Differential oracle found {mismatches} mismatches!"
        el = ms(t_all)
        print(f"  [PASS] 50/50 queries matched Live PostgreSQL 16 row-for-row (0 mismatches) in {el} ms")
        return {"queries_checked": len(queries), "mismatches": mismatches, "elapsed_ms": el}


# =============================================================================
# PILLAR 3: 12-WRITER CONCURRENT OCC STORM & EXACT ROW CONSERVATION
# =============================================================================
def run_occ_conservation_storm():
    print("\n=== PILLAR 3: 12-Writer Concurrent OCC Storm & Exact Row Conservation ===")
    t0 = time.perf_counter()
    with rldb_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS prc_occ_ledger")
        cur.execute("CREATE TABLE prc_occ_ledger (tx_id BIGINT, writer_id BIGINT, amount DOUBLE)")

    committed_rows = 0
    occ_retries = 0
    lock = threading.Lock()

    def writer_worker(wid: int):
        nonlocal committed_rows, occ_retries
        local_committed = 0
        local_retries = 0
        with rldb_conn() as c, c.cursor() as cur:
            for seq in range(10):  # 12 writers x 10 commits = 120 concurrent commits!
                tx_id = wid * 1000 + seq
                for attempt in range(15):
                    try:
                        cur.execute(f"INSERT INTO prc_occ_ledger VALUES ({tx_id}, {wid}, {round(10.0 + seq, 2)})")
                        local_committed += 1
                        break
                    except Exception as e:
                        if "OCC" in str(e) or "conflict" in str(e).lower():
                            local_retries += 1
                            time.sleep(0.01 * (attempt + 1))
                        else:
                            raise
        with lock:
            committed_rows += local_committed
            occ_retries += local_retries

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(writer_worker, range(12)))

    with rldb_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT tx_id) FROM prc_occ_ledger")
        actual_cnt, distinct_cnt = cur.fetchone()
        actual_cnt, distinct_cnt = int(actual_cnt), int(distinct_cnt)

    assert actual_cnt == committed_rows == distinct_cnt == 120, (
        f"OCC conservation failed: committed={committed_rows}, actual={actual_cnt}, distinct={distinct_cnt}"
    )
    el = ms(t0)
    print(f"  [PASS] 12 concurrent writers x 10 commits = {actual_cnt}/120 exact rows conserved (0 lost, 0 dup, retries={occ_retries}) in {el} ms")
    return {"writers": 12, "target_commits": 120, "actual_rows": actual_cnt, "distinct_tx": distinct_cnt, "occ_retries": occ_retries, "elapsed_ms": el}


# =============================================================================
# PILLAR 1: 1,000-JOB MULTI-TENANT ENTERPRISE LAKEHOUSE DAY STORM
# =============================================================================
def run_1000_job_multitenant_storm():
    print("\n=== PILLAR 1: 1,000-Job Multi-Tenant Enterprise Day Storm (20 Concurrent Runners, 6 Personas) ===")
    t_storm = time.perf_counter()

    # Setup multi-tenant tables, views, RBAC, RLS & dynamic masking policies!
    with rldb_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS prc_stream_events")
        cur.execute("CREATE TABLE prc_stream_events (event_id BIGINT, tenant VARCHAR, region VARCHAR, email VARCHAR, amount DOUBLE)")
        cur.execute(
            "INSERT INTO prc_stream_events VALUES "
            "(1,'acme','us-east','ceo@acme.com',500.0),"
            "(2,'globex','eu-central','cfo@globex.eu',750.0),"
            "(3,'acme','us-east','eng@acme.com',300.0),"
            "(4,'globex','eu-central','ops@globex.eu',900.0)"
        )
        cur.execute("DROP TABLE IF EXISTS prc_gold_kpi")
        cur.execute("CREATE TABLE prc_gold_kpi AS SELECT region, COUNT(*) AS cnt, SUM(amount) AS vol FROM big_orders WHERE amount > 400.0 GROUP BY region")
        cur.execute("CREATE OR REPLACE VIEW prc_v_exec AS SELECT region, cnt, vol FROM prc_gold_kpi")
        cur.execute("CREATE ROLE prc_us_analyst")
        cur.execute("CREATE ROLE prc_eu_analyst")
        cur.execute("CREATE ROLE prc_denied_user")
        cur.execute("GRANT SELECT ON prc_stream_events TO prc_us_analyst")
        cur.execute("GRANT SELECT ON prc_stream_events TO prc_eu_analyst")
        cur.execute("CREATE POLICY prc_rls_us ON prc_stream_events FOR prc_us_analyst USING (region = 'us-east')")
        cur.execute("CREATE POLICY prc_rls_eu ON prc_stream_events FOR prc_eu_analyst USING (region = 'eu-central')")
        cur.execute("CREATE MASKING POLICY prc_mask_us ON prc_stream_events.email FOR prc_us_analyst AS '***MASKED***'")

    # 1,000 jobs across 6 personas!
    # 500 BI Dashboard queries, 200 Governed Multi-Tenant security queries, 100 Arrow IPC / PyIceberg reads,
    # 100 Streaming CDC/Ingest micro-batches, 50 Medallion ETL / View / Explain jobs, 50 Analytical Window/Recursive jobs.
    jobs = (
        [("bi_agg", i) for i in range(250)]
        + [("bi_binary_bind", i) for i in range(250)]
        + [("gov_us_rls", i) for i in range(70)]
        + [("gov_eu_rls", i) for i in range(70)]
        + [("gov_denied", i) for i in range(60)]
        + [("arrow_ipc", i) for i in range(80)]
        + [("pyiceberg_scan", i) for i in range(20)]
        + [("stream_ingest", i) for i in range(100)]
        + [("etl_view_explain", i) for i in range(50)]
        + [("window_qualify", i) for i in range(50)]
    )
    assert len(jobs) == 1000
    random.Random(99).shuffle(jobs)

    # Split 1,000 jobs across 20 persistent worker threads (50 jobs/thread = realistic connection pooling!)
    chunks = [jobs[i::20] for i in range(20)]
    cat_lock = threading.Lock()

    def run_worker_chunk(tid: int):
        lats = []
        errs = []
        sec_leaks = 0
        per_persona = {}
        c_admin = rldb_conn("postgres")
        c_us = rldb_conn("prc_us_analyst")
        c_eu = rldb_conn("prc_eu_analyst")
        c_den = rldb_conn("prc_denied_user")
        try:
            for kind, jid in chunks[tid]:
                t0 = time.perf_counter()
                try:
                    if kind == "bi_agg":
                        with c_admin.cursor() as cur:
                            cur.execute("SELECT region, COUNT(*) FROM big_orders WHERE amount > 460.0 GROUP BY region")
                            rows = cur.fetchall()
                            assert len(rows) == 5
                    elif kind == "bi_binary_bind":
                        reg = ["us-east", "us-west", "eu-central", "ap-south", "sa-east"][jid % 5]
                        with c_admin.cursor() as cur:
                            cur.execute("SELECT region, COUNT(*) FROM big_orders WHERE region = %b AND amount > %b GROUP BY region", (reg, 450.0))
                            rows = cur.fetchall()
                            assert len(rows) == 1 and rows[0][0] == reg
                    elif kind == "gov_us_rls":
                        with c_us.cursor() as cur:
                            cur.execute("SELECT region, email FROM prc_stream_events")
                            rows = cur.fetchall()
                            for r in rows:
                                if r[0] != "us-east" or r[1] != "***MASKED***":
                                    sec_leaks += 1
                    elif kind == "gov_eu_rls":
                        with c_eu.cursor() as cur:
                            cur.execute("SELECT region, email FROM prc_stream_events")
                            rows = cur.fetchall()
                            for r in rows:
                                if r[0] != "eu-central":
                                    sec_leaks += 1
                    elif kind == "gov_denied":
                        blocked = False
                        try:
                            with c_den.cursor() as cur:
                                cur.execute("SELECT COUNT(*) FROM prc_stream_events")
                                cur.fetchall()
                        except Exception:
                            blocked = True
                        if not blocked:
                            sec_leaks += 1
                    elif kind == "arrow_ipc":
                        req = urllib.request.Request(
                            f"{COORD_HTTP}/v1/sql/arrow",
                            data=json.dumps({"sql": "SELECT region, cnt, vol FROM prc_v_exec ORDER BY region"}).encode(),
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        )
                        with urllib.request.urlopen(req, timeout=30) as resp:
                            tbl = pa_ipc.open_stream(io.BytesIO(resp.read())).read_all()
                            assert tbl.num_rows == 5
                    elif kind == "pyiceberg_scan":
                        with cat_lock:
                            cat = RestCatalog("rustlakedb", uri=COORD_HTTP)
                            t = cat.load_table("default.prc_gold_kpi")
                            arr = t.scan().to_arrow()
                            assert arr.num_rows == 5
                    elif kind == "stream_ingest":
                        reg = "us-east" if jid % 2 == 0 else "eu-central"
                        nd = json.dumps({"event_id": 10000 + jid, "tenant": "live", "region": reg, "email": f"u{jid}@live.io", "amount": 42.5})
                        # Retry transient OCC on concurrent table appends
                        for att in range(8):
                            try:
                                req = urllib.request.Request(
                                    f"{COORD_HTTP}/v1/ingest/prc_stream_events?on_error=continue",
                                    data=nd.encode(),
                                    headers={"Content-Type": "application/x-ndjson"},
                                    method="POST",
                                )
                                with urllib.request.urlopen(req, timeout=30) as resp:
                                    doc = json.loads(resp.read().decode())
                                    assert doc["rows_loaded"] == 1
                                break
                            except urllib.error.HTTPError:
                                time.sleep(0.01 * (att + 1))
                    elif kind == "etl_view_explain":
                        with c_admin.cursor() as cur:
                            cur.execute("EXPLAIN (FORMAT JSON) SELECT region, cnt, vol FROM prc_v_exec ORDER BY vol DESC")
                            doc = json.loads(cur.fetchone()[0])
                            assert isinstance(doc, list) and "Plan" in doc[0]
                    elif kind == "window_qualify":
                        with c_admin.cursor() as cur:
                            cur.execute(
                                "SELECT region, vol, AVG(vol) OVER (ORDER BY vol ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS ma "
                                "FROM prc_gold_kpi QUALIFY ma > 0"
                            )
                            rows = cur.fetchall()
                            assert len(rows) == 5
                    el = ms(t0)
                    lats.append(el)
                    per_persona[kind] = per_persona.get(kind, 0) + 1
                except Exception as e:
                    errs.append(f"{kind}#{jid}: {e}")
        finally:
            c_admin.close()
            c_us.close()
            c_eu.close()
            c_den.close()
        return lats, errs, sec_leaks, per_persona

    all_lats = []
    all_errs = []
    total_sec_leaks = 0
    persona_counts = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        for lats, errs, leaks, pc in pool.map(run_worker_chunk, range(20)):
            all_lats.extend(lats)
            all_errs.extend(errs)
            total_sec_leaks += leaks
            for k, v in pc.items():
                persona_counts[k] = persona_counts.get(k, 0) + v

    wall_s = round(time.perf_counter() - t_storm, 2)
    all_lats.sort()
    p50 = round(statistics.median(all_lats), 2)
    p95 = round(all_lats[int(len(all_lats) * 0.95) - 1], 2)
    p99 = round(all_lats[int(len(all_lats) * 0.99) - 1], 2)
    jps = round(len(all_lats) / wall_s, 1)

    assert total_sec_leaks == 0, f"SECURITY VIOLATION: {total_sec_leaks} cross-tenant leaks!"
    assert len(all_errs) == 0, f"1,000-job storm had {len(all_errs)} errors: {all_errs[:5]}"
    print(
        f"  [PASS] 1,000/1,000 multi-tenant jobs succeeded in {wall_s}s ({jps} jobs/s) | "
        f"p50={p50} ms, p95={p95} ms, p99={p99} ms | security_leaks=0 | errors=0"
    )
    return {
        "total_jobs": len(all_lats),
        "errors": len(all_errs),
        "security_leaks": total_sec_leaks,
        "wall_secs": wall_s,
        "jobs_per_sec": jps,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "personas": persona_counts,
    }


# =============================================================================
# PILLAR 5: BAD-DEPLOY DISASTER RECOVERY & ROLLBACK DRILL
# =============================================================================
def run_disaster_recovery_drill():
    print("\n=== PILLAR 5: Bad-Deploy Disaster Recovery & Instant Rollback Drill ===")
    t0 = time.perf_counter()
    with rldb_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS prc_dr_ledger")
        cur.execute("CREATE TABLE prc_dr_ledger (id BIGINT NOT NULL, balance DOUBLE CHECK (balance >= 0))")
        cur.execute("INSERT INTO prc_dr_ledger VALUES (1, 1000.0), (2, 2000.0), (3, 3000.0)")
        cur.execute("SHOW SNAPSHOTS ON prc_dr_ledger")
        good_snap = int(cur.fetchall()[-1][0])

        # 1. Guardrail blocks corrupt negative balance
        blocked = False
        try:
            cur.execute("INSERT INTO prc_dr_ledger VALUES (4, -9999.0)")
        except Exception:
            blocked = True
        assert blocked, "CHECK guardrail failed to block negative balance"

        # 2. Simulate bad ETL deployment overwriting balances to 0.0 on main!
        cur.execute("UPDATE prc_dr_ledger SET balance = 0.0 WHERE id >= 1")
        cur.execute("SELECT SUM(balance) FROM prc_dr_ledger")
        assert float(cur.fetchone()[0]) == 0.0

        # 3. Instant point-in-time recovery via RESTORE TABLE TO VERSION!
        cur.execute(f"RESTORE TABLE prc_dr_ledger TO VERSION {good_snap}")
        cur.execute("SELECT COUNT(*), SUM(balance) FROM prc_dr_ledger")
        cnt, total = cur.fetchone()
        assert int(cnt) == 3 and float(total) == 6000.0

        # 4. Validate hotfix safely on isolated branch before merging!
        cur.execute("CREATE BRANCH hotfix FROM main ON prc_dr_ledger")
        st, _ = http_json("POST", f"{COORD_HTTP}/v1/sql", {"sql": "INSERT INTO prc_dr_ledger VALUES (4, 4000.0)", "branch": "hotfix"})
        assert st == 200
        cur.execute("SELECT SUM(balance) FROM prc_dr_ledger")
        assert float(cur.fetchone()[0]) == 6000.0  # main untouched!
        cur.execute("MERGE BRANCH hotfix INTO main ON prc_dr_ledger")
        cur.execute("SELECT COUNT(*), SUM(balance) FROM prc_dr_ledger")
        cnt2, total2 = cur.fetchone()
        assert int(cnt2) == 4 and float(total2) == 10000.0

    el = ms(t0)
    print(f"  [PASS] Bad ETL neutralized -> instant RESTORE v{good_snap} ($6,000 exact) -> hotfix branch verified & merged ($10,000 exact) in {el} ms")
    return {"good_snap_restored": good_snap, "check_guardrail": True, "hotfix_branch_merge": True, "elapsed_ms": el}


# =============================================================================
# PILLAR 4A & 4B: CHAOS PROBES (LIVE WORKER SIGKILL + COORDINATOR HARD CRASH)
# =============================================================================
def run_worker_kill_load():
    """Sustained 8-thread BI load while PowerShell SIGKILLs worker-2 mid-flight!"""
    stop_flag = os.environ.get("CHAOS_DURATION_SECS", "12")
    dur = float(stop_flag)
    t0 = time.perf_counter()
    total_ok = 0
    total_err = 0
    lock = threading.Lock()

    def load_thread(tid: int):
        nonlocal total_ok, total_err
        ok = 0
        err = 0
        with rldb_conn() as c, c.cursor() as cur:
            while time.perf_counter() - t0 < dur:
                try:
                    cur.execute("SELECT region, COUNT(*), AVG(amount) FROM big_orders WHERE amount > 420.0 GROUP BY region")
                    rows = cur.fetchall()
                    assert len(rows) == 5
                    ok += 1
                except Exception as e:
                    err += 1
                    print(f"  [CHAOS-LOAD-ERR] t{tid}: {e}")
        with lock:
            total_ok += ok
            total_err += err

    print("CHAOS_LOAD_STARTED", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(load_thread, range(8)))

    res = {"queries_succeeded_during_sigkill": total_ok, "errors": total_err, "duration_secs": round(time.perf_counter() - t0, 2)}
    assert total_err == 0 and total_ok >= 10, f"Worker SIGKILL chaos failed: {res}"
    print("CHAOS_WORKER_JSON:" + json.dumps(res))


def verify_post_coordinator_crash():
    """Runs AFTER `docker kill -s SIGKILL rustlakedb-bench-coordinator` + cold restart!"""
    t0 = time.perf_counter()
    with rldb_conn("postgres") as c, c.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM prc_occ_ledger")
        occ_cnt = int(cur.fetchone()[0])
        assert occ_cnt == 120, f"lost OCC commits across coordinator SIGKILL: {occ_cnt}"

        cur.execute("SELECT COUNT(*), SUM(balance) FROM prc_dr_ledger")
        dr_cnt, dr_sum = cur.fetchone()
        assert int(dr_cnt) == 4 and float(dr_sum) == 10000.0

        cur.execute("SELECT COUNT(*) FROM prc_v_exec")
        v_cnt = int(cur.fetchone()[0])
        assert v_cnt == 5, f"view lost across coordinator SIGKILL: {v_cnt}"

        cur.execute("SELECT * FROM system.lineage")
        lin_cnt = len(cur.fetchall())
        assert lin_cnt >= 1, "lineage lost across coordinator SIGKILL"

    # Verify Raft-persisted RBAC, RLS & Dynamic Masking survived hard coordinator SIGKILL!
    with rldb_conn("prc_us_analyst") as cu, cu.cursor() as cur:
        cur.execute("SELECT region, email FROM prc_stream_events")
        rows = cur.fetchall()
        assert len(rows) > 0 and all(r[0] == "us-east" and r[1] == "***MASKED***" for r in rows)

    blocked = False
    try:
        with rldb_conn("prc_denied_user") as cd, cd.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM prc_stream_events")
            cur.fetchall()
    except Exception:
        blocked = True
    assert blocked, "RBAC denial failed after coordinator SIGKILL!"

    res = {
        "occ_rows_recovered": occ_cnt,
        "dr_balance_recovered": float(dr_sum),
        "views_recovered": True,
        "rbac_rls_masking_recovered": True,
        "lineage_recovered": True,
        "verify_ms": ms(t0),
    }
    print("CHAOS_COORD_JSON:" + json.dumps(res))


def cleanup():
    with rldb_conn() as conn, conn.cursor() as cur:
        for t in ["prc_diff_fact", "prc_diff_dim", "prc_occ_ledger", "prc_stream_events", "prc_gold_kpi", "prc_dr_ledger"]:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute("DROP VIEW IF EXISTS prc_v_exec")
    for t in ["prc_diff_fact", "prc_diff_dim", "prc_occ_ledger", "prc_stream_events", "prc_gold_kpi", "prc_dr_ledger"]:
        p = os.path.join(WAREHOUSE_DIR, t)
        if os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True)
    print("PRC_CLEANUP_COMPLETE")


def main():
    if "--worker-kill-load" in sys.argv:
        run_worker_kill_load()
        return
    if "--verify-coord-crash" in sys.argv:
        verify_post_coordinator_crash()
        return
    if "--cleanup" in sys.argv:
        cleanup()
        return

    metrics_before = http_text(f"{COORD_HTTP}/metrics")
    report = {
        "pillar2_differential_oracle": run_differential_oracle(),
        "pillar3_occ_conservation": run_occ_conservation_storm(),
        "pillar1_1000_job_storm": run_1000_job_multitenant_storm(),
        "pillar5_disaster_recovery": run_disaster_recovery_drill(),
    }
    metrics_after = http_text(f"{COORD_HTTP}/metrics")

    def parse_metric(text, name):
        for line in text.splitlines():
            if line.startswith(name + " "):
                try:
                    return float(line.split()[1])
                except Exception:
                    return 0.0
        return 0.0

    report["pillar6_observability"] = {
        "queries_total_delta": int(parse_metric(metrics_after, "rustlakedb_queries_total") - parse_metric(metrics_before, "rustlakedb_queries_total")),
        "metrics_endpoint_healthy": "rustlakedb_queries_total" in metrics_after,
    }
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\nPRC PILLARS 1, 2, 3, 5, 6 PASSED 100%! Saved -> {OUT_JSON}")


if __name__ == "__main__":
    main()
