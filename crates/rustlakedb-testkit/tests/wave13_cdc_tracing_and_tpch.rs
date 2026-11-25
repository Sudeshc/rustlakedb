// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

#[tokio::test]
async fn wave13_debezium_cdc_streaming_exactly_once_and_deletion_vectors() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE cdc_accounts (id BIGINT, owner VARCHAR, balance DOUBLE)", "main")
        .await
        .unwrap();

    let cp = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let addr = cp.local_addr().unwrap();
    let h = tokio::spawn(async move {
        let _ = cp.serve().await;
    });

    let client = reqwest::Client::new();

    // Batch 1: 3 Debezium inserts (`op: "c"`) at offsets 1..=3
    let batch1 = json!([
        {"op": "c", "offset": 1, "key_col": "id", "after": {"id": 1, "owner": "alice", "balance": 100.0}},
        {"op": "c", "offset": 2, "key_col": "id", "after": {"id": 2, "owner": "bob",   "balance": 200.0}},
        {"op": "c", "offset": 3, "key_col": "id", "after": {"id": 3, "owner": "carol", "balance": 300.0}}
    ]);
    let r1: serde_json::Value = client
        .post(format!("http://{addr}/v1/cdc/cdc_accounts?stream_id=pg_slot_1"))
        .json(&batch1)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r1["applied"], 3);
    assert_eq!(r1["last_offset"], 3);

    // Batch 2: Replay duplicate offset 2 & 3 + update id=1 (`op: "u"`) at offset 4 + delete id=3 (`op: "d"`) at offset 5!
    let batch2 = json!([
        {"op": "c", "offset": 2, "key_col": "id", "after": {"id": 2, "owner": "bob_dup", "balance": 9999.0}},
        {"op": "c", "offset": 3, "key_col": "id", "after": {"id": 3, "owner": "carol_dup", "balance": 9999.0}},
        {"op": "u", "offset": 4, "key_col": "id", "after": {"id": 1, "owner": "alice", "balance": 150.0}},
        {"op": "d", "offset": 5, "key_col": "id", "before": {"id": 3, "owner": "carol", "balance": 300.0}}
    ]);
    let r2: serde_json::Value = client
        .post(format!("http://{addr}/v1/cdc/cdc_accounts?stream_id=pg_slot_1"))
        .json(&batch2)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r2["skipped_duplicates"], 2);
    assert_eq!(r2["applied"], 2);
    assert_eq!(r2["upserts"], 1);
    assert_eq!(r2["deletes"], 1);
    assert_eq!(r2["last_offset"], 5);

    // Verify exact ACID state: id=1 updated to 150.0, id=2 untouched at 200.0, id=3 deleted!
    let res = coord
        .execute_sql("SELECT id, owner, balance FROM cdc_accounts ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(res.batch.rows[0].get("balance"), Some(&ScalarValue::Float64(150.0)));
    assert_eq!(res.batch.rows[1].get("owner"), Some(&ScalarValue::Utf8("bob".into())));
    assert_eq!(res.batch.rows[1].get("balance"), Some(&ScalarValue::Float64(200.0)));

    h.abort();
}

#[tokio::test]
async fn wave13_opentelemetry_traceparent_propagation_and_traces_api() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));
    coord
        .execute_sql("CREATE TABLE otel_t (id BIGINT)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO otel_t VALUES (1), (2)", "main")
        .await
        .unwrap();

    let cp = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let addr = cp.local_addr().unwrap();
    let h = tokio::spawn(async move {
        let _ = cp.serve().await;
    });

    let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let traceparent = format!("00-{trace_id}-00f067aa0ba902b7-01");
    let client = reqwest::Client::new();
    let sql_resp = client
        .post(format!("http://{addr}/v1/sql"))
        .header("traceparent", &traceparent)
        .json(&json!({"sql": "SELECT COUNT(*) AS c FROM otel_t"}))
        .send()
        .await
        .unwrap();
    assert_eq!(sql_resp.status(), reqwest::StatusCode::OK);

    let traces: serde_json::Value = client
        .get(format!("http://{addr}/v1/traces"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let spans = traces["spans"].as_array().unwrap();
    assert!(
        spans.iter().any(|s| s["trace_id"].as_str() == Some(trace_id)),
        "expected propagated W3C trace_id '{trace_id}' in /v1/traces: {traces}"
    );
    assert!(traces["resourceSpans"].as_array().is_some());

    h.abort();
}

#[tokio::test]
async fn wave13_tpch_analytical_benchmark_suite() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let w2 = Arc::new(WorkerNode::new("w2", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1.clone(), w2.clone()],
        PlannerConfig {
            default_shuffle_partitions: 2,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    );

    // Build TPC-H Star/Snowflake Schema: nation, customer, orders, lineitem
    coord.execute_sql("CREATE TABLE tpch_nation (n_nationkey BIGINT, n_name VARCHAR, n_region VARCHAR)", "main").await.unwrap();
    coord.execute_sql("CREATE TABLE tpch_customer (c_custkey BIGINT, c_nationkey BIGINT, c_mktsegment VARCHAR)", "main").await.unwrap();
    coord.execute_sql("CREATE TABLE tpch_orders (o_orderkey BIGINT, o_custkey BIGINT, o_orderdate VARCHAR, o_orderpriority VARCHAR, o_totalprice DOUBLE)", "main").await.unwrap();
    coord.execute_sql("CREATE TABLE tpch_lineitem (l_orderkey BIGINT, l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipmode VARCHAR, l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_shipdate VARCHAR)", "main").await.unwrap();

    coord
        .execute_sql(
            "INSERT INTO tpch_nation VALUES \
             (1, 'USA', 'AMERICA'), (2, 'GERMANY', 'EUROPE'), (3, 'JAPAN', 'ASIA')",
            "main",
        )
        .await
        .unwrap();

    let cust_vals: Vec<String> = (1..=60)
        .map(|i| {
            let n = (i % 3) + 1;
            let seg = if i % 2 == 0 { "BUILDING" } else { "AUTOMOBILE" };
            format!("({i}, {n}, '{seg}')")
        })
        .collect();
    coord
        .execute_sql(&format!("INSERT INTO tpch_customer VALUES {}", cust_vals.join(",")), "main")
        .await
        .unwrap();

    let ord_vals: Vec<String> = (1..=200)
        .map(|i| {
            let ck = (i % 60) + 1;
            let dt = if i % 2 == 0 { "1995-02-10" } else { "1995-06-20" };
            let pri = if i % 3 == 0 { "1-URGENT" } else { "3-MEDIUM" };
            format!("({i}, {ck}, '{dt}', '{pri}', {})", (i * 100) as f64)
        })
        .collect();
    coord
        .execute_sql(&format!("INSERT INTO tpch_orders VALUES {}", ord_vals.join(",")), "main")
        .await
        .unwrap();

    // Seed 2,000 lineitem rows across 4 Parquet splits!
    let flags = ["A", "N", "R"];
    let modes = ["MAIL", "SHIP", "AIR"];
    for s in 0..4 {
        let mut vals = Vec::with_capacity(500);
        for j in 0..500 {
            let idx = s * 500 + j + 1;
            let ok = (idx % 200) + 1;
            let rf = flags[idx % 3];
            let ls = if idx % 2 == 0 { "F" } else { "O" };
            let sm = modes[idx % 3];
            let qty = ((idx % 40) + 1) as f64;
            let ep = qty * 125.0;
            let disc = ((idx % 4) as f64) * 0.02 + 0.02; // 0.02, 0.04, 0.06, 0.08
            let sd = if idx % 3 == 0 { "1994-05-15" } else { "1995-04-20" };
            vals.push(format!("({ok}, '{rf}', '{ls}', '{sm}', {qty}, {ep}, {disc}, '{sd}')"));
        }
        coord
            .execute_sql(&format!("INSERT INTO tpch_lineitem VALUES {}", vals.join(",")), "main")
            .await
            .unwrap();
    }

    // TPC-H Q1: Pricing Summary Report
    let t0 = Instant::now();
    let q1 = coord
        .execute_sql(
            "SELECT l_returnflag, l_linestatus, COUNT(*) AS count_order, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, AVG(l_discount) AS avg_disc \
             FROM tpch_lineitem WHERE l_shipdate <= '1998-09-02' \
             GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(q1.batch.num_rows(), 6);
    println!("TPC-H Q1: {} groups in {:.2} ms", q1.batch.num_rows(), t0.elapsed().as_secs_f64() * 1000.0);

    // TPC-H Q3: Shipping Priority (3-way JOIN + Top-5 Pushdown)
    let t0 = Instant::now();
    let q3 = coord
        .execute_sql(
            "SELECT tpch_orders.o_orderkey, SUM(tpch_lineitem.l_extendedprice) AS revenue \
             FROM tpch_customer \
             INNER JOIN tpch_orders ON tpch_customer.c_custkey = tpch_orders.o_custkey \
             INNER JOIN tpch_lineitem ON tpch_orders.o_orderkey = tpch_lineitem.l_orderkey \
             WHERE tpch_customer.c_mktsegment = 'BUILDING' \
             GROUP BY tpch_orders.o_orderkey ORDER BY revenue DESC LIMIT 5",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(q3.batch.num_rows(), 5);
    println!("TPC-H Q3 (3-way JOIN Top-5): {:.2} ms", t0.elapsed().as_secs_f64() * 1000.0);

    // TPC-H Q5: Local Supplier / Regional Volume (4-way JOIN!)
    let t0 = Instant::now();
    let q5 = coord
        .execute_sql(
            "SELECT tpch_nation.n_name, SUM(tpch_lineitem.l_extendedprice) AS revenue \
             FROM tpch_nation \
             INNER JOIN tpch_customer ON tpch_nation.n_nationkey = tpch_customer.c_nationkey \
             INNER JOIN tpch_orders ON tpch_customer.c_custkey = tpch_orders.o_custkey \
             INNER JOIN tpch_lineitem ON tpch_orders.o_orderkey = tpch_lineitem.l_orderkey \
             GROUP BY tpch_nation.n_name ORDER BY revenue DESC",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(q5.batch.num_rows(), 3);
    println!("TPC-H Q5 (4-way JOIN): {} nations in {:.2} ms", q5.batch.num_rows(), t0.elapsed().as_secs_f64() * 1000.0);

    // TPC-H Q6: Forecasting Revenue Change (Vectorized Arrow Filtered Agg)
    let t0 = Instant::now();
    let q6 = coord
        .execute_sql(
            "SELECT SUM(l_extendedprice) AS rev_base, COUNT(*) AS matching_lines \
             FROM tpch_lineitem \
             WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount >= 0.04 AND l_discount <= 0.08 AND l_quantity < 24.0",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(q6.batch.num_rows(), 1);
    let q6_cnt = match q6.batch.rows[0].get("matching_lines") {
        Some(ScalarValue::Int64(n)) => *n,
        other => panic!("unexpected Q6 count: {other:?}"),
    };
    assert!(q6_cnt > 0);
    println!("TPC-H Q6 (Filtered Agg): {q6_cnt} lines in {:.2} ms", t0.elapsed().as_secs_f64() * 1000.0);

    // TPC-H Q10: Returned Item Reporting (DISTINCT customers + Top-5)
    let t0 = Instant::now();
    let q10 = coord
        .execute_sql(
            "SELECT tpch_customer.c_mktsegment, COUNT(DISTINCT tpch_customer.c_custkey) AS returned_custs, SUM(tpch_lineitem.l_extendedprice) AS lost_rev \
             FROM tpch_customer \
             INNER JOIN tpch_orders ON tpch_customer.c_custkey = tpch_orders.o_custkey \
             INNER JOIN tpch_lineitem ON tpch_orders.o_orderkey = tpch_lineitem.l_orderkey \
             WHERE tpch_lineitem.l_returnflag = 'R' \
             GROUP BY tpch_customer.c_mktsegment ORDER BY lost_rev DESC",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(q10.batch.num_rows(), 2);
    println!("TPC-H Q10 (Returned Items + COUNT DISTINCT): {:.2} ms", t0.elapsed().as_secs_f64() * 1000.0);

    // TPC-H Q12: Shipping Modes & Order Priority (`CASE WHEN` + `IN`)
    let t0 = Instant::now();
    let q12 = coord
        .execute_sql(
            "SELECT l_shipmode, COUNT(*) AS line_cnt, \
                    CASE WHEN l_shipmode = 'MAIL' THEN 'GROUND' ELSE 'EXPRESS' END AS tier_bucket \
             FROM tpch_lineitem WHERE l_shipmode IN ('MAIL', 'SHIP') \
             GROUP BY l_shipmode ORDER BY l_shipmode",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(q12.batch.num_rows(), 2);
    println!(
        "TPC-H Q12 (Shipping Modes): {:.2} ms | total vectorized rows={}",
        t0.elapsed().as_secs_f64() * 1000.0,
        w1.vectorized_rows_processed.load(Ordering::Relaxed)
            + w2.vectorized_rows_processed.load(Ordering::Relaxed)
    );
}
