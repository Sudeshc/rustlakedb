// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_gateway::PgGateway;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_testkit::PgTestClient;
use rustlakedb_worker::WorkerNode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave12_vectorized_filtered_scan_multicolumn_topn_and_count_distinct() {
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

    // 1. Iceberg Hidden Partitioning DDL (`PARTITION BY`) + NDV stats in EXPLAIN
    coord
        .execute_sql(
            "CREATE TABLE events_part (id BIGINT, region VARCHAR, user_id BIGINT, amount DOUBLE) PARTITION BY (region)",
            "main",
        )
        .await
        .unwrap();
    let meta = coord.catalog.get_table("events_part").await.unwrap();
    assert_eq!(meta.partition_by, vec!["region".to_string()]);

    coord
        .execute_sql(
            "INSERT INTO events_part VALUES \
             (1, 'us-east', 10, 100.0), (2, 'us-east', 10, 300.0), (3, 'us-east', 20, 200.0), \
             (4, 'eu-west', 30, 500.0), (5, 'eu-west', 30, 150.0), (6, 'ap-south', 40, 450.0)",
            "main",
        )
        .await
        .unwrap();

    // 2. Vectorized Arrow Filtered Scan (`WHERE amount >= 200`)
    let before_vec = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);
    let filt = coord
        .execute_sql(
            "SELECT id, region, amount FROM events_part WHERE amount >= 200.0 ORDER BY id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(filt.batch.num_rows(), 4);
    let after_vec = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);
    assert!(
        after_vec >= before_vec + 6,
        "expected Vectorized Arrow Filtered Scan to scan all 6 rows in Arrow"
    );

    // 3. Multi-Column Vectorized Arrow Top-N (`ORDER BY region ASC, amount DESC LIMIT 3`)
    let topn = coord
        .execute_sql(
            "SELECT id, region, amount FROM events_part ORDER BY region ASC, amount DESC LIMIT 3",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(topn.batch.num_rows(), 3);
    assert_eq!(topn.batch.rows[0].get("region"), Some(&ScalarValue::Utf8("ap-south".into())));
    assert_eq!(topn.batch.rows[1].get("region"), Some(&ScalarValue::Utf8("eu-west".into())));
    assert_eq!(topn.batch.rows[1].get("amount"), Some(&ScalarValue::Float64(500.0)));
    assert_eq!(topn.batch.rows[2].get("region"), Some(&ScalarValue::Utf8("eu-west".into())));
    assert_eq!(topn.batch.rows[2].get("amount"), Some(&ScalarValue::Float64(150.0)));

    // 4. Distributed `COUNT(DISTINCT user_id)` with `GROUP BY` and global!
    let cd_grp = coord
        .execute_sql(
            "SELECT region, COUNT(*) AS rows_cnt, COUNT(DISTINCT user_id) AS uniq_users \
             FROM events_part GROUP BY region ORDER BY region",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(cd_grp.batch.num_rows(), 3);
    assert_eq!(cd_grp.batch.rows[1].get("region"), Some(&ScalarValue::Utf8("eu-west".into())));
    assert_eq!(cd_grp.batch.rows[1].get("rows_cnt"), Some(&ScalarValue::Int64(2)));
    assert_eq!(cd_grp.batch.rows[1].get("uniq_users"), Some(&ScalarValue::Int64(1)));
    assert_eq!(cd_grp.batch.rows[2].get("region"), Some(&ScalarValue::Utf8("us-east".into())));
    assert_eq!(cd_grp.batch.rows[2].get("rows_cnt"), Some(&ScalarValue::Int64(3)));
    assert_eq!(cd_grp.batch.rows[2].get("uniq_users"), Some(&ScalarValue::Int64(2)));

    let cd_all = coord
        .execute_sql("SELECT COUNT(DISTINCT user_id) AS total_users FROM events_part", "main")
        .await
        .unwrap();
    assert_eq!(cd_all.batch.rows[0].get("total_users"), Some(&ScalarValue::Int64(4)));
}

#[tokio::test]
async fn wave12_intersect_except_correlated_exists_and_pgwire_bind_params() {
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

    coord.execute_sql("CREATE TABLE set_a (id BIGINT, name VARCHAR)", "main").await.unwrap();
    coord.execute_sql("CREATE TABLE set_b (id BIGINT, name VARCHAR)", "main").await.unwrap();
    coord.execute_sql("INSERT INTO set_a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')", "main").await.unwrap();
    coord.execute_sql("INSERT INTO set_b VALUES (2, 'bob'), (3, 'carol'), (4, 'dave')", "main").await.unwrap();

    // 1. INTERSECT & EXCEPT
    let inter = coord
        .execute_sql("SELECT id FROM set_a INTERSECT SELECT id FROM set_b ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(inter.batch.num_rows(), 2);
    assert_eq!(inter.batch.rows[0].get("id"), Some(&ScalarValue::Int64(2)));
    assert_eq!(inter.batch.rows[1].get("id"), Some(&ScalarValue::Int64(3)));

    let exc = coord
        .execute_sql("SELECT id FROM set_a EXCEPT SELECT id FROM set_b ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(exc.batch.num_rows(), 1);
    assert_eq!(exc.batch.rows[0].get("id"), Some(&ScalarValue::Int64(1)));

    // 2. Correlated `EXISTS` & `NOT EXISTS`
    coord.execute_sql("CREATE TABLE ord_c (oid BIGINT, cid BIGINT, amt DOUBLE)", "main").await.unwrap();
    coord.execute_sql("INSERT INTO ord_c VALUES (10, 1, 500.0), (11, 2, 50.0)", "main").await.unwrap();

    let corr_ex = coord
        .execute_sql(
            "SELECT a.id, a.name FROM set_a a WHERE EXISTS (SELECT 1 FROM ord_c o WHERE o.cid = a.id AND o.amt >= 100.0) ORDER BY a.id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(corr_ex.batch.num_rows(), 1);
    assert_eq!(corr_ex.batch.rows[0].get("name"), Some(&ScalarValue::Utf8("alice".into())));

    let corr_not_ex = coord
        .execute_sql(
            "SELECT a.id FROM set_a a WHERE NOT EXISTS (SELECT 1 FROM ord_c o WHERE o.cid = a.id) ORDER BY a.id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(corr_not_ex.batch.num_rows(), 1);
    assert_eq!(corr_not_ex.batch.rows[0].get("id"), Some(&ScalarValue::Int64(3)));

    // 3. PGWire v3 Extended Query Parameterized Binding (`$1`, `$2`)
    let gw = PgGateway::bind("127.0.0.1:0", coord).await.unwrap();
    let addr = gw.local_addr().unwrap();
    let h = tokio::spawn(async move {
        let _ = gw.serve().await;
    });
    let mut client = PgTestClient::connect(addr, "postgres", "rustlakedb").await.unwrap();
    let bound = client
        .query_extended(
            "SELECT id, name FROM set_a WHERE id >= $1 AND name = $2",
            &[Some("1"), Some("bob")],
        )
        .await
        .unwrap();
    assert_eq!(bound.rows.len(), 1);
    assert_eq!(bound.rows[0][0].as_deref(), Some("2"));
    assert_eq!(bound.rows[0][1].as_deref(), Some("bob"));
    h.abort();
}

#[tokio::test]
async fn wave12_external_nvme_spill_for_sort_and_hash_aggregate_under_strict_memory() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());

    // Strict 256-byte memory pool forces BOTH External Merge-Sort AND External HashAggregate to spill to NVMe!
    let w1 = Arc::new(WorkerNode::with_memory_limit(
        "spill-w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
        256,
    ));
    let tiny_pool = w1.memory_pool.clone();
    let coord = Coordinator::new(catalog, store, shuffle, vec![w1], PlannerConfig::default());

    coord
        .execute_sql("CREATE TABLE spill_data (id BIGINT, grp VARCHAR, val DOUBLE)", "main")
        .await
        .unwrap();
    let vals: Vec<String> = (1..=60)
        .map(|i| format!("({i}, 'g{}', {})", i % 5, (61 - i) as f64))
        .collect();
    coord
        .execute_sql(&format!("INSERT INTO spill_data VALUES {}", vals.join(",")), "main")
        .await
        .unwrap();

    // 1. Trigger External Merge-Sort NVMe Spill (ORDER BY without LIMIT so Sort runs directly!)
    let sorted = coord
        .execute_sql("SELECT id, val FROM spill_data ORDER BY val ASC", "main")
        .await
        .unwrap();
    assert_eq!(sorted.batch.num_rows(), 60);
    assert_eq!(sorted.batch.rows[0].get("val"), Some(&ScalarValue::Float64(1.0)));
    assert_eq!(sorted.batch.rows[59].get("val"), Some(&ScalarValue::Float64(60.0)));
    let spills_after_sort = tiny_pool.spill_events.load(Ordering::Relaxed);
    assert!(
        spills_after_sort >= 1,
        "expected External Merge-Sort to trigger NVMe spill under 256B budget"
    );

    // 2. Trigger External Hash-Aggregate NVMe Spill (COUNT DISTINCT forces non-vectorized hash agg with spill!)
    let agged = coord
        .execute_sql(
            "SELECT grp, COUNT(*) AS cnt, COUNT(DISTINCT id) AS uniq_ids FROM spill_data GROUP BY grp ORDER BY grp",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(agged.batch.num_rows(), 5);
    assert_eq!(agged.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(12)));
    assert_eq!(agged.batch.rows[0].get("uniq_ids"), Some(&ScalarValue::Int64(12)));
    assert!(
        tiny_pool.spill_events.load(Ordering::Relaxed) > spills_after_sort,
        "expected External Hash-Aggregate to trigger NVMe spill under 256B budget"
    );
}
