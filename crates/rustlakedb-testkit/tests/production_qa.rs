// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_coordinator::maintainer::{AutonomousMaintainer, MaintainerConfig};
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::{PgTestClient, TestCluster};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn grace_hash_join_spills_to_nvme_under_strict_memory_pool_budget() {
    // Enforce a tiny 64-byte per-worker MemoryPool cap so every non-empty HashJoin MUST spill to NVMe!
    let cluster = TestCluster::start_with_memory_limit(3, 64).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    let res = cluster
        .sql(
            "SELECT tier, SUM(amount) AS revenue \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             GROUP BY tier ORDER BY tier",
        )
        .await
        .unwrap();

    let total_spills: u64 = cluster
        .workers
        .iter()
        .map(|w| w.memory_pool.spill_events.load(Ordering::Relaxed))
        .sum();
    assert!(
        total_spills >= 1,
        "expected Grace Hash Join to spill to NVMe under 64-byte memory budget"
    );

    // Ensure MemoryPool reservations were cleanly released (zero memory leaks!).
    for w in &cluster.workers {
        assert_eq!(w.memory_pool.used_bytes(), 0);
    }

    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(
        res.batch.rows[0].get("revenue"),
        Some(&ScalarValue::Float64(1549.0))
    );
    assert_eq!(
        res.batch.rows[1].get("revenue"),
        Some(&ScalarValue::Float64(250.0))
    );
}

#[tokio::test]
async fn aqe_coalesces_empty_partitions_and_splits_skewed_join_runtime() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE clicks (hot_key TEXT, weight DOUBLE)")
        .await
        .unwrap();

    let mut skewed_values = Vec::new();
    for _ in 0..40 {
        skewed_values.push("('bot_user_000000000000000000', 1.0)".to_string());
    }
    skewed_values.push("('normal_a', 1.0)".to_string());
    skewed_values.push("('normal_b', 1.0)".to_string());
    let insert_sql = format!("INSERT INTO clicks VALUES {}", skewed_values.join(", "));
    // 2 splits -> skewed partition has 2 probe blocks so AQE splits them into 2 parallel sub-tasks!
    cluster.sql(&insert_sql).await.unwrap();
    cluster.sql(&insert_sql).await.unwrap();

    cluster
        .sql("CREATE TABLE users (hot_key TEXT, label TEXT)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO users VALUES ('bot_user_000000000000000000', 'bot'), ('normal_a', 'ok'), ('normal_b', 'ok')")
        .await
        .unwrap();

    let res = cluster
        .sql(
            "SELECT label, COUNT(*) AS cnt \
             FROM clicks JOIN users ON clicks.hot_key = users.hot_key \
             GROUP BY label ORDER BY label",
        )
        .await
        .unwrap();

    assert!(
        res.stats.aqe_skew_splits_applied >= 1,
        "expected AQE runtime skew split to fire on skewed join partition"
    );
    assert!(
        res.stats.aqe_partitions_coalesced >= 1,
        "expected AQE to coalesce empty hash partitions"
    );
    assert_eq!(res.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(80)));
    assert_eq!(res.batch.rows[1].get("cnt"), Some(&ScalarValue::Int64(4)));
}

#[tokio::test]
async fn autonomous_maintainer_auto_compacts_small_files_and_vacuums_orphans() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();
    cluster.sql("DELETE FROM orders WHERE order_id = 3").await.unwrap();

    let maintainer = AutonomousMaintainer::new(
        cluster.coordinator.clone(),
        MaintainerConfig {
            min_files_to_compact: 3,
            retain_snapshots: 1,
            ..Default::default()
        },
    );

    let cycle = maintainer.run_once().await.unwrap();
    assert_eq!(cycle.tables_checked, 2);
    assert_eq!(cycle.tables_compacted, 1); // `orders` had 3 files + DV
    assert_eq!(cycle.tables_vacuumed, 2); // expired old snapshots

    let data_dir = cluster.data_dir_path().join("orders/data");
    let parquet_count = std::fs::read_dir(data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .count();
    assert_eq!(parquet_count, 1);

    // Verify all 5 surviving orders still return exact sum after autonomous compaction!
    let check = cluster.sql("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM orders").await.unwrap();
    assert_eq!(check.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(5)));
    assert_eq!(check.batch.rows[0].get("total"), Some(&ScalarValue::Float64(800.0)));
}

#[tokio::test]
async fn ctes_jsonb_contains_has_key_pg_catalog_and_multistatement_pgwire() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster.sql("CREATE TABLE logs (id BIGINT, service TEXT, payload JSONB)").await.unwrap();
    cluster
        .sql(r#"INSERT INTO logs VALUES (1, 'api', '{"env": "prod", "trace_id": "t1"}'), (2, 'api', '{"env": "dev"}')"#)
        .await
        .unwrap();
    cluster
        .sql(r#"INSERT INTO logs VALUES (3, 'worker', '{"env": "staging"}')"#)
        .await
        .unwrap();

    // 1. JSONB `@>` containment with Puffin inverted index pruning (prunes split #2!).
    let contains = cluster
        .sql(r#"SELECT id FROM logs WHERE payload @> '{"env": "prod"}'"#)
        .await
        .unwrap();
    assert_eq!(contains.batch.num_rows(), 1);
    assert_eq!(contains.batch.rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(cluster.store.files_pruned_by_puffin.load(Ordering::Relaxed), 1);

    // 2. JSONB `?` key-exists operator with Puffin index pruning (prunes split #2 again!).
    let has_key = cluster
        .sql("SELECT id FROM logs WHERE payload ? 'trace_id'")
        .await
        .unwrap();
    assert_eq!(has_key.batch.num_rows(), 1);
    assert_eq!(cluster.store.files_pruned_by_puffin.load(Ordering::Relaxed), 2);

    // 3. SQL CTE (`WITH ... AS (...) SELECT ...`).
    let cte = cluster
        .sql(
            "WITH prod_logs AS (SELECT id, service FROM logs WHERE payload->>'env' = 'prod') \
             SELECT service, COUNT(*) AS cnt FROM prod_logs GROUP BY service",
        )
        .await
        .unwrap();
    assert_eq!(cte.batch.num_rows(), 1);
    assert_eq!(cte.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(1)));

    // 4. `information_schema` & `current_database()` introspection + multi-statement TCP pgwire!
    let addr = cluster.start_pg_gateway().await.unwrap();
    let mut client = PgTestClient::connect(addr, "postgres", "rustlakedb").await.unwrap();

    let db = client.query("SELECT current_database()").await.unwrap();
    assert_eq!(db.rows[0][0].as_deref(), Some("rustlakedb"));

    let info_tables = client.query("SELECT * FROM information_schema.tables").await.unwrap();
    assert_eq!(info_tables.rows.len(), 1);
    assert_eq!(info_tables.rows[0][1].as_deref(), Some("logs"));

    let info_cols = client.query("SELECT * FROM information_schema.columns").await.unwrap();
    assert_eq!(info_cols.rows.len(), 3);

    // Multi-statement Simple Query in one roundtrip!
    let multi = client
        .query("CREATE TABLE kv (k TEXT, v BIGINT); INSERT INTO kv VALUES ('a', 1), ('b', 2); SELECT SUM(v) AS total FROM kv;")
        .await
        .unwrap();
    assert_eq!(multi.rows.last().unwrap()[0].as_deref(), Some("3"));

    client.close().await.unwrap();
}
