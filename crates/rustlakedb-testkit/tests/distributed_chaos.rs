// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;
use std::collections::HashMap;

#[tokio::test]
async fn pipelined_and_staged_shuffles_produce_identical_distributed_join_results() {
    let sql = "SELECT tier, COUNT(*) AS cnt, SUM(amount) AS revenue \
               FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
               GROUP BY tier ORDER BY tier";

    let c_staged = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    c_staged.seed_ecommerce().await.unwrap();
    let res_staged = c_staged.sql(sql).await.unwrap();

    let c_pipe = TestCluster::start(4, ShuffleMode::Pipelined).unwrap();
    c_pipe.seed_ecommerce().await.unwrap();
    let res_pipe = c_pipe.sql(sql).await.unwrap();

    assert_eq!(res_staged.batch.rows, res_pipe.batch.rows);
    assert_eq!(res_staged.batch.num_rows(), 2);
    assert_eq!(
        res_staged.batch.rows[0].get("tier"),
        Some(&ScalarValue::Utf8("enterprise".into()))
    );
    // enterprise = 100 + 999 + 150 + 300 = 1549
    assert_eq!(
        res_staged.batch.rows[0].get("revenue"),
        Some(&ScalarValue::Float64(1549.0))
    );
    // pro = 200 + 50 = 250
    assert_eq!(
        res_staged.batch.rows[1].get("revenue"),
        Some(&ScalarValue::Float64(250.0))
    );
}

#[tokio::test]
async fn chaos_multiple_worker_crashes_recover_via_task_retries() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // Permanently kill worker-1 and inject a transient crash on worker-2!
    cluster.workers[0].set_dead(true);
    cluster.workers[1].inject_single_failure();

    let res = cluster
        .sql(
            "SELECT tier, SUM(amount) AS revenue \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             GROUP BY tier ORDER BY tier",
        )
        .await
        .unwrap();

    assert!(res.stats.tasks_retried >= 1, "expected retry after worker-2 transient failure");
    let mut by_tier: HashMap<String, f64> = HashMap::new();
    for r in &res.batch.rows {
        by_tier.insert(
            r.get("tier").unwrap().as_str().unwrap().to_string(),
            r.get("revenue").unwrap().as_f64().unwrap(),
        );
    }
    assert_eq!(by_tier.get("enterprise"), Some(&1549.0));
    assert_eq!(by_tier.get("pro"), Some(&250.0));
}

#[tokio::test]
async fn aqe_barrier_detects_heavily_skewed_shuffle_partition() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE clicks (hot_key TEXT, weight DOUBLE)")
        .await
        .unwrap();

    // Build a 40:1 skewed dataset across 2 splits so one hash partition dominates >3x median.
    let mut skewed_values = Vec::new();
    for _ in 0..40 {
        skewed_values.push("('bot_user_000000000000000000', 1.0)".to_string());
    }
    skewed_values.push("('normal_a', 1.0)".to_string());
    skewed_values.push("('normal_b', 1.0)".to_string());
    skewed_values.push("('normal_c', 1.0)".to_string());

    let insert_sql = format!("INSERT INTO clicks VALUES {}", skewed_values.join(", "));
    cluster.sql(&insert_sql).await.unwrap();
    cluster.sql(&insert_sql).await.unwrap();

    // Force a shuffle on raw rows via join where all 80 skewed rows hit 1 hash partition.
    cluster
        .sql("CREATE TABLE users (hot_key TEXT, label TEXT)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO users VALUES ('bot_user_000000000000000000', 'bot'), ('normal_a', 'ok'), ('normal_b', 'ok'), ('normal_c', 'ok')")
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
        !res.stats.skewed_partitions_detected.is_empty(),
        "expected AQE barrier to flag skewed join partition"
    );
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(
        res.batch.rows[0].get("label"),
        Some(&ScalarValue::Utf8("bot".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("cnt"),
        Some(&ScalarValue::Int64(80))
    );
    assert_eq!(
        res.batch.rows[1].get("label"),
        Some(&ScalarValue::Utf8("ok".into()))
    );
    assert_eq!(
        res.batch.rows[1].get("cnt"),
        Some(&ScalarValue::Int64(6))
    );
}
