// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::ScalarValue;
use rustlakedb_testkit::TestCluster;

#[tokio::test]
async fn real_tcp_multi_node_distributed_join_and_aggregation() {
    // Spins up 3 independent WorkerHttpServers over localhost TCP + Coordinator using HttpWorkerClient!
    let cluster = TestCluster::start_networked(3).await.unwrap();
    cluster.seed_ecommerce().await.unwrap();

    let res = cluster
        .sql(
            "SELECT tier, COUNT(*) AS cnt, SUM(amount) AS revenue \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             GROUP BY tier ORDER BY tier",
        )
        .await
        .unwrap();

    assert!(res.stats.stages_executed >= 3);
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(
        res.batch.rows[0].get("tier"),
        Some(&ScalarValue::Utf8("enterprise".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("revenue"),
        Some(&ScalarValue::Float64(1549.0))
    );
    assert_eq!(
        res.batch.rows[1].get("tier"),
        Some(&ScalarValue::Utf8("pro".into()))
    );
    assert_eq!(
        res.batch.rows[1].get("revenue"),
        Some(&ScalarValue::Float64(250.0))
    );
}

#[tokio::test]
async fn real_tcp_network_worker_503_failure_retries_cleanly() {
    let cluster = TestCluster::start_networked(3).await.unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // Force remote TCP worker #1 to return HTTP 503 Service Unavailable on its next task!
    cluster.workers[0].set_dead(true);

    let res = cluster
        .sql(
            "SELECT region, SUM(amount) AS total \
             FROM orders GROUP BY region ORDER BY region",
        )
        .await
        .unwrap();

    assert!(
        res.stats.tasks_retried >= 1,
        "expected HTTP 503 from dead TCP worker to trigger task-level retry on healthy worker"
    );
    assert_eq!(res.batch.num_rows(), 3);
    assert_eq!(
        res.batch.rows[0].get("region"),
        Some(&ScalarValue::Utf8("apac".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("total"),
        Some(&ScalarValue::Float64(300.0))
    );
}
