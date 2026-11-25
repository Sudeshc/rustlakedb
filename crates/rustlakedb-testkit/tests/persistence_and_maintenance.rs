// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;

#[tokio::test]
async fn crash_restart_recovery_preserves_tables_branches_indexes_and_dvs() {
    let persistent_dir = tempfile::tempdir().unwrap();
    let cfg = PlannerConfig {
        default_shuffle_partitions: 3,
        shuffle_mode: ShuffleMode::Staged,
        ..Default::default()
    };

    // Session 1: Create table, insert data, add column, index, update, delete, and branch.
    {
        let cluster = TestCluster::open_persistent(persistent_dir.path(), 3, cfg.clone()).unwrap();
        cluster
            .sql("CREATE TABLE ledger (id BIGINT, account TEXT, balance DOUBLE, meta JSONB)")
            .await
            .unwrap();
        cluster
            .sql("CREATE INDEX idx_meta_inv ON ledger USING INVERTED (meta)")
            .await
            .unwrap();
        cluster
            .sql(r#"INSERT INTO ledger VALUES (1, 'alice', 100.0, '{"tier": "gold"}'), (2, 'bob', 250.0, '{"tier": "silver"}')"#)
            .await
            .unwrap();
        cluster
            .sql("UPDATE ledger SET balance = 175.0 WHERE id = 1")
            .await
            .unwrap();
        cluster
            .sql("DELETE FROM ledger WHERE id = 2")
            .await
            .unwrap();
        cluster
            .sql("ALTER TABLE ledger ADD COLUMN currency TEXT")
            .await
            .unwrap();
        cluster
            .sql("CREATE BRANCH audit FROM main ON ledger")
            .await
            .unwrap();
    } // Cluster completely dropped (simulating full process restart!)

    // Session 2: Boot brand-new cluster against the exact same data directory.
    {
        let recovered = TestCluster::open_persistent(persistent_dir.path(), 3, cfg).unwrap();
        let tables = recovered.sql("SHOW TABLES").await.unwrap();
        assert_eq!(tables.batch.num_rows(), 1);

        let res = recovered
            .sql("SELECT id, account, balance, currency FROM ledger ORDER BY id")
            .await
            .unwrap();
        assert_eq!(res.batch.num_rows(), 1);
        assert_eq!(res.batch.rows[0].get("account"), Some(&ScalarValue::Utf8("alice".into())));
        assert_eq!(res.batch.rows[0].get("balance"), Some(&ScalarValue::Float64(175.0)));
        assert_eq!(res.batch.rows[0].get("currency"), Some(&ScalarValue::Null));

        // Verify index metadata and branch survived restart.
        let rest_addr = recovered.start_iceberg_rest().await.unwrap();
        let client = reqwest::Client::new();
        let tbl_doc: serde_json::Value = client
            .get(format!("http://{rest_addr}/v1/namespaces/default/tables/ledger"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(tbl_doc["metadata"]["format-version"], 3);
        assert!(tbl_doc["metadata"]["refs"]["audit"]["snapshot-id"].is_u64());
        assert_eq!(tbl_doc["metadata"]["refs"]["audit"]["type"], "branch");
        assert_eq!(tbl_doc["metadata"]["indexes"].as_array().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn compaction_dv_materialization_time_travel_and_vacuum_gc() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // Delete order #3 (creates a DV sidecar).
    cluster.sql("DELETE FROM orders WHERE order_id = 3").await.unwrap();

    // Time Travel: snapshot 2 is 100% immutable (still sees all 3 rows from INSERT batch 1,
    // unaffected by the later snapshot-scoped DELETE!).
    let tt = cluster
        .sql("SELECT COUNT(*) AS cnt FROM orders AT VERSION 2")
        .await
        .unwrap();
    assert_eq!(tt.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(3)));

    // Compact 3 small Parquet files + Deletion Vector -> 1 clean compacted Parquet file!
    let cmp = cluster.sql("COMPACT TABLE orders").await.unwrap();
    assert_eq!(cmp.command_tag, "COMPACT 3->1");

    // Verify all 5 surviving orders still query cleanly after compaction!
    let after_compact = cluster
        .sql("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM orders")
        .await
        .unwrap();
    assert_eq!(after_compact.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(5)));
    assert_eq!(after_compact.batch.rows[0].get("total"), Some(&ScalarValue::Float64(800.0)));

    // VACUUM expires old snapshots and physically deletes the 3 unreferenced old Parquet files + 1 expired immutable DV!
    let vac = cluster.sql("VACUUM TABLE orders RETAIN 1").await.unwrap();
    assert!(
        vac.command_tag.contains("deleted_files=4"),
        "expected VACUUM to delete 3 compacted-away Parquet files + 1 expired DV, got: {}",
        vac.command_tag
    );

    let data_dir = cluster.data_dir_path().join("orders/data");
    let remaining_parquet: Vec<_> = std::fs::read_dir(data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .collect();
    assert_eq!(remaining_parquet.len(), 1);
}

#[tokio::test]
async fn embedded_iceberg_v3_rest_catalog_http_api() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();
    let addr = cluster.start_iceberg_rest().await.unwrap();
    let http = reqwest::Client::new();

    let health: serde_json::Value = http
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["iceberg_format_version"], 3);

    let cfg: serde_json::Value = http
        .get(format!("http://{addr}/v1/config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cfg["defaults"]["format-version"], "3");

    let list: serde_json::Value = http
        .get(format!("http://{addr}/v1/namespaces/default/tables"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["identifiers"].as_array().unwrap().len(), 2);
}
