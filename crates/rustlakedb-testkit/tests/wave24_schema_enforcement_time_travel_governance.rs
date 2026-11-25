// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 24: Strict Schema Enforcement (`NOT NULL`, `DEFAULT`, `CHECK`), Timestamp Time Travel,
//! Atomic `RESTORE TABLE TO VERSION`, `SHOW SNAPSHOTS`, Catalog Tagging (`SET TAG`),
//! Automatic Data Lineage (`system.lineage`), and Centralized Governance REST APIs.

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;

fn make_cluster(dir: &std::path::Path) -> Arc<Coordinator> {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.join("shuffle")).unwrap());
    let workers: Vec<Arc<WorkerNode>> = (1..=2)
        .map(|i| {
            Arc::new(WorkerNode::new(
                format!("w-{i}"),
                catalog.clone(),
                shuffle.clone(),
                store.clone(),
            ))
        })
        .collect();
    Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        workers,
        PlannerConfig::default(),
    ))
}

#[tokio::test]
async fn wave24_schema_enforcement_not_null_default_check_and_time_travel_restore() {
    let dir = tempdir().unwrap();
    let coord = make_cluster(dir.path());

    // 1. Create table with NOT NULL, DEFAULT, and CHECK constraints
    coord
        .execute_sql(
            "CREATE TABLE ledger (
                id BIGINT NOT NULL,
                tier TEXT DEFAULT 'standard',
                amount DOUBLE NOT NULL CHECK (amount > 0),
                CHECK (tier IN ('enterprise', 'pro', 'standard'))
            )",
            "main",
        )
        .await
        .unwrap();

    // Valid insert relying on DEFAULT 'standard'
    coord
        .execute_sql("INSERT INTO ledger (id, amount) VALUES (1, 100.0)", "main")
        .await
        .unwrap();

    let r1 = coord
        .execute_sql("SELECT id, tier, amount FROM ledger", "main")
        .await
        .unwrap();
    assert_eq!(r1.batch.num_rows(), 1);
    assert_eq!(
        r1.batch.rows[0].get("tier"),
        Some(&ScalarValue::Utf8("standard".into()))
    );

    // 2. Reject NOT NULL violation (23502)
    let err_nn = coord
        .execute_sql("INSERT INTO ledger (id, tier, amount) VALUES (2, 'pro', NULL)", "main")
        .await
        .unwrap_err();
    assert!(
        err_nn.to_string().contains("23502") || err_nn.to_string().contains("NOT NULL"),
        "expected NOT NULL error, got: {err_nn}"
    );

    // 3. Reject CHECK constraint violation (amount <= 0)
    let err_chk = coord
        .execute_sql("INSERT INTO ledger (id, tier, amount) VALUES (3, 'pro', -50.0)", "main")
        .await
        .unwrap_err();
    assert!(
        err_chk.to_string().contains("23514") || err_chk.to_string().contains("CHECK"),
        "expected CHECK error, got: {err_chk}"
    );

    // 4. Reject table-level CHECK constraint violation (invalid tier)
    let err_tier = coord
        .execute_sql("INSERT INTO ledger (id, tier, amount) VALUES (4, 'unknown', 25.0)", "main")
        .await
        .unwrap_err();
    assert!(
        err_tier.to_string().contains("23514") || err_tier.to_string().contains("CHECK"),
        "expected tier CHECK error, got: {err_tier}"
    );

    // 5. Capture snapshot v2 timestamp & add a second valid batch (snapshot v3)
    let snaps_before = coord
        .execute_sql("SHOW SNAPSHOTS ON ledger", "main")
        .await
        .unwrap();
    assert_eq!(snaps_before.batch.num_rows(), 2); // initial snap 1 + insert snap 2
    let snap2_ts = match snaps_before.batch.rows[1].get("timestamp_ms") {
        Some(ScalarValue::Int64(ts)) => *ts as u64,
        other => panic!("unexpected timestamp_ms: {other:?}"),
    };
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    coord
        .execute_sql(
            "INSERT INTO ledger (id, tier, amount) VALUES (2, 'enterprise', 500.0)",
            "main",
        )
        .await
        .unwrap();
    let cur = coord
        .execute_sql("SELECT COUNT(*) AS cnt FROM ledger", "main")
        .await
        .unwrap();
    assert_eq!(cur.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(2)));

    // 6. Time Travel by Timestamp (epoch ms) sees only 1 row from snapshot 2!
    let tt_ts = coord
        .execute_sql(
            &format!("SELECT COUNT(*) AS cnt FROM ledger FOR SYSTEM_TIME AS OF TIMESTAMP '{snap2_ts}'"),
            "main",
        )
        .await
        .unwrap();
    assert_eq!(tt_ts.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(1)));

    // 7. Atomic RESTORE TABLE TO VERSION 2 rolls current main branch back to 1 row while preserving history!
    let restored = coord
        .execute_sql("RESTORE TABLE ledger TO VERSION 2", "main")
        .await
        .unwrap();
    assert!(restored.command_tag.contains("RESTORE TABLE"));

    let after_restore = coord
        .execute_sql("SELECT COUNT(*) AS cnt FROM ledger", "main")
        .await
        .unwrap();
    assert_eq!(
        after_restore.batch.rows[0].get("cnt"),
        Some(&ScalarValue::Int64(1))
    );

    // Audit history preserved: snapshot 4 created with operation='restore'
    let snaps_after = coord
        .execute_sql("SHOW SNAPSHOTS ON ledger", "main")
        .await
        .unwrap();
    assert_eq!(snaps_after.batch.num_rows(), 4);
}

#[tokio::test]
async fn wave24_centralized_governance_tags_lineage_system_tables_and_rest_api() {
    let dir = tempdir().unwrap();
    let coord = make_cluster(dir.path());

    coord
        .execute_sql("CREATE TABLE raw_orders (id BIGINT, region TEXT, amount DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO raw_orders VALUES (1, 'us-east', 100.0), (2, 'eu-west', 200.0)",
            "main",
        )
        .await
        .unwrap();

    // 1. Tagging & Classification
    coord
        .execute_sql(
            "ALTER TABLE raw_orders SET TAG ('pii'='false', 'domain'='commerce', 'tier'='bronze')",
            "main",
        )
        .await
        .unwrap();

    // 2. Downstream CTAS & INSERT SELECT automatically record data lineage!
    coord
        .execute_sql(
            "CREATE TABLE gold_orders AS SELECT region, SUM(amount) AS revenue FROM raw_orders GROUP BY region",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "ALTER TABLE gold_orders SET TAG ('domain'='commerce', 'tier'='gold')",
            "main",
        )
        .await
        .unwrap();

    // 3. Query virtual governance tables (`system.tags`, `system.lineage`, `system.tables`, `system.snapshots`)
    let tags = coord.execute_sql("SELECT * FROM system.tags", "main").await.unwrap();
    assert_eq!(tags.batch.num_rows(), 5);

    let lineage = coord
        .execute_sql("SELECT * FROM system.lineage", "main")
        .await
        .unwrap();
    assert!(
        lineage.batch.num_rows() >= 1,
        "expected automatic lineage edge raw_orders -> gold_orders"
    );
    assert_eq!(
        lineage.batch.rows[0].get("source_table"),
        Some(&ScalarValue::Utf8("raw_orders".into()))
    );
    assert_eq!(
        lineage.batch.rows[0].get("target_table"),
        Some(&ScalarValue::Utf8("gold_orders".into()))
    );

    let sys_tables = coord
        .execute_sql("SELECT * FROM system.tables", "main")
        .await
        .unwrap();
    assert_eq!(sys_tables.batch.num_rows(), 2);

    // 4. Verify Centralized Governance REST API endpoints (`/v1/governance/lineage`, `/tags`, `/audit`, `/v1/tables/{t}/snapshots`)
    let server = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve().await;
    });

    let client = reqwest::Client::new();
    let lin_json: serde_json::Value = client
        .get(format!("http://{addr}/v1/governance/lineage"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(lin_json["count"].as_u64().unwrap() >= 1);

    let tags_json: serde_json::Value = client
        .get(format!("http://{addr}/v1/governance/tags"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tags_json["tags"]["raw_orders"]["domain"], "commerce");
    assert_eq!(tags_json["tags"]["gold_orders"]["tier"], "gold");

    let snap_json: serde_json::Value = client
        .get(format!("http://{addr}/v1/tables/raw_orders/snapshots"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snap_json["table"], "raw_orders");
    assert_eq!(snap_json["snapshots"].as_array().unwrap().len(), 2);
}
