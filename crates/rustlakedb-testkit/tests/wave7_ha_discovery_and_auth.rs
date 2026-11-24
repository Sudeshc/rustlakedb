// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::raft::{RaftCatalogNode, RaftRole};
use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata};
use rustlakedb_coordinator::security::sign_jwt_hs256;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ShuffleMode;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_testkit::TestCluster;
use rustlakedb_worker::rpc::WorkerHttpServer;
use rustlakedb_worker::WorkerNode;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_wave7_dynamic_worker_self_registration_hot_join() {
    let data_dir = tempfile::tempdir().unwrap();
    let shuffle_dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(rustlakedb_catalog::InMemoryRaftCatalog::open(data_dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(data_dir.path()).unwrap());
    let coord_shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(shuffle_dir.path()).unwrap());

    // Start Coordinator with ZERO initial workers!
    let coord = Arc::new(Coordinator::with_clients(
        catalog.clone(),
        store.clone(),
        coord_shuffle,
        vec![],
        PlannerConfig {
            default_shuffle_partitions: 2,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    ));
    let cp = rustlakedb_coordinator::admin::ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let cp_addr = cp.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = cp.serve().await;
    });

    // Hot-join 2 remote HTTP workers dynamically via `POST /v1/workers/register`!
    let http = reqwest::Client::new();
    let mut workers = Vec::new();
    for i in 1..=2 {
        let w_shuf = Arc::new(
            HybridShuffleManager::new(shuffle_dir.path().join(format!("dyn_w{i}"))).unwrap(),
        );
        let worker = Arc::new(WorkerNode::new(
            format!("dyn-worker-{i}"),
            catalog.clone(),
            w_shuf.clone(),
            store.clone(),
        ));
        let srv = WorkerHttpServer::bind("127.0.0.1:0", worker.clone())
            .await
            .unwrap();
        let addr = srv.local_addr().unwrap();
        w_shuf.set_advertise_url(format!("http://{addr}"));
        tokio::spawn(async move {
            let _ = srv.serve().await;
        });
        let reg = http
            .post(format!("http://{cp_addr}/v1/workers/register"))
            .json(&serde_json::json!({
                "worker_id": format!("dyn-worker-{i}"),
                "url": format!("http://{addr}")
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(reg.status(), reqwest::StatusCode::OK);
        workers.push(worker);
    }

    assert_eq!(coord.workers_snapshot().len(), 2);

    // Run distributed SQL across the dynamically joined workers!
    coord
        .execute_sql("CREATE TABLE dyn_t (id BIGINT, val DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO dyn_t VALUES (1, 10.0), (2, 20.0)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO dyn_t VALUES (3, 30.0), (4, 40.0)", "main")
        .await
        .unwrap();
    let res = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(val) AS total FROM dyn_t", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1);
    let total_tasks: u64 = workers
        .iter()
        .map(|w| w.tasks_executed.load(Ordering::Relaxed))
        .sum();
    assert!(total_tasks >= 2, "expected dynamically joined workers to execute tasks");
}

#[tokio::test]
async fn test_wave7_jwt_bearer_authentication_with_rbac_and_rls() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // Enable HMAC-SHA256 JWT authentication on Control Plane!
    let secret = "super-secret-lakehouse-jwt-key-2026";
    cluster
        .coordinator
        .governance
        .set_jwt_secret(Some(secret.to_string()));
    cluster.sql("CREATE ROLE jwt_analyst").await.unwrap();
    cluster
        .sql("GRANT SELECT ON orders TO jwt_analyst")
        .await
        .unwrap();
    cluster
        .sql("CREATE POLICY us_only ON orders FOR jwt_analyst USING (region = 'us')")
        .await
        .unwrap();

    let cp_addr = cluster.start_control_plane().await.unwrap();
    let http = reqwest::Client::new();

    // 1. Missing Authorization header -> 401 UNAUTHORIZED
    let unauth = http
        .post(format!("http://{cp_addr}/v1/sql"))
        .json(&serde_json::json!({ "sql": "SELECT * FROM orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 2. Tampered / wrong-secret JWT -> 401 UNAUTHORIZED
    let bad_jwt = sign_jwt_hs256("wrong-secret", "jwt_analyst", "jwt_analyst");
    let bad_resp = http
        .post(format!("http://{cp_addr}/v1/sql"))
        .header("Authorization", format!("Bearer {bad_jwt}"))
        .json(&serde_json::json!({ "sql": "SELECT * FROM orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 3. Valid signed JWT for `jwt_analyst` -> 200 OK + RLS applied (only 3 'us' rows visible!)
    let good_jwt = sign_jwt_hs256(secret, "jwt_analyst", "jwt_analyst");
    let ok_resp = http
        .post(format!("http://{cp_addr}/v1/sql"))
        .header("Authorization", format!("Bearer {good_jwt}"))
        .json(&serde_json::json!({ "sql": "SELECT order_id, region FROM orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = ok_resp.json().await.unwrap();
    assert_eq!(
        body["row_count"], 3,
        "JWT-authenticated jwt_analyst must see only 3 US rows via RLS"
    );
}

#[tokio::test]
async fn test_wave7_persistent_raft_wal_recovery() {
    let d1 = tempfile::tempdir().unwrap();
    let n1 = RaftCatalogNode::open_persistent(1, HashMap::new(), d1.path()).unwrap();
    let a1 = n1.start_server("127.0.0.1:0").await.unwrap();
    n1.set_peers(HashMap::from([(1, format!("http://{a1}"))]));

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        if n1.status().role == RaftRole::Leader {
            break;
        }
    }
    assert_eq!(n1.status().role, RaftRole::Leader);

    n1.create_table(IcebergTableMetadata::new(
        "wal_persisted_table",
        vec![FieldSchema {
            field_id: 1,
            name: "id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        }],
    ))
    .await
    .unwrap();
    let committed_term = n1.status().term;
    let committed_idx = n1.status().commit_index;
    assert!(committed_idx >= 1);
    n1.stop();

    // Reopen Raft node from same data_dir -> verifies WAL recovered term, log, commit_index, and table!
    let recovered = RaftCatalogNode::open_persistent(1, HashMap::new(), d1.path()).unwrap();
    assert_eq!(recovered.status().term, committed_term);
    assert_eq!(recovered.status().commit_index, committed_idx);
    assert!(recovered
        .list_tables()
        .await
        .unwrap()
        .contains(&"wal_persisted_table".to_string()));
}
