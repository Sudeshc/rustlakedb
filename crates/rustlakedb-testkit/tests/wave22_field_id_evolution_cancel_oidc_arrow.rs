// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use axum::routing::get;
use axum::Router;
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::security::{epoch_ms, sign_jwt_rs256_pkcs8};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_common::QueryId;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::io::BufReader;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;

const TEST_RSA_KEY_PEM: &[u8] = include_bytes!("fixtures/mtls/server.key");

#[tokio::test]
async fn wave22_iceberg_field_id_zero_rewrite_schema_evolution_and_columnar_window() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new(
        "w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let coord = Coordinator::new(
        catalog.clone(),
        store.clone(),
        shuffle,
        vec![worker.clone()],
        PlannerConfig::default(),
    );

    // 1. Seed initial Parquet file (v1 schema: id, amt BIGINT, legacy_note TEXT)
    coord
        .execute_sql(
            "CREATE TABLE evo_orders (id BIGINT, amt BIGINT, legacy_note TEXT)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO evo_orders VALUES (1, 100, 'old-1'), (2, 250, 'old-2')",
            "main",
        )
        .await
        .unwrap();
    let written_before = store.parquet_files_written.load(Ordering::Relaxed);
    let orig_meta = catalog.get_table("evo_orders").await.unwrap();
    let amt_field_id = orig_meta
        .schema
        .iter()
        .find(|f| f.name == "amt")
        .unwrap()
        .field_id;

    // 2. Zero-rewrite Iceberg Field-ID evolution: RENAME COLUMN, DROP COLUMN, ALTER COLUMN TYPE!
    coord
        .execute_sql("ALTER TABLE evo_orders RENAME COLUMN amt TO gross_amount", "main")
        .await
        .unwrap();
    coord
        .execute_sql("ALTER TABLE evo_orders DROP COLUMN legacy_note", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "ALTER TABLE evo_orders ALTER COLUMN gross_amount TYPE DOUBLE",
            "main",
        )
        .await
        .unwrap();

    // ZERO historical Parquet files were rewritten!
    assert_eq!(
        store.parquet_files_written.load(Ordering::Relaxed),
        written_before,
        "Iceberg schema evolution must NOT rewrite historical Parquet files"
    );
    let evolved_meta = catalog.get_table("evo_orders").await.unwrap();
    let evolved_field = evolved_meta
        .schema
        .iter()
        .find(|f| f.name == "gross_amount")
        .unwrap();
    assert_eq!(
        evolved_field.field_id, amt_field_id,
        "Iceberg field_id MUST remain stable across RENAME COLUMN"
    );

    // 3. Insert new rows using the evolved schema (new Parquet file has `gross_amount` DOUBLE!)
    coord
        .execute_sql("INSERT INTO evo_orders VALUES (3, 399.75)", "main")
        .await
        .unwrap();

    // 4. Query seamlessly across BOTH old Parquet (`amt` BIGINT) AND new Parquet (`gross_amount` DOUBLE)!
    let res = coord
        .execute_sql("SELECT id, gross_amount FROM evo_orders ORDER BY id ASC", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 3);
    assert_eq!(
        res.batch.rows[0].get("gross_amount"),
        Some(&ScalarValue::Float64(100.0))
    );
    assert_eq!(
        res.batch.rows[1].get("gross_amount"),
        Some(&ScalarValue::Float64(250.0))
    );
    assert_eq!(
        res.batch.rows[2].get("gross_amount"),
        Some(&ScalarValue::Float64(399.75))
    );
    assert!(
        !res.batch.columns.iter().any(|c| c == "legacy_note"),
        "dropped column must not appear in evolved query results"
    );

    // 5. Verify pure Arrow RecordBatch Window fast path executes vectorized!
    coord
        .execute_sql("CREATE TABLE win_tbl (id BIGINT, grp TEXT, val DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO win_tbl VALUES (1,'a',10.0),(2,'a',30.0),(3,'b',20.0),(4,'b',50.0)",
            "main",
        )
        .await
        .unwrap();
    let vec_before = worker.vectorized_rows_processed.load(Ordering::Relaxed);
    let win_res = coord
        .execute_sql(
            "SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn FROM win_tbl ORDER BY id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(win_res.batch.num_rows(), 4);
    assert!(
        worker.vectorized_rows_processed.load(Ordering::Relaxed) >= vec_before + 4,
        "Window over single-split IcebergScan must use pure Arrow RecordBatch columnar path!"
    );
}

#[tokio::test]
async fn wave22_cooperative_query_cancellation_and_live_oidc_jwks_http_poller() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new(
        "w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let coord = Arc::new(Coordinator::new(
        catalog.clone(),
        store,
        shuffle,
        vec![worker.clone()],
        PlannerConfig::default(),
    ));

    // 1. Cooperative DAG cancellation: mark a planned DAG's QueryId as cancelled and verify immediate abort!
    coord
        .execute_sql("CREATE TABLE items (id BIGINT, val DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO items VALUES (1, 10.0), (2, 20.0)", "main")
        .await
        .unwrap();
    let mut dag = match coord
        .planner
        .plan_command("SELECT id, val FROM items", "main", catalog.as_ref())
        .await
        .unwrap()
    {
        rustlakedb_planner::PlannedCommand::Query(d) => d,
        _ => panic!("expected Query DAG"),
    };
    let cancel_uuid = uuid::Uuid::new_v4();
    dag.query_id = QueryId(cancel_uuid);
    coord.governance.cancel_query(&cancel_uuid.to_string());
    let err = coord.execute_dag(dag).await.unwrap_err();
    assert!(
        err.to_string().contains("cancelled by user"),
        "expected cooperative cancellation error, got: {err}"
    );

    // 2. Live OIDC JWKS HTTP refresh endpoint (`refresh_jwks_from_url`)!
    let mut reader = BufReader::new(TEST_RSA_KEY_PEM);
    let pk = rustls_pemfile::private_key(&mut reader).unwrap().unwrap();
    let (token, jwks) = sign_jwt_rs256_pkcs8(
        pk.secret_der(),
        "live-rot-key-2",
        "svc-etl",
        "admin",
        epoch_ms() / 1000 + 3600,
    )
    .unwrap();
    let jwks_str = jwks.to_string();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/.well-known/jwks.json",
        get(move || {
            let body = jwks_str.clone();
            async move { body }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let loaded = coord
        .governance
        .refresh_jwks_from_url(&format!("http://{jwks_addr}/.well-known/jwks.json"))
        .await
        .unwrap();
    assert_eq!(loaded, 1);
    let sess = coord.governance.verify_jwt_rs256(&token).unwrap();
    assert_eq!(sess.user, "svc-etl");
    assert_eq!(sess.role, "admin");
}
