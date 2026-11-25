// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use axum::routing::post;
use axum::{Json, Router};
use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex;

async fn setup_test_control_plane() -> (tempfile::TempDir, Arc<Coordinator>, String) {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::in_memory());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().to_path_buf()).unwrap());
    let w1 = Arc::new(WorkerNode::new(
        "worker-1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));
    let server = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    (dir, coord, format!("http://{addr}"))
}

#[tokio::test]
async fn wave41_latency_histogram_ops_metrics_table_health_and_otlp_push() {
    let (_dir, coord, base) = setup_test_control_plane().await;

    // Spin up a local OTLP/HTTP trace receiver (`POST /v1/traces`) to prove real push export
    let received_spans: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let rx_clone = received_spans.clone();
    let otlp_app = Router::new().route(
        "/v1/traces",
        post(move |Json(payload): Json<Value>| {
            let rx = rx_clone.clone();
            async move {
                rx.lock().await.push(payload);
                axum::http::StatusCode::OK
            }
        }),
    );
    let otlp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let otlp_addr = otlp_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(otlp_listener, otlp_app).await;
    });
    coord
        .governance
        .set_otlp_endpoint(Some(format!("http://{otlp_addr}")));

    let client = reqwest::Client::new();

    // 1. Verify Unified Control Console UI serves all 7 integrated workspaces
    let ui_html = client
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for ws_id in [
        "ws-overview",
        "ws-sql",
        "ws-catalog",
        "ws-workload",
        "ws-governance",
        "ws-federation",
        "ws-metrics",
    ] {
        assert!(
            ui_html.contains(ws_id),
            "Unified Control Console missing workspace '{ws_id}'"
        );
    }

    // 2. Create table, write 3 tiny Parquet files, query, trigger an auth denial, and compact
    for sql in [
        "CREATE TABLE ops_events (id BIGINT, region TEXT, amount DOUBLE)",
        "INSERT INTO ops_events VALUES (1, 'us-east', 10.5)",
        "INSERT INTO ops_events VALUES (2, 'eu-west', 20.0)",
        "INSERT INTO ops_events VALUES (3, 'ap-south', 30.0)",
        "ALTER TABLE ops_events SET TAGS ('tier' = 'gold')",
    ] {
        let r = client
            .post(format!("{base}/v1/sql"))
            .json(&json!({ "sql": sql, "user": "postgres" }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "SQL failed: {sql}");
    }

    // Check table health before compaction -> should detect 3 small files & recommend compaction
    let health_before: Value = client
        .get(format!("{base}/v1/tables/ops_events/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health_before["file_count"].as_u64(), Some(3));
    assert_eq!(health_before["total_rows"].as_u64(), Some(3));
    assert_eq!(
        health_before["recommendation"].as_str(),
        Some("COMPACT_RECOMMENDED")
    );
    assert_eq!(health_before["tags"]["tier"].as_str(), Some("gold"));

    // Run a SELECT query and check DAG profile + OTLP push
    let q_res: Value = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "SELECT region, SUM(amount) AS rev FROM ops_events GROUP BY region", "user": "postgres" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q_res["row_count"].as_u64(), Some(3));
    assert!(q_res["dag_stages"].is_array());

    // Trigger a 42501 auth denial via unauthorized user
    let denied = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "SELECT * FROM ops_events", "user": "intruder" }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 400);

    // Run COMPACT TABLE to bump compaction telemetry
    let cmp_res = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "COMPACT TABLE ops_events", "user": "postgres" }))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp_res.status(), 200);

    // Give spawned OTLP HTTP push task a brief moment to deliver
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let otlp_payloads = received_spans.lock().await;
    assert!(
        !otlp_payloads.is_empty(),
        "expected OTLP collector to receive pushed resourceSpans"
    );
    assert!(otlp_payloads[0]["resourceSpans"].is_array());
    drop(otlp_payloads);

    // 3. Verify Prometheus /metrics contains histogram buckets & all Wave 41 ops metrics
    let metrics = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for expected in [
        "rustlakedb_query_duration_ms_bucket{le=\"1\"}",
        "rustlakedb_query_duration_ms_bucket{le=\"+Inf\"}",
        "rustlakedb_query_duration_ms_count ",
        "rustlakedb_auth_denied_total ",
        "rustlakedb_compaction_runs_total ",
        "rustlakedb_compaction_files_rewritten_total ",
        "rustlakedb_small_files_total ",
        "rustlakedb_ingest_buffer_rows ",
        "rustlakedb_otlp_spans_exported_total ",
        "rustlakedb_node_draining 0",
    ] {
        assert!(
            metrics.contains(expected),
            "Prometheus /metrics missing '{expected}'"
        );
    }

    // 4. Verify /v1/governance/overview & /v1/cluster/status ops payload
    let gov: Value = client
        .get(format!("{base}/v1/governance/overview"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(gov["auth_denied_total"].as_u64().unwrap_or(0) >= 1);

    let status: Value = client
        .get(format!("{base}/v1/cluster/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status["ops"]["compaction_runs_total"].as_u64().unwrap_or(0) >= 1);
    assert!(status["ops"]["latency_ms"]["p99"].is_u64());
}

#[tokio::test]
async fn wave41_graceful_node_drain_and_verified_catalog_dr_backup() {
    let (_dir, coord, base) = setup_test_control_plane().await;
    let client = reqwest::Client::new();

    // Create table and stage 2 buffered streaming NDJSON rows (not yet flushed)
    let r = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "CREATE TABLE stream_tx (id BIGINT, note TEXT)", "user": "postgres" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let ing = client
        .post(format!("{base}/v1/ingest/stream_tx?buffered=true"))
        .body("{\"id\":1,\"note\":\"a\"}\n{\"id\":2,\"note\":\"b\"}\n")
        .send()
        .await
        .unwrap();
    assert_eq!(ing.status(), 200);

    // 1. Non-admin is forbidden from DR backup & node drain
    let denied_backup = client
        .post(format!("{base}/v1/admin/backup-catalog"))
        .json(&json!({ "user": "analyst" }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_backup.status(), 403);

    let denied_drain = client
        .post(format!("{base}/v1/admin/drain"))
        .json(&json!({ "user": "analyst" }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_drain.status(), 403);

    // 2. Admin triggers verified SHA-256 catalog DR backup
    let backup_resp = client
        .post(format!("{base}/v1/admin/backup-catalog"))
        .json(&json!({ "user": "postgres", "prefix": "_backups/dr_test" }))
        .send()
        .await
        .unwrap();
    assert_eq!(backup_resp.status(), 200);
    let backup: Value = backup_resp.json().await.unwrap();
    assert_eq!(backup["status"].as_str(), Some("verified"));
    assert_eq!(backup["checksum_verified"].as_bool(), Some(true));
    assert_eq!(backup["sha256"].as_str().unwrap().len(), 64);
    assert_eq!(backup["tables"].as_u64(), Some(1));
    let backup_path = backup["backup_path"].as_str().unwrap();
    let raw_bytes = coord.store.get_raw_bytes(backup_path).await.unwrap();
    let parsed: rustlakedb_catalog::FullCatalogSnapshot =
        serde_json::from_slice(&raw_bytes).unwrap();
    assert!(parsed.tables.contains_key("stream_tx"));

    // 3. Admin triggers graceful node drain -> flushes buffered rows, /readyz -> 503, /v1/sql -> 503
    let drain_resp = client
        .post(format!("{base}/v1/admin/drain"))
        .json(&json!({ "user": "postgres", "timeout_ms": 2000 }))
        .send()
        .await
        .unwrap();
    assert_eq!(drain_resp.status(), 200);
    let drain: Value = drain_resp.json().await.unwrap();
    assert_eq!(drain["draining"].as_bool(), Some(true));
    assert_eq!(drain["remaining_active_queries"].as_u64(), Some(0));

    let readyz_draining = client
        .get(format!("{base}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(readyz_draining.status(), 503);

    let sql_draining = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "SELECT COUNT(*) FROM stream_tx", "user": "postgres" }))
        .send()
        .await
        .unwrap();
    assert_eq!(sql_draining.status(), 503);

    let metrics_draining = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics_draining.contains("rustlakedb_node_draining 1"));

    // 4. Undrain resumes service cleanly & confirms buffered streaming rows were preserved!
    let undrain = client
        .post(format!("{base}/v1/admin/undrain"))
        .json(&json!({ "user": "postgres" }))
        .send()
        .await
        .unwrap();
    assert_eq!(undrain.status(), 200);

    let readyz_ok = client.get(format!("{base}/readyz")).send().await.unwrap();
    assert_eq!(readyz_ok.status(), 200);

    let cnt_res: Value = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "SELECT * FROM stream_tx", "user": "postgres" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cnt_res["row_count"].as_u64(), Some(2));
}
