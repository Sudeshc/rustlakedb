// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_testkit::TestCluster;
use rustlakedb_worker::WorkerNode;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

async fn setup_ai_control_plane() -> (tempfile::TempDir, Arc<Coordinator>, String) {
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
async fn wave42_native_vector_fixed_list_hnsw_worker_pushdown_dot_and_rrf() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();

    // 1. Native `VECTOR(3)` column stored as compact Arrow `FixedSizeList<Float32>` Parquet!
    cluster
        .sql("CREATE TABLE rag_corpus (doc_id BIGINT, body TEXT, emb VECTOR(3))")
        .await
        .unwrap();
    cluster
        .sql(
            "INSERT INTO rag_corpus VALUES \
             (1, 'Apache Iceberg v3 Rust lakehouse storage engine', '[0.95, 0.05, 0.0]'), \
             (2, 'Distributed Arrow Flight GPU data loader for PyTorch', '[0.60, 0.70, 0.10]'), \
             (3, 'French bakery sourdough bread recipe', '[0.02, 0.05, 0.98]')",
        )
        .await
        .unwrap();

    // 2. Create HNSW index, then INSERT a 4th row to verify automatic HNSW sidecar maintenance on INSERT!
    cluster
        .sql("CREATE INDEX idx_rag_emb ON rag_corpus USING HNSW (emb)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO rag_corpus VALUES (4, 'Rust Iceberg compaction and Puffin indexes', '[0.99, 0.01, 0.0]')")
        .await
        .unwrap();

    // 3. Query with Top-N ordered by VECTOR_COSINE -> must fire REAL worker-side HNSW ANN pushdown!
    let hits_before = cluster.store.hnsw_index_hits.load(Ordering::Relaxed);
    let top_ann = cluster
        .sql(
            "SELECT doc_id, VECTOR_COSINE(emb, '[1.0, 0.0, 0.0]') AS sim \
             FROM rag_corpus ORDER BY sim DESC LIMIT 2",
        )
        .await
        .unwrap();
    let hits_after = cluster.store.hnsw_index_hits.load(Ordering::Relaxed);
    assert!(
        hits_after > hits_before,
        "expected worker-side HNSW ANN pushdown to fire (hits before={hits_before}, after={hits_after})"
    );
    assert_eq!(top_ann.batch.num_rows(), 2);
    assert_eq!(top_ann.batch.rows[0].get("doc_id"), Some(&ScalarValue::Int64(4)));
    assert_eq!(top_ann.batch.rows[1].get("doc_id"), Some(&ScalarValue::Int64(1)));

    // 4. Inner product `VECTOR_DOT`
    let dot_res = cluster
        .sql(
            "SELECT doc_id, VECTOR_DOT(emb, '[0.0, 0.0, 1.0]') AS ip \
             FROM rag_corpus ORDER BY ip DESC LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(dot_res.batch.rows[0].get("doc_id"), Some(&ScalarValue::Int64(3)));

    // 5. Hybrid Reciprocal Rank Fusion (`RRF(BM25_SCORE(...), VECTOR_COSINE(...))`)
    let rrf_res = cluster
        .sql(
            "SELECT doc_id, RRF(BM25_SCORE(body, 'iceberg rust puffin'), VECTOR_COSINE(emb, '[1.0, 0.0, 0.0]')) AS hybrid_score \
             FROM rag_corpus ORDER BY hybrid_score DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(rrf_res.batch.num_rows(), 2);
    assert_eq!(rrf_res.batch.rows[0].get("doc_id"), Some(&ScalarValue::Int64(4)));
    let top_rrf = rrf_res.batch.rows[0]
        .get("hybrid_score")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert!(top_rrf > 0.03, "expected strong top-1 RRF score (~2/61), got {top_rrf}");
}

#[tokio::test]
async fn wave42_point_in_time_asof_join_feature_views_and_model_registry() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();

    // 1. Seed transaction labels & time-varying user risk features (with future rows that MUST NOT leak!)
    cluster
        .sql("CREATE TABLE tx_events (tx_id BIGINT, user_id BIGINT, event_ts BIGINT, amount DOUBLE)")
        .await
        .unwrap();
    cluster
        .sql("CREATE TABLE user_risk_fv_data (user_id BIGINT, feature_ts BIGINT, risk_score DOUBLE)")
        .await
        .unwrap();
    cluster
        .sql(
            "INSERT INTO tx_events VALUES \
             (101, 1, 1000, 50.0), \
             (102, 1, 2500, 900.0), \
             (103, 2, 1500, 20.0)",
        )
        .await
        .unwrap();
    cluster
        .sql(
            "INSERT INTO user_risk_fv_data VALUES \
             (1, 500, 0.12), \
             (1, 2000, 0.85), \
             (1, 3000, 0.99), \
             (2, 2000, 0.44)",
        )
        .await
        .unwrap();

    // 2. Point-in-time LEFT ASOF JOIN:
    //    - tx 101 (user 1, ts=1000) -> MUST get risk_score=0.12 (ts=500), NOT 0.85 or 0.99!
    //    - tx 102 (user 1, ts=2500) -> MUST get risk_score=0.85 (ts=2000), NOT 0.99!
    //    - tx 103 (user 2, ts=1500) -> MUST get NULL (only feature for user 2 is in the future at ts=2000)!
    let asof = cluster
        .sql(
            "SELECT e.tx_id, e.user_id, f.risk_score \
             FROM tx_events e \
             LEFT ASOF JOIN user_risk_fv_data f \
             ON e.user_id = f.user_id AND f.feature_ts <= e.event_ts \
             ORDER BY e.tx_id ASC",
        )
        .await
        .unwrap();
    assert_eq!(asof.batch.num_rows(), 3);
    assert_eq!(asof.batch.rows[0].get("tx_id"), Some(&ScalarValue::Int64(101)));
    assert_eq!(asof.batch.rows[0].get("risk_score"), Some(&ScalarValue::Float64(0.12)));
    assert_eq!(asof.batch.rows[1].get("tx_id"), Some(&ScalarValue::Int64(102)));
    assert_eq!(asof.batch.rows[1].get("risk_score"), Some(&ScalarValue::Float64(0.85)));
    assert_eq!(asof.batch.rows[2].get("tx_id"), Some(&ScalarValue::Int64(103)));
    assert_eq!(asof.batch.rows[2].get("risk_score"), Some(&ScalarValue::Null));

    // 3. Register ML Feature View
    cluster
        .sql(
            "CREATE FEATURE VIEW user_risk_features \
             ENTITY user_id TIMESTAMP feature_ts \
             AS SELECT user_id, feature_ts, risk_score FROM user_risk_fv_data",
        )
        .await
        .unwrap();
    let fvs = cluster.sql("SELECT * FROM system.feature_views").await.unwrap();
    assert_eq!(fvs.batch.num_rows(), 1);
    assert_eq!(
        fvs.batch.rows[0].get("feature_view"),
        Some(&ScalarValue::Utf8("user_risk_features".into())),
        "unexpected row: {:?}",
        fvs.batch.rows[0]
    );

    // 4. Register ML Model with automatic Iceberg dataset@snapshot_id lineage + governance stage promotion!
    cluster
        .sql(
            "CREATE MODEL fraud_detector VERSION 1 \
             FRAMEWORK 'pytorch' \
             ARTIFACT 's3://ml-models/fraud/v1.onnx' \
             FROM TABLE tx_events \
             METRICS ('auc' = 0.971, 'f1' = 0.895) \
             STAGE 'staging'",
        )
        .await
        .unwrap();

    // Non-admin cannot promote a model to `production` (42501)
    let ml_eng = rustlakedb_coordinator::security::SessionContext::for_user("ml_eng");
    let deny_prod = cluster
        .coordinator
        .execute_sql_with_session(
            "ALTER MODEL fraud_detector VERSION 1 SET STAGE 'production'",
            "main",
            &ml_eng,
        )
        .await;
    assert!(deny_prod.is_err());
    assert!(deny_prod.unwrap_err().to_string().contains("42501"));

    // Admin promotes model to `production`
    cluster
        .sql("ALTER MODEL fraud_detector VERSION 1 SET STAGE 'production'")
        .await
        .unwrap();
    let models = cluster.sql("SELECT * FROM system.models").await.unwrap();
    assert_eq!(models.batch.num_rows(), 1);
    assert_eq!(
        models.batch.rows[0].get("stage"),
        Some(&ScalarValue::Utf8("production".into()))
    );
    assert!(
        models.batch.rows[0]
            .get("training_snapshot_id")
            .and_then(|v| v.as_f64())
            .is_some(),
        "expected training_snapshot_id lineage to be captured automatically"
    );
}

#[tokio::test]
async fn wave42_governed_sql_ai_functions_mcp_agent_gateway_and_console_hub() {
    let (_dir, _coord, base) = setup_ai_control_plane().await;
    let client = reqwest::Client::new();

    // 1. Seed a table + test in-SQL AI_EMBED, AI_CLASSIFY, AI_COMPLETE, AI_QUERY
    for sql in [
        "CREATE TABLE tickets (id BIGINT, body TEXT)",
        "INSERT INTO tickets VALUES (1, 'Billing refund charge on invoice'), (2, 'Database query outage and crash')",
        "CREATE MODEL support_cls VERSION 1 FRAMEWORK 'onnx' ARTIFACT 's3://m/cls.onnx' FROM TABLE tickets METRICS ('acc' = 0.94) STAGE 'production'",
    ] {
        let r = client
            .post(format!("{base}/v1/sql"))
            .json(&json!({ "sql": sql, "user": "postgres" }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "failed SQL: {sql}");
    }

    let ai_res: Value = client
        .post(format!("{base}/v1/sql"))
        .json(&json!({
            "sql": "SELECT id, AI_CLASSIFY(body, 'billing,outage,general') AS label, AI_EMBED(body) AS emb, AI_COMPLETE(body, 'demo-llm') AS summary FROM tickets ORDER BY id",
            "user": "postgres"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ai_res["row_count"].as_u64(), Some(2));
    assert_eq!(ai_res["rows"][0][1]["Utf8"].as_str(), Some("billing"));
    assert_eq!(ai_res["rows"][1][1]["Utf8"].as_str(), Some("outage"));
    assert!(ai_res["rows"][0][2]["Json"].is_array());
    assert_eq!(ai_res["rows"][0][2]["Json"].as_array().unwrap().len(), 8);

    // 2. Test Governed MCP JSON-RPC 2.0 endpoint (`POST /v1/mcp`)
    let init: Value = client
        .post(format!("{base}/v1/mcp"))
        .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(init["result"]["serverInfo"]["name"].as_str(), Some("rustlakedb-mcp"));

    let tools: Value = client
        .post(format!("{base}/v1/mcp"))
        .json(&json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 5);

    // Authorized MCP agent calls `list_models` and `query_sql`
    let call_models: Value = client
        .post(format!("{base}/v1/mcp"))
        .json(&json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "list_models", "arguments": {} }
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(call_models["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        call_models["result"]["data"]["models"][0]["name"].as_str(),
        Some("support_cls")
    );

    // Unauthorized MCP agent calling `query_sql` is blocked by RBAC (42501)!
    let denied_mcp: Value = client
        .post(format!("{base}/v1/mcp"))
        .header("x-rustlakedb-user", "rogue_agent")
        .json(&json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "query_sql", "arguments": { "sql": "SELECT * FROM tickets" } }
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(denied_mcp["result"]["isError"].as_bool(), Some(true));
    assert!(
        denied_mcp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("42501")
    );

    // 3. Verify AI & ML Hub workspace in Unified Control Console + `/v1/ml/models` + `/metrics`
    let ui_html = client
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(ui_html.contains("ws-aiml"));
    assert!(ui_html.contains("/v1/mcp"));

    let ml_models: Value = client
        .get(format!("{base}/v1/ml/models"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ml_models["count"].as_u64(), Some(1));

    let metrics = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("rustlakedb_hnsw_index_hits_total "));
}
