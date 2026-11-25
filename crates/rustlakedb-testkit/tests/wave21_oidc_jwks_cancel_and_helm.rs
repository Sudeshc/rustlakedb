// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::security::{epoch_ms, sign_jwt_rs256_pkcs8};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use serde_json::json;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const TEST_RSA_KEY_PEM: &[u8] = include_bytes!("fixtures/mtls/server.key");

#[tokio::test]
async fn wave21_oidc_jwks_rs256_auth_query_cancel_web_ui_and_helm_validation() {
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
        catalog,
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE secrets (id BIGINT, owner TEXT, val DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO secrets VALUES (1, 'alice', 99.5), (2, 'bob', 42.0)", "main")
        .await
        .unwrap();
    coord.governance.grant("analyst", "secrets", "SELECT");

    // 1. Parse real RSA-2048 PKCS#8 private key and mint genuine RS256 OIDC JWT + RFC 7517 JWKS!
    let mut reader = BufReader::new(TEST_RSA_KEY_PEM);
    let pk = rustls_pemfile::private_key(&mut reader)
        .unwrap()
        .expect("RSA private key");
    let pkcs8_der = pk.secret_der();

    let now_secs = epoch_ms() / 1000;
    let (valid_jwt, jwks_val) =
        sign_jwt_rs256_pkcs8(pkcs8_der, "oidc-key-1", "alice", "analyst", now_secs + 3600).unwrap();
    let (expired_jwt, _) =
        sign_jwt_rs256_pkcs8(pkcs8_der, "oidc-key-1", "alice", "analyst", now_secs - 60).unwrap();

    let loaded_keys = coord
        .governance
        .set_jwks_json(&jwks_val.to_string())
        .unwrap();
    assert_eq!(loaded_keys, 1);

    let cp = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let addr = cp.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = cp.serve().await;
    });
    let base = format!("http://127.0.0.1:{}", addr.port());
    let http = reqwest::Client::new();

    // 2. Negative auth proofs: missing token, tampered RS256 signature, expired RS256 token -> 401!
    let no_auth = http
        .post(format!("{base}/v1/sql"))
        .json(&json!({ "sql": "SELECT * FROM secrets" }))
        .send()
        .await
        .unwrap();
    assert_eq!(no_auth.status(), 401);

    let tampered = format!("{valid_jwt}X");
    let bad_sig = http
        .post(format!("{base}/v1/sql"))
        .header("Authorization", format!("Bearer {tampered}"))
        .json(&json!({ "sql": "SELECT * FROM secrets" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_sig.status(), 401);

    let exp_resp = http
        .post(format!("{base}/v1/sql"))
        .header("Authorization", format!("Bearer {expired_jwt}"))
        .json(&json!({ "sql": "SELECT * FROM secrets" }))
        .send()
        .await
        .unwrap();
    assert_eq!(exp_resp.status(), 401);

    // 3. Positive OIDC RS256 proof: valid RS256 JWT authenticates & enforces RBAC!
    let ok_resp = http
        .post(format!("{base}/v1/sql"))
        .header("Authorization", format!("Bearer {valid_jwt}"))
        .json(&json!({ "sql": "SELECT id, owner FROM secrets ORDER BY id" }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), 200);
    let ok_json: serde_json::Value = ok_resp.json().await.unwrap();
    assert_eq!(ok_json["row_count"], 2);

    // 4. Active query cancellation (HTTP POST /v1/queries/{id}/cancel + SQL CANCEL QUERY + Web UI)
    coord
        .governance
        .register_active_query("q-long-123", "alice", "SELECT * FROM huge_table")
        .unwrap();
    let active_json: serde_json::Value = http
        .get(format!("{base}/v1/queries/active"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(active_json["active_queries"].as_array().unwrap().len(), 1);

    let cancel_json: serde_json::Value = http
        .post(format!("{base}/v1/queries/q-long-123/cancel"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cancel_json["cancelled"], true);
    assert_eq!(cancel_json["was_active"], true);
    assert!(coord.governance.is_query_cancelled("q-long-123"));

    // SQL CANCEL QUERY syntax
    let sql_cancel = coord
        .execute_sql("CANCEL QUERY 'q-sql-456'", "main")
        .await
        .unwrap();
    assert!(sql_cancel.command_tag.starts_with("CANCEL "));
    assert!(coord.governance.is_query_cancelled("q-sql-456"));

    // Web Console UI renders Cluster Topology, Flight SQL, OIDC/JWKS & Cancel controls
    let ui_html = http.get(format!("{base}/")).send().await.unwrap().text().await.unwrap();
    assert!(ui_html.contains("Cluster Topology"));
    assert!(ui_html.contains("Arrow Flight SQL (:8815)"));
    assert!(ui_html.contains("/v1/queries/"));

    // 5. Kubernetes Helm Chart structural & manifest validation
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let chart = std::fs::read_to_string(repo_root.join("deploy/helm/rustlakedb/Chart.yaml")).unwrap();
    let values = std::fs::read_to_string(repo_root.join("deploy/helm/rustlakedb/values.yaml")).unwrap();
    let tmpl = std::fs::read_to_string(repo_root.join("deploy/helm/rustlakedb/templates/cluster.yaml")).unwrap();
    assert!(chart.contains("name: rustlakedb"));
    assert!(values.contains("flightPort: 8815"));
    assert!(values.contains("oidcJwksUrl:"));
    assert!(values.contains("secretName: \"rustlakedb-cluster-mtls\""));
    assert!(tmpl.contains("kind: HorizontalPodAutoscaler"));
    assert!(tmpl.contains("autoscaling/v2"));
    assert!(tmpl.contains("--flight-addr"));
    assert!(tmpl.contains("RUSTLAKEDB_MTLS_CA"));
    assert!(tmpl.contains("RUSTLAKEDB_OIDC_JWKS_URL"));
}
