// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use rustlakedb_catalog::rest::{IcebergRestServer, StsConfig};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::security::{epoch_ms, sign_jwt_rs256_pkcs8_with_claims};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use serde_json::{json, Value};
use std::io::BufReader;
use std::sync::{Arc, RwLock};
use tempfile::TempDir;
use tokio::net::TcpListener;

const RSA_KEY_1_PEM: &[u8] = include_bytes!("fixtures/mtls/server.key");
const RSA_KEY_2_PEM: &[u8] = include_bytes!("fixtures/mtls/client.key");

fn load_pkcs8_der(pem: &[u8]) -> Vec<u8> {
    let mut reader = BufReader::new(pem);
    let pk = rustls_pemfile::private_key(&mut reader)
        .unwrap()
        .expect("RSA private key");
    pk.secret_der().to_vec()
}

#[tokio::test]
async fn wave36_oidc_discovery_auto_jwks_rotation_and_claims() {
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
        .execute_sql("CREATE TABLE oidc_docs (id BIGINT, title VARCHAR)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO oidc_docs VALUES (1, 'confidential'), (2, 'public')", "main")
        .await
        .unwrap();
    coord.governance.grant("finance", "oidc_docs", "SELECT");

    let key1 = load_pkcs8_der(RSA_KEY_1_PEM);
    let key2 = load_pkcs8_der(RSA_KEY_2_PEM);
    let now = epoch_ms() / 1000;

    // Spin up a real local HTTP OIDC Identity Provider (Okta/Auth0/Entra compatible).
    let idp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let idp_port = idp_listener.local_addr().unwrap().port();
    let issuer = format!("http://127.0.0.1:{idp_port}");
    let jwks_url = format!("{issuer}/jwks.json");

    let (tok_v1, jwks_v1) = sign_jwt_rs256_pkcs8_with_claims(
        &key1,
        "kid-v1",
        "alice",
        "finance",
        now + 3600,
        Some(&issuer),
        Some("rustlakedb-prod"),
    )
    .unwrap();
    let (tok_v2, jwks_v2) = sign_jwt_rs256_pkcs8_with_claims(
        &key2,
        "kid-v2",
        "bob",
        "finance",
        now + 3600,
        Some(&issuer),
        Some("rustlakedb-prod"),
    )
    .unwrap();
    let (tok_wrong_aud, _) = sign_jwt_rs256_pkcs8_with_claims(
        &key2,
        "kid-v2",
        "bob",
        "finance",
        now + 3600,
        Some(&issuer),
        Some("wrong-audience"),
    )
    .unwrap();

    let active_jwks = Arc::new(RwLock::new(jwks_v1));
    let issuer_cl = issuer.clone();
    let jwks_url_cl = jwks_url.clone();
    let idp_router = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(move || {
                let iss = issuer_cl.clone();
                let juri = jwks_url_cl.clone();
                async move {
                    Json(json!({
                        "issuer": iss,
                        "jwks_uri": juri,
                        "id_token_signing_alg_values_supported": ["RS256"]
                    }))
                }
            }),
        )
        .route(
            "/jwks.json",
            get(|State(st): State<Arc<RwLock<Value>>>| async move {
                let doc = st.read().unwrap().clone();
                Json(doc)
            }),
        )
        .with_state(active_jwks.clone());

    tokio::spawn(async move {
        let _ = axum::serve(idp_listener, idp_router).await;
    });

    // 1. Discover OIDC metadata & initial JWKS (kid-v1) via /.well-known/openid-configuration.
    let loaded = coord
        .governance
        .configure_oidc_discovery(&issuer, Some("rustlakedb-prod".into()))
        .await
        .unwrap();
    assert_eq!(loaded, 1);

    let cp = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let cp_addr = cp.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = cp.serve().await;
    });
    let sql_url = format!("http://127.0.0.1:{}/v1/sql", cp_addr.port());
    let http = reqwest::Client::new();

    // 2. Authenticate with kid-v1 token -> 200 OK!
    let r1 = http
        .post(&sql_url)
        .bearer_auth(&tok_v1)
        .json(&json!({"sql": "SELECT COUNT(*) AS cnt FROM oidc_docs"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);

    // 3. Rotate the IdP's signing key to kid-v2! RustLakeDB must auto-refresh JWKS & succeed!
    *active_jwks.write().unwrap() = jwks_v2;
    let r2 = http
        .post(&sql_url)
        .bearer_auth(&tok_v2)
        .json(&json!({"sql": "SELECT COUNT(*) AS cnt FROM oidc_docs"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        200,
        "expected automatic JWKS rotation refresh on new kid-v2"
    );

    // 4. Token with mismatched `aud` claim must be rejected (401 Unauthorized)!
    let r3 = http
        .post(&sql_url)
        .bearer_auth(&tok_wrong_aud)
        .json(&json!({"sql": "SELECT COUNT(*) AS cnt FROM oidc_docs"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 401);
}

#[tokio::test]
async fn wave36_iceberg_rest_vended_credentials_header_with_sigv4_sts() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog.clone(),
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE vended_sales (id BIGINT, amt DOUBLE)", "main")
        .await
        .unwrap();

    // Local mock AWS STS endpoint verifying AWS4-HMAC-SHA256 Authorization header.
    let sts_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sts_port = sts_listener.local_addr().unwrap().port();
    let sts_app = Router::new().route(
        "/",
        post(|headers: axum::http::HeaderMap, body: String| async move {
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            assert!(
                auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAVENDTEST/"),
                "missing SigV4 header: {auth}"
            );
            assert!(body.contains("Action=AssumeRole"));
            "<AssumeRoleResponse><AssumeRoleResult><Credentials>\
             <AccessKeyId>ASIAVENDED99999</AccessKeyId>\
             <SecretAccessKey>VendedSecretKeyXYZ</SecretAccessKey>\
             <SessionToken>VendedSessionTokABC</SessionToken>\
             <Expiration>2030-01-01T00:00:00Z</Expiration>\
             </Credentials></AssumeRoleResult></AssumeRoleResponse>"
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(sts_listener, sts_app).await;
    });

    let sts_cfg = StsConfig {
        endpoint: format!("http://127.0.0.1:{sts_port}/"),
        region: "us-east-1".into(),
        access_key_id: "AKIAVENDTEST".into(),
        secret_access_key: "secret-key-for-sigv4".into(),
        role_arn: "arn:aws:iam::123456789012:role/rustlakedb-reader".into(),
        duration_seconds: 3600,
    };
    let rest = IcebergRestServer::bind_with_sts("127.0.0.1:0", catalog, Some(sts_cfg))
        .await
        .unwrap();
    let rest_port = rest.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = rest.serve().await;
    });

    let http = reqwest::Client::new();
    let resp: Value = http
        .get(format!(
            "http://127.0.0.1:{rest_port}/v1/namespaces/default/tables/vended_sales"
        ))
        .header("X-Iceberg-Access-Delegation", "vended-credentials")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["config"]["s3.access-key-id"], "ASIAVENDED99999");
    assert_eq!(resp["config"]["s3.secret-access-key"], "VendedSecretKeyXYZ");
    assert_eq!(resp["config"]["s3.session-token"], "VendedSessionTokABC");
    assert_eq!(resp["config"]["vended-by"], "aws-sts-assume-role-sigv4");
    assert_eq!(
        resp["storage-credentials"][0]["config"]["s3.access-key-id"],
        "ASIAVENDED99999"
    );
}
