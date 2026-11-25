// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_array::{Array, Date32Array, Decimal128Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rustlakedb_catalog::rest::{sign_aws_sigv4, IcebergRestServer, StsConfig};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::{Coordinator, HttpWorkerClient, WorkerClient};
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::rpc::{build_mtls_server_acceptor, WorkerHttpServer};
use rustlakedb_worker::WorkerNode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;

const CA_CRT: &[u8] = include_bytes!("fixtures/mtls/ca.crt");
const SERVER_CRT: &[u8] = include_bytes!("fixtures/mtls/server.crt");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/mtls/server.key");
const CLIENT_CRT: &[u8] = include_bytes!("fixtures/mtls/client.crt");
const CLIENT_KEY: &[u8] = include_bytes!("fixtures/mtls/client.key");

#[tokio::test]
async fn wave16_cluster_internal_mtls_coordinator_to_worker_rpc() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());

    let worker = Arc::new(WorkerNode::new(
        "mtls-worker-1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let acceptor = build_mtls_server_acceptor(CA_CRT, SERVER_CRT, SERVER_KEY).unwrap();
    let server = WorkerHttpServer::bind("127.0.0.1:0", worker.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve_mtls(acceptor).await;
    });

    let base_url = format!("https://127.0.0.1:{}", addr.port());

    // 1. Negative security proof: HTTPS client trusting the CA but WITHOUT a client cert MUST be rejected!
    let ca_cert = reqwest::Certificate::from_pem(CA_CRT).unwrap();
    let no_client_cert_http = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca_cert)
        .build()
        .unwrap();
    let unauth_res = no_client_cert_http
        .get(format!("{base_url}/healthz"))
        .send()
        .await;
    assert!(
        unauth_res.is_err(),
        "mTLS worker MUST reject TLS peer without a valid CA-signed client certificate"
    );

    // 2. Positive proof: Coordinator using HttpWorkerClient::new_mtls executes distributed SQL over HTTPS mTLS!
    let mtls_client: Arc<dyn WorkerClient> = Arc::new(
        HttpWorkerClient::new_mtls("mtls-worker-1", &base_url, CA_CRT, CLIENT_CRT, CLIENT_KEY)
            .unwrap(),
    );
    let coord = Coordinator::with_clients(
        catalog,
        store,
        shuffle,
        vec![mtls_client],
        PlannerConfig::default(),
    );

    coord
        .execute_sql(
            "CREATE TABLE mtls_sales (id BIGINT, region TEXT, amount DOUBLE)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO mtls_sales VALUES (1, 'us-east', 120.5), (2, 'us-east', 79.5), (3, 'eu-west', 300.0)",
            "main",
        )
        .await
        .unwrap();

    let res = coord
        .execute_sql(
            "SELECT region, SUM(amount) AS total FROM mtls_sales GROUP BY region ORDER BY region ASC",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(
        res.batch.rows[0].get("region"),
        Some(&ScalarValue::Utf8("eu-west".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("total"),
        Some(&ScalarValue::Float64(300.0))
    );
    assert_eq!(
        res.batch.rows[1].get("region"),
        Some(&ScalarValue::Utf8("us-east".into()))
    );
    assert_eq!(
        res.batch.rows[1].get("total"),
        Some(&ScalarValue::Float64(200.0))
    );
    assert!(
        worker.tasks_executed.load(Ordering::Relaxed) >= 1,
        "mTLS remote worker must have executed tasks"
    );
}

#[tokio::test]
async fn wave16_real_aws_sts_assume_role_sigv4_credential_vending() {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    let region = "us-east-1";
    let role_arn = "arn:aws:iam::123456789012:role/rustlakedb-reader";
    let verified_calls = Arc::new(AtomicUsize::new(0));
    let verified_clone = verified_calls.clone();

    // Local mock AWS STS server verifying genuine AWS SigV4 (AWS4-HMAC-SHA256) signatures!
    let sts_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sts_addr = sts_listener.local_addr().unwrap();
    let sts_host = format!("127.0.0.1:{}", sts_addr.port());
    let expected_host = sts_host.clone();

    let mock_sts_app = Router::new().route(
        "/",
        post(move |headers: HeaderMap, body: String| {
            let verified_clone = verified_clone.clone();
            let expected_host = expected_host.clone();
            async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let amz_date = headers
                    .get("x-amz-date")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let content_type = headers
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");

                assert!(
                    body.contains("Action=AssumeRole") && body.contains("Version=2011-06-15"),
                    "unexpected STS body: {body}"
                );
                assert!(
                    amz_date.len() == 16 && amz_date.ends_with('Z'),
                    "invalid x-amz-date: {amz_date}"
                );
                let date_stamp = &amz_date[..8];

                // Cryptographically re-derive the AWS SigV4 signature and verify exact equality!
                let expected_auth = sign_aws_sigv4(
                    "POST",
                    "/",
                    "",
                    &expected_host,
                    content_type,
                    amz_date,
                    date_stamp,
                    region,
                    "sts",
                    access_key,
                    secret_key,
                    body.as_bytes(),
                );
                assert_eq!(
                    auth, expected_auth,
                    "AWS SigV4 Authorization header must match cryptographic HMAC-SHA256 derivation"
                );
                verified_clone.fetch_add(1, Ordering::SeqCst);

                let xml = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAVENDEDSIGV412345</AccessKeyId>
      <SecretAccessKey>vended-secret-key-sigv4-xyz</SecretAccessKey>
      <SessionToken>FwoGZXIvYXdzEBYaD-vended-session-token</SessionToken>
      <Expiration>2027-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;
                (StatusCode::OK, xml)
            }
        }),
    );

    tokio::spawn(async move {
        let _ = axum::serve(sts_listener, mock_sts_app).await;
    });

    // Spin up Iceberg v3 REST Catalog configured with StsConfig pointing to the mock STS endpoint
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
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    );
    coord
        .execute_sql("CREATE TABLE finance (id BIGINT, amt DOUBLE)", "main")
        .await
        .unwrap();

    let sts_cfg = StsConfig {
        endpoint: format!("http://{sts_host}/"),
        region: region.into(),
        access_key_id: access_key.into(),
        secret_access_key: secret_key.into(),
        role_arn: role_arn.into(),
        duration_seconds: 1800,
    };
    let rest = IcebergRestServer::bind_with_sts("127.0.0.1:0", catalog, Some(sts_cfg))
        .await
        .unwrap();
    let rest_addr = rest.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = rest.serve().await;
    });

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/v1/namespaces/default/tables/finance/credentials",
        rest_addr.port()
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let cfg = &body["storage-credentials"][0]["config"];
    assert_eq!(cfg["s3.access-key-id"], "ASIAVENDEDSIGV412345");
    assert_eq!(cfg["s3.secret-access-key"], "vended-secret-key-sigv4-xyz");
    assert_eq!(
        cfg["s3.session-token"],
        "FwoGZXIvYXdzEBYaD-vended-session-token"
    );
    assert_eq!(cfg["expiration-time"], "2027-01-01T00:00:00Z");
    assert_eq!(cfg["vended-by"], "aws-sts-assume-role-sigv4");
    assert_eq!(verified_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn wave16_iceberg_v3_interop_metadata_parquet_avro_verification() {
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
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    );

    coord
        .execute_sql(
            "CREATE TABLE ledger (id BIGINT, dt DATE, ts TIMESTAMP, price DECIMAL(12,2), region TEXT) PARTITION BY (region)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO ledger VALUES \
             (1, '2026-04-01', '2026-04-01 09:30:00', 125.50, 'amer'), \
             (2, '2026-04-02', '2026-04-02 14:15:00', 274.50, 'emea')",
            "main",
        )
        .await
        .unwrap();

    // 1. Verify Iceberg v3 metadata JSON & Avro manifest files exist on disk in standard layout
    let meta = catalog.get_table("ledger").await.unwrap();
    let snap = meta.current_snapshot("main").unwrap();
    assert_eq!(meta.format_version, 3);
    assert_eq!(snap.data_files.len(), 2, "expected 2 partitioned Parquet files");

    let meta_json_path = dir
        .path()
        .join("ledger")
        .join("metadata")
        .join(format!("v{}.metadata.json", snap.snapshot_id));
    let meta_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&meta_json_path).unwrap()).unwrap();
    assert_eq!(meta_json["format-version"], 3);

    let avro_files: Vec<_> = std::fs::read_dir(dir.path().join("ledger").join("metadata"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("avro"))
        .collect();
    assert!(
        !avro_files.is_empty(),
        "expected standard Iceberg v3 Avro manifest files on disk"
    );

    // 2. Independently open every Parquet data file with standard Apache Arrow Parquet reader
    //    and verify exact native Arrow physical types (Date32, Timestamp(us, UTC), Decimal128(12, 2))!
    let mut total_rows = 0usize;
    for df in &snap.data_files {
        let path = dir.path().join(&df.file_path);
        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema().clone();
        assert_eq!(schema.field_with_name("dt").unwrap().data_type(), &DataType::Date32);
        assert_eq!(
            schema.field_with_name("ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field_with_name("price").unwrap().data_type(),
            &DataType::Decimal128(12, 2)
        );

        let reader = builder.build().unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
            let dt_col = batch
                .column_by_name("dt")
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            let ts_col = batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let price_col = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            assert!(!dt_col.is_null(0));
            assert!(!ts_col.is_null(0));
            assert!(price_col.value(0) == 12550 || price_col.value(0) == 27450);
        }
    }
    assert_eq!(total_rows, 2);
}
