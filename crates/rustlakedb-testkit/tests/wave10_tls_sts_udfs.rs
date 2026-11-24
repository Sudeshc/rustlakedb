// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use bytes::{BufMut, BytesMut};
use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_gateway::{build_tls_acceptor_from_pem, PgAuthConfig, PgGateway};
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use rustls::RootCertStore;
use rustls_pki_types::ServerName;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const TEST_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBNTCB3aADAgECAgkA4a1TQGz7TcgwCgYIKoZIzj0EAwIwFDESMBAGA1UEAxMJ
bG9jYWxob3N0MB4XDTI2MTEyMzIwMzMzMloXDTQ2MTEyNDIwMzMzMlowFDESMBAG
A1UEAxMJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEmQnE981f
WATafL2SQyL7xuttrBORn+IWQZcSM4nGarw5q1FJhCfsV2qFoUYBrD7NFnpGT2Vo
I9BLaYUF8sobD6MYMBYwFAYDVR0RBA0wC4IJbG9jYWxob3N0MAoGCCqGSM49BAMC
A0cAMEQCIEpAvt92rDjMt/5XAeEMBNPbO7gTEW3ESYyzUhOeWci3AiBZuqVE7d5K
CUnwFaAfmrq2m4cGNcrvR80XaP3/96DLfA==
-----END CERTIFICATE-----
";

const TEST_KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgF1HAFSpXuYA3YB+Y
87yfhSQdLwz7T1XB2+DOllYrIDWhRANCAASZCcT3zV9YBNp8vZJDIvvG622sE5Gf
4hZBlxIzicZqvDmrUUmEJ+xXaoWhRgGsPs0WekZPZWgj0EtphQXyyhsP
-----END PRIVATE KEY-----
";

fn build_pg_startup(user: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(196_608); // PG v3.0
    body.put_slice(b"user\0");
    body.put_slice(user.as_bytes());
    body.put_u8(0);
    body.put_slice(b"database\0rustlakedb\0\0");
    let mut out = BytesMut::new();
    out.put_i32((body.len() + 4) as i32);
    out.put_slice(&body);
    out.to_vec()
}

fn build_pg_query(sql: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.put_u8(b'Q');
    out.put_i32((4 + sql.len() + 1) as i32);
    out.put_slice(sql.as_bytes());
    out.put_u8(0);
    out.to_vec()
}

async fn read_until_ready<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> Vec<(u8, Vec<u8>)> {
    let mut msgs = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        stream.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut payload = vec![0u8; len.saturating_sub(4)];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await.unwrap();
        }
        msgs.push((tag, payload));
        if tag == b'Z' {
            break;
        }
    }
    msgs
}

#[tokio::test]
async fn wave10_real_tls13_pgwire_ssl_request_and_sql_query() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));
    coord
        .execute_sql("CREATE TABLE tls_orders (id BIGINT, amount DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO tls_orders VALUES (1, 100.0), (2, 250.0)", "main")
        .await
        .unwrap();

    let acceptor = build_tls_acceptor_from_pem(TEST_CERT_PEM.as_bytes(), TEST_KEY_PEM.as_bytes())
        .expect("valid PEM cert+key");
    let gw = PgGateway::bind_full("127.0.0.1:0", coord, PgAuthConfig::Trust, Some(acceptor))
        .await
        .unwrap();
    let addr = gw.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        let _ = gw.serve().await;
    });

    // 1. Connect TCP and negotiate PG v3 SSLRequest -> 'S'
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    let mut ssl_req = BytesMut::new();
    ssl_req.put_i32(8);
    ssl_req.put_i32(80_877_103); // SSLRequest code
    tcp.write_all(&ssl_req).await.unwrap();

    let mut reply = [0u8; 1];
    tcp.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[0], b'S', "expected TLS accept 'S' from PgGateway");

    // 2. Upgrade live socket with genuine TLS 1.3 X.509 certificate verification!
    let mut root_store = RootCertStore::empty();
    let mut cert_reader = std::io::Cursor::new(TEST_CERT_PEM.as_bytes());
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let (added, _) = root_store.add_parsable_certificates(certs);
    assert_eq!(added, 1);

    let client_cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(client_cfg));
    let mut tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .expect("TLS 1.3 handshake must succeed");
    assert_eq!(
        tls.get_ref().1.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3),
        "expected negotiated TLS 1.3"
    );

    // 3. Run PostgreSQL v3 Startup + Simple Query over encrypted TLS 1.3 stream
    tls.write_all(&build_pg_startup("postgres")).await.unwrap();
    let startup_msgs = read_until_ready(&mut tls).await;
    assert!(startup_msgs.iter().any(|(t, _)| *t == b'R'));

    tls.write_all(&build_pg_query("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM tls_orders"))
        .await
        .unwrap();
    let q_msgs = read_until_ready(&mut tls).await;
    let data_rows: Vec<&Vec<u8>> = q_msgs
        .iter()
        .filter(|(t, _)| *t == b'D')
        .map(|(_, p)| p)
        .collect();
    assert_eq!(data_rows.len(), 1);
    let row_text = String::from_utf8_lossy(data_rows[0]);
    assert!(row_text.contains('2') && row_text.contains("350"), "unexpected TLS row: {row_text}");

    server_handle.abort();
}

#[tokio::test]
async fn wave10_iceberg_rest_sts_credential_vending_and_sql_udfs() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE prod_orders (id BIGINT, tag VARCHAR, amount DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO prod_orders VALUES (1, '  vip ', 100.0), (2, ' std ', 200.0)",
            "main",
        )
        .await
        .unwrap();

    // 1. SQL Scalar UDFs (`CREATE FUNCTION` & `CREATE OR REPLACE FUNCTION`)
    coord
        .execute_sql(
            "CREATE FUNCTION net_price(x DOUBLE) RETURNS DOUBLE AS 'ROUND(x * 0.9, 2)'",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "CREATE OR REPLACE FUNCTION clean_tag(s VARCHAR) RETURNS VARCHAR AS 'UPPER(TRIM(s))'",
            "main",
        )
        .await
        .unwrap();

    let udf_res = coord
        .execute_sql(
            "SELECT id, clean_tag(tag) AS ct, net_price(amount) AS np FROM prod_orders ORDER BY id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(udf_res.batch.num_rows(), 2);
    assert_eq!(udf_res.batch.rows[0].get("ct"), Some(&ScalarValue::Utf8("VIP".into())));
    assert_eq!(udf_res.batch.rows[0].get("np"), Some(&ScalarValue::Float64(90.0)));
    assert_eq!(udf_res.batch.rows[1].get("ct"), Some(&ScalarValue::Utf8("STD".into())));
    assert_eq!(udf_res.batch.rows[1].get("np"), Some(&ScalarValue::Float64(180.0)));

    // 2. Iceberg v3 REST Catalog Temporary STS Credential Vending
    let cp = ControlPlaneServer::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let cp_addr = cp.local_addr().unwrap();
    let cp_handle = tokio::spawn(async move {
        let _ = cp.serve().await;
    });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{cp_addr}/v1/namespaces/default/tables/prod_orders/credentials"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let creds = &body["storage-credentials"][0]["config"];
    assert!(
        creds["s3.access-key-id"]
            .as_str()
            .unwrap_or("")
            .starts_with("ASIA"),
        "expected temporary STS access key starting with ASIA: {body}"
    );
    assert!(creds["s3.session-token"].as_str().unwrap_or("").len() > 16);
    assert!(creds["expiration-time"].as_str().is_some());

    cp_handle.abort();
}
