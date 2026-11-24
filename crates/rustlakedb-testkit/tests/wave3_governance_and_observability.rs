// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::{PgTestClient, TestCluster};
use std::io::Cursor;

#[tokio::test]
async fn test_wave3_rbac_rls_and_dynamic_json_masking() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE customers_sec (id BIGINT, region TEXT, email TEXT, payload JSONB)")
        .await
        .unwrap();
    cluster
        .sql(
            r#"INSERT INTO customers_sec VALUES
               (1, 'us', 'alice@acme.com', '{"ssn": "111-22-3333", "tier": "vip"}'),
               (2, 'eu', 'bob@acme.eu', '{"ssn": "444-55-6666", "tier": "pro"}')"#,
        )
        .await
        .unwrap();

    // 1. Role `analyst` initially DENIED (42501)
    cluster.sql("CREATE ROLE analyst").await.unwrap();
    let denied = cluster
        .sql_as("analyst", "SELECT * FROM customers_sec")
        .await
        .unwrap_err();
    assert!(
        denied.to_string().contains("42501"),
        "expected permission denied 42501, got: {denied}"
    );

    // 2. GRANT SELECT + Row-Level Security (EU only) + Dynamic Column & JSON-Path Masking!
    cluster
        .sql("GRANT SELECT ON customers_sec TO analyst")
        .await
        .unwrap();
    cluster
        .sql("CREATE POLICY eu_only ON customers_sec FOR analyst USING (region = 'eu')")
        .await
        .unwrap();
    cluster
        .sql("CREATE MASKING POLICY mask_email ON customers_sec.email FOR analyst AS '***@redacted'")
        .await
        .unwrap();
    cluster
        .sql("CREATE MASKING POLICY mask_ssn ON customers_sec.payload.ssn FOR analyst AS '***-**-****'")
        .await
        .unwrap();

    // Verify via real PostgreSQL v3 Wire Protocol connection authenticated as `analyst`!
    let pg_addr = cluster.start_pg_gateway().await.unwrap();
    let mut pg_analyst = PgTestClient::connect(pg_addr, "analyst", "rustlakedb")
        .await
        .unwrap();
    let out = pg_analyst
        .query("SELECT id, region, email, payload FROM customers_sec")
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 1, "RLS policy must restrict analyst to EU rows only");
    assert_eq!(out.rows[0][0].as_deref(), Some("2"));
    assert_eq!(out.rows[0][1].as_deref(), Some("eu"));
    assert_eq!(out.rows[0][2].as_deref(), Some("***@redacted"));
    let json_txt = out.rows[0][3].as_deref().unwrap();
    assert!(
        json_txt.contains("***-**-****") && !json_txt.contains("444-55-6666"),
        "expected masked JSON ssn, got: {json_txt}"
    );
    pg_analyst.close().await.unwrap();

    // Superuser `postgres` still sees all rows unmasked!
    let admin_res = cluster
        .sql("SELECT id, email, payload->>'ssn' AS ssn FROM customers_sec ORDER BY id")
        .await
        .unwrap();
    assert_eq!(admin_res.batch.num_rows(), 2);
    assert_eq!(admin_res.batch.rows[1].get("ssn"), Some(&ScalarValue::Utf8("444-55-6666".into())));

    // REVOKE blocks analyst again
    cluster
        .sql("REVOKE SELECT ON customers_sec FROM analyst")
        .await
        .unwrap();
    assert!(cluster.sql_as("analyst", "SELECT * FROM customers_sec").await.is_err());

    // Audit log recorded GRANTED DDL & DENIED attempts!
    let audit = cluster.sql("SELECT * FROM system.audit_log").await.unwrap();
    assert!(audit.batch.num_rows() >= 4);
}

#[tokio::test]
async fn test_wave3_views_materialized_views_explain_analyze_and_control_plane() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // 1. Virtual VIEW
    cluster
        .sql("CREATE VIEW us_orders AS SELECT order_id, amount FROM orders WHERE region = 'us'")
        .await
        .unwrap();
    let vres = cluster
        .sql("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM us_orders")
        .await
        .unwrap();
    assert_eq!(vres.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(3)));

    // 2. MATERIALIZED VIEW + REFRESH MATERIALIZED VIEW
    cluster
        .sql("CREATE MATERIALIZED VIEW regional_summary AS SELECT region, COUNT(*) AS cnt, SUM(amount) AS rev FROM orders GROUP BY region")
        .await
        .unwrap();
    let mv1 = cluster
        .sql("SELECT region, cnt FROM regional_summary ORDER BY region")
        .await
        .unwrap();
    assert_eq!(mv1.batch.num_rows(), 3);

    // Insert new order and refresh materialized view
    cluster
        .sql(r#"INSERT INTO orders VALUES (7, 10, 'us', 500.0, '{"event_type": "purchase"}')"#)
        .await
        .unwrap();
    let ref_res = cluster
        .sql("REFRESH MATERIALIZED VIEW regional_summary")
        .await
        .unwrap();
    assert!(ref_res.command_tag.starts_with("REFRESH MATERIALIZED VIEW"));
    let mv2 = cluster
        .sql("SELECT cnt FROM regional_summary WHERE region = 'us'")
        .await
        .unwrap();
    assert_eq!(mv2.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(4)));

    // 3. EXPLAIN ANALYZE + system.query_history
    let ea = cluster
        .sql("EXPLAIN ANALYZE SELECT region, COUNT(*) AS cnt FROM orders GROUP BY region")
        .await
        .unwrap();
    let last_line = ea
        .batch
        .rows
        .last()
        .and_then(|r| r.get("QUERY PLAN"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        last_line.contains("Execution Summary:") && last_line.contains("elapsed="),
        "expected rich EXPLAIN ANALYZE summary, got: {last_line}"
    );
    let hist = cluster.sql("SELECT * FROM system.query_history").await.unwrap();
    assert!(hist.batch.num_rows() >= 5);

    // 4. Control Plane HTTP Server: Web UI (/), Prometheus (/metrics), JSON SQL, and Zero-Copy Arrow IPC Stream!
    let cp_addr = cluster.start_control_plane().await.unwrap();
    let http = reqwest::Client::new();

    let ui_html = http
        .get(format!("http://{cp_addr}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(ui_html.contains("RustLakeDB Lakehouse Console"));

    let prom = http
        .get(format!("http://{cp_addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(prom.contains("rustlakedb_queries_total"));
    assert!(prom.contains("rustlakedb_workers_healthy 2"));

    // Zero-Copy Arrow IPC Stream endpoint (`POST /v1/sql/arrow`)
    let arrow_resp = http
        .post(format!("http://{cp_addr}/v1/sql/arrow"))
        .json(&serde_json::json!({
            "sql": "SELECT region, COUNT(*) AS cnt FROM orders GROUP BY region ORDER BY region"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(arrow_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        arrow_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/vnd.apache.arrow.stream")
    );
    let ipc_bytes = arrow_resp.bytes().await.unwrap();
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(ipc_bytes), None).unwrap();
    let rb = reader.next().unwrap().unwrap();
    assert_eq!(rb.num_rows(), 3);
    assert_eq!(rb.num_columns(), 2);
}
