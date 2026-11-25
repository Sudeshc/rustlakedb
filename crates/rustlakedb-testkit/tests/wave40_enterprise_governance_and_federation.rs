// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::security::{sign_jwt_hs256, SessionContext};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;

fn make_coord(dir: &std::path::Path) -> Arc<Coordinator> {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.join("shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new(
        "w-gov",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    ))
}

#[tokio::test]
async fn wave40_enterprise_rbac_hierarchy_column_grants_tag_abac_and_admin_guard() {
    let dir = tempdir().unwrap();
    let coord = make_coord(dir.path());
    let admin = SessionContext::default();

    coord
        .execute_sql_with_session(
            "CREATE TABLE customers (id BIGINT, region TEXT, email TEXT, ssn TEXT)",
            "main",
            &admin,
        )
        .await
        .unwrap();
    coord
        .execute_sql_with_session(
            "INSERT INTO customers VALUES (1, 'us-east', 'alice@corp.io', '111-11-1111'), (2, 'eu-west', 'bob@corp.io', '222-22-2222')",
            "main",
            &admin,
        )
        .await
        .unwrap();

    // 1. Admin-only security guard: non-admin MUST be denied (42501) on governance DDL!
    let rogue = SessionContext::for_user("rogue_user");
    let err = coord
        .execute_sql_with_session("GRANT ALL ON customers TO rogue_user", "main", &rogue)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("42501"),
        "expected 42501 admin guard denial, got: {err}"
    );

    // 2. Role hierarchy + OIDC group inheritance + Schema-level hierarchical grant!
    coord
        .execute_sql_with_session("CREATE ROLE finance_reader", "main", &admin)
        .await
        .unwrap();
    coord
        .execute_sql_with_session("CREATE ROLE jr_analyst", "main", &admin)
        .await
        .unwrap();
    coord
        .execute_sql_with_session("GRANT SELECT ON SCHEMA public TO finance_reader", "main", &admin)
        .await
        .unwrap();
    coord
        .execute_sql_with_session("GRANT finance_reader TO jr_analyst", "main", &admin)
        .await
        .unwrap();
    coord
        .execute_sql_with_session(
            "CREATE POLICY us_only ON customers FOR finance_reader USING (region = 'us-east')",
            "main",
            &admin,
        )
        .await
        .unwrap();

    // Tag-based ABAC masking: tag table `pii=true` -> ANY table with that tag automatically masks `ssn`!
    coord
        .execute_sql_with_session(
            "ALTER TABLE customers SET TAG ('pii'='true', 'domain'='finance')",
            "main",
            &admin,
        )
        .await
        .unwrap();
    coord
        .execute_sql_with_session(
            "CREATE TAG MASKING POLICY mask_pii_ssn FOR finance_reader ON TAG pii=true MASK ssn AS 'XXX-XX-XXXX'",
            "main",
            &admin,
        )
        .await
        .unwrap();

    // jr_analyst inherits schema grant + RLS + Tag-ABAC masking from parent role `finance_reader`!
    let jr = SessionContext::for_user("jr_analyst");
    let res = coord
        .execute_sql_with_session("SELECT id, region, ssn FROM customers ORDER BY id", "main", &jr)
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1, "inherited RLS must filter to us-east only");
    assert_eq!(
        res.batch.rows[0].get("ssn"),
        Some(&ScalarValue::Utf8("XXX-XX-XXXX".into())),
        "Tag-based ABAC masking must automatically mask ssn on pii=true tables!"
    );

    // OIDC IdP group claim (`groups: ["finance_reader"]`) works identically without explicit local user grant!
    let oidc_user = SessionContext::with_groups("alice@okta.corp", "employee", vec!["finance_reader".into()]);
    let res_oidc = coord
        .execute_sql_with_session("SELECT id, region, ssn FROM customers", "main", &oidc_user)
        .await
        .unwrap();
    assert_eq!(res_oidc.batch.num_rows(), 1);
    assert_eq!(
        res_oidc.batch.rows[0].get("ssn"),
        Some(&ScalarValue::Utf8("XXX-XX-XXXX".into()))
    );

    // 3. Column-level SELECT grant: `col_user` can read `(id, region)` ONLY, denied on `ssn`!
    coord
        .execute_sql_with_session(
            "GRANT SELECT (id, region) ON customers TO col_user",
            "main",
            &admin,
        )
        .await
        .unwrap();
    let col_user = SessionContext::for_user("col_user");
    let ok_col = coord
        .execute_sql_with_session("SELECT id, region FROM customers ORDER BY id", "main", &col_user)
        .await
        .unwrap();
    assert_eq!(ok_col.batch.num_rows(), 2);

    let err_col = coord
        .execute_sql_with_session("SELECT id, ssn FROM customers", "main", &col_user)
        .await
        .unwrap_err();
    assert!(
        err_col.to_string().contains("42501"),
        "unauthorized column SELECT must deny with 42501, got: {err_col}"
    );

    // 4. Verify durable `system.access_history` and `system.audit_log` recorded accesses & denials!
    let acc = coord
        .execute_sql_with_session("SELECT * FROM system.access_history", "main", &admin)
        .await
        .unwrap();
    assert!(
        acc.batch.num_rows() >= 3,
        "system.access_history must record all table/column accesses"
    );
    let aud = coord
        .execute_sql_with_session("SELECT * FROM system.audit_log", "main", &admin)
        .await
        .unwrap();
    assert!(
        aud.batch
            .rows
            .iter()
            .any(|r| r.get("outcome") == Some(&ScalarValue::Utf8("DENIED (42501)".into()))),
        "system.audit_log must capture 42501 denials"
    );
}

#[tokio::test]
async fn wave40_iceberg_rest_rbac_enforcement_and_cross_catalog_federation() {
    let dir_src = tempdir().unwrap();
    let coord_src = make_coord(dir_src.path());
    coord_src.governance.set_jwt_secret(Some("super-secret-gov-key".into()));
    let admin = SessionContext::default();

    coord_src
        .execute_sql_with_session(
            "CREATE TABLE gold_revenue (id BIGINT, region TEXT, amount DOUBLE)",
            "main",
            &admin,
        )
        .await
        .unwrap();
    coord_src
        .execute_sql_with_session(
            "INSERT INTO gold_revenue VALUES (1, 'us-east', 500.0), (2, 'eu-west', 750.0)",
            "main",
            &admin,
        )
        .await
        .unwrap();
    coord_src
        .execute_sql_with_session("GRANT SELECT ON gold_revenue TO external_spark", "main", &admin)
        .await
        .unwrap();

    let server = ControlPlaneServer::bind("127.0.0.1:0", coord_src.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    let http = reqwest::Client::new();

    // 1. Iceberg REST RBAC Enforcement: unauthorized Bearer JWT gets 403 Forbidden!
    let denied_tok = sign_jwt_hs256("super-secret-gov-key", "unauthorized_bi", "unauthorized_bi");
    let denied_resp = http
        .get(format!("{base}/v1/namespaces/default/tables/gold_revenue"))
        .bearer_auth(&denied_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(denied_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // Authorized Bearer JWT gets 200 OK + vended credentials + recorded in access-history!
    let allowed_tok = sign_jwt_hs256("super-secret-gov-key", "spark_svc", "external_spark");
    let ok_resp = http
        .get(format!("{base}/v1/namespaces/default/tables/gold_revenue"))
        .bearer_auth(&allowed_tok)
        .header("X-Iceberg-Access-Delegation", "vended-credentials")
        .send()
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), reqwest::StatusCode::OK);

    // 2. Outbound Multi-Engine Federation Descriptors (Snowflake, Databricks Unity, AWS Glue, BigQuery)!
    let fed_doc: serde_json::Value = http
        .get(format!("{base}/v1/federation/export/gold_revenue"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(fed_doc["federation"]["snowflake"]["catalog_integration_sql"].as_str().unwrap().contains("ICEBERG_REST"));
    assert!(fed_doc["federation"]["databricks_unity_catalog"]["create_foreign_catalog_sql"].as_str().unwrap().contains("FOREIGN CATALOG"));
    assert_eq!(fed_doc["federation"]["aws_glue"]["table_input"]["Parameters"]["table_type"], "ICEBERG");
    assert!(fed_doc["federation"]["google_bigquery"]["create_external_table_sql"].as_str().unwrap().contains("format = 'ICEBERG'"));

    // 3. Inbound Cross-Catalog Federation: second RustLakeDB cluster attaches table from remote Iceberg REST!
    let dir_dst = tempdir().unwrap();
    let coord_dst = make_coord(dir_dst.path());
    coord_dst
        .execute_sql_with_session(
            &format!(
                "CREATE EXTERNAL CATALOG prod_snowflake TYPE SNOWFLAKE URL '{base}' TOKEN '{allowed_tok}'"
            ),
            "main",
            &admin,
        )
        .await
        .unwrap();
    let cats = coord_dst
        .execute_sql_with_session("SHOW EXTERNAL CATALOGS", "main", &admin)
        .await
        .unwrap();
    assert_eq!(cats.batch.num_rows(), 1);
    assert_eq!(
        cats.batch.rows[0].get("catalog_type"),
        Some(&ScalarValue::Utf8("SNOWFLAKE".into()))
    );

    coord_dst
        .execute_sql_with_session(
            "ATTACH EXTERNAL TABLE fed_gold_revenue FROM CATALOG prod_snowflake NAMESPACE 'default' TABLE 'gold_revenue'",
            "main",
            &admin,
        )
        .await
        .unwrap();
    let fed_meta = coord_dst.catalog.get_table("fed_gold_revenue").await.unwrap();
    assert_eq!(fed_meta.schema.len(), 3);
    assert_eq!(
        fed_meta.tags.get("federated_catalog").map(String::as_str),
        Some("prod_snowflake")
    );
    assert_eq!(
        fed_meta.tags.get("federated_type").map(String::as_str),
        Some("SNOWFLAKE")
    );

    // Verify governance access-history HTTP endpoint captured the ICEBERG_REST read!
    let acc_http: serde_json::Value = http
        .get(format!("{base}/v1/governance/access-history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(acc_http["count"].as_u64().unwrap_or(0) >= 1);
}
