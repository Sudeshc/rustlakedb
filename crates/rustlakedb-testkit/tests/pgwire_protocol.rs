// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::ShuffleMode;
use rustlakedb_testkit::{PgTestClient, TestCluster};

#[tokio::test]
async fn pgwire_v3_tcp_simple_and_extended_query_end_to_end() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    let addr = cluster.start_pg_gateway().await.unwrap();

    let mut client = PgTestClient::connect(addr, "postgres", "rustlakedb")
        .await
        .unwrap();

    // 1. Standard psql metadata probe.
    let ver = client.query("SELECT version()").await.unwrap();
    assert_eq!(ver.columns, vec!["version"]);
    assert!(ver.rows[0][0].as_deref().unwrap().contains("RustLakeDB"));

    // 2. DDL + JSONB Inserts over Simple Query protocol.
    let ct = client
        .query("CREATE TABLE telemetry (region TEXT, ms DOUBLE, payload JSONB)")
        .await
        .unwrap();
    assert_eq!(ct.command_tag, "CREATE TABLE");

    let ins1 = client
        .query(r#"INSERT INTO telemetry VALUES ('us', 10.0, '{"ok": true}'), ('us', 20.0, '{"ok": true}')"#)
        .await
        .unwrap();
    assert_eq!(ins1.command_tag, "INSERT 0 2");

    let ins2 = client
        .query(r#"INSERT INTO telemetry VALUES ('eu', 50.0, '{"ok": false}')"#)
        .await
        .unwrap();
    assert_eq!(ins2.command_tag, "INSERT 0 1");

    // 3. Distributed GROUP BY + JSON filter + ORDER BY over Simple Query.
    let q = client
        .query(
            "SELECT region, COUNT(*) AS cnt, AVG(ms) AS avg_ms \
             FROM telemetry WHERE payload->>'ok' = 'true' \
             GROUP BY region ORDER BY region",
        )
        .await
        .unwrap();
    assert_eq!(q.columns, vec!["region", "cnt", "avg_ms"]);
    assert_eq!(
        q.rows,
        vec![vec![
            Some("us".into()),
            Some("2".into()),
            Some("15".into())
        ]]
    );

    // 4. PostgreSQL Extended Query Protocol (`Parse -> Bind -> Describe -> Execute -> Sync`)
    //    with `$1`, `$2` bound parameters (used by JDBC, SQLx, psycopg, asyncpg)!
    let ext = client
        .query_extended(
            "SELECT region, ms FROM telemetry WHERE region = $1 AND ms > $2 ORDER BY ms",
            &[Some("us"), Some("15.0")],
        )
        .await
        .unwrap();
    assert_eq!(ext.columns, vec!["region", "ms"]);
    assert_eq!(
        ext.rows,
        vec![vec![Some("us".into()), Some("20".into())]]
    );

    // 5. Error handling keeps the TCP session healthy for subsequent queries!
    let err = client.query("SELECT * FROM nonexistent_table").await;
    assert!(err.is_err());

    let tables = client.query("SHOW TABLES").await.unwrap();
    assert_eq!(tables.rows, vec![vec![Some("telemetry".into())]]);

    // 6. DBeaver / pgjdbc catalog introspection queries (`pg_type`, `pg_database`, `pg_namespace`, `pg_class`, `pg_attribute`, `SHOW`).
    let pg_types = client
        .query("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = 'jsonb'")
        .await
        .unwrap();
    assert_eq!(
        pg_types.rows,
        vec![vec![Some("3802".into()), Some("jsonb".into())]]
    );

    let dbs = client.query("SELECT * FROM pg_catalog.pg_database").await.unwrap();
    assert_eq!(dbs.rows[0][1].as_deref(), Some("rustlakedb"));

    let classes = client.query("SELECT * FROM pg_catalog.pg_class").await.unwrap();
    assert_eq!(classes.rows[0][1].as_deref(), Some("telemetry"));

    let attrs = client.query("SELECT * FROM pg_catalog.pg_attribute").await.unwrap();
    assert_eq!(attrs.rows.len(), 3);

    let iso = client.query("SHOW transaction_isolation").await.unwrap();
    assert_eq!(iso.rows[0][0].as_deref(), Some("read committed"));

    let ping = client.query("SELECT 1").await.unwrap();
    assert_eq!(ping.rows[0][0].as_deref(), Some("1"));

    client.close().await.unwrap();
}
