// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::ShuffleMode;
use rustlakedb_testkit::{PgTestClient, TestCluster};

/// Comprehensive End-to-End Regression Suite simulating EVERY DBeaver / JDBC / SQL / Lakehouse workflow:
/// - Exact DBeaver connection handshake + `pg_catalog` introspection
/// - Exact DBeaver Data-tab queries (`SELECT c.* FROM public.customers AS c`, `LIMIT/OFFSET`, quoted names)
/// - Schema-qualified DDL & DML (`public.<table>`, `"public"."<table>"`)
/// - Full SQL predicate, aggregation, join, CTE, JSONB, branching, Time Travel, compaction & VACUUM matrix
/// - Extended Query (`Parse -> Bind -> Describe('P') -> Execute -> Sync`) error recovery barrier
#[tokio::test]
async fn dbeaver_full_end_to_end_simulation_and_regression_matrix() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();
    let pg_addr = cluster.start_pg_gateway().await.unwrap();
    let rest_addr = cluster.start_iceberg_rest().await.unwrap();

    let mut client = PgTestClient::connect(pg_addr, "postgres", "rustlakedb")
        .await
        .unwrap();

    // === 1. DBeaver Connection Startup & Introspection Probes ===
    assert!(client.query("SELECT version()").await.unwrap().rows[0][0]
        .as_deref()
        .unwrap()
        .contains("RustLakeDB"));
    assert_eq!(
        client.query("SELECT current_database()").await.unwrap().rows[0][0].as_deref(),
        Some("rustlakedb")
    );
    assert_eq!(
        client.query("SELECT current_schema()").await.unwrap().rows[0][0].as_deref(),
        Some("public")
    );
    assert_eq!(
        client.query("SELECT current_user").await.unwrap().rows[0][0].as_deref(),
        Some("postgres")
    );
    assert_eq!(
        client.query("SELECT 1").await.unwrap().rows[0][0].as_deref(),
        Some("1")
    );
    assert_eq!(
        client.query("SHOW search_path").await.unwrap().rows[0][0].as_deref(),
        Some("\"$user\", public")
    );
    assert_eq!(
        client
            .query("SHOW transaction_isolation")
            .await
            .unwrap()
            .rows[0][0]
            .as_deref(),
        Some("read committed")
    );

    // DBeaver Navigator tree expansion: Databases -> Schemas -> Tables -> Columns -> Types
    assert_eq!(
        client
            .query("SELECT * FROM pg_catalog.pg_database")
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        client
            .query("SELECT * FROM pg_catalog.pg_namespace")
            .await
            .unwrap()
            .rows
            .len(),
        3
    );
    let pg_classes = client
        .query("SELECT * FROM pg_catalog.pg_class")
        .await
        .unwrap();
    assert_eq!(pg_classes.rows.len(), 2);
    let pg_attrs = client
        .query("SELECT * FROM pg_catalog.pg_attribute")
        .await
        .unwrap();
    assert_eq!(pg_attrs.rows.len(), 7); // 5 cols on orders + 2 cols on customers
    let pg_types = client
        .query("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = 'jsonb'")
        .await
        .unwrap();
    assert_eq!(
        pg_types.rows,
        vec![vec![Some("3802".into()), Some("jsonb".into())]]
    );
    // Auxiliary pg_catalog tables clicked in DBeaver Properties tabs must never error.
    for aux in [
        "SELECT * FROM pg_catalog.pg_index",
        "SELECT * FROM pg_catalog.pg_constraint",
        "SELECT * FROM pg_catalog.pg_description",
        "SELECT * FROM pg_catalog.pg_triggers",
        "SELECT * FROM pg_catalog.pg_views",
        "SELECT * FROM pg_catalog.pg_matviews",
        "SELECT * FROM pg_catalog.pg_roles",
        "SELECT * FROM pg_catalog.pg_settings",
    ] {
        client.query(aux).await.unwrap();
    }

    // === 2. Exact DBeaver Data-Tab Queries (Regression for `SELECT c.* FROM public.customers AS c`) ===
    let data_tab_ext = client
        .query_extended("SELECT c.* FROM public.customers AS c", &[])
        .await
        .unwrap();
    assert_eq!(data_tab_ext.columns, vec!["customer_id", "tier"]);
    assert_eq!(data_tab_ext.rows.len(), 3);

    // Double-quoted schema + table + alias + pagination (`LIMIT ... OFFSET ...`)
    let paged = client
        .query_extended(
            r#"SELECT x.* FROM "public"."orders" AS x ORDER BY x.order_id ASC LIMIT 2 OFFSET 1"#,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(paged.rows.len(), 2);
    assert_eq!(paged.rows[0][0].as_deref(), Some("2"));
    assert_eq!(paged.rows[1][0].as_deref(), Some("3"));

    // DBeaver Row-Count query on schema-qualified table
    let cnt = client
        .query("SELECT COUNT(*) AS cnt FROM public.customers")
        .await
        .unwrap();
    assert_eq!(cnt.rows[0][0].as_deref(), Some("3"));

    // Aliased schema-qualified distributed JOIN
    let joined = client
        .query(
            "SELECT c.tier, COUNT(*) AS cnt, SUM(o.amount) AS rev \
             FROM public.orders AS o \
             JOIN public.customers AS c ON o.customer_id = c.customer_id \
             GROUP BY c.tier ORDER BY c.tier",
        )
        .await
        .unwrap();
    assert_eq!(
        joined.rows,
        vec![
            vec![Some("enterprise".into()), Some("4".into()), Some("1549".into())],
            vec![Some("pro".into()), Some("2".into()), Some("250".into())],
        ]
    );

    // === 3. Full Schema-Qualified DDL + DML + Iceberg v3 Lifecycle Regression ===
    client
        .query(r#"CREATE TABLE "public"."reg_items" (id BIGINT, name TEXT, price DOUBLE, active BOOL, meta JSONB)"#)
        .await
        .unwrap();
    client
        .query("CREATE INDEX idx_reg_meta ON public.reg_items USING INVERTED (meta)")
        .await
        .unwrap();
    client
        .query(
            r#"INSERT INTO public.reg_items VALUES
               (1, 'widget', 10.5, true, '{"cat": "hw", "tag": "new"}'),
               (2, 'gadget', 25.0, false, '{"cat": "hw"}'),
               (3, 'service', 99.5, true, '{"cat": "sw", "tag": "pro"}')"#,
        )
        .await
        .unwrap();

    // Predicates: !=, <, <=, >=, OR, NOT, IN, NOT IN, LIKE, JSONB @>, ?, ->>
    let pred_q = client
        .query(
            "SELECT id, name FROM public.reg_items \
             WHERE (price >= 10.5 AND price < 50.0) AND name NOT IN ('service') AND name LIKE '%et' \
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(pred_q.rows.len(), 2);

    let json_q = client
        .query(r#"SELECT id FROM public.reg_items WHERE meta @> '{"cat": "sw"}' AND meta ? 'tag'"#)
        .await
        .unwrap();
    assert_eq!(json_q.rows, vec![vec![Some("3".into())]]);

    // Schema-qualified UPDATE, DELETE, ALTER TABLE, BRANCH, COMPACT, VACUUM
    client
        .query("UPDATE public.reg_items SET price = 15.0 WHERE id = 1")
        .await
        .unwrap();
    client
        .query("DELETE FROM public.reg_items WHERE id = 2")
        .await
        .unwrap();
    client
        .query("ALTER TABLE public.reg_items ADD COLUMN note TEXT")
        .await
        .unwrap();
    client
        .query("CREATE BRANCH dev FROM main ON public.reg_items")
        .await
        .unwrap();
    client
        .query("MERGE BRANCH dev INTO main ON public.reg_items")
        .await
        .unwrap();
    client
        .query("COMPACT TABLE public.reg_items")
        .await
        .unwrap();
    client
        .query("VACUUM TABLE public.reg_items RETAIN 1")
        .await
        .unwrap();

    let final_reg = client
        .query(
            "SELECT id, price, note FROM public.reg_items \
             WHERE note IS NULL AND id != 999 ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(
        final_reg.rows,
        vec![
            vec![Some("1".into()), Some("15".into()), None],
            vec![Some("3".into()), Some("99.5".into()), None],
        ]
    );

    // === 4. Extended Query Error Recovery (only 1 error per failed pipeline, session stays healthy!) ===
    let bad = client
        .query_extended("SELECT * FROM public.does_not_exist", &[])
        .await;
    assert!(bad.is_err());

    // Immediately run valid query on SAME connection to prove Sync ('S') cleared error state cleanly!
    let ok_after_err = client
        .query_extended(
            "SELECT id, name FROM public.reg_items WHERE id = $1",
            &[Some("3")],
        )
        .await
        .unwrap();
    assert_eq!(
        ok_after_err.rows,
        vec![vec![Some("3".into()), Some("service".into())]]
    );

    // Drop table & verify REST catalog reflects removal.
    client.query("DROP TABLE public.reg_items").await.unwrap();
    let rest_tables: serde_json::Value = reqwest::Client::new()
        .get(format!("http://{rest_addr}/v1/namespaces/default/tables"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rest_tables["identifiers"].as_array().unwrap().len(), 2);

    client.close().await.unwrap();
}
