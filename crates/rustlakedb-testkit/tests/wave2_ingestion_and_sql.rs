// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::Catalog;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;

#[tokio::test]
async fn test_wave2_copy_from_csv_ndjson_dlq_and_parquet() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    let dir = tempfile::tempdir().unwrap();

    cluster
        .sql("CREATE TABLE events (id BIGINT, region TEXT, amount DOUBLE, payload JSONB)")
        .await
        .unwrap();

    // 1. COPY FROM CSV (standard RFC-4180 quoted JSON field with doubled quotes!)
    let csv_path = dir.path().join("events.csv");
    std::fs::write(
        &csv_path,
        "id,region,amount,payload\n\
         1,us-east,120.5,\"{\"\"src\"\":\"\"csv\"\"}\"\n\
         2,eu-west,250.0,\"{\"\"src\"\":\"\"csv\"\"}\"\n",
    )
    .unwrap();
    let csv_sql = format!(
        "COPY events FROM '{}' (FORMAT CSV)",
        csv_path.display().to_string().replace('\\', "/")
    );
    let r_csv = cluster.sql(&csv_sql).await.unwrap();
    assert_eq!(r_csv.command_tag, "COPY 2");

    // 2. COPY FROM NDJSON with ON_ERROR CONTINUE (Dead-Letter Queue routing!)
    let ndjson_path = dir.path().join("events.ndjson");
    std::fs::write(
        &ndjson_path,
        r#"{"id": 3, "region": "ap-south", "amount": 500.0, "payload": {"src": "ndjson"}}
THIS IS CORRUPT JSON LINE
{"id": "not_an_int", "region": "us-west", "amount": 10.0, "payload": {}}
{"id": 4, "region": "us-west", "amount": 75.0, "payload": {"src": "ndjson"}}
"#,
    )
    .unwrap();
    let json_sql = format!(
        "COPY events FROM '{}' (FORMAT NDJSON, ON_ERROR CONTINUE)",
        ndjson_path.display().to_string().replace('\\', "/")
    );
    let r_json = cluster.sql(&json_sql).await.unwrap();
    assert_eq!(r_json.command_tag, "COPY 2 (dlq=2)");

    // Verify DLQ table captured the 2 malformed lines!
    let dlq = cluster.sql("SELECT COUNT(*) AS bad FROM events_dlq").await.unwrap();
    assert_eq!(dlq.batch.rows[0].get("bad"), Some(&ScalarValue::Int64(2)));

    // 3. COPY FROM external Parquet file (copy an existing Parquet split into a new table!)
    cluster
        .sql("CREATE TABLE events_copy (id BIGINT, region TEXT, amount DOUBLE, payload JSONB)")
        .await
        .unwrap();
    let meta = cluster.catalog.get_table("events").await.unwrap();
    let first_pq = &meta.current_snapshot("main").unwrap().data_files[0].file_path;
    let abs_pq = cluster.data_dir_path().join(first_pq);
    let pq_sql = format!(
        "COPY events_copy FROM '{}' (FORMAT PARQUET)",
        abs_pq.display().to_string().replace('\\', "/")
    );
    let r_pq = cluster.sql(&pq_sql).await.unwrap();
    assert_eq!(r_pq.command_tag, "COPY 2");

    // 4. Streaming HTTP Micro-Batch NDJSON Ingest (`POST /v1/ingest/events`)
    let cp_addr = cluster.start_control_plane().await.unwrap();
    let http = reqwest::Client::new();
    let ingest_resp = http
        .post(format!("http://{cp_addr}/v1/ingest/events?on_error=continue"))
        .body("{\"id\": 5, \"region\": \"sa-east\", \"amount\": 310.0, \"payload\": {\"src\": \"stream\"}}\nbad_line\n")
        .send()
        .await
        .unwrap();
    assert_eq!(ingest_resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = ingest_resp.json().await.unwrap();
    assert_eq!(body["rows_loaded"], 1);
    assert_eq!(body["dlq_rows"], 1);

    let total = cluster.sql("SELECT COUNT(*) AS cnt FROM events").await.unwrap();
    assert_eq!(total.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(5)));
}

#[tokio::test]
async fn test_wave2_merge_into_acid_upsert_and_delete() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE accounts (id BIGINT, name TEXT, balance DOUBLE)")
        .await
        .unwrap();
    cluster
        .sql("CREATE TABLE cdc_batch (id BIGINT, name TEXT, balance DOUBLE)")
        .await
        .unwrap();

    cluster
        .sql("INSERT INTO accounts VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Carol', 300.0)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO cdc_batch VALUES (2, 'Bob Updated', 275.0), (4, 'Dave New', 400.0)")
        .await
        .unwrap();

    // Upsert: update id=2, insert id=4, keep id=1 & id=3!
    let m = cluster
        .sql("MERGE INTO accounts AS t USING cdc_batch AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name, balance = s.balance WHEN NOT MATCHED THEN INSERT (id, name, balance) VALUES (s.id, s.name, s.balance)")
        .await
        .unwrap();
    assert_eq!(m.command_tag, "MERGE 2");

    let res = cluster
        .sql("SELECT id, name, balance FROM accounts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 4);
    assert_eq!(res.batch.rows[1].get("name"), Some(&ScalarValue::Utf8("Bob Updated".into())));
    assert_eq!(res.batch.rows[1].get("balance"), Some(&ScalarValue::Float64(275.0)));
    assert_eq!(res.batch.rows[3].get("name"), Some(&ScalarValue::Utf8("Dave New".into())));

    // MERGE WHEN MATCHED THEN DELETE
    cluster.sql("CREATE TABLE deletes (id BIGINT)").await.unwrap();
    cluster.sql("INSERT INTO deletes VALUES (1), (3)").await.unwrap();
    let md = cluster
        .sql("MERGE INTO accounts AS t USING deletes AS d ON t.id = d.id WHEN MATCHED THEN DELETE")
        .await
        .unwrap();
    assert_eq!(md.command_tag, "MERGE 2");

    let after = cluster.sql("SELECT id FROM accounts ORDER BY id").await.unwrap();
    assert_eq!(after.batch.num_rows(), 2);
    assert_eq!(after.batch.rows[0].get("id"), Some(&ScalarValue::Int64(2)));
    assert_eq!(after.batch.rows[1].get("id"), Some(&ScalarValue::Int64(4)));
}

#[tokio::test]
async fn test_wave2_case_coalesce_date_trunc_union_and_windows() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE sales (id BIGINT, dept TEXT, nick TEXT, amount DOUBLE, ts TEXT)")
        .await
        .unwrap();
    cluster
        .sql(
            "INSERT INTO sales VALUES \
             (1, 'eng', NULL, 50.0, '2026-03-14T10:25:00Z'), \
             (2, 'eng', 'alice', 250.0, '2026-03-14T15:40:00Z'), \
             (3, 'eng', NULL, 600.0, '2026-03-15T08:05:00Z'), \
             (4, 'ops', 'bob', 150.0, '2026-03-15T22:10:00Z')",
        )
        .await
        .unwrap();

    // 1. CASE WHEN + COALESCE + DATE_TRUNC
    let scalar_res = cluster
        .sql(
            "SELECT id, \
             CASE WHEN amount >= 500 THEN 'whale' WHEN amount >= 100 THEN 'mid' ELSE 'small' END AS tier, \
             COALESCE(nick, dept, 'none') AS display_name, \
             DATE_TRUNC('day', ts) AS day_bucket \
             FROM sales ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(scalar_res.batch.num_rows(), 4);
    assert_eq!(scalar_res.batch.rows[0].get("tier"), Some(&ScalarValue::Utf8("small".into())));
    assert_eq!(scalar_res.batch.rows[0].get("display_name"), Some(&ScalarValue::Utf8("eng".into())));
    assert_eq!(scalar_res.batch.rows[0].get("day_bucket"), Some(&ScalarValue::Utf8("2026-03-14T00:00:00Z".into())));
    assert_eq!(scalar_res.batch.rows[1].get("display_name"), Some(&ScalarValue::Utf8("alice".into())));
    assert_eq!(scalar_res.batch.rows[2].get("tier"), Some(&ScalarValue::Utf8("whale".into())));

    // 2. Window Functions: ROW_NUMBER, RANK, SUM OVER (running total), LAG
    let win_res = cluster
        .sql(
            "SELECT id, dept, amount, \
             ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount ASC) AS rn, \
             SUM(amount) OVER (PARTITION BY dept ORDER BY amount ASC) AS running_sum, \
             LAG(amount, 1) OVER (PARTITION BY dept ORDER BY amount ASC) AS prev_amt \
             FROM sales ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(win_res.batch.num_rows(), 4);
    // id=1 (eng, 50) -> rn=1, running_sum=50, prev=NULL
    assert_eq!(win_res.batch.rows[0].get("rn"), Some(&ScalarValue::Int64(1)));
    assert_eq!(win_res.batch.rows[0].get("running_sum"), Some(&ScalarValue::Float64(50.0)));
    assert_eq!(win_res.batch.rows[0].get("prev_amt"), Some(&ScalarValue::Null));
    // id=2 (eng, 250) -> rn=2, running_sum=300, prev=50
    assert_eq!(win_res.batch.rows[1].get("rn"), Some(&ScalarValue::Int64(2)));
    assert_eq!(win_res.batch.rows[1].get("running_sum"), Some(&ScalarValue::Float64(300.0)));
    assert_eq!(win_res.batch.rows[1].get("prev_amt"), Some(&ScalarValue::Float64(50.0)));
    // id=3 (eng, 600) -> rn=3, running_sum=900, prev=250
    assert_eq!(win_res.batch.rows[2].get("rn"), Some(&ScalarValue::Int64(3)));
    assert_eq!(win_res.batch.rows[2].get("running_sum"), Some(&ScalarValue::Float64(900.0)));
    // id=4 (ops, 150) -> rn=1, running_sum=150
    assert_eq!(win_res.batch.rows[3].get("rn"), Some(&ScalarValue::Int64(1)));

    // 3. UNION ALL and UNION (distinct)
    let u_all = cluster
        .sql("SELECT dept FROM sales WHERE id <= 2 UNION ALL SELECT dept FROM sales WHERE id >= 2")
        .await
        .unwrap();
    assert_eq!(u_all.batch.num_rows(), 5);

    let u_dist = cluster
        .sql("SELECT dept FROM sales WHERE id <= 2 UNION SELECT dept FROM sales WHERE id >= 2 ORDER BY dept")
        .await
        .unwrap();
    assert_eq!(u_dist.batch.num_rows(), 2);
    assert_eq!(u_dist.batch.rows[0].get("dept"), Some(&ScalarValue::Utf8("eng".into())));
    assert_eq!(u_dist.batch.rows[1].get("dept"), Some(&ScalarValue::Utf8("ops".into())));
}
