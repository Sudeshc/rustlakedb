// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 32: Advanced Analytical SQL (`ROWS BETWEEN` Sliding Window Frames, `QUALIFY`,
//! `UNNEST` / `JSONB_ARRAY_ELEMENTS`, `EXPLAIN (FORMAT JSON)`) & Full PGWire Extended
//! Query Protocol (`Parse`/`Bind`/`Execute` with both Text AND Binary big-endian parameters!).

use bytes::{BufMut, BytesMut};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_gateway::PgGateway;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn wave32_window_frames_qualify_unnest_and_explain_json() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql(
            "CREATE TABLE metrics (day BIGINT, grp TEXT, val DOUBLE)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO metrics VALUES
                (1, 'a', 10.0), (2, 'a', 20.0), (3, 'a', 30.0), (4, 'a', 40.0),
                (1, 'b', 100.0), (2, 'b', 200.0)",
            "main",
        )
        .await
        .unwrap();

    // 1. Sliding Window Frame (`ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`) moving average!
    let wf = coord
        .execute_sql(
            "SELECT day, grp, AVG(val) OVER (PARTITION BY grp ORDER BY day ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ma2 FROM metrics ORDER BY grp, day",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(wf.batch.num_rows(), 6);
    // grp='a': day1=10, day2=(10+20)/2=15, day3=(20+30)/2=25, day4=(30+40)/2=35
    assert_eq!(wf.batch.rows[0].get("ma2"), Some(&ScalarValue::Float64(10.0)));
    assert_eq!(wf.batch.rows[1].get("ma2"), Some(&ScalarValue::Float64(15.0)));
    assert_eq!(wf.batch.rows[2].get("ma2"), Some(&ScalarValue::Float64(25.0)));
    assert_eq!(wf.batch.rows[3].get("ma2"), Some(&ScalarValue::Float64(35.0)));
    // grp='b': day1=100, day2=150
    assert_eq!(wf.batch.rows[4].get("ma2"), Some(&ScalarValue::Float64(100.0)));
    assert_eq!(wf.batch.rows[5].get("ma2"), Some(&ScalarValue::Float64(150.0)));

    // 2. QUALIFY post-window filtering (top 1 per group!)
    let qual = coord
        .execute_sql(
            "SELECT day, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn FROM metrics QUALIFY rn = 1 ORDER BY grp",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(qual.batch.num_rows(), 2);
    assert_eq!(qual.batch.rows[0].get("val"), Some(&ScalarValue::Float64(40.0)));
    assert_eq!(qual.batch.rows[1].get("val"), Some(&ScalarValue::Float64(200.0)));

    // 3. UNNEST(ARRAY[...]) and JSONB_ARRAY_ELEMENTS(...)
    let u1 = coord
        .execute_sql("SELECT * FROM UNNEST(ARRAY['alpha', 'beta', 'gamma'])", "main")
        .await
        .unwrap();
    assert_eq!(u1.batch.num_rows(), 3);
    assert_eq!(u1.batch.rows[0].get("unnest"), Some(&ScalarValue::Utf8("alpha".into())));

    coord
        .execute_sql("CREATE TABLE carts (id BIGINT, payload JSONB)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            r#"INSERT INTO carts VALUES (1, '{"items": ["laptop", "mouse"]}'), (2, '{"items": ["desk"]}')"#,
            "main",
        )
        .await
        .unwrap();
    let u2 = coord
        .execute_sql(
            "SELECT id, JSONB_ARRAY_ELEMENTS(payload->'items') AS item FROM carts",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(u2.batch.num_rows(), 3);

    // 4. EXPLAIN (FORMAT JSON)
    let exp = coord
        .execute_sql(
            "EXPLAIN (FORMAT JSON) SELECT grp, SUM(val) FROM metrics GROUP BY grp",
            "main",
        )
        .await
        .unwrap();
    let json_text = exp.batch.rows[0]
        .get("QUERY PLAN")
        .unwrap()
        .to_display_string();
    let parsed: serde_json::Value = serde_json::from_str(&json_text).unwrap();
    assert_eq!(parsed[0]["Plan"]["Engine"], "RustLakeDB");
    assert!(parsed[0]["Plan"]["StageCount"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn wave32_pgwire_extended_protocol_binary_and_text_bind_parameters() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE items (id BIGINT, name TEXT, price DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO items VALUES (1, 'book', 15.0), (2, 'phone', 800.0), (3, 'laptop', 1500.0)",
            "main",
        )
        .await
        .unwrap();

    let gw = PgGateway::bind("127.0.0.1:0", coord).await.unwrap();
    let addr = gw.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gw.serve().await;
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    // PGWire StartupMessage v3.0
    let mut startup = BytesMut::new();
    let mut body = BytesMut::new();
    body.put_i32(196608);
    body.put_slice(b"user\0postgres\0database\0rustlakedb\0\0");
    startup.put_i32((body.len() + 4) as i32);
    startup.put_slice(&body);
    stream.write_all(&startup).await.unwrap();

    // Drain until ReadyForQuery ('Z')
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    assert!(buf[..n].contains(&b'Z'), "expected ReadyForQuery");

    // Send Extended Query Parse ('P') with OIDs [20 (int8), 25 (text)] + Bind ('B') with binary int8=2 & text='phone' + Execute ('E') + Sync ('S')!
    let mut msg = BytesMut::new();

    // Parse: stmt="s1", sql="SELECT name, price FROM items WHERE id = $1 AND name = $2", 2 OIDs (20, 25)
    let mut p_body = BytesMut::new();
    p_body.put_slice(b"s1\0SELECT name, price FROM items WHERE id = $1 AND name = $2\0");
    p_body.put_i16(2);
    p_body.put_i32(20); // int8
    p_body.put_i32(25); // text
    msg.put_u8(b'P');
    msg.put_i32((p_body.len() + 4) as i32);
    msg.put_slice(&p_body);

    // Bind: portal="", stmt="s1", formats=[1 (binary), 0 (text)], params=[2i64 BE, "phone"], result_formats=[]
    let mut b_body = BytesMut::new();
    b_body.put_slice(b"\0s1\0");
    b_body.put_i16(2);
    b_body.put_i16(1); // binary for $1
    b_body.put_i16(0); // text for $2
    b_body.put_i16(2); // 2 params
    b_body.put_i32(8);
    b_body.put_i64(2); // binary int8 = 2
    b_body.put_i32(5);
    b_body.put_slice(b"phone");
    b_body.put_i16(0);
    msg.put_u8(b'B');
    msg.put_i32((b_body.len() + 4) as i32);
    msg.put_slice(&b_body);

    // Execute: portal="", max_rows=0
    msg.put_u8(b'E');
    msg.put_i32(9);
    msg.put_u8(0);
    msg.put_i32(0);

    // Sync
    msg.put_u8(b'S');
    msg.put_i32(4);

    stream.write_all(&msg).await.unwrap();
    let mut resp = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let k = stream.read(&mut tmp).await.unwrap();
        if k == 0 { break; }
        resp.extend_from_slice(&tmp[..k]);
        if resp.windows(6).any(|w| w[0] == b'Z' && w[4] == 5) || resp.contains(&b'Z') {
            break;
        }
    }
    let resp_text = String::from_utf8_lossy(&resp);
    assert!(
        resp_text.contains("phone") && resp_text.contains("800"),
        "expected PGWire binary+text Bind query to return ('phone', 800), got: {resp_text:?}"
    );
}
