// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use bytes::{Buf, BufMut, BytesMut};
use rustlakedb_catalog::rest::IcebergRestServer;
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::kafka_ingest::{
    encode_kafka_record_batch_v2, fetch_kafka_topic_partitions, list_kafka_offset, KafkaCdcConsumer,
};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{RowBatch, ScalarValue};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn wave17_bidirectional_iceberg_v3_rest_catalog_read_write_occ() {
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
        store.clone(),
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    );

    let rest = IcebergRestServer::bind("127.0.0.1:0", catalog.clone())
        .await
        .unwrap();
    let addr = rest.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = rest.serve().await;
    });

    let base = format!("http://127.0.0.1:{}", addr.port());
    let http = reqwest::Client::new();

    // 1. OAuth2 token exchange
    let tok_resp = http
        .post(format!("{base}/v1/oauth/tokens"))
        .send()
        .await
        .unwrap();
    assert_eq!(tok_resp.status(), 200);
    let tok_json: serde_json::Value = tok_resp.json().await.unwrap();
    assert_eq!(tok_json["token_type"], "bearer");
    assert!(tok_json["access_token"].as_str().unwrap().starts_with("rldb-oauth-"));

    // 2. Create & load namespace + duplicate 409 check
    let ns_resp = http
        .post(format!("{base}/v1/namespaces"))
        .json(&json!({
            "namespace": ["analytics"],
            "properties": { "owner": "lakehouse-team" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), 200);

    let dup_ns = http
        .post(format!("{base}/v1/namespaces"))
        .json(&json!({ "namespace": ["analytics"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(dup_ns.status(), 409);

    let get_ns: serde_json::Value = http
        .get(format!("{base}/v1/namespaces/analytics"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(get_ns["properties"]["owner"], "lakehouse-team");

    // 3. Create Iceberg v3 table via REST API (PyIceberg/Spark spec)
    let create_resp = http
        .post(format!("{base}/v1/namespaces/analytics/tables"))
        .json(&json!({
            "name": "orders_rest",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" },
                    { "id": 2, "name": "region", "required": false, "type": "string" },
                    { "id": 3, "name": "price", "required": false, "type": "decimal(12, 2)" }
                ]
            },
            "partition-spec": {
                "spec-id": 0,
                "fields": [
                    { "source-id": 2, "field-id": 1000, "name": "region", "transform": "identity" }
                ]
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), 200);
    let created: serde_json::Value = create_resp.json().await.unwrap();
    assert_eq!(created["metadata"]["format-version"], 3);
    let snap1 = created["metadata"]["current-snapshot-id"].as_u64().unwrap();
    assert_eq!(snap1, 1);

    // HEAD table exists check
    let head_resp = http
        .head(format!("{base}/v1/namespaces/analytics/tables/orders_rest"))
        .send()
        .await
        .unwrap();
    assert_eq!(head_resp.status(), 204);

    // 4. Write a real Parquet data file as an external engine (Spark/PyIceberg) would,
    //    then commit it atomically via POST /v1/namespaces/analytics/tables/orders_rest!
    let meta = catalog.get_table("orders_rest").await.unwrap();
    let batch = RowBatch::new(
        vec!["id".into(), "region".into(), "price".into()],
        vec![
            BTreeMap::from([
                ("id".into(), ScalarValue::Int64(101)),
                ("region".into(), ScalarValue::Utf8("us-east".into())),
                ("price".into(), ScalarValue::Decimal(4999, 2)),
            ]),
            BTreeMap::from([
                ("id".into(), ScalarValue::Int64(102)),
                ("region".into(), ScalarValue::Utf8("eu-west".into())),
                ("price".into(), ScalarValue::Decimal(10001, 2)),
            ]),
        ],
    );
    let df_entry = store
        .write_data_file("orders_rest/data/external_pyiceberg_001.parquet", &meta.schema, &batch)
        .await
        .unwrap();

    let commit_resp = http
        .post(format!("{base}/v1/namespaces/analytics/tables/orders_rest"))
        .json(&json!({
            "requirements": [
                { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": snap1 }
            ],
            "updates": [
                {
                    "action": "add-schema",
                    "schema": {
                        "type": "struct",
                        "fields": [
                            { "id": 1, "name": "id", "type": "long" },
                            { "id": 2, "name": "region", "type": "string" },
                            { "id": 3, "name": "price", "type": "decimal(12, 2)" },
                            { "id": 4, "name": "source_engine", "type": "string" }
                        ]
                    }
                },
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "added-files": [{
                            "file-path": df_entry.file_path,
                            "record-count": df_entry.row_count,
                            "file-size-in-bytes": df_entry.byte_size
                        }]
                    }
                }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(commit_resp.status(), 200);
    let committed: serde_json::Value = commit_resp.json().await.unwrap();
    let snap2 = committed["metadata"]["current-snapshot-id"].as_u64().unwrap();
    assert_eq!(snap2, 2);

    // 5. Stale OCC commit MUST fail with 409 Conflict!
    let stale_resp = http
        .post(format!("{base}/v1/namespaces/analytics/tables/orders_rest"))
        .json(&json!({
            "requirements": [
                { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": snap1 }
            ],
            "updates": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(stale_resp.status(), 409);

    // 6. Query the externally committed REST table immediately via RustLakeDB distributed SQL!
    let sql_res = coord
        .execute_sql(
            "SELECT id, region, price FROM orders_rest ORDER BY id ASC",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(sql_res.batch.num_rows(), 2);
    assert_eq!(
        sql_res.batch.rows[0].get("region"),
        Some(&ScalarValue::Utf8("us-east".into()))
    );
    assert_eq!(
        sql_res.batch.rows[1].get("id"),
        Some(&ScalarValue::Int64(102))
    );
}

#[tokio::test]
async fn wave17_kafka_record_batch_v2_zstd_metadata_list_offsets_cdc() {
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
        .execute_sql("CREATE TABLE accounts (id BIGINT, name TEXT, bal DOUBLE)", "main")
        .await
        .unwrap();

    // Build a real Zstd-compressed Kafka RecordBatch v2 (magic=2, codec=4) containing Debezium CDC events!
    let ev1 = br#"{"op":"c","after":{"id":1,"name":"Alice","bal":500.0}}"#;
    let ev2 = br#"{"op":"c","after":{"id":2,"name":"Bob","bal":300.0}}"#;
    let ev3 = br#"{"op":"u","before":{"id":1},"after":{"id":1,"name":"Alice","bal":750.0}}"#;
    let ev4 = br#"{"op":"d","before":{"id":2}}"#;
    let zstd_batch_v2 =
        encode_kafka_record_batch_v2(10, &[ev1, ev2, ev3, ev4], true).unwrap();

    // Spawn mock Kafka 3.x / Redpanda TCP broker handling Metadata v1, ListOffsets v1, and Fetch v4!
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_addr = listener.local_addr().unwrap().to_string();
    let batch_arc = Arc::new(zstd_batch_v2);

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let batch_arc = batch_arc.clone();
            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                if sock.read_exact(&mut len_buf).await.is_err() {
                    return;
                }
                let req_len = i32::from_be_bytes(len_buf) as usize;
                let mut req = vec![0u8; req_len];
                if sock.read_exact(&mut req).await.is_err() {
                    return;
                }
                let mut cur = &req[..];
                let api_key = cur.get_i16();
                let _api_ver = cur.get_i16();
                let corr_id = cur.get_i32();

                let mut resp = BytesMut::new();
                resp.put_i32(corr_id);
                match api_key {
                    3 => {
                        // Metadata v1 response: 1 broker, 1 topic ("cdc.accounts") with partitions [0, 1]
                        resp.put_i32(1); // brokers len
                        resp.put_i32(1); // node_id
                        resp.put_i16(9);
                        resp.put_slice(b"127.0.0.1");
                        resp.put_i32(9092);
                        resp.put_i16(-1); // null rack
                        resp.put_i32(1); // controller_id
                        resp.put_i32(1); // topics len
                        resp.put_i16(0); // err
                        let t = b"cdc.accounts";
                        resp.put_i16(t.len() as i16);
                        resp.put_slice(t);
                        resp.put_u8(0); // is_internal
                        resp.put_i32(2); // 2 partitions
                        for pid in [0i32, 1i32] {
                            resp.put_i16(0); // err
                            resp.put_i32(pid);
                            resp.put_i32(1); // leader
                            resp.put_i32(1);
                            resp.put_i32(1); // replicas
                            resp.put_i32(1);
                            resp.put_i32(1); // isr
                        }
                    }
                    2 => {
                        // ListOffsets v1 response: offset = 14
                        resp.put_i32(1); // topics len
                        let t = b"cdc.accounts";
                        resp.put_i16(t.len() as i16);
                        resp.put_slice(t);
                        resp.put_i32(1); // partitions len
                        resp.put_i32(0); // pid
                        resp.put_i16(0); // err
                        resp.put_i64(1_700_000_000_000);
                        resp.put_i64(14); // latest offset
                    }
                    1 => {
                        // Fetch v4 response with Zstd RecordBatch v2!
                        resp.put_i32(0); // throttle_time_ms
                        resp.put_i32(1); // topics len
                        let t = b"cdc.accounts";
                        resp.put_i16(t.len() as i16);
                        resp.put_slice(t);
                        resp.put_i32(1); // partitions len
                        resp.put_i32(0); // pid
                        resp.put_i16(0); // err
                        resp.put_i64(14); // high_watermark
                        resp.put_i64(14); // last_stable_offset
                        resp.put_i32(0); // aborted_tx len
                        resp.put_i32(batch_arc.len() as i32);
                        resp.put_slice(&batch_arc);
                    }
                    _ => {}
                }
                let mut frame = BytesMut::with_capacity(4 + resp.len());
                frame.put_i32(resp.len() as i32);
                frame.put_slice(&resp);
                let _ = sock.write_all(&frame).await;
            });
        }
    });

    // 1. Metadata v1 topic partition discovery
    let parts = fetch_kafka_topic_partitions(&broker_addr, "cdc.accounts")
        .await
        .unwrap();
    assert_eq!(parts, vec![0, 1]);

    // 2. ListOffsets v1
    let latest = list_kafka_offset(&broker_addr, "cdc.accounts", 0, -1)
        .await
        .unwrap();
    assert_eq!(latest, 14);

    // 3. Poll Zstd RecordBatch v2 CDC events into Iceberg v3!
    let consumer = KafkaCdcConsumer::new(
        coord.clone(),
        &broker_addr,
        "cdc.accounts",
        0,
        "accounts",
        dir.path().join("_kafka"),
    )
    .unwrap();
    let rep1 = consumer.poll_once().await.unwrap();
    assert_eq!(rep1.messages_fetched, 4);
    assert_eq!(rep1.applied, 4);
    assert_eq!(rep1.next_offset, 14);

    // Verify exact final state: Alice=750.0 (updated), Bob deleted!
    let res = coord
        .execute_sql("SELECT id, name, bal FROM accounts", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1);
    assert_eq!(
        res.batch.rows[0].get("name"),
        Some(&ScalarValue::Utf8("Alice".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("bal"),
        Some(&ScalarValue::Float64(750.0))
    );

    // 4. Second poll from checkpoint offset=14 returns 0 new messages (exactly-once!)
    let rep2 = consumer.poll_once().await.unwrap();
    assert_eq!(rep2.messages_fetched, 0);
    assert_eq!(rep2.next_offset, 14);
}

#[tokio::test]
#[ignore = "requires live Redpanda/Kafka broker at 127.0.0.1:19092 with topic cdc.orders"]
async fn live_redpanda_record_batch_v2_end_to_end() {
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
        .execute_sql(
            "CREATE TABLE rp_orders (id BIGINT, customer TEXT, amount DOUBLE)",
            "main",
        )
        .await
        .unwrap();

    let broker = "127.0.0.1:19092";
    let parts = fetch_kafka_topic_partitions(broker, "cdc.orders")
        .await
        .unwrap();
    println!("Live Redpanda Metadata v1 partitions for cdc.orders: {parts:?}");
    assert_eq!(parts, vec![0]);

    let latest = list_kafka_offset(broker, "cdc.orders", 0, -1).await.unwrap();
    println!("Live Redpanda ListOffsets v1 latest offset: {latest}");
    assert!(latest >= 4);

    let v4_direct = rustlakedb_coordinator::kafka_ingest::fetch_kafka_v4_records(
        broker, "cdc.orders", 0, 0, 1024 * 1024,
    )
    .await;
    println!("Direct Fetch v4 result: {v4_direct:?}");
    let consumer = KafkaCdcConsumer::new(
        coord.clone(),
        broker,
        "cdc.orders",
        0,
        "rp_orders",
        dir.path().join("_kafka"),
    )
    .unwrap();
    let rep = consumer.poll_once().await.unwrap();
    println!("Live Redpanda Fetch v4 RecordBatch v2 CDC report: {rep:?}");
    assert_eq!(rep.messages_fetched, 4);
    assert_eq!(rep.applied, 4);
    assert_eq!(rep.next_offset, 4);

    let res = coord
        .execute_sql("SELECT id, customer, amount FROM rp_orders ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1);
    assert_eq!(
        res.batch.rows[0].get("customer"),
        Some(&ScalarValue::Utf8("Alice".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("amount"),
        Some(&ScalarValue::Float64(300.0))
    );
    println!("LIVE REDPANDA STREAMING CDC -> ICEBERG v3 VERIFIED 100%!");
}
