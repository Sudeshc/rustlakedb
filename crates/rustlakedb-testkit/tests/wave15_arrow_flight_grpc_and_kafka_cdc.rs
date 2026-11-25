// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_array::{Decimal128Array, Int64Array, StringArray};
use arrow_flight::{FlightClient, FlightDescriptor};
use bytes::{Buf, BufMut, BytesMut};
use futures::TryStreamExt;
use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::kafka_ingest::KafkaCdcConsumer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_gateway::flight::FlightGateway;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tonic::transport::Endpoint;

#[tokio::test]
async fn wave15_apache_arrow_flight_grpc_end_to_end() {
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
        .execute_sql(
            "CREATE TABLE flight_sales (id BIGINT, region VARCHAR, revenue DECIMAL(18, 2))",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO flight_sales VALUES (1, 'us-east', 150.25), (2, 'eu-west', 299.75)",
            "main",
        )
        .await
        .unwrap();

    // Start real Apache Arrow Flight HTTP/2 gRPC server!
    let fg = FlightGateway::bind("127.0.0.1:0", coord).await.unwrap();
    let addr = fg.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        let _ = fg.serve().await;
    });

    // Connect with official Apache Arrow `FlightClient` over gRPC!
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);

    // 1. Handshake
    let hs = client.handshake(bytes::Bytes::new()).await.unwrap();
    assert!(hs.starts_with(b"rustlakedb-flight"));

    // 2. ListFlights enumerates Iceberg tables
    let flights: Vec<_> = client
        .list_flights(bytes::Bytes::new())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        flights
            .iter()
            .any(|f| f.flight_descriptor.as_ref().is_some_and(|d| d.path == vec!["flight_sales"]))
    );

    // 3. GetFlightInfo + DoGet streaming native Arrow RecordBatches (Int64, Utf8, Decimal128)!
    let desc = FlightDescriptor::new_cmd(
        "SELECT id, region, revenue FROM flight_sales ORDER BY id".to_string(),
    );
    let info = client.get_flight_info(desc).await.unwrap();
    assert_eq!(info.total_records, 2);
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let batches: Vec<_> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let rb = &batches[0];
    assert_eq!(rb.num_rows(), 2);
    let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let regs = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let revs = rb.column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(regs.value(0), "us-east");
    assert_eq!(revs.value(0), 15_025);
    assert_eq!(ids.value(1), 2);
    assert_eq!(revs.value(1), 29_975);

    srv.abort();
}

/// Spawns a tiny pure-Rust Kafka Fetch v0 binary wire-protocol broker serving `(offset, json_payload)` messages.
async fn spawn_mock_kafka_broker(messages: Arc<Vec<(i64, Vec<u8>)>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let msgs = messages.clone();
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
                let _api_key = cur.get_i16();
                let _api_ver = cur.get_i16();
                let corr_id = cur.get_i32();
                let clen = cur.get_i16() as usize;
                cur.advance(clen);
                // replica_id, max_wait, min_bytes, num_topics
                cur.advance(16);
                let tlen = cur.get_i16() as usize;
                let topic = String::from_utf8_lossy(&cur[..tlen]).to_string();
                cur.advance(tlen);
                let _num_parts = cur.get_i32();
                let partition = cur.get_i32();
                let fetch_offset = cur.get_i64();

                // Build Kafka v0 MessageSet for records with offset >= fetch_offset
                let mut mset = BytesMut::new();
                for (off, val) in msgs.iter().filter(|(o, _)| *o >= fetch_offset) {
                    let mut msg = BytesMut::new();
                    msg.put_i32(0); // crc placeholder
                    msg.put_i8(0); // magic v0
                    msg.put_i8(0); // attributes
                    msg.put_i32(-1); // null key
                    msg.put_i32(val.len() as i32);
                    msg.put_slice(val);
                    mset.put_i64(*off);
                    mset.put_i32(msg.len() as i32);
                    mset.put_slice(&msg);
                }

                let mut resp = BytesMut::new();
                resp.put_i32(corr_id);
                resp.put_i32(1); // 1 topic
                resp.put_i16(topic.len() as i16);
                resp.put_slice(topic.as_bytes());
                resp.put_i32(1); // 1 partition
                resp.put_i32(partition);
                resp.put_i16(0); // error_code = 0 (OK)
                resp.put_i64(msgs.len() as i64); // high_watermark
                resp.put_i32(mset.len() as i32);
                resp.put_slice(&mset);

                let mut frame = BytesMut::with_capacity(4 + resp.len());
                frame.put_i32(resp.len() as i32);
                frame.put_slice(&resp);
                let _ = sock.write_all(&frame).await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn wave15_native_kafka_binary_protocol_cdc_consumer_exactly_once() {
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
        .execute_sql("CREATE TABLE kafka_users (id BIGINT, name VARCHAR, plan VARCHAR)", "main")
        .await
        .unwrap();

    let records = Arc::new(vec![
        (0i64, br#"{"op":"c","key_col":"id","after":{"id":1,"name":"alice","plan":"free"}}"#.to_vec()),
        (1i64, br#"{"op":"c","key_col":"id","after":{"id":2,"name":"bob","plan":"pro"}}"#.to_vec()),
        (2i64, br#"{"op":"u","key_col":"id","after":{"id":1,"name":"alice","plan":"enterprise"}}"#.to_vec()),
        (3i64, br#"{"op":"d","key_col":"id","before":{"id":2,"name":"bob","plan":"pro"}}"#.to_vec()),
    ]);
    let broker_addr = spawn_mock_kafka_broker(records).await;

    let cp_dir = dir.path().join("_kafka");
    let consumer = KafkaCdcConsumer::new(
        coord.clone(),
        broker_addr.to_string(),
        "dbserver1.public.users",
        0,
        "kafka_users",
        &cp_dir,
    )
    .unwrap();

    // Poll 1: consumes offsets 0..=3 via Kafka binary Fetch v0 protocol & commits Iceberg v3 snapshot!
    let rep1 = consumer.poll_once().await.unwrap();
    assert_eq!(rep1.messages_fetched, 4);
    assert_eq!(rep1.applied, 4);
    assert_eq!(rep1.next_offset, 4);

    // Poll 2: resumes at offset 4 -> 0 new messages, strictly idempotent!
    let rep2 = consumer.poll_once().await.unwrap();
    assert_eq!(rep2.messages_fetched, 0);
    assert_eq!(rep2.next_offset, 4);

    // Verify exact final ACID state: id=1 upgraded to 'enterprise', id=2 deleted via Iceberg v3 DV!
    let res = coord
        .execute_sql("SELECT id, name, plan FROM kafka_users ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1);
    assert_eq!(res.batch.rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(res.batch.rows[0].get("plan"), Some(&ScalarValue::Utf8("enterprise".into())));
}
