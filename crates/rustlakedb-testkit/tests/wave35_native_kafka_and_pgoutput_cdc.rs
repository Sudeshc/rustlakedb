// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use bytes::{Buf, BufMut, BytesMut};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::kafka_ingest::{encode_kafka_record_batch_v2, KafkaCdcConsumer};
use rustlakedb_coordinator::pgoutput::{
    encode_pg_xlog_copy_data_frame, encode_pgoutput_message, PgOutputCdcConsumer, PgOutputMessage,
    PgRelationColumn, PgTupleColumn,
};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type KafkaLog = Arc<Mutex<Vec<(i64, Vec<u8>)>>>;

/// Minimal real TCP Kafka v4 Fetch broker for deterministic Wave 35 CDC testing.
async fn start_kafka_v4_broker(topic: &'static str, log: KafkaLog) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                if sock.read_exact(&mut len_buf).await.is_err() {
                    return;
                }
                let req_len = i32::from_be_bytes(len_buf).max(0) as usize;
                let mut body = vec![0u8; req_len];
                if sock.read_exact(&mut body).await.is_err() {
                    return;
                }
                let mut cur = &body[..];
                let _api_key = cur.get_i16();
                let _api_ver = cur.get_i16();
                let corr_id = cur.get_i32();
                let cid_len = cur.get_i16().max(0) as usize;
                cur.advance(cid_len);
                // Fetch v4: replica_id(4), max_wait(4), min_bytes(4), max_bytes(4), isolation(1), topics_len(4)
                cur.advance(4 + 4 + 4 + 4 + 1 + 4);
                let tlen = cur.get_i16().max(0) as usize;
                cur.advance(tlen);
                let _num_parts = cur.get_i32();
                let pid = cur.get_i32();
                let fetch_offset = cur.get_i64();

                let slice: Vec<(i64, Vec<u8>)> = {
                    let g = log.lock().unwrap();
                    g.iter()
                        .filter(|(off, _)| *off >= fetch_offset)
                        .cloned()
                        .collect()
                };
                let base_offset = slice.first().map(|(o, _)| *o).unwrap_or(fetch_offset);
                let payloads: Vec<&[u8]> = slice.iter().map(|(_, p)| p.as_slice()).collect();
                let rbatch = if payloads.is_empty() {
                    Vec::new()
                } else {
                    encode_kafka_record_batch_v2(base_offset, &payloads, true).unwrap()
                };

                let mut resp = BytesMut::new();
                resp.put_i32(corr_id);
                resp.put_i32(0); // throttle_time_ms
                resp.put_i32(1); // topics count
                resp.put_i16(topic.len() as i16);
                resp.put_slice(topic.as_bytes());
                resp.put_i32(1); // partitions count
                resp.put_i32(pid);
                resp.put_i16(0); // error_code
                resp.put_i64(base_offset + payloads.len() as i64); // high_watermark
                resp.put_i64(base_offset + payloads.len() as i64); // last_stable_offset
                resp.put_i32(0); // aborted_transactions len
                resp.put_i32(rbatch.len() as i32);
                resp.put_slice(&rbatch);

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
async fn wave35_native_pgoutput_tcp_logical_replication_cdc_with_equality_deletes() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog.clone(),
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql(
            "CREATE TABLE pg_cdc_accounts (id BIGINT, owner VARCHAR, balance DOUBLE)",
            "main",
        )
        .await
        .unwrap();

    // Build real PostgreSQL pgoutput binary WAL stream (Tx 1: inserts; Tx 2: update id=1 + delete id=2).
    let rel_msg = encode_pgoutput_message(&PgOutputMessage::Relation {
        rel_id: 16384,
        namespace: "public".into(),
        name: "pg_cdc_accounts".into(),
        replica_identity: b'd',
        columns: vec![
            PgRelationColumn { flags: 1, name: "id".into(), type_oid: 20, type_modifier: -1 },
            PgRelationColumn { flags: 0, name: "owner".into(), type_oid: 25, type_modifier: -1 },
            PgRelationColumn { flags: 0, name: "balance".into(), type_oid: 701, type_modifier: -1 },
        ],
    });
    let frames = vec![
        encode_pg_xlog_copy_data_frame(100, 100, 1, &rel_msg),
        encode_pg_xlog_copy_data_frame(101, 110, 1, &encode_pgoutput_message(&PgOutputMessage::Begin { final_lsn: 110, commit_time_us: 1, xid: 10 })),
        encode_pg_xlog_copy_data_frame(102, 110, 1, &encode_pgoutput_message(&PgOutputMessage::Insert {
            rel_id: 16384,
            tuple: vec![PgTupleColumn::Text("1".into()), PgTupleColumn::Text("Alice".into()), PgTupleColumn::Text("100.0".into())],
        })),
        encode_pg_xlog_copy_data_frame(103, 110, 1, &encode_pgoutput_message(&PgOutputMessage::Insert {
            rel_id: 16384,
            tuple: vec![PgTupleColumn::Text("2".into()), PgTupleColumn::Text("Bob".into()), PgTupleColumn::Text("250.0".into())],
        })),
        encode_pg_xlog_copy_data_frame(110, 110, 1, &encode_pgoutput_message(&PgOutputMessage::Commit { flags: 0, commit_lsn: 110, end_lsn: 110, commit_time_us: 1 })),
        // Transaction 2: Update Alice -> 555.5 and Delete Bob!
        encode_pg_xlog_copy_data_frame(120, 130, 2, &encode_pgoutput_message(&PgOutputMessage::Begin { final_lsn: 130, commit_time_us: 2, xid: 11 })),
        encode_pg_xlog_copy_data_frame(121, 130, 2, &encode_pgoutput_message(&PgOutputMessage::Update {
            rel_id: 16384,
            old_tuple: Some(vec![PgTupleColumn::Text("1".into()), PgTupleColumn::Text("Alice".into()), PgTupleColumn::Text("100.0".into())]),
            new_tuple: vec![PgTupleColumn::Text("1".into()), PgTupleColumn::Text("Alice".into()), PgTupleColumn::Text("555.5".into())],
        })),
        encode_pg_xlog_copy_data_frame(122, 130, 2, &encode_pgoutput_message(&PgOutputMessage::Delete {
            rel_id: 16384,
            key_tuple: vec![PgTupleColumn::Text("2".into()), PgTupleColumn::Null, PgTupleColumn::Null],
        })),
        encode_pg_xlog_copy_data_frame(130, 130, 2, &encode_pgoutput_message(&PgOutputMessage::Commit { flags: 0, commit_lsn: 130, end_lsn: 130, commit_time_us: 2 })),
    ];

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let frames_clone = frames.clone();
    tokio::spawn(async move {
        // Serve two connections: initial stream + exact replay to prove idempotent LSN checkpointing!
        for _ in 0..2 {
            if let Ok((mut sock, _)) = listener.accept().await {
                for f in &frames_clone {
                    let _ = sock.write_all(f).await;
                }
                let _ = sock.write_all(&[b'c', 0, 0, 0, 4]).await; // CopyDone
            }
        }
    });

    let cp_dir = dir.path().join("pgoutput_cp");
    let consumer = PgOutputCdcConsumer::new(coord.clone(), "pg_cdc_accounts", "id", "slot_main", &cp_dir).unwrap();
    let rep1 = consumer.consume_tcp(&addr).await.unwrap();
    assert_eq!(rep1.transactions_committed, 2);
    assert_eq!(rep1.inserts_applied, 2);
    assert_eq!(rep1.updates_applied, 1);
    assert_eq!(rep1.deletes_applied, 1);
    assert_eq!(rep1.committed_lsn, 130);

    // Replay the exact same WAL stream after simulated consumer restart -> 0 duplicates!
    let consumer2 = PgOutputCdcConsumer::new(coord.clone(), "pg_cdc_accounts", "id", "slot_main", &cp_dir).unwrap();
    let rep2 = consumer2.consume_tcp(&addr).await.unwrap();
    assert_eq!(rep2.transactions_committed, 0);
    assert_eq!(rep2.skipped_replayed_txs, 2);

    // Query Iceberg table: Bob deleted via Iceberg v3 Equality Delete, Alice updated to 555.5!
    let res = coord
        .execute_sql("SELECT id, owner, balance FROM pg_cdc_accounts ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.rows.len(), 1);
    assert_eq!(res.batch.rows[0].get("owner").and_then(|v| v.to_pg_text()).as_deref(), Some("Alice"));
    assert_eq!(res.batch.rows[0].get("balance").and_then(|v| v.as_f64()), Some(555.5));
}

#[tokio::test]
async fn wave35_kafka_tcp_zero_read_equality_delete_cdc() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog.clone(),
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE kafka_eq_orders (id BIGINT, status VARCHAR, total DOUBLE)", "main")
        .await
        .unwrap();

    let log = Arc::new(Mutex::new(vec![
        (0, json!({"op": "c", "after": {"id": 10, "status": "NEW", "total": 50.0}}).to_string().into_bytes()),
        (1, json!({"op": "c", "after": {"id": 20, "status": "NEW", "total": 75.0}}).to_string().into_bytes()),
    ]));
    let broker_addr = start_kafka_v4_broker("orders_cdc", log.clone()).await;

    let consumer = KafkaCdcConsumer::new(
        coord.clone(),
        broker_addr,
        "orders_cdc",
        0,
        "kafka_eq_orders",
        dir.path().join("kafka_cp"),
    )
    .unwrap();
    let r1 = consumer.poll_once_equality_deletes("id").await.unwrap();
    assert_eq!(r1.applied, 2);
    assert_eq!(r1.next_offset, 2);

    // Stream an update for id=10 and delete for id=20 (zero-read Iceberg v3 Equality Deletes!).
    {
        let mut g = log.lock().unwrap();
        g.push((2, json!({"op": "u", "before": {"id": 10}, "after": {"id": 10, "status": "SHIPPED", "total": 55.0}}).to_string().into_bytes()));
        g.push((3, json!({"op": "d", "before": {"id": 20}}).to_string().into_bytes()));
    }
    let r2 = consumer.poll_once_equality_deletes("id").await.unwrap();
    assert_eq!(r2.applied, 2);
    assert_eq!(r2.next_offset, 4);

    let res = coord
        .execute_sql("SELECT id, status, total FROM kafka_eq_orders ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.rows.len(), 1);
    assert_eq!(res.batch.rows[0].get("status").and_then(|v| v.to_pg_text()).as_deref(), Some("SHIPPED"));
    assert_eq!(res.batch.rows[0].get("total").and_then(|v| v.as_f64()), Some(55.0));

    // Confirm Iceberg snapshot actually recorded an Equality Delete Parquet file!
    let meta = catalog.get_table("kafka_eq_orders").await.unwrap();
    assert!(!meta.current_snapshot("main").unwrap().equality_deletes.is_empty());
}
