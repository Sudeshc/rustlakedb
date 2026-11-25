// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rustlakedb_catalog::raft::RaftCatalogNode;
use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, InMemoryRaftCatalog};
use rustlakedb_coordinator::admin::ControlPlaneServer;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::iceberg_avro::{
    decode_iceberg_v3_manifest_entries, decode_iceberg_v3_manifest_list_entries,
    parse_and_verify_avro_ocf,
};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::rpc::WorkerHttpServer;
use rustlakedb_worker::WorkerNode;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn wave11_raft_wal_log_compaction_and_snapshot_recovery_across_restart() {
    let dir = tempdir().unwrap();
    let d1 = dir.path().join("n1");
    let d2 = dir.path().join("n2");
    let d3 = dir.path().join("n3");

    let n1 = RaftCatalogNode::open_persistent(1, HashMap::new(), &d1).unwrap();
    let n2 = RaftCatalogNode::open_persistent(2, HashMap::new(), &d2).unwrap();
    let n3 = RaftCatalogNode::open_persistent(3, HashMap::new(), &d3).unwrap();
    n1.set_snapshot_threshold(4);
    n2.set_snapshot_threshold(4);
    n3.set_snapshot_threshold(4);

    let a1 = n1.start_server("127.0.0.1:0").await.unwrap();
    let a2 = n2.start_server("127.0.0.1:0").await.unwrap();
    let a3 = n3.start_server("127.0.0.1:0").await.unwrap();
    let peers = HashMap::from([
        (1, format!("http://{a1}")),
        (2, format!("http://{a2}")),
        (3, format!("http://{a3}")),
    ]);
    n1.set_peers(peers.clone());
    n2.set_peers(peers.clone());
    n3.set_peers(peers);

    // Wait for leader election
    let mut elected = false;
    for _ in 0..40 {
        if n1.status().leader_id.is_some() {
            elected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(elected, "Raft quorum failed to elect leader");

    // Commit 9 tables so automatic WAL compaction triggers multiple times (threshold = 4)!
    let schema = vec![FieldSchema {
        field_id: 1,
        name: "id".into(),
        data_type: ColumnType::Int64,
        nullable: false,
    }];
    for i in 1..=9 {
        n1.create_table(IcebergTableMetadata::new(format!("snap_tbl_{i}"), schema.clone()))
            .await
            .unwrap();
    }

    let st = n1.status();
    assert!(
        st.snapshot_index >= 4,
        "expected automatic Raft WAL compaction to advance snapshot_index >= 4, got {st:?}"
    );
    assert!(
        st.log_len < 9,
        "expected compacted Raft WAL log_len < 9, got {st:?}"
    );
    assert_eq!(st.commit_index, 9);

    n1.stop();
    n2.stop();
    n3.stop();

    // Cold restart from disk and verify ALL 9 tables + snapshot_index survived!
    let recovered = RaftCatalogNode::open_persistent(1, HashMap::new(), &d1).unwrap();
    let rec_st = recovered.status();
    assert_eq!(rec_st.commit_index, 9);
    assert!(rec_st.snapshot_index >= 4);
    let tables = recovered.list_tables().await.unwrap();
    assert_eq!(tables.len(), 9);
    assert!(tables.contains(&"snap_tbl_1".to_string()));
    assert!(tables.contains(&"snap_tbl_9".to_string()));
}

#[tokio::test]
async fn wave11_zero_touch_multi_coordinator_ha_worker_discovery_failover() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());

    // Start 2 independent Coordinators (representing HA primary & standby) with 0 static workers
    let coord1 = Arc::new(Coordinator::with_clients(
        catalog.clone(),
        store.clone(),
        shuffle.clone(),
        vec![],
        PlannerConfig::default(),
    ));
    let coord2 = Arc::new(Coordinator::with_clients(
        catalog.clone(),
        store.clone(),
        shuffle.clone(),
        vec![],
        PlannerConfig::default(),
    ));

    let cp1 = ControlPlaneServer::bind("127.0.0.1:0", coord1.clone())
        .await
        .unwrap();
    let cp2 = ControlPlaneServer::bind("127.0.0.1:0", coord2.clone())
        .await
        .unwrap();
    let cp1_addr = cp1.local_addr().unwrap();
    let cp2_addr = cp2.local_addr().unwrap();
    let h1 = tokio::spawn(async move {
        let _ = cp1.serve().await;
    });
    let h2 = tokio::spawn(async move {
        let _ = cp2.serve().await;
    });

    // Start 1 remote HTTP worker and broadcast registration to BOTH coordinators!
    let worker = Arc::new(WorkerNode::new(
        "ha-worker-1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let w_srv = WorkerHttpServer::bind("127.0.0.1:0", worker).await.unwrap();
    let w_url = format!("http://{}", w_srv.local_addr().unwrap());
    let wh = tokio::spawn(async move {
        let _ = w_srv.serve().await;
    });

    let client = reqwest::Client::new();
    for cp_addr in [cp1_addr, cp2_addr] {
        let resp = client
            .post(format!("http://{cp_addr}/v1/workers/register"))
            .json(&serde_json::json!({ "worker_id": "ha-worker-1", "url": w_url }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    // Create & write via Coordinator 1
    coord1
        .execute_sql("CREATE TABLE ha_multi (id BIGINT, v DOUBLE)", "main")
        .await
        .unwrap();
    coord1
        .execute_sql("INSERT INTO ha_multi VALUES (1, 42.5), (2, 57.5)", "main")
        .await
        .unwrap();

    // Kill Coordinator 1 control plane; Coordinator 2 ALREADY knows ha-worker-1 (ZERO manual re-registration!)
    h1.abort();
    let res = coord2
        .execute_sql("SELECT SUM(v) AS total FROM ha_multi", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.rows[0].get("total"), Some(&ScalarValue::Float64(100.0)));

    h2.abort();
    wh.abort();
}

#[tokio::test]
async fn wave11_iceberg_v3_external_avro_manifest_and_parquet_interop() {
    let dir = tempdir().unwrap();
    let wh_root = dir.path().join("wh");
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(&wh_root).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(catalog, store, shuffle, vec![w1], PlannerConfig::default());

    coord
        .execute_sql("CREATE TABLE ext_sales (id BIGINT, region VARCHAR, revenue DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO ext_sales VALUES (1, 'us-east', 100.0), (2, 'eu-west', 200.0), (3, 'ap-south', 300.0)",
            "main",
        )
        .await
        .unwrap();

    // 1. Parse Iceberg v3 metadata.json independently like DuckDB / PyIceberg / Spark!
    let meta_path = wh_root.join("ext_sales/metadata/v2.metadata.json");
    let meta_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    assert_eq!(meta_json["format-version"].as_u64(), Some(3));
    let current_snap_id = meta_json["current-snapshot-id"].as_u64().unwrap();
    let snap_obj = meta_json["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["snapshot-id"].as_u64() == Some(current_snap_id))
        .unwrap();
    let ml_rel = snap_obj["manifest-list"].as_str().unwrap();

    // 2. Independently decode the Avro OCF Manifest List (`snap-*.avro`)
    let ml_bytes = std::fs::read(wh_root.join(ml_rel)).unwrap();
    let ml_ocf = parse_and_verify_avro_ocf(&ml_bytes).unwrap();
    assert_eq!(ml_ocf.metadata.get("format-version").map(String::as_str), Some("3"));
    let ml_entries = decode_iceberg_v3_manifest_list_entries(&ml_bytes).unwrap();
    assert_eq!(ml_entries.len(), 1);
    assert_eq!(ml_entries[0].added_files_count, 1);
    assert_eq!(ml_entries[0].added_rows_count, 3);

    // 3. Follow `manifest_path` and independently decode the Avro OCF Manifest (`*-m0.avro`)
    let m_bytes = std::fs::read(wh_root.join(&ml_entries[0].manifest_path)).unwrap();
    let m_entries = decode_iceberg_v3_manifest_entries(&m_bytes).unwrap();
    assert_eq!(m_entries.len(), 1);
    assert_eq!(m_entries[0].file_format, "PARQUET");
    assert_eq!(m_entries[0].record_count, 3);
    assert!(m_entries[0].file_size_in_bytes > 0);

    // 4. Read the referenced Zstd Parquet data file directly with Apache Arrow/Parquet reader!
    let pq_bytes = Bytes::from(std::fs::read(wh_root.join(&m_entries[0].file_path)).unwrap());
    let reader = ParquetRecordBatchReaderBuilder::try_new(pq_bytes)
        .unwrap()
        .build()
        .unwrap();
    let mut total_rows = 0usize;
    for batch_res in reader {
        let rb = batch_res.unwrap();
        total_rows += rb.num_rows();
        assert_eq!(rb.schema().field(0).name(), "id");
        assert_eq!(rb.schema().field(1).name(), "region");
        assert_eq!(rb.schema().field(2).name(), "revenue");
    }
    assert_eq!(total_rows, 3);
}
