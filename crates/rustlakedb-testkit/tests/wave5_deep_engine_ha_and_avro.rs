// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::raft::{RaftCatalogNode, RaftRole};
use rustlakedb_catalog::{
    Catalog, ColumnType, FieldSchema, IcebergTableMetadata, InMemoryRaftCatalog, TableCommitProposal,
};
use rustlakedb_protocol::{RowBatch, ScalarValue, ShuffleMode};
use rustlakedb_shuffle::{decode_shuffle_block, encode_shuffle_block};
use rustlakedb_storage::iceberg_avro::parse_and_verify_avro_ocf;
use rustlakedb_testkit::{PgTestClient, TestCluster};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_wave5_vectorized_arrow_join_and_arrow_ipc_zstd_shuffle() {
    // 1. Verify binary Arrow IPC + Zstd shuffle codec (`ARW1` header + lossless round-trip)
    let sample = RowBatch::new(
        vec!["id".into(), "tier".into(), "amount".into()],
        vec![
            BTreeMap::from([
                ("id".into(), ScalarValue::Int64(1)),
                ("tier".into(), ScalarValue::Utf8("enterprise".into())),
                ("amount".into(), ScalarValue::Float64(420.5)),
            ]),
            BTreeMap::from([
                ("id".into(), ScalarValue::Int64(2)),
                ("tier".into(), ScalarValue::Utf8("pro".into())),
                ("amount".into(), ScalarValue::Float64(99.0)),
            ]),
        ],
    );
    let encoded = encode_shuffle_block(&sample).unwrap();
    assert_eq!(
        &encoded[..4],
        b"ARW1",
        "expected binary Arrow IPC + Zstd ARW1 shuffle header"
    );
    let decoded = decode_shuffle_block(&encoded).unwrap();
    assert_eq!(decoded.num_rows(), 2);
    assert_eq!(decoded.rows[0].get("amount"), Some(&ScalarValue::Float64(420.5)));

    // 2. End-to-end networked 3-worker cluster testing Broadcast Vectorized Arrow Join + Fused Filter + Binary Shuffle!
    let cluster = TestCluster::start_networked_with_config(
        3,
        rustlakedb_planner::PlannerConfig {
            default_shuffle_partitions: 3,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let order_schema = vec![
        FieldSchema { field_id: 1, name: "order_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "customer_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 3, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 4, name: "amount".into(), data_type: ColumnType::Float64, nullable: false },
    ];
    let cust_schema = vec![
        FieldSchema { field_id: 1, name: "customer_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "tier".into(), data_type: ColumnType::Utf8, nullable: false },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("v_orders", order_schema.clone()))
        .await
        .unwrap();
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("v_customers", cust_schema.clone()))
        .await
        .unwrap();

    // Seed 20,000 orders across 8 splits and 500 customers across 2 splits
    let regions = ["us-east", "eu-central", "ap-south", "us-west"];
    let mut order_entries = Vec::new();
    for s in 0..8usize {
        let rows: Vec<BTreeMap<String, ScalarValue>> = (0..2_500usize)
            .map(|i| {
                let id = (s * 2_500 + i + 1) as i64;
                BTreeMap::from([
                    ("order_id".into(), ScalarValue::Int64(id)),
                    ("customer_id".into(), ScalarValue::Int64((id % 500) + 1)),
                    ("region".into(), ScalarValue::Utf8(regions[((id / 5) as usize) % 4].into())),
                    ("amount".into(), ScalarValue::Float64(((id % 400) + 100) as f64)),
                ])
            })
            .collect();
        let entry = cluster
            .store
            .write_data_file(
                &format!("v_orders/data/part-{s:02}.parquet"),
                &order_schema,
                &RowBatch::new(
                    vec!["order_id".into(), "customer_id".into(), "region".into(), "amount".into()],
                    rows,
                ),
            )
            .await
            .unwrap();
        order_entries.push(entry);
    }
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "v_orders".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: order_entries,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    let tiers = ["enterprise", "pro", "starter", "growth"];
    let mut cust_entries = Vec::new();
    for s in 0..2usize {
        let rows: Vec<BTreeMap<String, ScalarValue>> = (0..250usize)
            .map(|i| {
                let cid = (s * 250 + i + 1) as i64;
                BTreeMap::from([
                    ("customer_id".into(), ScalarValue::Int64(cid)),
                    ("tier".into(), ScalarValue::Utf8(tiers[(cid as usize) % 4].into())),
                ])
            })
            .collect();
        let entry = cluster
            .store
            .write_data_file(
                &format!("v_customers/data/part-{s:02}.parquet"),
                &cust_schema,
                &RowBatch::new(vec!["customer_id".into(), "tier".into()], rows),
            )
            .await
            .unwrap();
        cust_entries.push(entry);
    }
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "v_customers".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: cust_entries,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    // Unfiltered vectorized broadcast/hash JOIN + GROUP BY
    let r1 = cluster
        .sql("SELECT c.tier, COUNT(*) AS orders, SUM(o.amount) AS rev FROM v_orders o JOIN v_customers c ON o.customer_id = c.customer_id GROUP BY c.tier ORDER BY c.tier")
        .await
        .unwrap();
    assert_eq!(r1.batch.num_rows(), 4);
    let total1: i64 = r1
        .batch
        .rows
        .iter()
        .map(|r| match r.get("orders") {
            Some(ScalarValue::Int64(n)) => *n,
            _ => 0,
        })
        .sum();
    assert_eq!(total1, 20_000);

    // Filtered vectorized JOIN (`IN` + `>=`) + GROUP BY
    let r2 = cluster
        .sql("SELECT c.tier, COUNT(*) AS orders, SUM(o.amount) AS rev FROM v_orders o JOIN v_customers c ON o.customer_id = c.customer_id WHERE o.region IN ('us-east', 'eu-central') AND o.amount >= 250.0 GROUP BY c.tier ORDER BY c.tier")
        .await
        .unwrap();
    assert_eq!(r2.batch.num_rows(), 4);
    let total2: i64 = r2
        .batch
        .rows
        .iter()
        .map(|r| match r.get("orders") {
            Some(ScalarValue::Int64(n)) => *n,
            _ => 0,
        })
        .sum();
    assert!(total2 > 5_000 && total2 < 10_000);

    // Prove the vectorized Arrow join fast path processed the probe rows!
    let vec_rows: u64 = cluster
        .workers
        .iter()
        .map(|w| w.vectorized_rows_processed.load(Ordering::Relaxed))
        .sum();
    println!("Wave 5 vectorized Arrow Join rows processed across 3 HTTP workers: {vec_rows}");
    assert!(
        vec_rows >= 40_000,
        "expected >= 40,000 vectorized join rows across 2 queries, got {vec_rows}"
    );
}

#[tokio::test]
async fn test_wave5_iceberg_v3_avro_manifests_and_manifest_list_spec() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE avro_orders (id BIGINT, region TEXT, amount DOUBLE, payload JSONB)")
        .await
        .unwrap();
    cluster
        .sql(r#"INSERT INTO avro_orders VALUES (1, 'us', 100.0, '{"k":"v"}'), (2, 'eu', 200.0, '{"k":"v2"}')"#)
        .await
        .unwrap();
    cluster
        .sql(r#"INSERT INTO avro_orders VALUES (3, 'ap', 300.0, '{"k":"v3"}')"#)
        .await
        .unwrap();

    // 1. Verify v3.metadata.json references standard Iceberg v3 manifest-list (`snap-3.avro`)
    let meta_bytes = cluster
        .store
        .read_raw_object("avro_orders/metadata/v3.metadata.json")
        .await
        .unwrap();
    let meta_json: serde_json::Value = serde_json::from_slice(&meta_bytes).unwrap();
    assert_eq!(meta_json["format-version"], 3);
    assert_eq!(meta_json["current-snapshot-id"], 3);
    let snaps = meta_json["snapshots"].as_array().unwrap();
    let snap3 = snaps.iter().find(|s| s["snapshot-id"] == 3).unwrap();
    let manifest_list_path = snap3["manifest-list"].as_str().unwrap();
    assert_eq!(manifest_list_path, "avro_orders/metadata/snap-3.avro");

    // 2. Verify Avro Object Container File (`Obj\x01`) for Manifest List (`snap-3.avro`)
    let ml_bytes = cluster.store.read_raw_object(manifest_list_path).await.unwrap();
    let ml_ocf = parse_and_verify_avro_ocf(&ml_bytes).unwrap();
    assert_eq!(ml_ocf.metadata.get("format-version").map(String::as_str), Some("3"));
    assert_eq!(ml_ocf.block_record_count, 1);
    assert!(ml_ocf.metadata["avro.schema"].contains("manifest_file"));

    // 3. Verify Avro Object Container File (`Obj\x01`) for Manifest (`snap-3-m0.avro`)
    let m_bytes = cluster
        .store
        .read_raw_object("avro_orders/metadata/snap-3-m0.avro")
        .await
        .unwrap();
    let m_ocf = parse_and_verify_avro_ocf(&m_bytes).unwrap();
    assert_eq!(m_ocf.metadata.get("format-version").map(String::as_str), Some("3"));
    assert_eq!(m_ocf.metadata.get("content").map(String::as_str), Some("data"));
    assert_eq!(m_ocf.block_record_count, 2, "expected 2 Parquet data files in manifest");
    assert!(m_ocf.metadata["avro.schema"].contains("manifest_entry"));
}

#[tokio::test]
async fn test_wave5_multinode_raft_ha_quorum_election_and_leader_failover() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let n1 = RaftCatalogNode::new(1, HashMap::new(), Arc::new(InMemoryRaftCatalog::open(d1.path()).unwrap()));
    let n2 = RaftCatalogNode::new(2, HashMap::new(), Arc::new(InMemoryRaftCatalog::open(d2.path()).unwrap()));
    let n3 = RaftCatalogNode::new(3, HashMap::new(), Arc::new(InMemoryRaftCatalog::open(d3.path()).unwrap()));

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

    let nodes = [n1.clone(), n2.clone(), n3.clone()];

    // 1. Wait for automatic Raft Leader Election across the 3-node quorum!
    let mut elected_leader_idx = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(idx) = nodes.iter().position(|n| n.status().role == RaftRole::Leader) {
            elected_leader_idx = Some(idx);
            break;
        }
    }
    let leader_idx = elected_leader_idx.expect("3-node Raft quorum must elect a Leader");
    let follower_idx = (leader_idx + 1) % 3;
    let initial_term = nodes[leader_idx].status().term;
    println!(
        "Raft 3-node quorum elected Leader node-{} in term {initial_term}",
        nodes[leader_idx].node_id
    );

    // 2. Write a table THROUGH A FOLLOWER -> verifies transparent Follower->Leader forwarding + quorum replication!
    let schema = vec![FieldSchema {
        field_id: 1,
        name: "id".into(),
        data_type: ColumnType::Int64,
        nullable: false,
    }];
    nodes[follower_idx]
        .create_table(IcebergTableMetadata::new("raft_accounts", schema.clone()))
        .await
        .unwrap();

    // Every node in the quorum must now see `raft_accounts`!
    for n in &nodes {
        let tables = n.list_tables().await.unwrap();
        assert!(
            tables.contains(&"raft_accounts".to_string()),
            "Raft node-{} missing replicated table",
            n.node_id
        );
    }

    // 3. KILL THE LEADER -> verify surviving 2 nodes elect a NEW Leader and keep accepting ACID commits!
    nodes[leader_idx].stop();
    let mut new_leader_idx = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        if let Some(idx) = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_idx)
            .find(|(_, n)| n.status().role == RaftRole::Leader && n.status().term > initial_term)
            .map(|(i, _)| i)
        {
            new_leader_idx = Some(idx);
            break;
        }
    }
    let new_lidx = new_leader_idx.expect("surviving 2-node majority must elect a NEW Raft leader");
    println!(
        "After killing Leader node-{}, surviving quorum elected NEW Leader node-{} in term {}",
        nodes[leader_idx].node_id,
        nodes[new_lidx].node_id,
        nodes[new_lidx].status().term
    );

    // Commit a new table on the surviving quorum & verify zero data loss!
    nodes[new_lidx]
        .create_table(IcebergTableMetadata::new("raft_post_failover", schema))
        .await
        .unwrap();
    for (i, n) in nodes.iter().enumerate() {
        if i != leader_idx {
            let tables = n.list_tables().await.unwrap();
            assert!(tables.contains(&"raft_accounts".to_string()));
            assert!(tables.contains(&"raft_post_failover".to_string()));
        }
        n.stop();
    }
}

#[tokio::test]
async fn test_wave5_pgwire_scram_sha256_authentication() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    let users = HashMap::from([
        ("postgres".to_string(), "s3cr3t-lakehouse!".to_string()),
        ("analyst".to_string(), "readonly-pw".to_string()),
    ]);
    let pg_addr = cluster.start_pg_gateway_scram(users).await.unwrap();

    // 1. Wrong password MUST be rejected with PostgreSQL SQLSTATE 28P01!
    let bad = PgTestClient::connect_scram(pg_addr, "postgres", "wrong-password", "rustlakedb")
        .await
        .err()
        .expect("wrong SCRAM-SHA-256 password must fail");
    assert!(
        bad.to_string().contains("28P01"),
        "expected SQLSTATE 28P01 on bad password, got: {bad}"
    );

    // 2. Unknown user MUST be rejected with 28P01!
    let unknown = PgTestClient::connect_scram(pg_addr, "hacker", "s3cr3t-lakehouse!", "rustlakedb")
        .await
        .err()
        .expect("unknown user must fail");
    assert!(unknown.to_string().contains("28P01"));

    // 3. Valid SCRAM-SHA-256 credentials verify both ClientProof and ServerSignature and run SQL!
    let mut client =
        PgTestClient::connect_scram(pg_addr, "postgres", "s3cr3t-lakehouse!", "rustlakedb")
            .await
            .unwrap();
    let out = client
        .query("SELECT COUNT(*) AS cnt FROM orders")
        .await
        .unwrap();
    assert_eq!(out.rows[0][0].as_deref(), Some("6"));
    client.close().await.unwrap();
}
