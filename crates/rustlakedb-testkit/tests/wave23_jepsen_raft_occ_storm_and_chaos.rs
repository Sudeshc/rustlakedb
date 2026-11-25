// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::raft::{RaftCatalogNode, RaftRole};
use rustlakedb_catalog::{
    Catalog, ColumnType, FieldSchema, IcebergTableMetadata, InMemoryRaftCatalog,
    TableCommitProposal,
};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{RowBatch, ScalarValue, ShuffleMode};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn wave23_jepsen_50_writer_concurrent_occ_commit_storm() {
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

    let schema = vec![
        FieldSchema {
            field_id: 1,
            name: "writer_id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        },
        FieldSchema {
            field_id: 2,
            name: "amount".into(),
            data_type: ColumnType::Float64,
            nullable: false,
        },
    ];
    catalog
        .create_table(IcebergTableMetadata::new("storm_ledger", schema.clone()))
        .await
        .unwrap();

    // 1. 50-writer simultaneous single-shot OCC race against expected_snapshot_id = 1:
    //    EXACTLY 1 writer MUST win; 49 MUST be rejected with OCC conflict (linearizability)!
    let mut handles = Vec::new();
    for wid in 0..50i64 {
        let cat = catalog.clone();
        let st = store.clone();
        let sch = schema.clone();
        handles.push(tokio::spawn(async move {
            let batch = RowBatch::new(
                vec!["writer_id".into(), "amount".into()],
                vec![BTreeMap::from([
                    ("writer_id".into(), ScalarValue::Int64(wid)),
                    ("amount".into(), ScalarValue::Float64(10.0)),
                ])],
            );
            let df = st
                .write_data_file(&format!("storm_ledger/data/race_{wid}.parquet"), &sch, &batch)
                .await
                .unwrap();
            cat.commit_table(TableCommitProposal {
                table_name: "storm_ledger".into(),
                branch: "main".into(),
                expected_snapshot_id: 1,
                added_files: vec![df],
                removed_files: vec![],
                // Mutating/non-append commit enforces strict serializable OCC snapshot match!
                is_append_only: false,
            })
            .await
        }));
    }
    let mut wins = 0usize;
    let mut conflicts = 0usize;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => wins += 1,
            Err(_) => conflicts += 1,
        }
    }
    assert_eq!(wins, 1, "EXACTLY 1 writer must win a simultaneous OCC race");
    assert_eq!(conflicts, 49, "49 concurrent stale writers must fail OCC");

    // 2. 25 concurrent writers with OCC retry loop -> ALL 25 commit with ZERO lost updates!
    let mut retry_handles = Vec::new();
    for wid in 100..125i64 {
        let cat = catalog.clone();
        let st = store.clone();
        let sch = schema.clone();
        retry_handles.push(tokio::spawn(async move {
            let batch = RowBatch::new(
                vec!["writer_id".into(), "amount".into()],
                vec![BTreeMap::from([
                    ("writer_id".into(), ScalarValue::Int64(wid)),
                    ("amount".into(), ScalarValue::Float64(20.0)),
                ])],
            );
            let df = st
                .write_data_file(&format!("storm_ledger/data/retry_{wid}.parquet"), &sch, &batch)
                .await
                .unwrap();
            loop {
                let cur_snap = cat
                    .get_table("storm_ledger")
                    .await
                    .unwrap()
                    .refs
                    .get("main")
                    .copied()
                    .unwrap();
                match cat
                    .commit_table(TableCommitProposal {
                        table_name: "storm_ledger".into(),
                        branch: "main".into(),
                        expected_snapshot_id: cur_snap,
                        added_files: vec![df.clone()],
                        removed_files: vec![],
                        is_append_only: true,
                    })
                    .await
                {
                    Ok(s) => return s,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        }));
    }
    for h in retry_handles {
        let _ = h.await.unwrap();
    }

    let res = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM storm_ledger", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(26)));
    assert_eq!(
        res.batch.rows[0].get("total"),
        Some(&ScalarValue::Float64(10.0 + 25.0 * 20.0))
    );
}

#[tokio::test]
async fn wave23_jepsen_raft_split_brain_leader_fencing_and_failover() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let dir3 = TempDir::new().unwrap();

    // Reserve 3 loopback ports cleanly
    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l3 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a1 = l1.local_addr().unwrap();
    let a2 = l2.local_addr().unwrap();
    let a3 = l3.local_addr().unwrap();
    drop((l1, l2, l3));

    let all_peers = HashMap::from([
        (1u64, format!("http://{a1}")),
        (2u64, format!("http://{a2}")),
        (3u64, format!("http://{a3}")),
    ]);
    let p1: HashMap<u64, String> = all_peers.iter().filter(|(k, _)| **k != 1).map(|(k, v)| (*k, v.clone())).collect();
    let p2: HashMap<u64, String> = all_peers.iter().filter(|(k, _)| **k != 2).map(|(k, v)| (*k, v.clone())).collect();
    let p3: HashMap<u64, String> = all_peers.iter().filter(|(k, _)| **k != 3).map(|(k, v)| (*k, v.clone())).collect();

    let n1 = RaftCatalogNode::open_persistent(1, p1, dir1.path()).unwrap();
    let n2 = RaftCatalogNode::open_persistent(2, p2, dir2.path()).unwrap();
    let n3 = RaftCatalogNode::open_persistent(3, p3, dir3.path()).unwrap();

    let _ = n1.start_server(&a1.to_string()).await.unwrap();
    let _ = n2.start_server(&a2.to_string()).await.unwrap();
    let _ = n3.start_server(&a3.to_string()).await.unwrap();

    // Wait for automatic Raft Leader Election across the 3-node quorum!
    let nodes = [n1.clone(), n2.clone(), n3.clone()];
    let mut leader_idx = None;
    for _ in 0..40 {
        if let Some(pos) = nodes.iter().position(|n| n.status().role == RaftRole::Leader) {
            leader_idx = Some(pos);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let l_idx = leader_idx.expect("3-node Raft quorum must elect an initial leader");
    let leader = nodes[l_idx].clone();
    let term1 = leader.status().term;
    leader
        .create_table(IcebergTableMetadata::new(
            "ha_bank",
            vec![FieldSchema {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            }],
        ))
        .await
        .unwrap();

    // Partition / kill the active Leader!
    leader.stop();
    // Stale partitioned leader MUST reject writes (fenced!)
    let stale_err = leader
        .add_column("ha_bank", "illegal_split_brain".into(), ColumnType::Utf8)
        .await;
    assert!(
        stale_err.is_err(),
        "stopped/partitioned old leader must be fenced from committing writes"
    );

    // Surviving 2-node majority automatically elects a new Leader at strictly higher term!
    let survivors: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != l_idx)
        .map(|(_, n)| n.clone())
        .collect();
    let mut new_leader = None;
    let mut follower = None;
    for _ in 0..60 {
        if let Some(pos) = survivors
            .iter()
            .position(|n| n.status().role == RaftRole::Leader && n.status().term > term1)
        {
            new_leader = Some(survivors[pos].clone());
            follower = Some(survivors[1 - pos].clone());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let new_leader = new_leader.expect("surviving majority must elect new Raft leader");
    let follower = follower.unwrap();
    assert!(
        new_leader.status().term > term1,
        "new leader term must strictly exceed old leader term"
    );
    // Commit new schema evolution through new leader & verify follower replicates it!
    new_leader
        .add_column("ha_bank", "balance".into(), ColumnType::Float64)
        .await
        .unwrap();
    let tbl_on_follower = follower.get_table("ha_bank").await.unwrap();
    assert!(
        tbl_on_follower.schema.iter().any(|f| f.name == "balance"),
        "surviving follower must replicate commits from new Raft leader"
    );
    assert!(
        !tbl_on_follower
            .schema
            .iter()
            .any(|f| f.name == "illegal_split_brain"),
        "fenced split-brain write must never appear on quorum"
    );
}

#[tokio::test]
async fn wave23_mid_shuffle_worker_crash_soak_and_observability_pack() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let w2 = Arc::new(WorkerNode::new("w2", catalog.clone(), shuffle.clone(), store.clone()));
    let w3 = Arc::new(WorkerNode::new("w3", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1.clone(), w2.clone(), w3.clone()],
        PlannerConfig {
            default_shuffle_partitions: 3,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    );

    coord
        .execute_sql("CREATE TABLE soak_dim (k BIGINT, grp TEXT)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("CREATE TABLE soak_fact (k BIGINT, amt DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO soak_dim VALUES (1,'A'),(2,'B'),(3,'C')", "main")
        .await
        .unwrap();
    for s in 0..3 {
        coord
            .execute_sql(
                &format!(
                    "INSERT INTO soak_fact VALUES (1, {}), (2, {}), (3, {})",
                    10.0 * (s + 1) as f64,
                    20.0 * (s + 1) as f64,
                    30.0 * (s + 1) as f64
                ),
                "main",
            )
            .await
            .unwrap();
    }

    // 10-iteration chaos soak injecting worker faults mid-execution on alternating workers!
    let workers = [w1, w2, w3];
    for iter in 0..10 {
        if iter % 2 == 0 {
            workers[iter % 3].inject_single_failure();
        }
        let res = coord
            .execute_sql(
                "SELECT soak_dim.grp, SUM(soak_fact.amt) AS total \
                 FROM soak_fact INNER JOIN soak_dim ON soak_fact.k = soak_dim.k \
                 GROUP BY soak_dim.grp ORDER BY soak_dim.grp",
                "main",
            )
            .await
            .unwrap();
        assert_eq!(res.batch.num_rows(), 3);
        assert_eq!(res.batch.rows[0].get("total"), Some(&ScalarValue::Float64(60.0)));
        assert_eq!(res.batch.rows[1].get("total"), Some(&ScalarValue::Float64(120.0)));
        assert_eq!(res.batch.rows[2].get("total"), Some(&ScalarValue::Float64(180.0)));
    }

    // Validate Track 3 Observability Pack, dbt Quickstart, and Production Deployment Playbook!
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let grafana: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("deploy/observability/grafana-dashboard.json")).unwrap(),
    )
    .unwrap();
    assert!(grafana["panels"].as_array().unwrap().len() >= 6);
    assert!(root.join("deploy/observability/prometheus-alerts.yaml").exists());
    assert!(root.join("examples/dbt_quickstart/dbt_project.yml").exists());
    assert!(root.join("examples/dbt_quickstart/profiles.yml").exists());
    assert!(root.join("docs/PRODUCTION_DEPLOYMENT.md").exists());
}
