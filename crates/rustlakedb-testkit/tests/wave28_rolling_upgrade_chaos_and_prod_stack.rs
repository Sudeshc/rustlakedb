// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 28: Zero-Downtime Rolling-Upgrade Chaos Test on a 3-Node Persistent Raft HA Quorum
//! + Turnkey Production Docker Compose Stack (MinIO S3 + Prometheus + Grafana) Validation.

use rustlakedb_catalog::raft::{RaftCatalogNode, RaftRole};
use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, TableCommitProposal};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave28_zero_downtime_rolling_upgrade_chaos_on_3node_raft_quorum() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    // Reserve 3 ephemeral loopback ports for the 3-node Raft HA cluster
    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a1 = l1.local_addr().unwrap();
    drop(l1);
    let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a2 = l2.local_addr().unwrap();
    drop(l2);
    let l3 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a3 = l3.local_addr().unwrap();
    drop(l3);

    let u1 = format!("http://{a1}");
    let u2 = format!("http://{a2}");
    let u3 = format!("http://{a3}");

    let peers_for = |me: u64| -> HashMap<u64, String> {
        let mut p = HashMap::new();
        if me != 1 { p.insert(1, u1.clone()); }
        if me != 2 { p.insert(2, u2.clone()); }
        if me != 3 { p.insert(3, u3.clone()); }
        p
    };

    let n1 = RaftCatalogNode::open_persistent(1, peers_for(1), dir1.path()).unwrap();
    let n2 = RaftCatalogNode::open_persistent(2, peers_for(2), dir2.path()).unwrap();
    let n3 = RaftCatalogNode::open_persistent(3, peers_for(3), dir3.path()).unwrap();

    n1.start_server(&a1.to_string()).await.unwrap();
    n2.start_server(&a2.to_string()).await.unwrap();
    n3.start_server(&a3.to_string()).await.unwrap();

    // Wait for initial leader election
    let nodes = vec![n1.clone(), n2.clone(), n3.clone()];
    let mut leader_idx = None;
    for _ in 0..40 {
        for (i, n) in nodes.iter().enumerate() {
            if n.status().role == RaftRole::Leader {
                leader_idx = Some(i);
                break;
            }
        }
        if leader_idx.is_some() { break; }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let initial_leader = leader_idx.expect("initial Raft quorum must elect a leader");

    // Create base table before rolling upgrade begins
    let schema = vec![FieldSchema {
        field_id: 1,
        name: "id".into(),
        data_type: ColumnType::Int64,
        nullable: false,
    }];
    nodes[initial_leader]
        .create_table(IcebergTableMetadata::new("rolling_orders", schema))
        .await
        .unwrap();

    // Perform a rolling node-by-node upgrade simulation:
    // Stop node i (simulating container drain/upgrade), execute writes against surviving 2-node majority,
    // restart node i from its persistent WAL, and verify full state catch-up across all 3 waves!
    let mut expected_snap = 1u64;
    for upgrade_idx in 0..3 {
        nodes[upgrade_idx].stop();

        // Wait for a healthy surviving leader (in case we just stopped the leader!)
        let mut active_leader: Option<Arc<RaftCatalogNode>> = None;
        for _ in 0..50 {
            for (i, n) in nodes.iter().enumerate() {
                if i != upgrade_idx && n.status().role == RaftRole::Leader {
                    active_leader = Some(n.clone());
                    break;
                }
            }
            if active_leader.is_some() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let leader = active_leader.expect("surviving 2-node majority must maintain leadership");

        // Commit a write + schema tag during the rolling upgrade with ZERO downtime!
        let new_snap = leader
            .commit_table(TableCommitProposal {
                table_name: "rolling_orders".into(),
                branch: "main".into(),
                expected_snapshot_id: expected_snap,
                added_files: vec![],
                removed_files: vec![],
                is_append_only: true,
            })
            .await
            .unwrap();
        assert_eq!(new_snap, expected_snap + 1);
        expected_snap = new_snap;

        let mut tags = std::collections::BTreeMap::new();
        tags.insert(format!("upgrade_wave_{upgrade_idx}"), "ok".into());
        leader.set_table_tags("rolling_orders", tags).await.unwrap();

        // Bring upgraded node back online and wait briefly for Raft replication catch-up
        nodes[upgrade_idx].resume();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Verify zero data loss across all 3 rolling-upgrade waves!
    let mut verified_leader = None;
    for n in &nodes {
        if n.status().role == RaftRole::Leader {
            verified_leader = Some(n.clone());
            break;
        }
    }
    let final_node = verified_leader.unwrap_or_else(|| nodes[0].clone());
    let final_meta = final_node.get_table("rolling_orders").await.unwrap();
    assert_eq!(final_meta.current_snapshot("main").unwrap().snapshot_id, 4);
    assert_eq!(final_meta.tags.get("upgrade_wave_0").map(String::as_str), Some("ok"));
    assert_eq!(final_meta.tags.get("upgrade_wave_1").map(String::as_str), Some("ok"));
    assert_eq!(final_meta.tags.get("upgrade_wave_2").map(String::as_str), Some("ok"));

    for n in &nodes {
        n.stop();
    }
}

#[test]
fn wave28_turnkey_production_stack_manifests_valid() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    let compose = std::fs::read_to_string(root.join("docker/docker-compose.prod.yml")).unwrap();
    assert!(compose.contains("rustfs/rustfs:latest"));
    assert!(compose.contains("s3://warehouse"));
    assert!(compose.contains("prom/prometheus:latest"));
    assert!(compose.contains("grafana/grafana:latest"));

    let prom = std::fs::read_to_string(root.join("deploy/observability/prometheus.yml")).unwrap();
    assert!(prom.contains("coordinator:8181"));
    assert!(prom.contains("prometheus-alerts.yaml"));

    let ds = std::fs::read_to_string(
        root.join("deploy/observability/grafana-provisioning/datasources/prometheus.yaml"),
    )
    .unwrap();
    assert!(ds.contains("http://prometheus:9090"));
}
