// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 29: Raft-Replicated, Crash-Safe Enterprise Governance —
//! 1. Single-node crash & restart preserves RBAC grants, RLS filters, Dynamic Masking rules, and Data Lineage!
//! 2. Multi-Node 3-Coordinator HA Raft Quorum replicates RBAC, RLS, Masking, and Lineage automatically
//!    from Leader Coordinator #1 to Follower Coordinators #2 & #3!

use rustlakedb_catalog::raft::{RaftCatalogNode, RaftRole};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::security::SessionContext;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn make_coord_with_catalog(
    dir: &std::path::Path,
    catalog: Arc<dyn Catalog>,
    id: &str,
) -> Arc<Coordinator> {
    let store = Arc::new(IcebergObjectStore::local_fs(dir).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.join(format!("shuffle_{id}"))).unwrap());
    let w = Arc::new(WorkerNode::new(
        format!("w-{id}"),
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w],
        PlannerConfig::default(),
    ))
}

#[tokio::test]
async fn wave29_governance_rbac_rls_masking_lineage_survives_crash_restart() {
    let dir = tempdir().unwrap();

    // Boot #1: Create table, data, role, GRANT, RLS policy, masking policy, and CTAS lineage
    {
        let cat: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
        let coord = make_coord_with_catalog(dir.path(), cat, "boot1");
        coord
            .execute_sql(
                "CREATE TABLE customers (id BIGINT, region TEXT, email TEXT)",
                "main",
            )
            .await
            .unwrap();
        coord
            .execute_sql(
                "INSERT INTO customers VALUES (1, 'us-east', 'alice@acme.io'), (2, 'eu-west', 'bob@acme.io')",
                "main",
            )
            .await
            .unwrap();
        coord.execute_sql("CREATE ROLE analyst", "main").await.unwrap();
        coord
            .execute_sql("GRANT SELECT ON customers TO analyst", "main")
            .await
            .unwrap();
        coord
            .execute_sql(
                "CREATE POLICY p_us ON customers FOR analyst USING (region = 'us-east')",
                "main",
            )
            .await
            .unwrap();
        coord
            .execute_sql(
                "CREATE MASKING POLICY m_email ON customers.email FOR analyst AS '***REDACTED***'",
                "main",
            )
            .await
            .unwrap();
        coord
            .execute_sql(
                "CREATE TABLE cust_summary AS SELECT region, COUNT(*) AS cnt FROM customers GROUP BY region",
                "main",
            )
            .await
            .unwrap();
    }

    // Boot #2 (Fresh Coordinator + fresh GovernanceState from same data_dir after restart!)
    {
        let cat2: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
        let coord2 = make_coord_with_catalog(dir.path(), cat2, "boot2");

        // Analyst STILL has SELECT grant, RLS (sees only us-east!), AND dynamic masking on email!
        let analyst = SessionContext::for_user("analyst");
        let res = coord2
            .execute_sql_with_session("SELECT id, region, email FROM customers", "main", &analyst)
            .await
            .unwrap();
        assert_eq!(res.batch.num_rows(), 1, "RLS policy must survive restart");
        assert_eq!(
            res.batch.rows[0].get("region"),
            Some(&ScalarValue::Utf8("us-east".into()))
        );
        assert_eq!(
            res.batch.rows[0].get("email"),
            Some(&ScalarValue::Utf8("***REDACTED***".into())),
            "Dynamic masking policy must survive restart"
        );

        // Unauthorized role MUST STILL be denied!
        let rogue = SessionContext::for_user("rogue");
        let err = coord2
            .execute_sql_with_session("SELECT * FROM customers", "main", &rogue)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("42501"));

        // Data lineage MUST STILL survive restart!
        let lin = coord2
            .execute_sql("SELECT * FROM system.lineage", "main")
            .await
            .unwrap();
        assert_eq!(lin.batch.num_rows(), 1);
        assert_eq!(
            lin.batch.rows[0].get("target_table"),
            Some(&ScalarValue::Utf8("cust_summary".into()))
        );
    }
}

#[tokio::test]
async fn wave29_raft_quorum_replicates_governance_across_ha_coordinators() {
    let wh_dir = tempdir().unwrap();
    let d1 = tempdir().unwrap();
    let d2 = tempdir().unwrap();
    let d3 = tempdir().unwrap();

    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a1 = l1.local_addr().unwrap();
    drop(l1);
    let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a2 = l2.local_addr().unwrap();
    drop(l2);
    let l3 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a3 = l3.local_addr().unwrap();
    drop(l3);

    let (u1, u2, u3) = (format!("http://{a1}"), format!("http://{a2}"), format!("http://{a3}"));
    let peers_for = |me: u64| -> HashMap<u64, String> {
        let mut p = HashMap::new();
        if me != 1 { p.insert(1, u1.clone()); }
        if me != 2 { p.insert(2, u2.clone()); }
        if me != 3 { p.insert(3, u3.clone()); }
        p
    };

    let n1 = RaftCatalogNode::open_persistent(1, peers_for(1), d1.path()).unwrap();
    let n2 = RaftCatalogNode::open_persistent(2, peers_for(2), d2.path()).unwrap();
    let n3 = RaftCatalogNode::open_persistent(3, peers_for(3), d3.path()).unwrap();
    n1.start_server(&a1.to_string()).await.unwrap();
    n2.start_server(&a2.to_string()).await.unwrap();
    n3.start_server(&a3.to_string()).await.unwrap();

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
    let li = leader_idx.expect("Raft leader elected");
    let fi = (li + 1) % 3;

    // Coordinator A uses Leader Raft node; Coordinator B uses Follower Raft node!
    let coord_leader = make_coord_with_catalog(wh_dir.path(), nodes[li].clone(), "leader");
    let coord_follower = make_coord_with_catalog(wh_dir.path(), nodes[fi].clone(), "follower");

    // Apply DDL + Governance policies on Leader Coordinator A
    coord_leader
        .execute_sql("CREATE TABLE secrets (id BIGINT, dept TEXT, ssn TEXT)", "main")
        .await
        .unwrap();
    coord_leader
        .execute_sql(
            "INSERT INTO secrets VALUES (1, 'eng', '111-11-1111'), (2, 'hr', '222-22-2222')",
            "main",
        )
        .await
        .unwrap();
    coord_leader
        .execute_sql("GRANT SELECT ON secrets TO eng_user", "main")
        .await
        .unwrap();
    coord_leader
        .execute_sql(
            "CREATE POLICY p_eng ON secrets FOR eng_user USING (dept = 'eng')",
            "main",
        )
        .await
        .unwrap();
    coord_leader
        .execute_sql(
            "CREATE MASKING POLICY m_ssn ON secrets.ssn FOR eng_user AS 'XXX-XX-XXXX'",
            "main",
        )
        .await
        .unwrap();

    // Give Raft follower 150ms to apply replicated log entries
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Query on COMPLETELY INDEPENDENT Follower Coordinator B as `eng_user`!
    let eng_sess = SessionContext::for_user("eng_user");
    let res = coord_follower
        .execute_sql_with_session("SELECT id, dept, ssn FROM secrets", "main", &eng_sess)
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1, "Follower coordinator must enforce replicated RLS!");
    assert_eq!(
        res.batch.rows[0].get("dept"),
        Some(&ScalarValue::Utf8("eng".into()))
    );
    assert_eq!(
        res.batch.rows[0].get("ssn"),
        Some(&ScalarValue::Utf8("XXX-XX-XXXX".into())),
        "Follower coordinator must enforce replicated dynamic masking!"
    );

    for n in &nodes {
        n.stop();
    }
}
