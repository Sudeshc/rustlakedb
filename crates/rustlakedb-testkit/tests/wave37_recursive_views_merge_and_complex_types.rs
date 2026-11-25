// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave37_with_recursive_org_hierarchy_and_series() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(catalog, store, shuffle, vec![w1], PlannerConfig::default());

    coord
        .execute_sql(
            "CREATE TABLE employees (emp_id BIGINT, name VARCHAR, mgr_id BIGINT)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO employees VALUES \
             (1, 'CEO', 0), \
             (2, 'VP_Eng', 1), \
             (3, 'VP_Sales', 1), \
             (4, 'Staff_Eng', 2), \
             (5, 'Senior_Eng', 4)",
            "main",
        )
        .await
        .unwrap();

    // 1. Recursive org chart traversal from CEO (emp_id=1) down 4 levels!
    let org = coord
        .execute_sql(
            "WITH RECURSIVE org_tree (emp_id, name, depth) AS (\
                SELECT emp_id, name, 0 AS depth FROM employees WHERE emp_id = 1 \
                UNION ALL \
                SELECT e.emp_id, e.name, t.depth + 1 AS depth \
                FROM employees e JOIN org_tree t ON e.mgr_id = t.emp_id\
             ) SELECT emp_id, name, depth FROM org_tree ORDER BY depth, emp_id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(org.batch.rows.len(), 5);
    assert_eq!(org.batch.rows[0].get("name").and_then(|v| v.to_pg_text()).as_deref(), Some("CEO"));
    assert_eq!(org.batch.rows[0].get("depth").and_then(|v| v.as_f64()), Some(0.0));
    assert_eq!(org.batch.rows[4].get("name").and_then(|v| v.to_pg_text()).as_deref(), Some("Senior_Eng"));
    assert_eq!(org.batch.rows[4].get("depth").and_then(|v| v.as_f64()), Some(3.0));

    // 2. Recursive numeric sequence generation (`SELECT 1 UNION ALL SELECT n + 1 ... WHERE n < 5`)
    let seq = coord
        .execute_sql(
            "WITH RECURSIVE nums (n) AS (\
                SELECT 1 AS n \
                UNION ALL \
                SELECT n + 1 AS n FROM nums WHERE n < 5\
             ) SELECT n FROM nums ORDER BY n",
            "main",
        )
        .await
        .unwrap();
    let vals: Vec<i64> = seq
        .batch
        .rows
        .iter()
        .map(|r| r.get("n").and_then(|v| v.as_f64()).unwrap() as i64)
        .collect();
    assert_eq!(vals, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn wave37_persistent_views_conditional_merge_and_complex_types() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog.clone(),
        store.clone(),
        shuffle.clone(),
        vec![w1],
        PlannerConfig::default(),
    );

    // 1. Complex Nested Types: ARRAY<TEXT> / TEXT[], MAP, STRUCT DDL + Functions!
    coord
        .execute_sql(
            "CREATE TABLE catalog_items (id BIGINT, tags TEXT[], attrs MAP, meta STRUCT<brand VARCHAR>)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO catalog_items VALUES \
             (1, ARRAY['rust', 'iceberg', 'sql'], MAP('tier', 'gold', 'region', 'us'), STRUCT('brand', 'RustLake')), \
             (2, ARRAY['python', 'arrow'], MAP('tier', 'silver'), STRUCT('brand', 'Apache'))",
            "main",
        )
        .await
        .unwrap();

    let cx = coord
        .execute_sql(
            "SELECT id, ARRAY_LENGTH(tags) AS tag_cnt, ARRAY_CONTAINS(tags, 'iceberg') AS has_ice, \
             ELEMENT_AT(tags, 2) AS second_tag, ELEMENT_AT(attrs, 'tier') AS tier_val \
             FROM catalog_items ORDER BY id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(cx.batch.rows.len(), 2);
    assert_eq!(cx.batch.rows[0].get("tag_cnt").and_then(|v| v.as_f64()), Some(3.0));
    assert_eq!(cx.batch.rows[0].get("has_ice").and_then(|v| v.to_pg_text()).as_deref(), Some("t"));
    assert_eq!(cx.batch.rows[0].get("second_tag").and_then(|v| v.to_pg_text()).as_deref(), Some("iceberg"));
    assert_eq!(cx.batch.rows[0].get("tier_val").and_then(|v| v.to_pg_text()).as_deref(), Some("gold"));
    assert_eq!(cx.batch.rows[1].get("tag_cnt").and_then(|v| v.as_f64()), Some(2.0));
    assert_eq!(cx.batch.rows[1].get("has_ice").and_then(|v| v.to_pg_text()).as_deref(), Some("f"));

    // 2. Full Multi-Clause Conditional `MERGE INTO` (simultaneous DELETE + UPDATE + INSERT via USING subquery!)
    coord
        .execute_sql("CREATE TABLE inv_target (id BIGINT, qty BIGINT, status VARCHAR)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO inv_target VALUES (1, 10, 'ACTIVE'), (2, 20, 'ACTIVE'), (3, 30, 'ACTIVE')",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql("CREATE TABLE inv_delta (id BIGINT, qty BIGINT, status VARCHAR, op VARCHAR)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO inv_delta VALUES \
             (1, 99, 'UPDATED', 'U'), \
             (2, 0, 'REMOVED', 'D'), \
             (4, 40, 'NEW', 'I'), \
             (5, 0, 'SKIP', 'D')",
            "main",
        )
        .await
        .unwrap();

    coord
        .execute_sql(
            "MERGE INTO inv_target AS t \
             USING (SELECT id, qty, status, op FROM inv_delta) AS s \
             ON t.id = s.id \
             WHEN MATCHED AND s.op = 'D' THEN DELETE \
             WHEN MATCHED AND s.op = 'U' THEN UPDATE SET qty = s.qty, status = s.status \
             WHEN NOT MATCHED AND s.op = 'I' THEN INSERT VALUES (s.id, s.qty, s.status)",
            "main",
        )
        .await
        .unwrap();

    let merged = coord
        .execute_sql("SELECT id, qty, status FROM inv_target ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(merged.batch.rows.len(), 3); // id=1 updated, id=2 deleted, id=3 untouched, id=4 inserted, id=5 skipped!
    assert_eq!(merged.batch.rows[0].get("qty").and_then(|v| v.as_f64()), Some(99.0));
    assert_eq!(merged.batch.rows[1].get("id").and_then(|v| v.as_f64()), Some(3.0));
    assert_eq!(merged.batch.rows[2].get("id").and_then(|v| v.as_f64()), Some(4.0));

    // 3. Persistent View across Coordinator restart + SHOW VIEWS + DROP VIEW
    coord
        .execute_sql(
            "CREATE OR REPLACE VIEW v_active_inv AS SELECT id, qty FROM inv_target WHERE qty >= 30",
            "main",
        )
        .await
        .unwrap();
    let views = coord.execute_sql("SHOW VIEWS", "main").await.unwrap();
    assert_eq!(views.batch.rows.len(), 1);

    // Restart fresh Coordinator against the same persisted catalog directory!
    let catalog2: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let w2 = Arc::new(WorkerNode::new("w2", catalog2.clone(), shuffle.clone(), store.clone()));
    let coord2 = Coordinator::new(catalog2, store, shuffle, vec![w2], PlannerConfig::default());
    let vres = coord2
        .execute_sql("SELECT id, qty FROM v_active_inv ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(vres.batch.rows.len(), 3); // 1(99), 3(30), 4(40)
    coord2.execute_sql("DROP VIEW v_active_inv", "main").await.unwrap();
    assert_eq!(coord2.execute_sql("SHOW VIEWS", "main").await.unwrap().batch.rows.len(), 0);
}
