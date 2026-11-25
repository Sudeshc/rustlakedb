// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 26: `COPY TO` / `COPY FROM` (Parquet, CSV, NDJSON + Lineage),
//! Real `ANALYZE TABLE` Column Statistics persisted for CBO,
//! and Strict Memory-Bounded NVMe External Spill (Sort + Grace Hash Join).

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave26_copy_to_from_parquet_csv_json_and_analyze_cbo_stats() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new(
        "w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let coord = Arc::new(Coordinator::new(
        catalog.clone(),
        store,
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql(
            "CREATE TABLE shipments (id BIGINT, region TEXT, weight DOUBLE)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO shipments VALUES (1, 'us-east', 10.5), (2, 'eu-west', 20.0), (3, 'us-east', NULL), (4, 'ap-south', 50.0)",
            "main",
        )
        .await
        .unwrap();

    // 1. ANALYZE TABLE persists real NDV, null_count, min/max stats and surfaces in EXPLAIN!
    let stats_res = coord.execute_sql("ANALYZE TABLE shipments", "main").await.unwrap();
    assert_eq!(stats_res.batch.num_rows(), 3);
    let meta = catalog.get_table("shipments").await.unwrap();
    assert_eq!(meta.column_stats["region"].ndv, 3);
    assert_eq!(meta.column_stats["weight"].null_count, 1);
    assert_eq!(meta.column_stats["weight"].min_value.as_deref(), Some("10.5"));
    assert_eq!(meta.column_stats["weight"].max_value.as_deref(), Some("50"));

    let explain = coord
        .execute_sql("EXPLAIN SELECT region, COUNT(*) FROM shipments GROUP BY region", "main")
        .await
        .unwrap();
    let plan_text: String = explain
        .batch
        .rows
        .iter()
        .filter_map(|r| r.get("QUERY PLAN").map(|v| v.to_display_string()))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("analyzed=3"),
        "expected EXPLAIN CBO notes to include analyzed=3, got:\n{plan_text}"
    );

    // 2. COPY TO + COPY FROM across Parquet, CSV, and JSON!
    let pq_path = dir.path().join("export.parquet");
    let csv_path = dir.path().join("export.csv");
    let json_path = dir.path().join("export.ndjson");

    let pq_str = pq_path.to_string_lossy().replace('\\', "/");
    let csv_str = csv_path.to_string_lossy().replace('\\', "/");
    let json_str = json_path.to_string_lossy().replace('\\', "/");

    coord
        .execute_sql(
            &format!("COPY (SELECT id, region, weight FROM shipments WHERE weight IS NOT NULL) TO '{pq_str}' WITH (FORMAT PARQUET)"),
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            &format!("COPY (SELECT id, region, weight FROM shipments WHERE weight IS NOT NULL) TO '{csv_str}' WITH (FORMAT CSV)"),
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            &format!("COPY (SELECT id, region, weight FROM shipments WHERE weight IS NOT NULL) TO '{json_str}' WITH (FORMAT JSON)"),
            "main",
        )
        .await
        .unwrap();

    assert!(pq_path.exists() && std::fs::metadata(&pq_path).unwrap().len() > 0);
    assert!(csv_path.exists() && std::fs::metadata(&csv_path).unwrap().len() > 0);
    assert!(json_path.exists() && std::fs::metadata(&json_path).unwrap().len() > 0);

    // Round-trip COPY FROM Parquet, CSV, and JSON into target tables!
    for (tbl, path, fmt) in [
        ("ship_pq", &pq_str, "PARQUET"),
        ("ship_csv", &csv_str, "CSV"),
        ("ship_json", &json_str, "JSON"),
    ] {
        coord
            .execute_sql(
                &format!("CREATE TABLE {tbl} (id BIGINT, region TEXT, weight DOUBLE)"),
                "main",
            )
            .await
            .unwrap();
        coord
            .execute_sql(
                &format!("COPY {tbl} FROM '{path}' WITH (FORMAT {fmt})"),
                "main",
            )
            .await
            .unwrap();
        let cnt = coord
            .execute_sql(&format!("SELECT COUNT(*) AS c, SUM(weight) AS s FROM {tbl}"), "main")
            .await
            .unwrap();
        assert_eq!(cnt.batch.rows[0].get("c"), Some(&ScalarValue::Int64(3)));
        assert_eq!(cnt.batch.rows[0].get("s"), Some(&ScalarValue::Float64(80.5)));
    }
}

#[tokio::test]
async fn wave26_strict_memory_bounded_nvme_external_sort_and_grace_hash_join() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());

    // Ultra-tight 8 KB RAM ceiling per worker to force real NVMe external spilling!
    let w1 = Arc::new(WorkerNode::with_memory_limit(
        "spill-w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
        8 * 1024,
    ));
    let w2 = Arc::new(WorkerNode::with_memory_limit(
        "spill-w2",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
        8 * 1024,
    ));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1.clone(), w2.clone()],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE big_left (k BIGINT, payload TEXT, val DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("CREATE TABLE big_right (k BIGINT, label TEXT)", "main")
        .await
        .unwrap();

    // Insert 400 rows (~60 KB > 8 KB memory limit) across both tables
    let mut vals_l = Vec::new();
    let mut vals_r = Vec::new();
    for i in 1..=200i64 {
        vals_l.push(format!("({i}, 'payload_padding_string_for_nvme_spill_{i:04}', {}.0)", i * 2));
        vals_r.push(format!("({i}, 'label_{i:04}')"));
    }
    coord
        .execute_sql(&format!("INSERT INTO big_left VALUES {}", vals_l.join(", ")), "main")
        .await
        .unwrap();
    coord
        .execute_sql(&format!("INSERT INTO big_right VALUES {}", vals_r.join(", ")), "main")
        .await
        .unwrap();

    // 1. Memory-bounded External Merge Sort under 8 KB cap
    let sorted = coord
        .execute_sql("SELECT k, val FROM big_left ORDER BY k DESC LIMIT 5", "main")
        .await
        .unwrap();
    assert_eq!(sorted.batch.num_rows(), 5);
    assert_eq!(sorted.batch.rows[0].get("k"), Some(&ScalarValue::Int64(200)));
    assert_eq!(sorted.batch.rows[4].get("k"), Some(&ScalarValue::Int64(196)));

    // 2. Memory-bounded NVMe Grace Hash Join under 8 KB cap
    let joined = coord
        .execute_sql(
            "SELECT COUNT(*) AS cnt, SUM(l.val) AS total FROM big_left l JOIN big_right r ON l.k = r.k",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(joined.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(200)));
    // Sum of 2*1..2*200 = 2 * (200*201/2) = 40200
    assert_eq!(
        joined.batch.rows[0].get("total"),
        Some(&ScalarValue::Float64(40200.0))
    );
}
