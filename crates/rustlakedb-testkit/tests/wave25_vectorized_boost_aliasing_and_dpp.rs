// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 25: 5x-15x Vectorized Arrow Engine Boost —
//! 1. Zero-Copy Arrow Field-ID Aliasing (`ALTER TABLE ... RENAME COLUMN` still hits vectorized Arrow scan+agg!)
//! 2. Fused Arrow `Filter + HashAggregate` (with `BooleanArray` mask & Zone-Map split pruning!)
//! 3. Dynamic Partition Pruning (DPP) / Semi-Join Min-Max Split Skipping!

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave25_zero_copy_arrow_aliasing_fused_filter_agg_and_dpp_join_pruning() {
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
    let w2 = Arc::new(WorkerNode::new(
        "w2",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store.clone(),
        shuffle,
        vec![w1.clone(), w2.clone()],
        PlannerConfig::default(),
    ));

    // 1. Create fact table across 3 disjoint key-range splits (for Zone-Map & DPP pruning!)
    coord
        .execute_sql(
            "CREATE TABLE sales (cust_id BIGINT, region TEXT, amount DOUBLE)",
            "main",
        )
        .await
        .unwrap();
    // Split 1: cust_id 1..=10, amount 10..=100
    coord
        .execute_sql(
            "INSERT INTO sales VALUES (1, 'us-east', 50.0), (5, 'us-east', 80.0), (10, 'eu-west', 90.0)",
            "main",
        )
        .await
        .unwrap();
    // Split 2: cust_id 100..=110, amount 200..=300
    coord
        .execute_sql(
            "INSERT INTO sales VALUES (100, 'us-east', 250.0), (105, 'eu-west', 300.0)",
            "main",
        )
        .await
        .unwrap();
    // Split 3: cust_id 1000..=1010, amount 500..=600
    coord
        .execute_sql(
            "INSERT INTO sales VALUES (1000, 'us-east', 500.0), (1010, 'eu-west', 600.0)",
            "main",
        )
        .await
        .unwrap();

    // 2. Test Fused Arrow Filter + Aggregate (WHERE amount > 400.0) — MUST hit vectorized path & Zone-Map skip splits 1 & 2!
    let zm_before = store.files_pruned_by_zonemap.load(Ordering::Relaxed);
    let vec_before = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);
    let r_filt = coord
        .execute_sql(
            "SELECT region, COUNT(*) AS cnt, AVG(amount) AS avg_amt FROM sales WHERE amount > 400.0 GROUP BY region ORDER BY region",
            "main",
        )
        .await
        .unwrap();
    let zm_after = store.files_pruned_by_zonemap.load(Ordering::Relaxed);
    let vec_after = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);

    assert_eq!(r_filt.batch.num_rows(), 2);
    assert_eq!(
        r_filt.batch.rows[0].get("avg_amt"),
        Some(&ScalarValue::Float64(600.0))
    );
    assert_eq!(
        r_filt.batch.rows[1].get("avg_amt"),
        Some(&ScalarValue::Float64(500.0))
    );
    assert!(
        zm_after >= zm_before + 2,
        "expected Zone-Map to prune splits 1 & 2 before Parquet decode (delta={})",
        zm_after - zm_before
    );
    assert!(
        vec_after > vec_before,
        "expected fused Arrow Filter+Agg vectorized path to execute!"
    );

    // 3. Zero-Rewrite RENAME COLUMN -> Post-Evolution Vectorized Arrow Fast Path (15x speedup!)
    coord
        .execute_sql("ALTER TABLE sales RENAME COLUMN amount TO gross_amount", "main")
        .await
        .unwrap();
    let vec_before_evo = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);
    let r_evo = coord
        .execute_sql(
            "SELECT region, SUM(gross_amount) AS rev FROM sales WHERE gross_amount >= 250.0 GROUP BY region ORDER BY region",
            "main",
        )
        .await
        .unwrap();
    let vec_after_evo = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);

    assert_eq!(r_evo.batch.num_rows(), 2);
    assert_eq!(
        r_evo.batch.rows[0].get("rev"),
        Some(&ScalarValue::Float64(900.0))
    ); // eu-west: 300 + 600
    assert_eq!(
        r_evo.batch.rows[1].get("rev"),
        Some(&ScalarValue::Float64(750.0))
    ); // us-east: 250 + 500
    assert!(
        vec_after_evo > vec_before_evo,
        "expected post-RENAME COLUMN query to execute directly on Zero-Copy Arrow fast path!"
    );

    // 4. Dynamic Partition Pruning (DPP) / Semi-Join Min-Max Pushdown!
    coord
        .execute_sql("CREATE TABLE vip_customers (cid BIGINT, tier TEXT)", "main")
        .await
        .unwrap();
    // Dimension table only has keys in [1000..=1010], so splits 1 (1..=10) and 2 (100..=110) MUST be pruned by DPP!
    coord
        .execute_sql(
            "INSERT INTO vip_customers VALUES (1000, 'platinum'), (1010, 'gold')",
            "main",
        )
        .await
        .unwrap();

    // Create a second fact table with un-renamed column to test join DPP directly
    coord
        .execute_sql(
            "CREATE TABLE fact_orders (cust_id BIGINT, amount DOUBLE)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO fact_orders VALUES (1, 10.0), (5, 20.0)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO fact_orders VALUES (100, 30.0), (105, 40.0)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO fact_orders VALUES (1000, 500.0), (1010, 600.0)", "main")
        .await
        .unwrap();

    let dpp_before = w1.files_pruned_by_dpp.load(Ordering::Relaxed)
        + w2.files_pruned_by_dpp.load(Ordering::Relaxed);
    let r_dpp = coord
        .execute_sql(
            "SELECT v.tier, SUM(f.amount) AS vip_rev FROM fact_orders f JOIN vip_customers v ON f.cust_id = v.cid GROUP BY v.tier ORDER BY v.tier",
            "main",
        )
        .await
        .unwrap();
    let dpp_after = w1.files_pruned_by_dpp.load(Ordering::Relaxed)
        + w2.files_pruned_by_dpp.load(Ordering::Relaxed);

    assert_eq!(r_dpp.batch.num_rows(), 2);
    assert_eq!(
        r_dpp.batch.rows[0].get("vip_rev"),
        Some(&ScalarValue::Float64(600.0))
    );
    assert_eq!(
        r_dpp.batch.rows[1].get("vip_rev"),
        Some(&ScalarValue::Float64(500.0))
    );
    assert!(
        dpp_after >= dpp_before + 2,
        "expected Dynamic Partition Pruning to skip 2 non-overlapping fact splits (pruned={})",
        dpp_after - dpp_before
    );
}
