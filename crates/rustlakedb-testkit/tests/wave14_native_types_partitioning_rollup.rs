// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_schema::{DataType, TimeUnit};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave14_native_arrow_date32_timestamp_and_decimal128_types() {
    let dir = tempdir().unwrap();
    let wh = dir.path().join("wh");
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(&wh).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(catalog, store, shuffle, vec![w1.clone()], PlannerConfig::default());

    coord
        .execute_sql(
            "CREATE TABLE ledger (id BIGINT, tx_date DATE, created_at TIMESTAMP, price DECIMAL(18, 2))",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO ledger VALUES \
             (1, '2026-11-20', '2026-11-20T10:15:00Z', 19.95), \
             (2, '2026-11-24', '2026-11-24T14:30:00Z', 100.05)",
            "main",
        )
        .await
        .unwrap();

    // 1. Inspect the raw Parquet file's physical Arrow schema to prove native Date32, Timestamp(us, UTC), and Decimal128(18, 2)!
    let meta = coord.catalog.get_table("ledger").await.unwrap();
    let snap = meta.current_snapshot("main").unwrap();
    let pq_bytes = Bytes::from(std::fs::read(wh.join(&snap.data_files[0].file_path)).unwrap());
    let arrow_schema = ParquetRecordBatchReaderBuilder::try_new(pq_bytes)
        .unwrap()
        .schema()
        .clone();
    assert_eq!(arrow_schema.field_with_name("tx_date").unwrap().data_type(), &DataType::Date32);
    assert_eq!(
        arrow_schema.field_with_name("created_at").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        arrow_schema.field_with_name("price").unwrap().data_type(),
        &DataType::Decimal128(18, 2)
    );

    // 2. Query with DATE predicate + Vectorized Arrow Project + exact Decimal128 values!
    let res = coord
        .execute_sql(
            "SELECT id, tx_date, price FROM ledger WHERE tx_date >= '2026-11-24' ORDER BY id",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1);
    assert_eq!(res.batch.rows[0].get("tx_date").and_then(|v| v.to_pg_text()).as_deref(), Some("2026-11-24"));
    assert_eq!(res.batch.rows[0].get("price"), Some(&ScalarValue::Decimal(10_005, 2)));
    assert_eq!(res.batch.rows[0].get("price").and_then(|v| v.to_pg_text()).as_deref(), Some("100.05"));

    // 3. Unfiltered projection triggers Vectorized Arrow Scan+Project!
    let before = w1.vectorized_rows_processed.load(Ordering::Relaxed);
    let proj = coord.execute_sql("SELECT id, price FROM ledger", "main").await.unwrap();
    assert_eq!(proj.batch.num_rows(), 2);
    assert!(w1.vectorized_rows_processed.load(Ordering::Relaxed) >= before + 2);
}

#[tokio::test]
async fn wave14_write_time_hidden_partition_routing_and_layer0_pruning() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog,
        store.clone(),
        shuffle,
        vec![w1],
        PlannerConfig {
            default_shuffle_partitions: 2,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    );

    // Table partitioned by identity `region` AND hidden transform `days(ts)`!
    coord
        .execute_sql(
            "CREATE TABLE clicks (id BIGINT, region VARCHAR, ts TIMESTAMP, rev DOUBLE) PARTITION BY (region, days(ts))",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO clicks VALUES \
             (1, 'us-east', '2026-11-20T08:00:00Z', 10.0), \
             (2, 'us-east', '2026-11-21T09:00:00Z', 20.0), \
             (3, 'eu-west', '2026-11-20T10:00:00Z', 30.0), \
             (4, 'ap-south', '2026-11-22T11:00:00Z', 40.0)",
            "main",
        )
        .await
        .unwrap();

    // Verify 4 distinct physical partition Parquet files were created with stamped `partition_values`!
    let meta = coord.catalog.get_table("clicks").await.unwrap();
    let snap = meta.current_snapshot("main").unwrap();
    assert_eq!(snap.data_files.len(), 4);
    assert!(snap.data_files.iter().all(|f| f.file_path.contains("region=") && f.file_path.contains("days_ts=")));

    // Query filtering on `region = 'eu-west'` MUST prune 3 of the 4 partition files in Layer 0!
    let before_pruned = store.files_pruned_by_partition.load(Ordering::Relaxed);
    let res = coord
        .execute_sql("SELECT id, rev FROM clicks WHERE region = 'eu-west'", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 1);
    assert_eq!(res.batch.rows[0].get("id"), Some(&ScalarValue::Int64(3)));
    let pruned = store.files_pruned_by_partition.load(Ordering::Relaxed) - before_pruned;
    assert_eq!(pruned, 3, "expected Layer 0 partition pruning to skip exactly 3/4 files");
}

#[tokio::test]
async fn wave14_rollup_cube_and_grouping_sets_subtotals() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(catalog, store, shuffle, vec![w1], PlannerConfig::default());

    coord
        .execute_sql("CREATE TABLE sales_olap (region VARCHAR, cat VARCHAR, rev DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO sales_olap VALUES \
             ('us', 'books', 100.0), ('us', 'games', 200.0), \
             ('eu', 'books', 150.0), ('eu', 'games', 50.0)",
            "main",
        )
        .await
        .unwrap();

    // 1. GROUP BY ROLLUP(region, cat) -> 4 detail + 2 region subtotals + 1 grand total = 7 rows!
    let rollup = coord
        .execute_sql(
            "SELECT region, cat, SUM(rev) AS total FROM sales_olap GROUP BY ROLLUP(region, cat)",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(rollup.batch.num_rows(), 7);
    let grand = rollup
        .batch
        .rows
        .iter()
        .find(|r| {
            matches!(r.get("region"), Some(ScalarValue::Null))
                && matches!(r.get("cat"), Some(ScalarValue::Null))
        })
        .expect("grand total row missing in ROLLUP");
    assert_eq!(grand.get("total"), Some(&ScalarValue::Float64(500.0)));

    // 2. GROUP BY CUBE(region, cat) -> 4 detail + 2 region + 2 cat + 1 grand = 9 rows!
    let cube = coord
        .execute_sql(
            "SELECT region, cat, SUM(rev) AS total FROM sales_olap GROUP BY CUBE(region, cat)",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(cube.batch.num_rows(), 9);

    // 3. GROUP BY GROUPING SETS ((region), ()) -> 2 region subtotals + 1 grand = 3 rows!
    let gsets = coord
        .execute_sql(
            "SELECT region, SUM(rev) AS total FROM sales_olap GROUP BY GROUPING SETS ((region), ())",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(gsets.batch.num_rows(), 3);
}
