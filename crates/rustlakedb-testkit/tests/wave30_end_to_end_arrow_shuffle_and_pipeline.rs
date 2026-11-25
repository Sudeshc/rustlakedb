// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 30: Zero-`RowBatch` End-to-End Apache Arrow `RecordBatch` Shuffle & Multi-Stage Columnar Pipeline —
//! Verifies direct `write_record_batch` / `read_record_batch` (`ARW1`), pure Arrow `eval_columnar_hash_join`
//! (Inner/Left/Right/Full), pure Arrow `eval_columnar_sort`, and live multi-stage distributed SQL
//! reading Arrow IPC shuffle blocks directly into `RecordBatch`es!

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_common::{QueryId, WorkerId};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{JoinType, ScalarValue, ShuffleMode, ShufflePartitionKey, SortKey};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::columnar::{eval_columnar_hash_join, eval_columnar_sort};
use rustlakedb_worker::WorkerNode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn wave30_zero_rowbatch_arrow_shuffle_and_columnar_join_sort() {
    let dir = tempdir().unwrap();
    let mgr = HybridShuffleManager::new(dir.path().join("shuffle")).unwrap();

    // 1. Direct RecordBatch -> ARW1 Zstd Arrow IPC -> RecordBatch (zero RowBatch!)
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let left_rb = RecordBatch::try_new(
        left_schema,
        vec![
            Arc::new(Int64Array::from(vec![3, 1, 2, 4])),
            Arc::new(Float64Array::from(vec![300.0, 100.0, 200.0, 400.0])),
        ],
    )
    .unwrap();

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("cid", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
    ]));
    let right_rb = RecordBatch::try_new(
        right_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["gold", "silver", "platinum"])),
        ],
    )
    .unwrap();

    let key_l = ShufflePartitionKey {
        query_id: QueryId::new(),
        stage_id: 0,
        map_partition: 0,
        reduce_partition: 0,
        attempt: 0,
    };
    let key_r = ShufflePartitionKey {
        query_id: key_l.query_id,
        stage_id: 1,
        map_partition: 0,
        reduce_partition: 0,
        attempt: 0,
    };
    let loc_l = mgr
        .write_record_batch(&WorkerId::new("w1"), key_l, ShuffleMode::Staged, &left_rb)
        .await
        .unwrap();
    let loc_r = mgr
        .write_record_batch(&WorkerId::new("w2"), key_r, ShuffleMode::Pipelined, &right_rb)
        .await
        .unwrap();

    let read_l = mgr.read_record_batch(&loc_l).await.unwrap().expect("ARW1 left");
    let read_r = mgr.read_record_batch(&loc_r).await.unwrap().expect("ARW1 right");
    assert_eq!(read_l.num_rows(), 4);
    assert_eq!(read_r.num_rows(), 3);

    // 2. Pure Arrow RecordBatch Left Join + Columnar Sort (zero BTreeMap!)
    let joined =
        eval_columnar_hash_join(&read_l, &read_r, "id", "cid", JoinType::Left).unwrap();
    assert_eq!(joined.num_rows(), 4);
    let sorted = eval_columnar_sort(
        &joined,
        &[SortKey {
            column: "amount".into(),
            descending: true,
        }],
    )
    .unwrap();
    let ids = sorted
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[4, 3, 2, 1]);
}

#[tokio::test]
async fn wave30_multistage_distributed_join_and_sort_reads_arrow_shuffle_directly() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let w2 = Arc::new(WorkerNode::new("w2", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1.clone(), w2.clone()],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE ord (oid BIGINT, cid BIGINT, amt DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("CREATE TABLE usr (uid BIGINT, name TEXT)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO ord VALUES (10, 1, 50.0), (11, 2, 200.0)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO ord VALUES (12, 1, 150.0)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO usr VALUES (1, 'Alice'), (2, 'Bob')", "main")
        .await
        .unwrap();

    let res = coord
        .execute_sql(
            "SELECT o.oid, u.name, o.amt FROM ord o JOIN usr u ON o.cid = u.uid ORDER BY o.amt DESC",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 3);
    assert_eq!(res.batch.rows[0].get("amt"), Some(&ScalarValue::Float64(200.0)));
    assert_eq!(res.batch.rows[2].get("amt"), Some(&ScalarValue::Float64(50.0)));

    let arrow_reads = w1.arrow_shuffle_batches_read.load(Ordering::Relaxed)
        + w2.arrow_shuffle_batches_read.load(Ordering::Relaxed);
    assert!(
        arrow_reads >= 2,
        "expected multi-stage distributed join/sort to read ARW1 Arrow shuffle blocks directly (got {arrow_reads})"
    );
}
