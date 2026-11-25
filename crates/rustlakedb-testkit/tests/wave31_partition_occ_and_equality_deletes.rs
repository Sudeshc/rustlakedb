// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 31: Partition-Level Disjoint OCC Conflict Resolution & Native Apache Iceberg v3
//! Equality Delete Parquet Files for Zero-Read Streaming CDC Upserts/Deletes.

use rustlakedb_catalog::{
    Catalog, ColumnType, DataFileEntry, FieldSchema, FilePuffinIndex, IcebergTableMetadata,
    InMemoryRaftCatalog, TableCommitProposal,
};
use rustlakedb_common::RustLakeError;
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{RowBatch, ScalarValue};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::tempdir;

fn make_part_file(path: &str, region: &str) -> DataFileEntry {
    let mut pv = HashMap::new();
    pv.insert("region".into(), region.into());
    DataFileEntry {
        file_path: path.into(),
        row_group_id: 0,
        row_count: 10,
        byte_size: 256,
        deletion_vector: roaring::RoaringBitmap::new(),
        puffin_index: FilePuffinIndex::default(),
        ndv_estimates: HashMap::new(),
        column_min: HashMap::new(),
        column_max: HashMap::new(),
        partition_values: pv,
        dv_path: None,
    }
}

#[tokio::test]
async fn wave31_partition_level_disjoint_occ_allows_parallel_writers_and_blocks_overlap() {
    let cat = InMemoryRaftCatalog::new();
    let schema = vec![
        FieldSchema { field_id: 1, name: "id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
    ];
    let mut meta = IcebergTableMetadata::new("part_events", schema);
    meta.partition_by = vec!["region".into()];
    cat.create_table(meta).await.unwrap();

    // Base snapshot 2 contains two partitions: us-east and eu-west
    let base_snap = cat
        .commit_table(TableCommitProposal {
            table_name: "part_events".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: vec![
                make_part_file("part_events/us-east/p1.parquet", "us-east"),
                make_part_file("part_events/eu-west/p1.parquet", "eu-west"),
            ],
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();
    assert_eq!(base_snap, 2);

    // Writer A mutates partition `us-east` starting from base_snap=2 (is_append_only=false)
    let snap_a = cat
        .commit_table(TableCommitProposal {
            table_name: "part_events".into(),
            branch: "main".into(),
            expected_snapshot_id: base_snap,
            added_files: vec![make_part_file("part_events/us-east/p2.parquet", "us-east")],
            removed_files: vec!["part_events/us-east/p1.parquet".into()],
            is_append_only: false,
        })
        .await
        .unwrap();
    assert_eq!(snap_a, 3);

    // Writer B concurrently mutates DISJOINT partition `eu-west` from STALE base_snap=2!
    // Partition-level OCC MUST auto-rebase and SUCCEED!
    let snap_b = cat
        .commit_table(TableCommitProposal {
            table_name: "part_events".into(),
            branch: "main".into(),
            expected_snapshot_id: base_snap,
            added_files: vec![make_part_file("part_events/eu-west/p2.parquet", "eu-west")],
            removed_files: vec!["part_events/eu-west/p1.parquet".into()],
            is_append_only: false,
        })
        .await
        .unwrap();
    assert_eq!(snap_b, 4);

    // Writer C concurrently mutates OVERLAPPING partition `us-east` from STALE base_snap=2!
    // MUST STILL REJECT with OccConflict!
    let err_c = cat
        .commit_table(TableCommitProposal {
            table_name: "part_events".into(),
            branch: "main".into(),
            expected_snapshot_id: base_snap,
            added_files: vec![make_part_file("part_events/us-east/p3.parquet", "us-east")],
            removed_files: vec![],
            is_append_only: false,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err_c, RustLakeError::OccConflict { .. }),
        "overlapping partition write must reject with OccConflict, got {err_c:?}"
    );
}

#[tokio::test]
async fn wave31_iceberg_v3_equality_deletes_zero_read_cdc_upsert_and_delete() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store.clone(),
        shuffle,
        vec![w1],
        PlannerConfig::default(),
    ));

    coord
        .execute_sql("CREATE TABLE accounts (id BIGINT, owner TEXT, balance DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql(
            "INSERT INTO accounts VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Carol', 300.0)",
            "main",
        )
        .await
        .unwrap();

    // Perform ZERO-READ streaming CDC upsert (update id=1 -> 999.0, delete id=2) via Iceberg v3 Equality Deletes!
    let reads_before = store.parquet_files_read.load(Ordering::Relaxed);
    let upsert_batch = RowBatch::new(
        vec!["id".into(), "owner".into(), "balance".into()],
        vec![BTreeMap::from([
            ("id".into(), ScalarValue::Int64(1)),
            ("owner".into(), ScalarValue::Utf8("Alice".into())),
            ("balance".into(), ScalarValue::Float64(999.0)),
        ])],
    );
    coord
        .ingest_cdc_equality_deletes(
            "accounts",
            "main",
            "id",
            vec![ScalarValue::Int64(1), ScalarValue::Int64(2)],
            upsert_batch,
        )
        .await
        .unwrap();
    let reads_after = store.parquet_files_read.load(Ordering::Relaxed);
    assert_eq!(
        reads_after, reads_before,
        "Equality Delete CDC ingest must perform ZERO Parquet data-file reads!"
    );

    // Query must reflect: id=1 updated to 999.0 (new snapshot survives!), id=2 deleted, id=3 intact (300.0)!
    let res = coord
        .execute_sql("SELECT id, owner, balance FROM accounts ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(res.batch.rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(res.batch.rows[0].get("balance"), Some(&ScalarValue::Float64(999.0)));
    assert_eq!(res.batch.rows[1].get("id"), Some(&ScalarValue::Int64(3)));
    assert_eq!(res.batch.rows[1].get("balance"), Some(&ScalarValue::Float64(300.0)));
}

#[tokio::test]
async fn prc_immutable_dv_preserves_restore_after_update_delete_and_explain_view() {
    let dir = tempdir().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new("w-prc", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    );

    coord
        .execute_sql("CREATE TABLE dr_ledger (id BIGINT NOT NULL, balance DOUBLE)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO dr_ledger VALUES (1, 1000.0), (2, 2000.0), (3, 3000.0)", "main")
        .await
        .unwrap();
    let snaps = coord
        .execute_sql("SHOW SNAPSHOTS ON dr_ledger", "main")
        .await
        .unwrap();
    let good_snap = match snaps.batch.rows.last().unwrap().get("snapshot_id").unwrap() {
        ScalarValue::Int64(v) => *v as u64,
        other => panic!("unexpected snapshot_id: {other:?}"),
    };

    // Corrupt via UPDATE & DELETE (creates new immutable snapshot-scoped DVs!)
    coord
        .execute_sql("UPDATE dr_ledger SET balance = 0.0 WHERE id >= 1", "main")
        .await
        .unwrap();
    coord
        .execute_sql("DELETE FROM dr_ledger WHERE id = 3", "main")
        .await
        .unwrap();
    let after_bad = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(balance) AS total FROM dr_ledger", "main")
        .await
        .unwrap();
    assert_eq!(after_bad.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(2)));
    assert_eq!(after_bad.batch.rows[0].get("total"), Some(&ScalarValue::Float64(0.0)));

    // Restore back to good_snap -> must recover ALL 3 rows and exact $6,000 balance!
    coord
        .execute_sql(&format!("RESTORE TABLE dr_ledger TO VERSION {good_snap}"), "main")
        .await
        .unwrap();
    let restored = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(balance) AS total FROM dr_ledger", "main")
        .await
        .unwrap();
    assert_eq!(restored.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(3)));
    assert_eq!(restored.batch.rows[0].get("total"), Some(&ScalarValue::Float64(6000.0)));

    // Verify EXPLAIN (FORMAT JSON) over a virtual VIEW works cleanly!
    coord
        .execute_sql("CREATE VIEW v_dr AS SELECT id, balance FROM dr_ledger", "main")
        .await
        .unwrap();
    let exp = coord
        .execute_sql("EXPLAIN (FORMAT JSON) SELECT id, balance FROM v_dr ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(exp.batch.num_rows(), 1);
}
