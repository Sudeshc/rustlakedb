// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog, TableCommitProposal};
use rustlakedb_coordinator::maintainer::{AutonomousMaintainer, MaintainerConfig};
use rustlakedb_coordinator::{select_compaction_candidates, CompactionPolicy, Coordinator};
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ScalarValue;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::tempdir;

fn start_local_coord(dir: &std::path::Path) -> Arc<Coordinator> {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.join("shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new(
        "w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    ))
}

#[tokio::test]
async fn wave39_selective_binpack_oversized_split_and_delete_ratio_auto_maintainer() {
    let dir = tempdir().unwrap();
    let coord = start_local_coord(dir.path());

    coord
        .execute_sql("CREATE TABLE stream_tx (id BIGINT, region TEXT, amount DOUBLE)", "main")
        .await
        .unwrap();

    // 1. Write 1 larger "healthy" batch (600 rows) + 4 tiny 1-row streaming batches
    let mut big_vals = Vec::new();
    for i in 1..=600 {
        big_vals.push(format!("({i}, 'us-east', 10.0)"));
    }
    coord
        .execute_sql(
            &format!("INSERT INTO stream_tx VALUES {}", big_vals.join(", ")),
            "main",
        )
        .await
        .unwrap();
    for i in 601..=604 {
        coord
            .execute_sql(
                &format!("INSERT INTO stream_tx VALUES ({i}, 'us-east', 10.0)"),
                "main",
            )
            .await
            .unwrap();
    }

    let meta_before = coord.catalog.get_table("stream_tx").await.unwrap();
    let snap_before = meta_before.current_snapshot("main").unwrap();
    assert_eq!(snap_before.data_files.len(), 5);
    let healthy_file = snap_before
        .data_files
        .iter()
        .max_by_key(|f| f.byte_size)
        .unwrap()
        .clone();
    let small_max_size = snap_before
        .data_files
        .iter()
        .filter(|f| f.file_path != healthy_file.file_path)
        .map(|f| f.byte_size)
        .max()
        .unwrap();

    // Verify selective policy leaves healthy_file untouched and selects ONLY the 4 tiny streaming files!
    let selective_policy = CompactionPolicy {
        target_file_bytes: healthy_file.byte_size * 2,
        min_file_bytes: small_max_size + 1,
        max_file_bytes: healthy_file.byte_size * 4,
        min_small_files_per_partition: 3,
        delete_ratio_threshold: 0.20,
        max_bytes_per_run: 128 * 1024 * 1024,
    };
    let sel = select_compaction_candidates(snap_before, &selective_policy, false);
    assert_eq!(sel.total_candidate_files, 4);
    assert_eq!(sel.skipped_healthy_files, 1);

    // 2. Verify oversized/skewed file splitting: set max_file_bytes below healthy_file.byte_size
    let split_files = coord
        .store
        .compact_arrow_files_to_target_size(
            "stream_tx",
            "split-test",
            &[(healthy_file.file_path.clone(), None)],
            &healthy_file.partition_values,
            (healthy_file.byte_size / 3).max(1024),
        )
        .await
        .unwrap();
    assert!(
        split_files.len() >= 2,
        "oversized skewed Parquet must split into >= 2 target-sized files, got {}",
        split_files.len()
    );
    let split_rows: u64 = split_files.iter().map(|f| f.row_count).sum();
    assert_eq!(split_rows, 600);

    // 3. Verify delete-ratio threshold trigger on AutonomousMaintainer:
    // Delete only 10 rows out of 600 (1.6% < 20% threshold) -> maintainer should NOT rewrite healthy file!
    coord
        .execute_sql("DELETE FROM stream_tx WHERE id <= 10", "main")
        .await
        .unwrap();
    let meta_after_small_del = coord.catalog.get_table("stream_tx").await.unwrap();
    let snap_small_del = meta_after_small_del.current_snapshot("main").unwrap();
    let healthy_after_small = snap_small_del
        .data_files
        .iter()
        .find(|f| f.file_path == healthy_file.file_path)
        .unwrap();
    assert_eq!(healthy_after_small.deleted_row_count, 10);
    let sel_low_dv = select_compaction_candidates(snap_small_del, &selective_policy, false);
    assert_eq!(
        sel_low_dv.skipped_healthy_files, 1,
        "1.6% deleted rows must NOT force rewrite of healthy file"
    );

    // Delete 200 more rows (total 210 / 600 = 35% >= 20% threshold) -> maintainer MUST select healthy file!
    coord
        .execute_sql("DELETE FROM stream_tx WHERE id > 10 AND id <= 210", "main")
        .await
        .unwrap();
    let meta_after_big_del = coord.catalog.get_table("stream_tx").await.unwrap();
    let snap_big_del = meta_after_big_del.current_snapshot("main").unwrap();
    let sel_high_dv = select_compaction_candidates(snap_big_del, &selective_policy, false);
    assert_eq!(
        sel_high_dv.skipped_healthy_files, 0,
        "35% deleted rows MUST trigger automatic DV materialization rewrite!"
    );

    // Run AutonomousMaintainer cycle -> compacts all candidates, strips all 210 deleted rows!
    let maint = AutonomousMaintainer::new(coord.clone(), MaintainerConfig::default());
    let rep = maint.run_once().await.unwrap();
    assert_eq!(rep.tables_compacted, 1);

    let check = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM stream_tx", "main")
        .await
        .unwrap();
    assert_eq!(check.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(394)));
    assert_eq!(check.batch.rows[0].get("total"), Some(&ScalarValue::Float64(3940.0)));
}

#[tokio::test]
async fn wave39_rewrite_files_occ_rebases_cleanly_over_concurrent_streaming_appends() {
    let dir = tempdir().unwrap();
    let coord = start_local_coord(dir.path());

    coord
        .execute_sql("CREATE TABLE live_stream (id BIGINT, amount DOUBLE)", "main")
        .await
        .unwrap();
    coord.execute_sql("INSERT INTO live_stream VALUES (1, 10.0)", "main").await.unwrap();
    coord.execute_sql("INSERT INTO live_stream VALUES (2, 20.0)", "main").await.unwrap();
    coord.execute_sql("INSERT INTO live_stream VALUES (3, 30.0)", "main").await.unwrap();

    // Snapshot S before compaction begins
    let meta_s = coord.catalog.get_table("live_stream").await.unwrap();
    let snap_s = meta_s.current_snapshot("main").unwrap();
    let old_files: Vec<(String, Option<String>)> = snap_s
        .data_files
        .iter()
        .map(|f| (f.file_path.clone(), f.dv_path.clone()))
        .collect();
    let removed_paths: Vec<String> = old_files.iter().map(|(p, _)| p.clone()).collect();

    // Build compacted file from snapshot S
    let new_files = coord
        .store
        .compact_arrow_files_to_target_size(
            "live_stream",
            "concurrent-compact",
            &old_files,
            &std::collections::HashMap::new(),
            128 * 1024 * 1024,
        )
        .await
        .unwrap();

    // WHILE compaction was running, a streaming writer concurrently appends id=4 (snapshot S+1)!
    coord.execute_sql("INSERT INTO live_stream VALUES (4, 40.0)", "main").await.unwrap();

    // Commit compaction with STALE expected_snapshot_id = snap_s.snapshot_id!
    // Iceberg RewriteFiles OCC must auto-rebase cleanly and keep BOTH compacted (1,2,3) AND concurrent (4)!
    coord
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "live_stream".into(),
            branch: "main".into(),
            expected_snapshot_id: snap_s.snapshot_id,
            added_files: new_files,
            removed_files: removed_paths,
            is_append_only: false,
        })
        .await
        .unwrap();

    let meta_final = coord.catalog.get_table("live_stream").await.unwrap();
    assert_eq!(meta_final.current_snapshot("main").unwrap().data_files.len(), 2);

    let res = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM live_stream", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(4)));
    assert_eq!(res.batch.rows[0].get("total"), Some(&ScalarValue::Float64(100.0)));
}

#[tokio::test]
async fn wave39_buffered_streaming_ingest_coalesces_50_single_records_into_1_parquet() {
    let dir = tempdir().unwrap();
    let coord = start_local_coord(dir.path());

    coord
        .execute_sql("CREATE TABLE iot_ticks (id BIGINT, val DOUBLE)", "main")
        .await
        .unwrap();

    // Send 50 individual 1-row streaming records via micro-batch buffer!
    for i in 1..=50 {
        let line = format!(r#"{{"id":{i},"val":1.5}}"#);
        let (_, _, flushed) = coord
            .ingest_ndjson_buffered("iot_ticks", "main", &line, false, false)
            .await
            .unwrap();
        assert!(!flushed, "50 rows < default 1,000 flush threshold");
    }

    // Timer / explicit flush commits all 50 buffered records as ONE single Parquet file!
    let flushed_rows = coord.flush_all_ingest_buffers().await;
    assert_eq!(flushed_rows, 50);

    let meta = coord.catalog.get_table("iot_ticks").await.unwrap();
    assert_eq!(
        meta.current_snapshot("main").unwrap().data_files.len(),
        1,
        "50 single-record streaming ingests must coalesce into exactly 1 Parquet file!"
    );

    let res = coord
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(val) AS total FROM iot_ticks", "main")
        .await
        .unwrap();
    assert_eq!(res.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(50)));
    assert_eq!(res.batch.rows[0].get("total"), Some(&ScalarValue::Float64(75.0)));
}
