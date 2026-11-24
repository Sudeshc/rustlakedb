// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, TableCommitProposal};
use rustlakedb_protocol::{RowBatch, ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Release QA performance benchmark:
/// Writes 10,000 JSON-bearing rows across 10 real Zstd Parquet files + Puffin JSON indexes,
/// then benchmarks selective JSON lookup pruning (9/10 files pruned!) and full distributed GROUP BY.
#[tokio::test]
async fn bench_distributed_parquet_and_puffin_json_pruning_10k_rows() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    let schema = vec![
        FieldSchema { field_id: 1, name: "id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 3, name: "amount".into(), data_type: ColumnType::Float64, nullable: false },
        FieldSchema { field_id: 4, name: "payload".into(), data_type: ColumnType::Variant, nullable: false },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("bench_events", schema.clone()))
        .await
        .unwrap();

    let t_write = Instant::now();
    let regions = ["us-east", "us-west", "eu-central", "ap-south"];
    let mut added_files = Vec::new();

    // 10 Parquet splits x 1,000 rows = 10,000 rows. Only split #7 contains `needle_trace`!
    for file_idx in 0..10usize {
        let mut rows = Vec::with_capacity(1000);
        for row_idx in 0..1000usize {
            let global_id = (file_idx * 1000 + row_idx) as i64;
            let trace = if file_idx == 7 && row_idx == 42 {
                "needle_trace"
            } else {
                "normal_trace"
            };
            let mut r = BTreeMap::new();
            r.insert("id".into(), ScalarValue::Int64(global_id));
            r.insert("region".into(), ScalarValue::Utf8(regions[row_idx % 4].into()));
            r.insert("amount".into(), ScalarValue::Float64((row_idx % 100) as f64));
            r.insert(
                "payload".into(),
                ScalarValue::Json(json!({ "trace": trace, "bucket": file_idx })),
            );
            rows.push(r);
        }
        let batch = RowBatch::new(
            vec!["id".into(), "region".into(), "amount".into(), "payload".into()],
            rows,
        );
        let path = format!("bench_events/data/part-{file_idx:04}.parquet");
        let entry = cluster.store.write_data_file(&path, &schema, &batch).await.unwrap();
        added_files.push(entry);
    }

    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "bench_events".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();
    let write_ms = t_write.elapsed().as_millis();

    // 1) Selective JSON lookup — Puffin index must prune 9 out of 10 Parquet files!
    let t_json = Instant::now();
    let needle = cluster
        .sql("SELECT id, region FROM bench_events WHERE payload->>'trace' = 'needle_trace'")
        .await
        .unwrap();
    let json_ms = t_json.elapsed().as_millis();

    assert_eq!(needle.batch.num_rows(), 1);
    assert_eq!(needle.batch.rows[0].get("id"), Some(&ScalarValue::Int64(7042)));
    assert_eq!(
        cluster.store.files_pruned_by_puffin.load(Ordering::Relaxed),
        9,
        "Puffin JSON bloom/inverted index must prune 9/10 Parquet files!"
    );

    // 2) Full 10,000-row distributed 4-worker GROUP BY aggregation.
    let t_agg = Instant::now();
    let agg = cluster
        .sql("SELECT region, COUNT(*) AS cnt, SUM(amount) AS total FROM bench_events GROUP BY region ORDER BY region")
        .await
        .unwrap();
    let agg_ms = t_agg.elapsed().as_millis();

    assert_eq!(agg.batch.num_rows(), 4);
    for row in &agg.batch.rows {
        assert_eq!(row.get("cnt"), Some(&ScalarValue::Int64(2500)));
    }

    println!(
        "[BENCH] 10k rows (10 Parquet + Puffin): write={write_ms}ms | selective_json(9/10 pruned)={json_ms}ms | dist_group_by_4w={agg_ms}ms"
    );
}

#[tokio::test]
async fn bench_50k_rows_scale_and_puffin_pruning() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    let schema = vec![
        FieldSchema { field_id: 1, name: "id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 3, name: "amount".into(), data_type: ColumnType::Float64, nullable: false },
        FieldSchema { field_id: 4, name: "payload".into(), data_type: ColumnType::Variant, nullable: false },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("scale_events", schema.clone()))
        .await
        .unwrap();

    let t_write = Instant::now();
    let regions = ["na", "eu", "apac", "latam", "mea"];
    let mut added_files = Vec::new();

    // 20 Parquet splits x 2,500 rows = 50,000 rows; only split #19 has `vip_incident`!
    for file_idx in 0..20usize {
        let mut rows = Vec::with_capacity(2500);
        for row_idx in 0..2500usize {
            let gid = (file_idx * 2500 + row_idx) as i64;
            let tag = if file_idx == 19 && row_idx == 100 {
                "vip_incident"
            } else {
                "routine"
            };
            let mut r = BTreeMap::new();
            r.insert("id".into(), ScalarValue::Int64(gid));
            r.insert("region".into(), ScalarValue::Utf8(regions[row_idx % 5].into()));
            r.insert("amount".into(), ScalarValue::Float64((row_idx % 200) as f64));
            r.insert("payload".into(), ScalarValue::Json(json!({ "tag": tag })));
            rows.push(r);
        }
        let batch = RowBatch::new(
            vec!["id".into(), "region".into(), "amount".into(), "payload".into()],
            rows,
        );
        let path = format!("scale_events/data/part-{file_idx:04}.parquet");
        added_files.push(cluster.store.write_data_file(&path, &schema, &batch).await.unwrap());
    }
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "scale_events".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();
    let write_ms = t_write.elapsed().as_millis();

    let t_prune = Instant::now();
    let hit = cluster
        .sql(r#"SELECT id FROM scale_events WHERE payload @> '{"tag": "vip_incident"}'"#)
        .await
        .unwrap();
    let prune_ms = t_prune.elapsed().as_millis();

    assert_eq!(hit.batch.num_rows(), 1);
    assert_eq!(cluster.store.files_pruned_by_puffin.load(Ordering::Relaxed), 19);

    let t_agg = Instant::now();
    let agg = cluster
        .sql("SELECT region, COUNT(*) AS cnt, SUM(amount) AS total FROM scale_events GROUP BY region ORDER BY region")
        .await
        .unwrap();
    let agg_ms = t_agg.elapsed().as_millis();
    assert_eq!(agg.batch.num_rows(), 5);
    assert_eq!(agg.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(10_000)));

    println!(
        "[BENCH] 50k rows (20 Parquet + Puffin): write={write_ms}ms | json_contains(19/20 pruned)={prune_ms}ms | dist_group_by_4w={agg_ms}ms"
    );
}
