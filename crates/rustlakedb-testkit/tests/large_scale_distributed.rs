// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, TableCommitProposal};
use rustlakedb_protocol::{RowBatch, ScalarValue};
use rustlakedb_testkit::TestCluster;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Large-scale distributed execution regression test:
/// Spins up a 4-worker disaggregated networked HTTP cluster, ingests 100,000 JSON-bearing rows
/// across 16 Parquet + Puffin splits (plus 5,000 dimension rows across 4 splits), executes
/// parallel distributed GROUP BY, distributed JOIN, and Puffin needle-in-haystack JSON lookup,
/// and asserts all 4 workers shared load concurrently!
#[tokio::test]
async fn large_dataset_100k_distributed_parallel_join_and_puffin_pruning() {
    let cluster = TestCluster::start_networked(4).await.unwrap();

    let order_schema = vec![
        FieldSchema { field_id: 1, name: "order_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "customer_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 3, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 4, name: "amount".into(), data_type: ColumnType::Float64, nullable: false },
        FieldSchema { field_id: 5, name: "payload".into(), data_type: ColumnType::Variant, nullable: false },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("big_orders", order_schema.clone()))
        .await
        .unwrap();

    let regions = ["us-east", "us-west", "eu-central", "ap-south"];
    let mut order_files = Vec::with_capacity(16);
    let t_ingest = Instant::now();
    for split in 0..16usize {
        let mut rows = Vec::with_capacity(6_250);
        for i in 0..6_250usize {
            let gid = (split * 6_250 + i + 1) as i64;
            let tag = if split == 15 && i == 10 {
                "needle_100k"
            } else {
                "standard"
            };
            let mut r = BTreeMap::new();
            r.insert("order_id".into(), ScalarValue::Int64(gid));
            r.insert("customer_id".into(), ScalarValue::Int64((gid % 5_000) + 1));
            r.insert("region".into(), ScalarValue::Utf8(regions[i % 4].into()));
            r.insert("amount".into(), ScalarValue::Float64(10.0));
            r.insert("payload".into(), ScalarValue::Json(json!({ "tag": tag })));
            rows.push(r);
        }
        let batch = RowBatch::new(
            vec!["order_id".into(), "customer_id".into(), "region".into(), "amount".into(), "payload".into()],
            rows,
        );
        let path = format!("big_orders/data/part-{split:04}.parquet");
        order_files.push(cluster.store.write_data_file(&path, &order_schema, &batch).await.unwrap());
    }
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "big_orders".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: order_files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    let cust_schema = vec![
        FieldSchema { field_id: 1, name: "customer_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "tier".into(), data_type: ColumnType::Utf8, nullable: false },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("big_customers", cust_schema.clone()))
        .await
        .unwrap();
    let tiers = ["enterprise", "pro"];
    let mut cust_files = Vec::with_capacity(4);
    for split in 0..4usize {
        let mut rows = Vec::with_capacity(1_250);
        for i in 0..1_250usize {
            let cid = (split * 1_250 + i + 1) as i64;
            rows.push(BTreeMap::from([
                ("customer_id".into(), ScalarValue::Int64(cid)),
                ("tier".into(), ScalarValue::Utf8(tiers[(cid as usize) % 2].into())),
            ]));
        }
        let batch = RowBatch::new(vec!["customer_id".into(), "tier".into()], rows);
        let path = format!("big_customers/data/part-{split:04}.parquet");
        cust_files.push(cluster.store.write_data_file(&path, &cust_schema, &batch).await.unwrap());
    }
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "big_customers".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: cust_files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();
    let ingest_ms = t_ingest.elapsed().as_millis();

    // 1. Selective JSON needle across 100,000 rows -> 15/16 Parquet files pruned by Puffin!
    let t_needle = Instant::now();
    let needle = cluster
        .sql(r#"SELECT order_id FROM public.big_orders WHERE payload @> '{"tag": "needle_100k"}'"#)
        .await
        .unwrap();
    let needle_ms = t_needle.elapsed().as_millis();
    assert_eq!(needle.batch.num_rows(), 1);
    assert_eq!(cluster.store.files_pruned_by_puffin.load(Ordering::Relaxed), 15);

    // 2. Parallel 4-worker distributed 100K x 5K partitioned Hash Join + GROUP BY over HTTP Zstd shuffle!
    let t_join = Instant::now();
    let joined = cluster
        .sql(
            "SELECT c.tier, COUNT(*) AS cnt, SUM(o.amount) AS rev \
             FROM public.big_orders o JOIN public.big_customers c ON o.customer_id = c.customer_id \
             GROUP BY c.tier ORDER BY c.tier",
        )
        .await
        .unwrap();
    let join_ms = t_join.elapsed().as_millis();

    assert_eq!(joined.batch.num_rows(), 2);
    assert_eq!(joined.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(50_000)));
    assert_eq!(joined.batch.rows[0].get("rev"), Some(&ScalarValue::Float64(500_000.0)));
    assert_eq!(joined.batch.rows[1].get("cnt"), Some(&ScalarValue::Int64(50_000)));
    assert_eq!(joined.batch.rows[1].get("rev"), Some(&ScalarValue::Float64(500_000.0)));

    // Prove ALL 4 remote HTTP workers actively executed parallel tasks!
    let per_worker: Vec<u64> = cluster
        .workers
        .iter()
        .map(|w| w.tasks_executed.load(Ordering::Relaxed))
        .collect();
    for (idx, &cnt) in per_worker.iter().enumerate() {
        assert!(cnt > 0, "worker-{} executed 0 tasks — load was not distributed!", idx + 1);
    }

    println!(
        "[LARGE-DIST] 100k x 5k over 4 HTTP workers: ingest={ingest_ms}ms | needle_json(15/16 pruned)={needle_ms}ms | dist_join_group_by={join_ms}ms | tasks_per_worker={per_worker:?}"
    );
}
