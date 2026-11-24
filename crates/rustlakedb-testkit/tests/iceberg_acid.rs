// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, TableCommitProposal};
use rustlakedb_common::RustLakeError;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn iceberg_v3_deletion_vectors_and_puffin_json_pruning_end_to_end() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // Delete fraudulent order #3 via Iceberg v3 Deletion Vector (does NOT rewrite Parquet file!).
    let parquet_before = cluster.store.parquet_files_written.load(Ordering::Relaxed);
    let del = cluster.sql("DELETE FROM orders WHERE order_id = 3").await.unwrap();
    assert_eq!(del.command_tag, "DELETE 1");
    assert_eq!(
        cluster.store.parquet_files_written.load(Ordering::Relaxed),
        parquet_before,
        "v3 Deletion Vector delete must NOT rewrite immutable Parquet data files"
    );

    // Query with JSONB `->>` predicate -> Puffin index prunes the 3rd file ('view' only),
    // and Iceberg v3 DV skips order_id=3 ($999)!
    let res = cluster
        .sql(
            "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total \
             FROM orders WHERE payload->>'event_type' = 'purchase' \
             GROUP BY region ORDER BY region",
        )
        .await
        .unwrap();

    assert_eq!(cluster.store.files_pruned_by_puffin.load(Ordering::Relaxed), 1);
    assert_eq!(cluster.store.rows_skipped_by_dv.load(Ordering::Relaxed), 1);
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(res.batch.rows[0].get("region"), Some(&ScalarValue::Utf8("apac".into())));
    assert_eq!(res.batch.rows[0].get("total"), Some(&ScalarValue::Float64(300.0)));
    assert_eq!(res.batch.rows[1].get("region"), Some(&ScalarValue::Utf8("us".into())));
    assert_eq!(res.batch.rows[1].get("total"), Some(&ScalarValue::Float64(250.0)));
}

#[tokio::test]
async fn git_style_branching_isolates_writes_until_merge() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE items (id BIGINT, name TEXT)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO items VALUES (1, 'alpha')")
        .await
        .unwrap();

    // Branch `staging` from `main` and write exclusively to `staging`.
    cluster
        .sql("CREATE BRANCH staging FROM main ON items")
        .await
        .unwrap();
    cluster
        .sql_on_branch("INSERT INTO items VALUES (2, 'beta')", "staging")
        .await
        .unwrap();

    let main_before = cluster
        .sql_on_branch("SELECT COUNT(*) AS cnt FROM items", "main")
        .await
        .unwrap();
    let staging_before = cluster
        .sql_on_branch("SELECT COUNT(*) AS cnt FROM items", "staging")
        .await
        .unwrap();
    assert_eq!(main_before.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(1)));
    assert_eq!(staging_before.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(2)));

    // Merge `staging` -> `main`.
    cluster
        .sql("MERGE BRANCH staging INTO main ON items")
        .await
        .unwrap();
    let main_after = cluster
        .sql_on_branch("SELECT COUNT(*) AS cnt FROM items", "main")
        .await
        .unwrap();
    assert_eq!(main_after.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(2)));
}

#[tokio::test]
async fn multi_table_acid_transaction_rejects_stale_snapshot_atomically() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    let orders_snap = cluster
        .catalog
        .get_table("orders")
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;
    let cust_snap = cluster
        .catalog
        .get_table("customers")
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;

    // Attempt non-append multi-table commit with stale snapshot on `customers` -> must fail atomically!
    let err = cluster
        .catalog
        .commit_multi_table(vec![
            TableCommitProposal {
                table_name: "orders".into(),
                branch: "main".into(),
                expected_snapshot_id: orders_snap,
                added_files: vec![],
                removed_files: vec![],
                is_append_only: false,
            },
            TableCommitProposal {
                table_name: "customers".into(),
                branch: "main".into(),
                expected_snapshot_id: cust_snap - 1,
                added_files: vec![],
                removed_files: vec![],
                is_append_only: false,
            },
        ])
        .await
        .unwrap_err();
    assert!(matches!(err, RustLakeError::OccConflict { .. }));

    // `orders` snapshot must remain untouched!
    let orders_after = cluster
        .catalog
        .get_table("orders")
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;
    assert_eq!(orders_after, orders_snap);
}
