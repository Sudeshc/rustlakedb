// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::{run_slt_file, TestCluster};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn slt_core_sql_suite_passes_over_real_parquet() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    let slt_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/sql/core_sql.slt");
    let count = run_slt_file(&cluster, slt_path).await.unwrap();
    assert!(count >= 6);
    assert!(cluster.store.parquet_files_written.load(Ordering::Relaxed) >= 2);
    assert!(cluster.store.parquet_files_read.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn real_parquet_puffin_and_iceberg_metadata_files_written_to_disk() {
    let cluster = TestCluster::start(3, ShuffleMode::Staged).unwrap();
    cluster.seed_ecommerce().await.unwrap();

    // Verify real Apache Parquet, Puffin JSON sidecars, and Iceberg v3 metadata JSON exist on disk!
    let meta_dir = cluster.data_dir_path().join("orders/metadata");
    let data_dir = cluster.data_dir_path().join("orders/data");
    assert!(meta_dir.join("v4.metadata.json").exists());

    let mut parquet_count = 0;
    let mut puffin_count = 0;
    for entry in std::fs::read_dir(&data_dir).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().to_string();
        if name.ends_with(".parquet") {
            parquet_count += 1;
        } else if name.ends_with(".puffin.json") {
            puffin_count += 1;
        }
    }
    assert_eq!(parquet_count, 3);
    assert_eq!(puffin_count, 3);

    // Verify EXPLAIN & SHOW TABLES work end-to-end.
    let tables = cluster.sql("SHOW TABLES").await.unwrap();
    assert_eq!(tables.batch.num_rows(), 2);

    let plan = cluster
        .sql("EXPLAIN SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY total DESC LIMIT 2")
        .await
        .unwrap();
    assert!(plan.batch.num_rows() >= 2);

    // Verify ORDER BY DESC + LIMIT returns exact top-2 regions.
    let top = cluster
        .sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY total DESC LIMIT 2")
        .await
        .unwrap();
    assert_eq!(top.batch.num_rows(), 2);
    assert_eq!(
        top.batch.rows[0].get("region"),
        Some(&ScalarValue::Utf8("us".into()))
    );
    assert_eq!(
        top.batch.rows[1].get("region"),
        Some(&ScalarValue::Utf8("apac".into()))
    );
}
