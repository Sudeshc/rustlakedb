// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::Catalog;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn test_wave4_liquid_clustering_zonemap_pruning_and_dynamic_json_shredding() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE telemetry (id BIGINT, region TEXT, latency DOUBLE, payload JSONB)")
        .await
        .unwrap();

    // Insert 40 unsorted rows across regions & nested JSON paths
    let mut values = Vec::new();
    for i in 1..=40i64 {
        let region = match i % 4 {
            0 => "ap-south",
            1 => "eu-west",
            2 => "sa-east",
            _ => "us-east",
        };
        let latency = (i * 10) as f64;
        values.push(format!(
            r#"({i}, '{region}', {latency}, '{{"meta": {{"env": "prod", "tier": "{region}"}}}}')"#
        ));
    }
    cluster
        .sql(&format!("INSERT INTO telemetry VALUES {}", values.join(", ")))
        .await
        .unwrap();

    // Query a nested JSON path so Coordinator's hot-path tracker records `payload.meta.env`!
    let pre = cluster
        .sql("SELECT COUNT(*) AS cnt FROM telemetry WHERE payload->>'meta.env' = 'prod'")
        .await
        .unwrap();
    assert_eq!(pre.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(40)));

    // Enable Liquid / Multi-Column Clustering & run OPTIMIZE TABLE!
    let alt = cluster
        .sql("ALTER TABLE telemetry CLUSTER BY (region, latency)")
        .await
        .unwrap();
    assert!(alt.command_tag.contains("CLUSTER BY (region,latency)"));

    let opt = cluster.sql("OPTIMIZE TABLE telemetry").await.unwrap();
    assert!(
        opt.command_tag.contains("clustered_by=(region,latency)"),
        "unexpected OPTIMIZE tag: {}",
        opt.command_tag
    );

    // Verify 4 clustered files exist with tight non-overlapping zone maps AND promoted shredded nested path!
    let meta = cluster.catalog.get_table("telemetry").await.unwrap();
    let snap = meta.current_snapshot("main").unwrap();
    assert_eq!(snap.data_files.len(), 4);
    assert!(
        meta.json_shredding
            .shredded_paths
            .contains(&"payload.meta.env".to_string()),
        "expected dynamic JSON re-shredding to promote nested path payload.meta.env"
    );

    // Query a single region -> Zone-Map pruning skips 3 out of 4 clustered Parquet files!
    let zm_before = cluster.store.files_pruned_by_zonemap.load(Ordering::Relaxed);
    let q = cluster
        .sql("SELECT id, region, latency FROM telemetry WHERE region = 'ap-south' ORDER BY latency")
        .await
        .unwrap();
    let zm_after = cluster.store.files_pruned_by_zonemap.load(Ordering::Relaxed);
    assert_eq!(q.batch.num_rows(), 10);
    assert!(
        zm_after - zm_before >= 3,
        "expected Liquid Clustering zone-maps to prune >= 3 of 4 files, pruned {}",
        zm_after - zm_before
    );
}

#[tokio::test]
async fn test_wave4_hybrid_vector_ai_search_and_wlm() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE docs (doc_id BIGINT, category TEXT, title TEXT, embedding JSONB)")
        .await
        .unwrap();
    cluster
        .sql(
            r#"INSERT INTO docs VALUES
               (1, 'db', 'Apache Iceberg v3 Guide', '[0.95, 0.05, 0.10]'),
               (2, 'db', 'Distributed Hash Joins in Rust', '[0.85, 0.15, 0.20]'),
               (3, 'cooking', 'Sourdough Bread Recipe', '[0.05, 0.95, 0.10]'),
               (4, 'ai', 'Vector Search with Puffin', '[0.60, 0.10, 0.90]')"#,
        )
        .await
        .unwrap();

    // 1. Cosine similarity top-2 search
    let cos = cluster
        .sql("SELECT doc_id, title, VECTOR_COSINE(embedding, '[1.0, 0.0, 0.0]') AS sim FROM docs ORDER BY sim DESC LIMIT 2")
        .await
        .unwrap();
    assert_eq!(cos.batch.num_rows(), 2);
    assert_eq!(cos.batch.rows[0].get("doc_id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(cos.batch.rows[1].get("doc_id"), Some(&ScalarValue::Int64(2)));

    // 2. Hybrid structured filter + L2 distance (`VECTOR_L2`)
    let l2 = cluster
        .sql("SELECT doc_id, title, VECTOR_L2(embedding, '[0.0, 1.0, 0.0]') AS dist FROM docs ORDER BY dist ASC LIMIT 1")
        .await
        .unwrap();
    assert_eq!(l2.batch.num_rows(), 1);
    assert_eq!(l2.batch.rows[0].get("doc_id"), Some(&ScalarValue::Int64(3)));

    // 3. WLM SHOW QUERIES works cleanly
    let wlm = cluster.sql("SHOW QUERIES").await.unwrap();
    assert_eq!(wlm.command_tag, "SHOW QUERIES");
}
