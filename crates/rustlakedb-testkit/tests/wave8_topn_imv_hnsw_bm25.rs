// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, TableCommitProposal};
use rustlakedb_protocol::{RowBatch, ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;
use std::collections::BTreeMap;

#[tokio::test]
async fn test_wave8_distributed_topn_pushdown_minimizes_shuffle() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    let schema = vec![
        FieldSchema { field_id: 1, name: "id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "score".into(), data_type: ColumnType::Float64, nullable: false },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("scores_topn", schema.clone()))
        .await
        .unwrap();

    // Seed 16,000 rows across 8 splits
    let mut entries = Vec::new();
    for s in 0..8usize {
        let rows: Vec<BTreeMap<String, ScalarValue>> = (0..2_000usize)
            .map(|i| {
                let id = (s * 2_000 + i + 1) as i64;
                BTreeMap::from([
                    ("id".into(), ScalarValue::Int64(id)),
                    ("score".into(), ScalarValue::Float64((id as f64) * 1.5)),
                ])
            })
            .collect();
        let entry = cluster
            .store
            .write_data_file(
                &format!("scores_topn/data/part-{s:02}.parquet"),
                &schema,
                &RowBatch::new(vec!["id".into(), "score".into()], rows),
            )
            .await
            .unwrap();
        entries.push(entry);
    }
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "scores_topn".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: entries,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    // Verify EXPLAIN notes Top-N pushdown AND shuffled bytes across 8 splits is tiny (only 8 x 5 = 40 rows)!
    let exp = cluster
        .sql("EXPLAIN SELECT id, score FROM scores_topn ORDER BY score DESC LIMIT 5")
        .await
        .unwrap();
    let has_topn_note = exp.batch.rows.iter().any(|r| {
        r.get("QUERY PLAN")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.contains("Distributed Top-N Pushdown"))
    });
    assert!(has_topn_note, "expected Distributed Top-N Pushdown CBO note");

    let res = cluster
        .sql("SELECT id, score FROM scores_topn ORDER BY score DESC LIMIT 5")
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 5);
    assert_eq!(res.batch.rows[0].get("id"), Some(&ScalarValue::Int64(16_000)));
    assert_eq!(res.batch.rows[4].get("id"), Some(&ScalarValue::Int64(15_996)));
    println!(
        "Wave 8 Distributed Top-N over 16,000 rows (8 splits) shuffled only {} bytes!",
        res.stats.total_shuffled_bytes
    );
    assert!(
        res.stats.total_shuffled_bytes < 10_000,
        "Top-N pushdown must shuffle < 10 KB, shuffled {} B",
        res.stats.total_shuffled_bytes
    );
}

#[tokio::test]
async fn test_wave8_incremental_materialized_view_refresh() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE raw_clicks (click_id BIGINT, page TEXT)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO raw_clicks VALUES (1, '/home'), (2, '/pricing')")
        .await
        .unwrap();

    cluster
        .sql("CREATE MATERIALIZED VIEW mv_clicks AS SELECT click_id, page FROM raw_clicks")
        .await
        .unwrap();
    let c1 = cluster.sql("SELECT COUNT(*) AS c FROM mv_clicks").await.unwrap();
    assert_eq!(c1.batch.rows[0].get("c"), Some(&ScalarValue::Int64(2)));

    // Append new Parquet split to base table -> REFRESH MATERIALIZED VIEW runs INCREMENTALLY!
    cluster
        .sql("INSERT INTO raw_clicks VALUES (3, '/docs'), (4, '/blog'), (5, '/pricing')")
        .await
        .unwrap();
    let ref_res = cluster
        .sql("REFRESH MATERIALIZED VIEW mv_clicks")
        .await
        .unwrap();
    assert!(
        ref_res.command_tag.contains("INCREMENTAL (+3 rows, 1 delta files)"),
        "expected incremental delta refresh, got: {}",
        ref_res.command_tag
    );

    let c2 = cluster.sql("SELECT COUNT(*) AS c FROM mv_clicks").await.unwrap();
    assert_eq!(c2.batch.rows[0].get("c"), Some(&ScalarValue::Int64(5)));
}

#[tokio::test]
async fn test_wave8_hnsw_vector_index_and_bm25_fulltext_hybrid_search() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE kb_docs (doc_id BIGINT, body TEXT, embedding JSONB)")
        .await
        .unwrap();
    cluster
        .sql(
            r#"INSERT INTO kb_docs VALUES
               (1, 'Apache Iceberg v3 lakehouse storage with Rust Parquet and Puffin indexes', '[0.95, 0.05, 0.10]'),
               (2, 'Distributed Rust SQL execution with vectorized Arrow joins', '[0.80, 0.20, 0.15]'),
               (3, 'French pastry baking recipes for croissants and sourdough', '[0.05, 0.95, 0.10]')"#,
        )
        .await
        .unwrap();

    // 1. Build HNSW Vector Index persisted as `.hnsw.json` Puffin sidecar blob!
    cluster
        .sql("CREATE INDEX idx_kb_emb ON kb_docs USING HNSW (embedding)")
        .await
        .unwrap();
    let meta = cluster.catalog.get_table("kb_docs").await.unwrap();
    let first_file = &meta.current_snapshot("main").unwrap().data_files[0].file_path;
    let hnsw = cluster
        .store
        .read_hnsw_index(first_file)
        .await
        .unwrap()
        .expect("HNSW sidecar graph must be persisted");
    let ann = hnsw.search_cosine(&[1.0, 0.0, 0.0], 2);
    assert_eq!(ann.len(), 2);
    assert_eq!(ann[0].0, 0, "row 0 (Iceberg Rust doc) must be top-1 HNSW neighbor");

    // 2. Hybrid Full-Text BM25 + Vector Cosine SQL Search!
    let hybrid = cluster
        .sql(
            "SELECT doc_id, BM25_SCORE(body, 'iceberg rust puffin') AS bm25, \
             VECTOR_COSINE(embedding, '[1.0, 0.0, 0.0]') AS vec_sim \
             FROM kb_docs ORDER BY bm25 DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(hybrid.batch.num_rows(), 2);
    assert_eq!(hybrid.batch.rows[0].get("doc_id"), Some(&ScalarValue::Int64(1)));
    let top_bm25 = hybrid.batch.rows[0]
        .get("bm25")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert!(top_bm25 > 3.0, "expected strong BM25 score for matching doc, got {top_bm25}");
}
