// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use async_trait::async_trait;
use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, InMemoryRaftCatalog, TableCommitProposal};
use rustlakedb_common::{Result, WorkerId};
use rustlakedb_coordinator::autoloader::AutoLoader;
use rustlakedb_coordinator::{Coordinator, WorkerClient};
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{RowBatch, ScalarValue, ShuffleMode, TaskCompletionReport, TaskSpec};
use rustlakedb_shuffle::HybridShuffleManager;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Helper wrapper that injects an artificial delay on a single worker to test Wave 9
/// Speculative Straggler Execution deterministically!
struct SlowStragglerWorker {
    inner: Arc<WorkerNode>,
    delay_ms: u64,
}

#[async_trait]
impl WorkerClient for SlowStragglerWorker {
    fn id(&self) -> &WorkerId {
        &self.inner.id
    }
    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }
    async fn execute_task(&self, task: TaskSpec) -> Result<TaskCompletionReport> {
        if self.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        }
        self.inner.execute_task(task).await
    }
}

#[tokio::test]
async fn wave9_vectorized_arrow_topn_executes_directly_on_columnar_batches() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());

    let schema = vec![
        FieldSchema { field_id: 1, name: "id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 3, name: "score".into(), data_type: ColumnType::Float64, nullable: false },
    ];
    catalog
        .create_table(IcebergTableMetadata::new("scores", schema.clone()))
        .await
        .unwrap();

    let mut files = Vec::new();
    for s in 0..4 {
        let mut rows = Vec::with_capacity(2_500);
        for i in 0..2_500 {
            let id = (s * 2_500 + i + 1) as i64;
            let score = (id as f64) * 1.5;
            rows.push(BTreeMap::from([
                ("id".into(), ScalarValue::Int64(id)),
                ("region".into(), ScalarValue::Utf8(format!("r{}", id % 4))),
                ("score".into(), ScalarValue::Float64(score)),
            ]));
        }
        let batch = RowBatch::new(vec!["id".into(), "region".into(), "score".into()], rows);
        let entry = store
            .write_data_file(&format!("scores/data/p-{s}.parquet"), &schema, &batch)
            .await
            .unwrap();
        files.push(entry);
    }
    catalog
        .commit_table(TableCommitProposal {
            table_name: "scores".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let w2 = Arc::new(WorkerNode::new("w2", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1.clone(), w2.clone()],
        PlannerConfig {
            default_shuffle_partitions: 2,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    );

    let res = coord
        .execute_sql(
            "SELECT id, UPPER(region) AS reg, score FROM scores ORDER BY score DESC LIMIT 5",
            "main",
        )
        .await
        .unwrap();

    assert_eq!(res.batch.num_rows(), 5);
    assert_eq!(res.batch.rows[0].get("id"), Some(&ScalarValue::Int64(10_000)));
    assert_eq!(res.batch.rows[4].get("id"), Some(&ScalarValue::Int64(9_996)));
    let vec_rows = w1.vectorized_rows_processed.load(Ordering::Relaxed)
        + w2.vectorized_rows_processed.load(Ordering::Relaxed);
    assert!(
        vec_rows >= 10_000,
        "expected Vectorized Arrow Top-N to process all 10,000 rows natively in Arrow, got {vec_rows}"
    );
    assert!(
        res.stats.total_shuffled_bytes < 4_096,
        "expected tiny Top-N shuffle (<4 KB), got {}B",
        res.stats.total_shuffled_bytes
    );
}

#[tokio::test]
async fn wave9_dbt_ddl_not_in_not_exists_and_3way_cbo_join_order() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(catalog, store, shuffle, vec![w1], PlannerConfig::default());

    // 1. dbt Schema + Idempotent IF NOT EXISTS / IF EXISTS DDL
    coord.execute_sql("CREATE SCHEMA IF NOT EXISTS analytics", "main").await.unwrap();
    coord.execute_sql("CREATE SCHEMA IF NOT EXISTS analytics", "main").await.unwrap();
    assert!(coord.governance.list_schemas().contains(&"analytics".to_string()));
    coord.execute_sql("DROP SCHEMA IF EXISTS analytics", "main").await.unwrap();
    coord.execute_sql("DROP SCHEMA IF EXISTS analytics", "main").await.unwrap();

    coord
        .execute_sql("CREATE TABLE IF NOT EXISTS stg_items (id BIGINT, name VARCHAR, cat_id BIGINT)", "main")
        .await
        .unwrap();
    // Second CREATE TABLE IF NOT EXISTS must succeed idempotently
    coord
        .execute_sql("CREATE TABLE IF NOT EXISTS stg_items (id BIGINT, name VARCHAR, cat_id BIGINT)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("INSERT INTO stg_items VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30)", "main")
        .await
        .unwrap();

    // 2. ALTER TABLE RENAME TO + TRUNCATE TABLE (preserving Time Travel!)
    coord
        .execute_sql("ALTER TABLE stg_items RENAME TO items", "main")
        .await
        .unwrap();
    let snap_before_trunc = coord
        .catalog
        .get_table("items")
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;
    coord.execute_sql("TRUNCATE TABLE items", "main").await.unwrap();
    let after_trunc = coord.execute_sql("SELECT COUNT(*) AS c FROM items", "main").await.unwrap();
    assert_eq!(after_trunc.batch.rows[0].get("c"), Some(&ScalarValue::Int64(0)));
    // Time Travel still sees pre-TRUNCATE snapshot!
    let tt = coord
        .execute_sql(&format!("SELECT COUNT(*) AS c FROM items AT VERSION {snap_before_trunc}"), "main")
        .await
        .unwrap();
    assert_eq!(tt.batch.rows[0].get("c"), Some(&ScalarValue::Int64(3)));

    // 3. Re-populate + View lifecycle with DROP VIEW IF EXISTS
    coord
        .execute_sql("INSERT INTO items VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30)", "main")
        .await
        .unwrap();
    coord
        .execute_sql("CREATE VIEW v_items AS SELECT id, name FROM items", "main")
        .await
        .unwrap();
    let vr = coord.execute_sql("SELECT COUNT(*) AS c FROM v_items", "main").await.unwrap();
    assert_eq!(vr.batch.rows[0].get("c"), Some(&ScalarValue::Int64(3)));
    coord.execute_sql("DROP VIEW IF EXISTS v_items", "main").await.unwrap();
    coord.execute_sql("DROP VIEW IF EXISTS v_items", "main").await.unwrap();

    // 4. NOT IN (SELECT ...) & NOT EXISTS (SELECT ...) subqueries
    coord.execute_sql("CREATE TABLE blocked (id BIGINT)", "main").await.unwrap();
    coord.execute_sql("INSERT INTO blocked VALUES (2)", "main").await.unwrap();
    let not_in = coord
        .execute_sql("SELECT id FROM items WHERE id NOT IN (SELECT id FROM blocked) ORDER BY id", "main")
        .await
        .unwrap();
    assert_eq!(not_in.batch.num_rows(), 2);
    assert_eq!(not_in.batch.rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(not_in.batch.rows[1].get("id"), Some(&ScalarValue::Int64(3)));

    let not_ex = coord
        .execute_sql("SELECT COUNT(*) AS c FROM items WHERE NOT EXISTS (SELECT id FROM blocked WHERE id = 999)", "main")
        .await
        .unwrap();
    assert_eq!(not_ex.batch.rows[0].get("c"), Some(&ScalarValue::Int64(3)));

    // 5. 3-Table CBO Join Ordering (joins larger table first in SQL; CBO reorders smallest dim first!)
    coord.execute_sql("CREATE TABLE fact_t (fid BIGINT, mid BIGINT, smid BIGINT)", "main").await.unwrap();
    coord.execute_sql("CREATE TABLE med_t (mid BIGINT, mlabel VARCHAR)", "main").await.unwrap();
    coord.execute_sql("CREATE TABLE small_t (smid BIGINT, slabel VARCHAR)", "main").await.unwrap();
    let med_vals: Vec<String> = (1..=40).map(|i| format!("({i}, 'med_payload_text_padding_{i:04}')")).collect();
    coord.execute_sql(&format!("INSERT INTO med_t VALUES {}", med_vals.join(",")), "main").await.unwrap();
    coord.execute_sql("INSERT INTO small_t VALUES (1, 'S1')", "main").await.unwrap();
    coord.execute_sql("INSERT INTO fact_t VALUES (100, 1, 1), (101, 2, 1)", "main").await.unwrap();

    let explain = coord
        .execute_sql(
            "EXPLAIN SELECT fact_t.fid, med_t.mlabel, small_t.slabel FROM fact_t INNER JOIN med_t ON fact_t.mid = med_t.mid INNER JOIN small_t ON fact_t.smid = small_t.smid",
            "main",
        )
        .await
        .unwrap();
    let plan_txt: String = explain
        .batch
        .rows
        .iter()
        .filter_map(|r| r.get("QUERY PLAN").and_then(|v| v.to_pg_text()))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_txt.contains("reordered 3-way INNER JOIN"),
        "expected 3-way CBO join reorder note in EXPLAIN:\n{plan_txt}"
    );
    let jres = coord
        .execute_sql(
            "SELECT fact_t.fid, small_t.slabel FROM fact_t INNER JOIN med_t ON fact_t.mid = med_t.mid INNER JOIN small_t ON fact_t.smid = small_t.smid ORDER BY fid",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(jres.batch.num_rows(), 2);

    coord.execute_sql("DROP TABLE IF EXISTS nonexistent_table_xyz", "main").await.unwrap();
}

#[tokio::test]
async fn wave9_autoloader_exactly_once_and_dlq_plus_ttl_eviction_and_speculative_hedge() {
    let dir = tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path().join("wh")).unwrap());
    let shuffle = Arc::new(HybridShuffleManager::new(dir.path().join("shuf")).unwrap());

    let slow_node = Arc::new(WorkerNode::new("slow-w1", catalog.clone(), shuffle.clone(), store.clone()));
    let fast_node = Arc::new(WorkerNode::new("fast-w2", catalog.clone(), shuffle.clone(), store.clone()));

    let slow_client: Arc<dyn WorkerClient> = Arc::new(SlowStragglerWorker {
        inner: slow_node,
        delay_ms: 400,
    });
    let fast_client: Arc<dyn WorkerClient> = Arc::new(SlowStragglerWorker {
        inner: fast_node,
        delay_ms: 0,
    });

    let coord = Arc::new(Coordinator::with_clients(
        catalog,
        store,
        shuffle,
        vec![slow_client, fast_client],
        PlannerConfig::default(),
    ));
    coord
        .execute_sql("CREATE TABLE stream_events (id BIGINT, kind VARCHAR)", "main")
        .await
        .unwrap();

    // 1. Auto-Loader exactly-once ingestion across NDJSON (with 1 corrupt line -> DLQ) + CSV!
    let landing = dir.path().join("landing");
    let cp_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&landing).unwrap();
    std::fs::write(
        landing.join("001_batch.ndjson"),
        "{\"id\":1,\"kind\":\"click\"}\n{bad_json_line\n{\"id\":2,\"kind\":\"buy\"}\n",
    )
    .unwrap();
    std::fs::write(landing.join("002_batch.csv"), "id,kind\n3,signup\n4,click\n").unwrap();

    let loader = AutoLoader::new(coord.clone(), "stream_events", &landing, &cp_dir).unwrap();
    let rep1 = loader.run_once().await.unwrap();
    assert_eq!(rep1.files_ingested, 2);
    assert_eq!(rep1.rows_loaded, 4);
    assert_eq!(rep1.dlq_rows, 1);

    // Re-running Auto-Loader must be strictly idempotent (0 duplicate rows!)
    let rep2 = loader.run_once().await.unwrap();
    assert_eq!(rep2.files_ingested, 0);
    assert_eq!(rep2.rows_loaded, 0);

    // 2. Speculative Straggler Execution: set threshold to 30ms so any task landing on slow-w1 (400ms)
    // immediately hedges onto fast-w2 and finishes far faster than 400ms!
    coord.speculative_threshold_ms.store(30, Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    let res = coord
        .execute_sql("SELECT COUNT(*) AS c FROM stream_events", "main")
        .await
        .unwrap();
    let elapsed_ms = t0.elapsed().as_millis();
    assert_eq!(res.batch.rows[0].get("c"), Some(&ScalarValue::Int64(4)));
    assert!(
        res.stats.speculative_hedges >= 1,
        "expected speculative straggler hedge to trigger on slow-w1, got {:?}",
        res.stats
    );
    assert!(
        elapsed_ms < 300,
        "expected hedged query to beat 400ms straggler, took {elapsed_ms}ms"
    );

    // Verify DLQ table captured the bad NDJSON row!
    let dlq = coord
        .execute_sql("SELECT COUNT(*) AS c FROM stream_events_dlq", "main")
        .await
        .unwrap();
    assert_eq!(dlq.batch.rows[0].get("c"), Some(&ScalarValue::Int64(1)));

    // 3. Dynamic Worker Heartbeat TTL Eviction
    let count_after_reg = coord.register_worker("ephemeral-w9", "http://127.0.0.1:19999");
    assert_eq!(count_after_reg, 3);
    // Simulate time advancing +60s past a 30s TTL
    let evicted = coord.evict_stale_workers_at(u64::MAX / 2, 30_000);
    assert_eq!(evicted, 1);
    assert_eq!(coord.workers_snapshot().len(), 2);
}
