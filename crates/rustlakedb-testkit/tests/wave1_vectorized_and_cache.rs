// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, ColumnType, FieldSchema, IcebergTableMetadata, TableCommitProposal};
use rustlakedb_coordinator::{Coordinator, HttpWorkerClient, WorkerClient};
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{RowBatch, ScalarValue, ShuffleMode};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_testkit::TestCluster;
use rustlakedb_worker::rpc::WorkerHttpServer;
use rustlakedb_worker::WorkerNode;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

async fn seed_splits(cluster: &TestCluster, table: &str, rows: usize, splits: usize) {
    let schema = vec![
        FieldSchema {
            field_id: 1,
            name: "order_id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        },
        FieldSchema {
            field_id: 2,
            name: "customer_id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        },
        FieldSchema {
            field_id: 3,
            name: "region".into(),
            data_type: ColumnType::Utf8,
            nullable: false,
        },
        FieldSchema {
            field_id: 4,
            name: "amount".into(),
            data_type: ColumnType::Float64,
            nullable: false,
        },
        FieldSchema {
            field_id: 5,
            name: "payload".into(),
            data_type: ColumnType::Variant,
            nullable: true,
        },
    ];
    let meta = IcebergTableMetadata::new(table, schema.clone());
    cluster.catalog.create_table(meta).await.unwrap();

    let regions = ["us-east", "us-west", "eu-central", "ap-south"];
    let channels = ["web", "mobile", "api", "partner"];
    let cols: Vec<String> = schema.iter().map(|f| f.name.clone()).collect();
    let per_split = rows / splits;
    let mut added = Vec::with_capacity(splits);

    for s in 0..splits {
        let mut batch_rows = Vec::with_capacity(per_split);
        for i in 0..per_split {
            let oid = (s * per_split + i + 1) as i64;
            let cid = (oid % 500) + 1;
            let region = regions[(oid as usize) % regions.len()];
            let channel = channels[(oid as usize) % channels.len()];
            let amount = ((oid % 100) + 1) as f64 * 10.0;
            let payload = serde_json::json!({
                "channel": channel,
                "device": if oid % 2 == 0 { "ios" } else { "android" }
            });
            let mut row = BTreeMap::new();
            row.insert("order_id".into(), ScalarValue::Int64(oid));
            row.insert("customer_id".into(), ScalarValue::Int64(cid));
            row.insert("region".into(), ScalarValue::Utf8(region.into()));
            row.insert("amount".into(), ScalarValue::Float64(amount));
            row.insert("payload".into(), ScalarValue::Json(payload));
            batch_rows.push(row);
        }
        let batch = RowBatch::new(cols.clone(), batch_rows);
        let path = format!("{table}/data/part-{s:04}.parquet");
        let entry = cluster
            .store
            .write_data_file(&path, &schema, &batch)
            .await
            .unwrap();
        added.push(entry);
    }

    let snap_id = cluster
        .catalog
        .get_table(table)
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: table.into(),
            branch: "main".into(),
            expected_snapshot_id: snap_id,
            added_files: added,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_wave1_vectorized_arrow_and_shredded_json_fast_path() {
    let cluster = TestCluster::start(4, ShuffleMode::Staged).unwrap();
    seed_splits(&cluster, "orders_w1", 20_000, 8).await;

    // Seed small customers dimension table to verify projection pushdown across JOINs.
    let cust_schema = vec![
        FieldSchema {
            field_id: 1,
            name: "customer_id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        },
        FieldSchema {
            field_id: 2,
            name: "tier".into(),
            data_type: ColumnType::Utf8,
            nullable: false,
        },
    ];
    cluster
        .catalog
        .create_table(IcebergTableMetadata::new("customers_w1", cust_schema.clone()))
        .await
        .unwrap();
    let tiers = ["enterprise", "pro", "starter"];
    let cust_rows: Vec<BTreeMap<String, ScalarValue>> = (1..=500i64)
        .map(|cid| {
            let mut m = BTreeMap::new();
            m.insert("customer_id".into(), ScalarValue::Int64(cid));
            m.insert("tier".into(), ScalarValue::Utf8(tiers[(cid as usize) % 3].into()));
            m
        })
        .collect();
    let cust_entry = cluster
        .store
        .write_data_file(
            "customers_w1/data/part-0000.parquet",
            &cust_schema,
            &RowBatch::new(vec!["customer_id".into(), "tier".into()], cust_rows),
        )
        .await
        .unwrap();
    let csnap = cluster
        .catalog
        .get_table("customers_w1")
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;
    cluster
        .catalog
        .commit_table(TableCommitProposal {
            table_name: "customers_w1".into(),
            branch: "main".into(),
            expected_snapshot_id: csnap,
            added_files: vec![cust_entry],
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    // 1. Vectorized Arrow GROUP BY region (COUNT, SUM, AVG, MIN, MAX)
    let res1 = cluster
        .sql("SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_a, MIN(amount) AS min_a, MAX(amount) AS max_a FROM orders_w1 GROUP BY region ORDER BY region")
        .await
        .unwrap();
    assert_eq!(res1.batch.num_rows(), 4);
    let total_cnt: i64 = res1
        .batch
        .rows
        .iter()
        .map(|r| match r.get("cnt") {
            Some(ScalarValue::Int64(n)) => *n,
            _ => 0,
        })
        .sum();
    assert_eq!(total_cnt, 20_000);

    // 2. Vectorized Shredded-JSON Zero-Parse GROUP BY (reads `__shred_payload_channel` directly!)
    let res2 = cluster
        .sql("SELECT payload->>'channel' AS channel, COUNT(*) AS cnt, SUM(amount) AS rev FROM orders_w1 GROUP BY payload->>'channel' ORDER BY channel")
        .await
        .unwrap();
    assert_eq!(res2.batch.num_rows(), 4);
    let json_cnt: i64 = res2
        .batch
        .rows
        .iter()
        .map(|r| match r.get("cnt") {
            Some(ScalarValue::Int64(n)) => *n,
            _ => 0,
        })
        .sum();
    assert_eq!(json_cnt, 20_000);

    // 3. Global COUNT(*) fast path (zero columns projected beyond column 0)
    let res3 = cluster
        .sql("SELECT COUNT(*) AS total FROM orders_w1")
        .await
        .unwrap();
    assert_eq!(res3.batch.rows[0].get("total"), Some(&ScalarValue::Int64(20_000)));

    let vec_rows: u64 = cluster
        .workers
        .iter()
        .map(|w| w.vectorized_rows_processed.load(Ordering::Relaxed))
        .sum();
    println!("Wave 1 vectorized rows processed across 4 workers: {vec_rows}");
    assert!(
        vec_rows >= 60_000,
        "expected >= 60,000 vectorized rows across 3 queries, got {vec_rows}"
    );

    // 4. Projection pushdown across distributed JOIN + GROUP BY
    let join_res = cluster
        .sql("SELECT c.tier, COUNT(*) AS cnt, SUM(o.amount) AS rev FROM orders_w1 o JOIN customers_w1 c ON o.customer_id = c.customer_id GROUP BY c.tier ORDER BY c.tier")
        .await
        .unwrap();
    assert_eq!(join_res.batch.num_rows(), 3);
    let join_cnt: i64 = join_res
        .batch
        .rows
        .iter()
        .map(|r| match r.get("cnt") {
            Some(ScalarValue::Int64(n)) => *n,
            _ => 0,
        })
        .sum();
    assert_eq!(join_cnt, 20_000);

    // 5. Verify vectorized path strictly obeys Iceberg v3 Deletion Vectors!
    let del = cluster
        .sql("DELETE FROM orders_w1 WHERE order_id <= 200")
        .await
        .unwrap();
    assert_eq!(del.command_tag, "DELETE 200");
    let after_del = cluster
        .sql("SELECT COUNT(*) AS total FROM orders_w1")
        .await
        .unwrap();
    assert_eq!(
        after_del.batch.rows[0].get("total"),
        Some(&ScalarValue::Int64(19_800))
    );
}

#[tokio::test]
async fn test_wave1_hybrid_ram_and_nvme_cache_hits() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    seed_splits(&cluster, "cached_orders", 4_000, 4).await;

    // Fresh store over the same warehouse directory has empty RAM cache,
    // but `_nvme_cache` already holds the written Parquet files!
    let cold_ram_store = IcebergObjectStore::local_fs(cluster.data_dir_path()).unwrap();
    let (batches, _dv) = cold_ram_store
        .read_arrow_batches("cached_orders/data/part-0000.parquet", None)
        .await
        .unwrap();
    assert!(!batches.is_empty());
    assert_eq!(
        cold_ram_store.cache_hits.load(Ordering::Relaxed),
        1,
        "expected NVMe disk cache hit on cold RAM restart"
    );
    assert_eq!(
        cold_ram_store.object_store_gets.load(Ordering::Relaxed),
        0,
        "expected 0 object_store GETs when NVMe cache serves the Parquet file"
    );

    // Repeated cluster query hits RAM cache with ZERO additional object_store GETs!
    let _ = cluster
        .sql("SELECT region, COUNT(*) AS cnt FROM cached_orders GROUP BY region")
        .await
        .unwrap();
    let gets_after_q1 = cluster.store.object_store_gets.load(Ordering::Relaxed);
    let hits_after_q1 = cluster.store.cache_hits.load(Ordering::Relaxed);

    let _ = cluster
        .sql("SELECT region, SUM(amount) AS rev FROM cached_orders GROUP BY region")
        .await
        .unwrap();
    let gets_after_q2 = cluster.store.object_store_gets.load(Ordering::Relaxed);
    let hits_after_q2 = cluster.store.cache_hits.load(Ordering::Relaxed);

    assert_eq!(
        gets_after_q2, gets_after_q1,
        "expected ZERO additional object_store GETs on warm cache query"
    );
    assert!(
        hits_after_q2 > hits_after_q1,
        "expected cache_hits to increase ({hits_after_q1} -> {hits_after_q2})"
    );
}

#[tokio::test]
async fn test_wave1_subsecond_failover_circuit_breaker() {
    let data_dir = tempfile::tempdir().unwrap();
    let shuffle_dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(rustlakedb_catalog::InMemoryRaftCatalog::open(data_dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(data_dir.path()).unwrap());
    let coord_shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(shuffle_dir.path()).unwrap());

    // Bind 2 healthy HTTP workers + 1 dead worker (a listener we immediately drop so TCP rejects/fails fast!)
    let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);

    let mut clients: Vec<Arc<dyn WorkerClient>> = vec![Arc::new(HttpWorkerClient::new(
        "dead-worker-1",
        format!("http://{dead_addr}"),
    ))];

    for i in 2..=3 {
        let priv_dir = shuffle_dir.path().join(format!("w{i}"));
        let w_shuf = Arc::new(HybridShuffleManager::new(priv_dir).unwrap());
        let worker = Arc::new(WorkerNode::new(
            format!("live-worker-{i}"),
            catalog.clone(),
            w_shuf.clone(),
            store.clone(),
        ));
        let srv = WorkerHttpServer::bind("127.0.0.1:0", worker).await.unwrap();
        let addr = srv.local_addr().unwrap();
        w_shuf.set_advertise_url(format!("http://{addr}"));
        tokio::spawn(async move {
            let _ = srv.serve().await;
        });
        clients.push(Arc::new(HttpWorkerClient::new(
            format!("live-worker-{i}"),
            format!("http://{addr}"),
        )));
    }

    let coordinator = Coordinator::with_clients(
        catalog.clone(),
        store.clone(),
        coord_shuffle,
        clients.clone(),
        PlannerConfig {
            default_shuffle_partitions: 3,
            shuffle_mode: ShuffleMode::Staged,
            broadcast_join_threshold_bytes: 0,
            ..Default::default()
        },
    );

    // Seed 12 splits (6,000 rows)
    let schema = vec![
        FieldSchema {
            field_id: 1,
            name: "id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        },
        FieldSchema {
            field_id: 2,
            name: "region".into(),
            data_type: ColumnType::Utf8,
            nullable: false,
        },
        FieldSchema {
            field_id: 3,
            name: "amount".into(),
            data_type: ColumnType::Float64,
            nullable: false,
        },
    ];
    catalog
        .create_table(IcebergTableMetadata::new("failover_t", schema.clone()))
        .await
        .unwrap();
    let mut added = Vec::new();
    for s in 0..12 {
        let rows: Vec<BTreeMap<String, ScalarValue>> = (0..500)
            .map(|i| {
                let id = (s * 500 + i + 1) as i64;
                BTreeMap::from([
                    ("id".into(), ScalarValue::Int64(id)),
                    (
                        "region".into(),
                        ScalarValue::Utf8(if id % 2 == 0 { "us" } else { "eu" }.into()),
                    ),
                    ("amount".into(), ScalarValue::Float64(10.0)),
                ])
            })
            .collect();
        let entry = store
            .write_data_file(
                &format!("failover_t/data/part-{s:02}.parquet"),
                &schema,
                &RowBatch::new(vec!["id".into(), "region".into(), "amount".into()], rows),
            )
            .await
            .unwrap();
        added.push(entry);
    }
    let snap = catalog
        .get_table("failover_t")
        .await
        .unwrap()
        .current_snapshot("main")
        .unwrap()
        .snapshot_id;
    catalog
        .commit_table(TableCommitProposal {
            table_name: "failover_t".into(),
            branch: "main".into(),
            expected_snapshot_id: snap,
            added_files: added,
            removed_files: vec![],
            is_append_only: true,
        })
        .await
        .unwrap();

    let t0 = Instant::now();
    let res = coordinator
        .execute_sql(
            "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total FROM failover_t GROUP BY region ORDER BY region",
            "main",
        )
        .await
        .unwrap();
    let elapsed_ms = t0.elapsed().as_millis();
    println!("Wave 1 dead-worker failover across 12 splits completed in {elapsed_ms} ms (retries={})", res.stats.tasks_retried);

    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(res.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(3_000)));
    assert_eq!(res.batch.rows[1].get("cnt"), Some(&ScalarValue::Int64(3_000)));
    assert!(
        !clients[0].is_healthy(),
        "dead worker's circuit breaker must be open after failure"
    );
    assert!(
        elapsed_ms < 2500,
        "expected fast circuit-breaker failover (<2500ms), took {elapsed_ms}ms"
    );

    // Follow-up query must skip the tripped worker with ZERO retries!
    let t1 = Instant::now();
    let res2 = coordinator
        .execute_sql("SELECT COUNT(*) AS cnt FROM failover_t", "main")
        .await
        .unwrap();
    let follow_ms = t1.elapsed().as_millis();
    assert_eq!(res2.batch.rows[0].get("cnt"), Some(&ScalarValue::Int64(6_000)));
    assert_eq!(
        res2.stats.tasks_retried, 0,
        "follow-up query should skip dead worker immediately via circuit breaker"
    );
    println!("Wave 1 follow-up query with open circuit breaker completed in {follow_ms} ms (0 retries)");
}
