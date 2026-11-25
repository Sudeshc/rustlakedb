// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{JoinType, SortKey};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::columnar::{
    eval_columnar_hash_join, eval_columnar_hash_join_with_spill, eval_columnar_sort,
    eval_columnar_sort_with_spill,
};
use rustlakedb_worker::WorkerNode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn wave34_nvme_arrow_spill_sort_and_grace_hash_join_parity() {
    let spill_dir = TempDir::new().unwrap();

    // 1. Build 12,000-row Arrow RecordBatch with unsorted keys for multi-key Sort.
    let n = 12_000usize;
    let ids: Vec<i64> = (0..n as i64).map(|i| (i * 7919) % 1000).collect();
    let vals: Vec<f64> = (0..n).map(|i| ((n - i) as f64) * 1.25).collect();
    let tags: Vec<String> = (0..n).map(|i| format!("grp_{}", i % 17)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Float64, true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(vals)),
            Arc::new(StringArray::from(tags)),
        ],
    )
    .unwrap();

    let keys = vec![
        SortKey {
            column: "id".into(),
            descending: false,
        },
        SortKey {
            column: "val".into(),
            descending: true,
        },
    ];

    let in_mem_sorted = eval_columnar_sort(&rb, &keys).unwrap();
    // Force NVMe spilling with a tiny 32 KiB RAM cap!
    let (spilled_sorted, sort_metrics) =
        eval_columnar_sort_with_spill(&rb, &keys, 32 * 1024, spill_dir.path()).unwrap();

    assert!(
        sort_metrics.spill_files_created >= 2,
        "expected >=2 sorted Arrow runs spilled to NVMe, got {:?}",
        sort_metrics
    );
    assert!(sort_metrics.spill_bytes_written > 0);
    assert_eq!(in_mem_sorted.num_rows(), spilled_sorted.num_rows());
    assert_eq!(
        in_mem_sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        spilled_sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
    );
    assert_eq!(
        in_mem_sorted
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        spilled_sorted
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
    );
    // Verify RAII guard deleted all temporary NVMe spill files!
    let leftover = std::fs::read_dir(spill_dir.path()).unwrap().count();
    assert_eq!(leftover, 0, "expected 0 leftover spill files after sort");

    // 2. Grace Hash Join with NVMe partition spilling (Inner & Left outer parity).
    let dim_n = 1_500usize;
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("cust_id", DataType::Int64, true),
        Field::new("tier", DataType::Utf8, true),
    ]));
    let dim_rb = RecordBatch::try_new(
        dim_schema,
        vec![
            Arc::new(Int64Array::from((0..dim_n as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..dim_n)
                    .map(|i| format!("tier_{}", i % 5))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let fact_n = 10_000usize;
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("c_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    // Include unmatched keys (>= 1500) so Left Join verifies unmatched rows across spilled partitions!
    let fact_rb = RecordBatch::try_new(
        fact_schema,
        vec![
            Arc::new(Int64Array::from((0..fact_n as i64).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                (0..fact_n as i64).map(|i| i % 2000).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..fact_n).map(|i| (i as f64) * 0.5).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    for jt in [JoinType::Inner, JoinType::Left] {
        let in_mem = eval_columnar_hash_join(&fact_rb, &dim_rb, "c_id", "cust_id", jt).unwrap();
        let (spilled, join_metrics) = eval_columnar_hash_join_with_spill(
            &fact_rb,
            &dim_rb,
            "c_id",
            "cust_id",
            jt,
            32 * 1024,
            spill_dir.path(),
        )
        .unwrap();

        assert!(
            join_metrics.spill_files_created >= 4,
            "expected >=4 hash-partition spill files for {:?}, got {:?}",
            jt,
            join_metrics
        );
        assert!(join_metrics.spill_bytes_written > 0);
        assert_eq!(
            in_mem.num_rows(),
            spilled.num_rows(),
            "row count mismatch for {:?}",
            jt
        );
        let in_mem_sum: f64 = in_mem
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten()
            .sum();
        let spilled_sum: f64 = spilled
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten()
            .sum();
        assert!((in_mem_sum - spilled_sum).abs() < 1e-6);
        assert_eq!(std::fs::read_dir(spill_dir.path()).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn wave34_distributed_worker_low_ram_arrow_spill_e2e() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("shuffle")).unwrap());

    // Tiny 8 KiB worker memory budget forces out-of-core Arrow NVMe spilling end-to-end!
    let w1 = Arc::new(WorkerNode::with_memory_limit(
        "w-spill-1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
        8 * 1024,
    ));
    let w2 = Arc::new(WorkerNode::with_memory_limit(
        "w-spill-2",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
        8 * 1024,
    ));
    let coord = Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1.clone(), w2.clone()],
        PlannerConfig::default(),
    );

    coord
        .execute_sql(
            "CREATE TABLE spill_orders (id BIGINT, cid BIGINT, amount DOUBLE)",
            "main",
        )
        .await
        .unwrap();
    coord
        .execute_sql(
            "CREATE TABLE spill_customers (cid BIGINT, region VARCHAR)",
            "main",
        )
        .await
        .unwrap();

    // Insert enough rows (~600 orders, 100 customers) to exceed the 8 KiB cap across Arrow shuffle!
    let mut cust_vals = Vec::new();
    for c in 1..=100 {
        cust_vals.push(format!("({c}, 'reg_{}')", c % 5));
    }
    coord
        .execute_sql(
            &format!("INSERT INTO spill_customers VALUES {}", cust_vals.join(",")),
            "main",
        )
        .await
        .unwrap();

    for chunk in 0..3 {
        let mut vals = Vec::new();
        for i in 1..=200 {
            let id = chunk * 200 + i;
            let cid = (id % 100) + 1;
            vals.push(format!("({id}, {cid}, {}.5)", id * 3));
        }
        coord
            .execute_sql(
                &format!("INSERT INTO spill_orders VALUES {}", vals.join(",")),
                "main",
            )
            .await
            .unwrap();
    }

    let join_res = coord
        .execute_sql(
            "SELECT o.id, c.region, o.amount FROM spill_orders o JOIN spill_customers c ON o.cid = c.cid ORDER BY o.amount DESC",
            "main",
        )
        .await
        .unwrap();
    assert_eq!(join_res.batch.rows.len(), 600);
    // Highest order id = 600 -> amount = 1800.5
    assert_eq!(join_res.batch.rows[0].get("amount").and_then(|v| v.as_f64()), Some(1800.5));
    assert_eq!(join_res.batch.rows[599].get("amount").and_then(|v| v.as_f64()), Some(3.5));

    let total_spill_bytes = w1.arrow_spill_bytes_written.load(Ordering::Relaxed)
        + w2.arrow_spill_bytes_written.load(Ordering::Relaxed);
    let total_spill_files = w1.arrow_spill_files_created.load(Ordering::Relaxed)
        + w2.arrow_spill_files_created.load(Ordering::Relaxed);
    assert!(
        total_spill_bytes > 0 && total_spill_files > 0,
        "expected workers to spill Arrow batches to NVMe under 8 KiB budget, got bytes={total_spill_bytes}, files={total_spill_files}"
    );
}
