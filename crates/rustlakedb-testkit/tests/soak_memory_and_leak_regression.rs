// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{
    Catalog, ColumnType, DataFileEntry, FieldSchema, IcebergTableMetadata, InMemoryRaftCatalog,
    TableCommitProposal,
};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn soak_regression_worker_heartbeat_reuses_http_client_zero_leak() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(InMemoryRaftCatalog::new());
    let store = Arc::new(IcebergObjectStore::in_memory());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().to_path_buf()).unwrap());
    let coord = Coordinator::with_clients(
        catalog,
        store,
        shuffle,
        vec![],
        PlannerConfig::default(),
    );

    assert_eq!(coord.register_worker("worker-ha-1", "http://127.0.0.1:7001"), 1);
    let initial_arc = coord.workers_snapshot()[0].clone();

    // Simulate 2,000 periodic 2-second heartbeats -> MUST reuse exact same Arc<HttpWorkerClient>
    // (zero new reqwest::Client / rustls RootCertStore allocations!)
    for _ in 0..2_000 {
        assert_eq!(
            coord.register_worker("worker-ha-1", "http://127.0.0.1:7001/"),
            1
        );
    }
    let after_arc = coord.workers_snapshot()[0].clone();
    assert!(
        Arc::ptr_eq(&initial_arc, &after_arc),
        "register_worker must reuse existing HttpWorkerClient on unchanged URL to prevent TLS/pool leaks"
    );

    // Changing URL must still replace the client cleanly
    assert_eq!(coord.register_worker("worker-ha-1", "http://127.0.0.1:7099"), 1);
    let updated_arc = coord.workers_snapshot()[0].clone();
    assert!(!Arc::ptr_eq(&initial_arc, &updated_arc));
    assert_eq!(updated_arc.base_url(), Some("http://127.0.0.1:7099"));
}

#[tokio::test]
async fn soak_regression_deduplicated_catalog_v2_stays_compact_and_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let cat = InMemoryRaftCatalog::open(dir.path()).unwrap();
    let schema = vec![
        FieldSchema {
            field_id: 1,
            name: "id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        },
        FieldSchema {
            field_id: 2,
            name: "payload".into(),
            data_type: ColumnType::Utf8,
            nullable: true,
        },
    ];
    cat.create_table(IcebergTableMetadata::new("high_churn", schema))
        .await
        .unwrap();

    // Commit 120 sequential snapshots, each appending a file with zone-map metadata
    let mut snap_id = 1u64;
    for i in 0..120u64 {
        let mut col_min = HashMap::new();
        let mut col_max = HashMap::new();
        col_min.insert("id".into(), format!("{}", i * 100));
        col_max.insert("id".into(), format!("{}", i * 100 + 99));
        let entry = DataFileEntry {
            file_path: format!("high_churn/data/part-{i:06}.parquet"),
            row_group_id: 0,
            row_count: 100,
            byte_size: 4096,
            deletion_vector: Default::default(),
            puffin_index: Default::default(),
            ndv_estimates: HashMap::from([("id".into(), 100)]),
            column_min: col_min,
            column_max: col_max,
            partition_values: HashMap::from([("day".into(), "2026-11-26".into())]),
            dv_path: None,
            deleted_row_count: 0,
        };
        snap_id = cat
            .commit_table(TableCommitProposal {
                table_name: "high_churn".into(),
                branch: "main".into(),
                expected_snapshot_id: snap_id,
                added_files: vec![entry],
                removed_files: vec![],
                is_append_only: true,
            })
            .await
            .unwrap();
    }

    // Verify bounded snapshot retention (default 16) AND compact deduplicated v2 state.json!
    let (_tbls, max_snaps) = cat.catalog_stats();
    assert!(
        max_snaps <= 16,
        "expected snapshot retention bounded to <= 16, got {max_snaps}"
    );

    let state_path = dir.path().join("_catalog").join("state.json");
    let v2_bytes = std::fs::read(&state_path).unwrap();
    let v2_str = String::from_utf8_lossy(&v2_bytes);
    assert!(
        v2_str.contains("\"format_version\":2"),
        "expected deduplicated PersistedCatalogV2 format"
    );

    // Compare against naive legacy full-snapshot JSON size (must be >5x smaller!)
    let meta = cat.get_table("high_churn").await.unwrap();
    let legacy_map = HashMap::from([("high_churn".to_string(), meta.clone())]);
    let legacy_bytes = serde_json::to_vec(&legacy_map).unwrap();
    assert!(
        v2_bytes.len() * 5 < legacy_bytes.len(),
        "expected PersistedCatalogV2 ({} B) to be >5x smaller than duplicated legacy JSON ({} B)",
        v2_bytes.len(),
        legacy_bytes.len()
    );

    // Cold reopen from disk must reconstruct identical snapshot & file state!
    let reopened = InMemoryRaftCatalog::open(dir.path()).unwrap();
    let meta2 = reopened.get_table("high_churn").await.unwrap();
    assert_eq!(meta2.snapshots.len(), meta.snapshots.len());
    assert_eq!(
        meta2.current_snapshot("main").unwrap().data_files.len(),
        120
    );
}

#[tokio::test]
async fn soak_regression_governance_maps_stay_strictly_bounded() {
    let gov = rustlakedb_coordinator::security::GovernanceState::default();
    for i in 0..2_500usize {
        gov.cancel_query(&format!("q-{i}"));
        gov.record_hot_json_path("events", "payload", &format!("k_{i}"));
    }
    assert!(gov.hot_json_paths_for("events").len() <= 16);
}
