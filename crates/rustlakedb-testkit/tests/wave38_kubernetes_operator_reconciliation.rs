// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use axum::extract::State;
use axum::routing::{get, patch, put};
use axum::{Json, Router};
use rustlakedb_operator::{
    plan_reconciliation, ClusterPhase, KubeOperatorClient, ObjectMeta, ObservedClusterState,
    ReconcileAction, RustLakeCluster, RustLakeClusterSpec, RustLakeClusterStatus,
    WorkerAutoscaleSpec, CRD_API_VERSION, CRD_KIND,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

fn sample_cluster(image: &str) -> RustLakeCluster {
    RustLakeCluster {
        api_version: CRD_API_VERSION.into(),
        kind: CRD_KIND.into(),
        metadata: ObjectMeta {
            name: "prod-lakehouse".into(),
            namespace: "default".into(),
            labels: BTreeMap::new(),
        },
        spec: RustLakeClusterSpec {
            image: image.into(),
            coordinators: 3,
            workers: WorkerAutoscaleSpec {
                min_replicas: 2,
                max_replicas: 10,
                target_queries_per_worker: 4,
                scale_down_cooldown_secs: 120,
                nvme_spill_gi: 200,
            },
            warehouse_uri: "s3://warehouse".into(),
            enable_raft_ha: true,
            coordinator_service_url: None,
        },
        status: None,
    }
}

#[test]
fn wave38_operator_autoscaling_and_zero_downtime_raft_rolling_upgrade() {
    // 1. Validate Kubernetes Operator CRD & Deployment manifests on disk.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let crd_yaml = std::fs::read_to_string(root.join("deploy/operator/crd-rustlakecluster.yaml"))
        .expect("CRD manifest missing");
    assert!(crd_yaml.contains("rustlakeclusters.lakehouse.rustlakedb.io"));
    assert!(crd_yaml.contains("kind: RustLakeCluster"));
    let dep_yaml = std::fs::read_to_string(root.join("deploy/operator/operator-deployment.yaml"))
        .expect("Operator deployment manifest missing");
    assert!(dep_yaml.contains("kind: ClusterRole"));
    assert!(dep_yaml.contains("kind: RustLakeCluster"));

    // 2. Load-driven scale-up (20 active queries / 4 per worker = 5 workers!).
    let mut cr = sample_cluster("ghcr.io/sudeshc/rustlakedb:9.0.0");
    let obs_spike = ObservedClusterState {
        coordinator_pods: vec![
            ("c-0".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
            ("c-1".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
            ("c-2".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
        ],
        worker_pods: vec![
            ("w-0".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
            ("w-1".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
        ],
        raft_leader_pod: Some("c-0".into()),
        active_queries: 20,
    };
    let acts = plan_reconciliation(&cr, &obs_spike, 1_000);
    assert!(acts.iter().any(|a| matches!(
        a,
        ReconcileAction::ScaleWorkers {
            from: 2,
            to: 5,
            ..
        }
    )));

    // 3. Cooldown prevents premature scale-down (at t=1050 < 1000+120 -> stays at 5).
    cr.status = Some(RustLakeClusterStatus {
        phase: ClusterPhase::Ready,
        ready_coordinators: 3,
        ready_workers: 5,
        desired_workers: 5,
        current_leader: Some("c-0".into()),
        last_scale_epoch_secs: 1_000,
        message: "ok".into(),
    });
    let obs_idle = ObservedClusterState {
        coordinator_pods: obs_spike.coordinator_pods.clone(),
        worker_pods: (0..5)
            .map(|i| (format!("w-{i}"), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()))
            .collect(),
        raft_leader_pod: Some("c-0".into()),
        active_queries: 0,
    };
    let acts_cooldown = plan_reconciliation(&cr, &obs_idle, 1_050);
    assert!(
        !acts_cooldown
            .iter()
            .any(|a| matches!(a, ReconcileAction::ScaleWorkers { .. })),
        "must not scale down during cooldown"
    );
    // After cooldown expires (t=1200 >= 1120) -> scales down cleanly to min_replicas=2!
    let acts_down = plan_reconciliation(&cr, &obs_idle, 1_200);
    assert!(acts_down.iter().any(|a| matches!(
        a,
        ReconcileAction::ScaleWorkers {
            from: 5,
            to: 2,
            ..
        }
    )));

    // 4. Zero-downtime Raft rolling upgrade ordering: Workers -> Follower Coordinators -> Leader LAST!
    let cr_upg = sample_cluster("ghcr.io/sudeshc/rustlakedb:10.0.0");
    let obs_stage_a = ObservedClusterState {
        coordinator_pods: vec![
            ("c-0".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
            ("c-1".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
            ("c-2".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
        ],
        worker_pods: vec![("w-0".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into())],
        raft_leader_pod: Some("c-0".into()),
        active_queries: 0,
    };
    assert!(matches!(
        plan_reconciliation(&cr_upg, &obs_stage_a, 2_000).first(),
        Some(ReconcileAction::RollingUpgradeWorker { pod, .. }) if pod == "w-0"
    ));

    // Once workers are upgraded, follower coordinator `c-1` upgrades BEFORE leader `c-0`!
    let obs_stage_b = ObservedClusterState {
        worker_pods: vec![("w-0".into(), "ghcr.io/sudeshc/rustlakedb:10.0.0".into())],
        ..obs_stage_a.clone()
    };
    assert!(matches!(
        plan_reconciliation(&cr_upg, &obs_stage_b, 2_010).first(),
        Some(ReconcileAction::RollingUpgradeFollowerCoordinator { pod, .. }) if pod == "c-1"
    ));

    // Once all followers are upgraded, Raft leader `c-0` steps down and upgrades LAST!
    let obs_stage_c = ObservedClusterState {
        coordinator_pods: vec![
            ("c-0".into(), "ghcr.io/sudeshc/rustlakedb:9.0.0".into()),
            ("c-1".into(), "ghcr.io/sudeshc/rustlakedb:10.0.0".into()),
            ("c-2".into(), "ghcr.io/sudeshc/rustlakedb:10.0.0".into()),
        ],
        ..obs_stage_b
    };
    assert!(matches!(
        plan_reconciliation(&cr_upg, &obs_stage_c, 2_020).first(),
        Some(ReconcileAction::StepDownAndUpgradeRaftLeader { pod, .. }) if pod == "c-0"
    ));
}

#[tokio::test]
async fn wave38_live_kubernetes_api_reconciliation_loop() {
    let applied_calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let cr = sample_cluster("ghcr.io/sudeshc/rustlakedb:9.0.0");
    let cr_json = serde_json::to_value(&cr).unwrap();

    let calls_sts = applied_calls.clone();
    let calls_dep = applied_calls.clone();
    let calls_st = applied_calls.clone();
    let k8s_app = Router::new()
        .route(
            "/apis/lakehouse.rustlakedb.io/v1alpha1/namespaces/default/rustlakeclusters/prod-lakehouse",
            get(move || {
                let c = cr_json.clone();
                async move { Json(c) }
            }),
        )
        .route(
            "/apis/apps/v1/namespaces/default/statefulsets/prod-lakehouse-coordinator",
            put(|State(log): State<Arc<Mutex<Vec<String>>>>, Json(body): Json<Value>| async move {
                log.lock()
                    .unwrap()
                    .push(format!("PUT_STS:replicas={}", body["spec"]["replicas"]));
                Json(json!({"status": "ok"}))
            })
            .with_state(calls_sts),
        )
        .route(
            "/apis/apps/v1/namespaces/default/deployments/prod-lakehouse-worker",
            put(|State(log): State<Arc<Mutex<Vec<String>>>>, Json(body): Json<Value>| async move {
                log.lock()
                    .unwrap()
                    .push(format!("PUT_DEP:replicas={}", body["spec"]["replicas"]));
                Json(json!({"status": "ok"}))
            })
            .with_state(calls_dep),
        )
        .route(
            "/apis/lakehouse.rustlakedb.io/v1alpha1/namespaces/default/rustlakeclusters/prod-lakehouse/status",
            patch(|State(log): State<Arc<Mutex<Vec<String>>>>, Json(body): Json<Value>| async move {
                log.lock()
                    .unwrap()
                    .push(format!("PATCH_STATUS:phase={}", body["status"]["phase"]));
                Json(json!({"status": "ok"}))
            })
            .with_state(calls_st),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, k8s_app).await;
    });

    let client = KubeOperatorClient::new(format!("http://127.0.0.1:{port}"), Some("k8s-sa-tok".into())).unwrap();
    let actions = client
        .reconcile_once("default", "prod-lakehouse", &ObservedClusterState::default(), 5_000)
        .await
        .unwrap();
    assert!(!actions.is_empty());

    let log = applied_calls.lock().unwrap().clone();
    assert!(log.contains(&"PUT_STS:replicas=3".to_string()), "log={log:?}");
    assert!(log.contains(&"PUT_DEP:replicas=2".to_string()), "log={log:?}");
    assert!(
        log.iter().any(|s| s.starts_with("PATCH_STATUS:")),
        "log={log:?}"
    );
}
