// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use clap::{Parser, ValueEnum};
use rustlakedb_catalog::rest::IcebergRestServer;
use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_coordinator::{Coordinator, HttpWorkerClient, WorkerClient};
use rustlakedb_gateway::flight::FlightGateway;
use rustlakedb_gateway::PgGateway;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ShuffleMode;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::rpc::WorkerHttpServer;
use rustlakedb_worker::WorkerNode;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone, ValueEnum)]
enum CliMode {
    Local,
    Standalone,
    Coordinator,
    Worker,
    Gateway,
    Maintainer,
    Autoloader,
    KafkaIngest,
    SeedLarge,
}

#[derive(Debug, Parser)]
#[command(
    name = "rustlakedb",
    version,
    about = "RustLakeDB — 100% Rust Distributed SQL & Lakehouse Engine over Apache Iceberg v3"
)]
struct Args {
    #[arg(long, value_enum, default_value = "local")]
    mode: CliMode,

    /// Number of in-process workers when running in `local`/`standalone`/`gateway` mode.
    #[arg(long, default_value_t = 3)]
    workers: usize,

    /// Persistent root directory for local Raft WAL, catalog metadata, and NVMe shuffle scratch.
    #[arg(long, default_value = "./rustlakedb_data")]
    data_dir: PathBuf,

    /// Optional cloud/object-store URI for Iceberg v3 Parquet/Puffin/Avro files (`s3://bucket`, `gs://bucket`, `az://container`, `file:///path`). Defaults to `--data-dir`.
    #[arg(long)]
    storage_uri: Option<String>,

    /// Shared directory for staged shuffle exchange files (or local NVMe scratch).
    #[arg(long)]
    shuffle_dir: Option<PathBuf>,

    /// Worker ID when running in `--mode worker`.
    #[arg(long, default_value = "worker-1")]
    worker_id: String,

    /// Bind address for `--mode worker` RPC server (e.g. `0.0.0.0:7001`).
    #[arg(long, default_value = "127.0.0.1:7001")]
    bind: String,

    /// Reachable HTTP URL advertised to peer workers for cross-container/cross-host shuffle fetch (e.g. `http://worker-1:7001`).
    #[arg(long)]
    advertise_url: Option<String>,

    /// Comma-separated remote worker RPC URLs for `--mode coordinator` (e.g. `http://127.0.0.1:7001,http://127.0.0.1:7002`).
    #[arg(long)]
    worker_urls: Option<String>,

    /// Optional Coordinator Control Plane URL for dynamic worker self-registration (`--mode worker`).
    #[arg(long)]
    coordinator_url: Option<String>,

    /// Comma-separated Coordinator Control Plane URLs for zero-touch HA failover (`--mode worker`, e.g. `http://coord-1:8181,http://coord-2:8181,http://coord-3:8181`).
    #[arg(long)]
    coordinator_urls: Option<String>,

    /// Optional Raft node ID (`1`, `2`, `3`) to join a Multi-Node HA Coordinator Raft Quorum.
    #[arg(long)]
    raft_node_id: Option<u64>,

    /// Raft HTTP RPC bind address when `--raft-node-id` is set (e.g. `0.0.0.0:9001`).
    #[arg(long)]
    raft_bind: Option<String>,

    /// Comma-separated Raft peers (`1=http://coord-1:9001,2=http://coord-2:9002,3=http://coord-3:9003`).
    #[arg(long)]
    raft_peers: Option<String>,

    /// PostgreSQL v3 Wire Protocol bind address (e.g. `127.0.0.1:5432`).
    #[arg(long)]
    pg_addr: Option<String>,

    /// Embedded Iceberg v3 REST Catalog HTTP bind address (e.g. `127.0.0.1:8181`).
    #[arg(long)]
    rest_addr: Option<String>,

    /// Optional Apache Arrow Flight gRPC bind address (e.g. `0.0.0.0:8815`, Wave 15).
    #[arg(long)]
    flight_addr: Option<String>,

    /// Kafka / Redpanda TCP broker address for `--mode kafka-ingest` (e.g. `127.0.0.1:9092`).
    #[arg(long, default_value = "127.0.0.1:9092")]
    kafka_broker: String,

    /// Kafka / Redpanda topic name for `--mode kafka-ingest`.
    #[arg(long, default_value = "dbserver1.public.events")]
    kafka_topic: String,

    /// Kafka partition ID for `--mode kafka-ingest`.
    #[arg(long, default_value_t = 0)]
    kafka_partition: i32,

    /// Total rows to generate when running `--mode seed-large` (default 500,000).
    #[arg(long, default_value_t = 500_000)]
    rows: usize,

    /// Number of Parquet + Puffin splits for `--mode seed-large` (default 20).
    #[arg(long, default_value_t = 20)]
    splits: usize,

    /// Target table name for `--mode autoloader`.
    #[arg(long, default_value = "events")]
    table: String,

    /// Landing directory to watch for `--mode autoloader`.
    #[arg(long)]
    landing_dir: Option<PathBuf>,

    /// Enterprise OIDC Issuer URL for automatic `/.well-known/openid-configuration` discovery + JWKS rotation (e.g. Okta / Entra ID / Auth0 / Google).
    #[arg(long)]
    oidc_issuer: Option<String>,

    /// Optional expected JWT `aud` claim when `--oidc-issuer` is configured.
    #[arg(long)]
    oidc_audience: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let args = Args::parse();
    println!(
        "RustLakeDB v{} | mode={:?} | data_dir={}",
        env!("CARGO_PKG_VERSION"),
        args.mode,
        args.data_dir.display()
    );

    let catalog: Arc<dyn rustlakedb_catalog::Catalog> = if let Some(node_id) = args.raft_node_id {
        let mut peers = std::collections::HashMap::new();
        if let Some(ref raw_peers) = args.raft_peers {
            for item in raw_peers.split(',').filter(|s| !s.trim().is_empty()) {
                if let Some((id_s, url)) = item.split_once('=')
                    && let Ok(pid) = id_s.trim().parse::<u64>()
                {
                    peers.insert(pid, url.trim().trim_end_matches('/').to_string());
                }
            }
        }
        let raft_node =
            rustlakedb_catalog::raft::RaftCatalogNode::open_persistent(node_id, peers, &args.data_dir)?;
        let raft_bind = args.raft_bind.as_deref().unwrap_or("0.0.0.0:9001");
        let bound = raft_node.start_server(raft_bind).await?;
        println!("Raft HA Consensus Quorum node-{node_id} listening on http://{bound}");
        raft_node
    } else {
        Arc::new(InMemoryRaftCatalog::open(&args.data_dir)?)
    };
    let storage_uri = args
        .storage_uri
        .clone()
        .unwrap_or_else(|| args.data_dir.to_string_lossy().to_string());
    let store = Arc::new(IcebergObjectStore::from_uri(&storage_uri)?);
    let shuffle_dir = args
        .shuffle_dir
        .clone()
        .unwrap_or_else(|| args.data_dir.join(format!("_shuffle_{}", args.worker_id)));
    let shuffle_mgr = Arc::new(HybridShuffleManager::new(shuffle_dir)?);
    let shuffle: Arc<dyn ShuffleTransport> = shuffle_mgr.clone();

    // Launch Coordinator Lakehouse Control Plane (Iceberg v3 REST + Web UI + Prometheus /metrics + Arrow IPC SQL + Ingest)
    // when running in Coordinator/Gateway/Standalone modes, or lightweight Iceberg REST in Worker/Maintainer modes.
    let spawn_rest_only = |rest_addr: String, cat: Arc<dyn rustlakedb_catalog::Catalog>| async move {
        match IcebergRestServer::bind(&rest_addr, cat).await {
            Ok(rest) => {
                if let Ok(addr) = rest.local_addr() {
                    println!("Iceberg v3 REST Catalog listening on http://{addr}");
                }
                tokio::spawn(async move {
                    if let Err(e) = rest.serve().await {
                        tracing::error!(error = %e, "Iceberg REST server stopped");
                    }
                });
            }
            Err(e) => tracing::error!(error = %e, "failed to bind Iceberg REST server"),
        }
    };

    match args.mode {
        CliMode::Worker => {
            if let Some(ref rest_addr) = args.rest_addr {
                spawn_rest_only(rest_addr.clone(), catalog.clone()).await;
            }
            let worker = Arc::new(WorkerNode::new(
                args.worker_id.clone(),
                catalog,
                shuffle,
                store,
            ));
            let server = WorkerHttpServer::bind(&args.bind, worker).await?;
            let bound = server.local_addr()?;
            let mtls_acceptor = match (
                std::env::var("RUSTLAKEDB_MTLS_CA"),
                std::env::var("RUSTLAKEDB_MTLS_CERT"),
                std::env::var("RUSTLAKEDB_MTLS_KEY"),
            ) {
                (Ok(ca), Ok(cert), Ok(key)) => Some(
                    rustlakedb_worker::rpc::build_mtls_server_acceptor_from_files(ca, cert, key)?,
                ),
                _ => None,
            };
            let scheme = if mtls_acceptor.is_some() { "https" } else { "http" };
            let adv = args
                .advertise_url
                .clone()
                .unwrap_or_else(|| format!("{scheme}://{bound}"));
            shuffle_mgr.set_advertise_url(&adv);
            // Wave 7 + Wave 11 zero-touch HA multi-coordinator worker self-registration & heartbeat broadcast!
            let mut coord_targets: Vec<String> = Vec::new();
            if let Some(ref single) = args.coordinator_url {
                for u in single.split(',').filter(|s| !s.trim().is_empty()) {
                    coord_targets.push(u.trim().trim_end_matches('/').to_string());
                }
            }
            if let Some(ref multi) = args.coordinator_urls {
                for u in multi.split(',').filter(|s| !s.trim().is_empty()) {
                    let clean = u.trim().trim_end_matches('/').to_string();
                    if !coord_targets.contains(&clean) {
                        coord_targets.push(clean);
                    }
                }
            }
            if !coord_targets.is_empty() {
                let wid = args.worker_id.clone();
                let adv_clone = adv.clone();
                tokio::spawn(async move {
                    let client = reqwest::Client::builder()
                        .connect_timeout(std::time::Duration::from_millis(500))
                        .timeout(std::time::Duration::from_secs(2))
                        .build()
                        .unwrap_or_else(|_| reqwest::Client::new());
                    loop {
                        for base in &coord_targets {
                            let reg_url = format!("{base}/v1/workers/register");
                            let _ = client
                                .post(&reg_url)
                                .json(&serde_json::json!({ "worker_id": wid, "url": adv_clone }))
                                .send()
                                .await;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                });
            }
            if let Some(acceptor) = mtls_acceptor {
                println!(
                    "Worker '{}' mTLS RPC listening on https://{} (advertising {})",
                    args.worker_id, bound, adv
                );
                server.serve_mtls(acceptor).await?;
            } else {
                println!(
                    "Worker '{}' RPC listening on http://{} (advertising {})",
                    args.worker_id, bound, adv
                );
                server.serve().await?;
            }
        }
        CliMode::Coordinator => {
            let clients: Vec<Arc<dyn WorkerClient>> = args
                .worker_urls
                .as_deref()
                .unwrap_or("")
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .enumerate()
                .map(|(i, url)| {
                    Arc::new(HttpWorkerClient::new(
                        format!("remote-worker-{}", i + 1),
                        url.trim(),
                    )) as Arc<dyn WorkerClient>
                })
                .collect();

            let coord = Arc::new(Coordinator::with_clients(
                catalog,
                store,
                shuffle,
                clients,
                PlannerConfig {
                    default_shuffle_partitions: args.workers,
                    shuffle_mode: ShuffleMode::Staged,
                    ..Default::default()
                },
            ));
            let oidc_issuer = args
                .oidc_issuer
                .clone()
                .or_else(|| std::env::var("RUSTLAKEDB_OIDC_ISSUER").ok())
                .filter(|s| !s.trim().is_empty());
            let oidc_audience = args
                .oidc_audience
                .clone()
                .or_else(|| std::env::var("RUSTLAKEDB_OIDC_AUDIENCE").ok())
                .filter(|s| !s.trim().is_empty());
            if let Some(issuer) = oidc_issuer {
                match coord
                    .governance
                    .configure_oidc_discovery(&issuer, oidc_audience.clone())
                    .await
                {
                    Ok(keys) => println!(
                        "OIDC Discovery enabled for {issuer} (loaded {keys} JWKS keys, aud={:?})",
                        oidc_audience
                    ),
                    Err(e) => tracing::warn!(
                        issuer = %issuer,
                        error = %e,
                        "OIDC discovery failed at startup; will retry on JWT verification"
                    ),
                }
            }
            if let Ok(jwks_url) = std::env::var("RUSTLAKEDB_OIDC_JWKS_URL")
                && !jwks_url.trim().is_empty()
            {
                println!("OIDC JWKS Auto-Refresh Poller enabled for {jwks_url}");
                coord.governance.clone().spawn_oidc_jwks_poller(
                    jwks_url,
                    std::time::Duration::from_secs(300),
                );
            }
            if let Some(ref rest_addr) = args.rest_addr {
                let cp = rustlakedb_coordinator::admin::ControlPlaneServer::bind(
                    rest_addr,
                    coord.clone(),
                )
                .await?;
                println!(
                    "Lakehouse Control Plane (Web UI + Prometheus /metrics + Arrow IPC SQL + Iceberg v3 REST) on http://{}",
                    cp.local_addr()?
                );
                tokio::spawn(async move {
                    if let Err(e) = cp.serve().await {
                        tracing::error!(error = %e, "Control plane server stopped");
                    }
                });
            }
            if let Some(ref flight_addr) = args.flight_addr {
                let fg = FlightGateway::bind(flight_addr, coord.clone()).await?;
                println!(
                    "Apache Arrow Flight gRPC Gateway listening on grpc://{}",
                    fg.local_addr()?
                );
                tokio::spawn(async move {
                    if let Err(e) = fg.serve().await {
                        tracing::error!(error = %e, "Arrow Flight server stopped");
                    }
                });
            }
            let pg_addr = args.pg_addr.as_deref().unwrap_or("127.0.0.1:5432");
            let gw = PgGateway::bind(pg_addr, coord).await?;
            println!("Coordinator PostgreSQL v3 Gateway listening on {}", gw.local_addr()?);
            gw.serve().await?;
        }
        CliMode::Gateway | CliMode::Standalone | CliMode::Local => {
            let workers: Vec<Arc<WorkerNode>> = (1..=args.workers)
                .map(|i| {
                    Arc::new(WorkerNode::new(
                        format!("worker-{i}"),
                        catalog.clone(),
                        shuffle.clone(),
                        store.clone(),
                    ))
                })
                .collect();

            let coord = Arc::new(Coordinator::new(
                catalog,
                store.clone(),
                shuffle,
                workers,
                PlannerConfig {
                    default_shuffle_partitions: args.workers,
                    shuffle_mode: ShuffleMode::Staged,
                    ..Default::default()
                },
            ));

            if let Some(ref rest_addr) = args.rest_addr {
                let cp = rustlakedb_coordinator::admin::ControlPlaneServer::bind(
                    rest_addr,
                    coord.clone(),
                )
                .await?;
                println!(
                    "Lakehouse Control Plane (Web UI + Prometheus /metrics + Arrow IPC SQL + Iceberg v3 REST) on http://{}",
                    cp.local_addr()?
                );
                tokio::spawn(async move {
                    if let Err(e) = cp.serve().await {
                        tracing::error!(error = %e, "Control plane server stopped");
                    }
                });
            }

            if let Some(ref flight_addr) = args.flight_addr {
                let fg = FlightGateway::bind(flight_addr, coord.clone()).await?;
                println!(
                    "Apache Arrow Flight gRPC Gateway listening on grpc://{}",
                    fg.local_addr()?
                );
                tokio::spawn(async move {
                    if let Err(e) = fg.serve().await {
                        tracing::error!(error = %e, "Arrow Flight server stopped");
                    }
                });
            }

            if let Some(addr) = args.pg_addr {
                let gw = PgGateway::bind(&addr, coord).await?;
                println!("PostgreSQL v3 Gateway listening on {}", gw.local_addr()?);
                gw.serve().await?;
            } else if matches!(args.mode, CliMode::Gateway) {
                let gw = PgGateway::bind("127.0.0.1:5432", coord).await?;
                println!("PostgreSQL v3 Gateway listening on {}", gw.local_addr()?);
                gw.serve().await?;
            } else {
                run_local_demo(&coord, &store, args.workers).await?;
            }
        }
        CliMode::Maintainer => {
            if let Some(ref rest_addr) = args.rest_addr {
                spawn_rest_only(rest_addr.clone(), catalog.clone()).await;
            }
            let coord = Arc::new(Coordinator::new(
                catalog,
                store,
                shuffle,
                vec![],
                PlannerConfig::default(),
            ));
            let maintainer = rustlakedb_coordinator::maintainer::AutonomousMaintainer::new(
                coord,
                rustlakedb_coordinator::maintainer::MaintainerConfig::default(),
            );
            println!("Autonomous Maintainer daemon active (compaction + DV materialization + VACUUM).");
            maintainer.run_loop().await?;
        }
        CliMode::Autoloader => {
            let coord = Arc::new(Coordinator::new(
                catalog,
                store,
                shuffle,
                vec![],
                PlannerConfig::default(),
            ));
            let landing = args
                .landing_dir
                .clone()
                .unwrap_or_else(|| args.data_dir.join("_landing"));
            let cp_dir = args.data_dir.join("_autoloader");
            let loader = rustlakedb_coordinator::autoloader::AutoLoader::new(
                coord,
                &args.table,
                &landing,
                &cp_dir,
            )?;
            println!(
                "Auto-Loader daemon watching {} -> table '{}' (exactly-once checkpoints in {})",
                landing.display(),
                args.table,
                cp_dir.display()
            );
            loader.run_loop(std::time::Duration::from_secs(2)).await?;
        }
        CliMode::KafkaIngest => {
            let coord = Arc::new(Coordinator::new(
                catalog,
                store,
                shuffle,
                vec![],
                PlannerConfig::default(),
            ));
            let cp_dir = args.data_dir.join("_kafka");
            let consumer = rustlakedb_coordinator::kafka_ingest::KafkaCdcConsumer::new(
                coord,
                &args.kafka_broker,
                &args.kafka_topic,
                args.kafka_partition,
                &args.table,
                &cp_dir,
            )?;
            println!(
                "Kafka CDC consumer connected to {} topic='{}' partition={} -> Iceberg table '{}'",
                args.kafka_broker, args.kafka_topic, args.kafka_partition, args.table
            );
            consumer.run_loop(std::time::Duration::from_secs(1)).await?;
        }
        CliMode::SeedLarge => {
            seed_large_dataset(&catalog, &store, args.rows, args.splits).await?;
        }
    }

    Ok(())
}

async fn seed_large_dataset(
    catalog: &Arc<dyn rustlakedb_catalog::Catalog>,
    store: &Arc<IcebergObjectStore>,
    total_rows: usize,
    splits: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use rustlakedb_catalog::{
        ColumnType, FieldSchema, IcebergTableMetadata, PuffinIndexSpec, PuffinIndexType,
        TableCommitProposal,
    };
    use rustlakedb_protocol::{RowBatch, ScalarValue};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::time::Instant;

    let t0 = Instant::now();
    let num_splits = std::cmp::max(1, splits);
    let rows_per_split = total_rows / num_splits;

    // 1. Seed `big_orders` across `num_splits` Zstd Parquet files + Puffin JSON Inverted/Bloom indexes.
    let order_schema = vec![
        FieldSchema { field_id: 1, name: "order_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "customer_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 3, name: "region".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 4, name: "amount".into(), data_type: ColumnType::Float64, nullable: false },
        FieldSchema { field_id: 5, name: "payload".into(), data_type: ColumnType::Variant, nullable: false },
    ];
    let _ = catalog.drop_table("big_orders").await;
    catalog
        .create_table(IcebergTableMetadata::new("big_orders", order_schema.clone()))
        .await?;
    catalog
        .add_index(
            "big_orders",
            PuffinIndexSpec {
                name: "idx_big_orders_payload".into(),
                column: "payload".into(),
                json_path: None,
                index_type: PuffinIndexType::Inverted,
            },
        )
        .await?;

    let regions = ["us-east", "us-west", "eu-central", "ap-south", "sa-east"];
    let channels = ["web", "mobile", "api", "partner"];
    let mut order_files = Vec::with_capacity(num_splits);
    let mut total_parquet_bytes = 0u64;

    for s in 0..num_splits {
        let mut rows = Vec::with_capacity(rows_per_split);
        for i in 0..rows_per_split {
            let gid = (s * rows_per_split + i + 1) as i64;
            let cid = (gid % 20_000) + 1;
            let region = regions[(gid as usize) % regions.len()];
            let channel = channels[(gid as usize) % channels.len()];
            let amount = ((gid % 500) + 10) as f64;
            let tag = if s == num_splits - 1 && i == 42 {
                "needle_whale_999"
            } else {
                "standard"
            };
            let mut r = BTreeMap::new();
            r.insert("order_id".into(), ScalarValue::Int64(gid));
            r.insert("customer_id".into(), ScalarValue::Int64(cid));
            r.insert("region".into(), ScalarValue::Utf8(region.into()));
            r.insert("amount".into(), ScalarValue::Float64(amount));
            r.insert(
                "payload".into(),
                ScalarValue::Json(json!({ "channel": channel, "tag": tag })),
            );
            rows.push(r);
        }
        let batch = RowBatch::new(
            vec![
                "order_id".into(),
                "customer_id".into(),
                "region".into(),
                "amount".into(),
                "payload".into(),
            ],
            rows,
        );
        let path = format!("big_orders/data/part-{s:04}.parquet");
        let entry = store.write_data_file(&path, &order_schema, &batch).await?;
        total_parquet_bytes += entry.byte_size;
        order_files.push(entry);
    }

    catalog
        .commit_table(TableCommitProposal {
            table_name: "big_orders".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: order_files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await?;
    let meta = catalog.get_table("big_orders").await?;
    store.write_iceberg_metadata(&meta, "main").await?;

    // 2. Seed `big_customers` (20,000 rows across 4 Zstd Parquet splits for large distributed JOINs!).
    let cust_schema = vec![
        FieldSchema { field_id: 1, name: "customer_id".into(), data_type: ColumnType::Int64, nullable: false },
        FieldSchema { field_id: 2, name: "tier".into(), data_type: ColumnType::Utf8, nullable: false },
        FieldSchema { field_id: 3, name: "segment".into(), data_type: ColumnType::Utf8, nullable: false },
    ];
    let _ = catalog.drop_table("big_customers").await;
    catalog
        .create_table(IcebergTableMetadata::new("big_customers", cust_schema.clone()))
        .await?;

    let tiers = ["enterprise", "growth", "pro", "starter"];
    let segments = ["fintech", "retail", "saas", "healthcare", "media"];
    let mut cust_files = Vec::with_capacity(4);
    for s in 0..4usize {
        let mut rows = Vec::with_capacity(5_000);
        for i in 0..5_000usize {
            let cid = (s * 5_000 + i + 1) as i64;
            let mut r = BTreeMap::new();
            r.insert("customer_id".into(), ScalarValue::Int64(cid));
            r.insert("tier".into(), ScalarValue::Utf8(tiers[(cid as usize) % 4].into()));
            r.insert("segment".into(), ScalarValue::Utf8(segments[(cid as usize) % 5].into()));
            rows.push(r);
        }
        let batch = RowBatch::new(
            vec!["customer_id".into(), "tier".into(), "segment".into()],
            rows,
        );
        let path = format!("big_customers/data/part-{s:04}.parquet");
        let entry = store.write_data_file(&path, &cust_schema, &batch).await?;
        total_parquet_bytes += entry.byte_size;
        cust_files.push(entry);
    }

    catalog
        .commit_table(TableCommitProposal {
            table_name: "big_customers".into(),
            branch: "main".into(),
            expected_snapshot_id: 1,
            added_files: cust_files,
            removed_files: vec![],
            is_append_only: true,
        })
        .await?;
    let cmeta = catalog.get_table("big_customers").await?;
    store.write_iceberg_metadata(&cmeta, "main").await?;

    let elapsed = t0.elapsed().as_secs_f64();
    println!(
        "Seeded big_orders ({} rows, {} Parquet+Puffin splits) & big_customers (20,000 rows, 4 splits) = {:.2} MB compressed Parquet in {:.2}s",
        rows_per_split * num_splits,
        num_splits,
        (total_parquet_bytes as f64) / (1024.0 * 1024.0),
        elapsed
    );
    Ok(())
}

async fn run_local_demo(
    coord: &Coordinator,
    store: &IcebergObjectStore,
    num_workers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = coord
        .execute_sql(
            "CREATE TABLE events (region TEXT, revenue DOUBLE, payload JSONB)",
            "main",
        )
        .await;
    coord
        .execute_sql(
            r#"INSERT INTO events VALUES ('us-east', 120.5, '{"type": "checkout"}')"#,
            "main",
        )
        .await?;
    coord
        .execute_sql(
            r#"INSERT INTO events VALUES ('eu-west', 89.0, '{"type": "checkout"}')"#,
            "main",
        )
        .await?;
    coord
        .execute_sql(
            r#"INSERT INTO events VALUES ('ap-south', 45.0, '{"type": "pageview"}')"#,
            "main",
        )
        .await?;

    let sql = "SELECT region, COUNT(*) AS orders, SUM(revenue) AS total_rev \
               FROM events WHERE payload->>'type' = 'checkout' GROUP BY region ORDER BY region";
    println!("\nExecuting distributed SQL across local[{num_workers}] over real Parquet + Puffin:\n  {sql}\n");
    let res = coord.execute_sql(sql, "main").await?;
    println!(
        "Executed {} stages, {} tasks, wrote {} Parquet files, read {} Parquet files, pruned {} via Puffin JSON index.",
        res.stats.stages_executed,
        res.stats.tasks_executed,
        store.parquet_files_written.load(Ordering::Relaxed),
        store.parquet_files_read.load(Ordering::Relaxed),
        store.files_pruned_by_puffin.load(Ordering::Relaxed),
    );
    for row in &res.batch.rows {
        println!("  {row:?}");
    }
    Ok(())
}
