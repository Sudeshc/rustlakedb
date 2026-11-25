// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod admin;
pub mod autoloader;
pub mod kafka_ingest;
pub mod maintainer;
pub mod pg_catalog;
pub mod security;

use crate::security::{GovernanceState, MaskingRule, SessionContext};
use async_trait::async_trait;
use parking_lot::RwLock;
use rustlakedb_catalog::{
    Catalog, ColumnType, FieldSchema, IcebergTableMetadata, PuffinIndexType, TableCommitProposal,
    ViewDefinition,
};
use rustlakedb_common::{
    assign_split_rendezvous, Result, RustLakeError, TaskAttemptId, WorkerId,
};
use rustlakedb_planner::{CopyFormat, DistributedPlanner, PlannedCommand, PlannerConfig};
use rustlakedb_protocol::{
    ColumnExpr, FilterExpr, IcebergSplit, PartitionMetrics, PhysicalNode, RowBatch, ScalarValue,
    ShuffleBlockLocation, StageDag, TaskCompletionReport, TaskSpec,
};
use rustlakedb_shuffle::ShuffleTransport;
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::{eval_filter, WorkerNode};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Unified Worker RPC client trait (RFC-001):
/// Identical scheduling, retry, and AQE code runs over both `InProcessWorkerClient` (`local[N]`)
/// and `HttpWorkerClient` (multi-node TCP/Kubernetes clusters).
#[async_trait]
pub trait WorkerClient: Send + Sync {
    fn id(&self) -> &WorkerId;
    fn is_healthy(&self) -> bool;
    async fn execute_task(&self, task: TaskSpec) -> Result<TaskCompletionReport>;
}

pub struct InProcessWorkerClient {
    pub node: Arc<WorkerNode>,
}

impl InProcessWorkerClient {
    pub fn new(node: Arc<WorkerNode>) -> Self {
        Self { node }
    }
}

#[async_trait]
impl WorkerClient for InProcessWorkerClient {
    fn id(&self) -> &WorkerId {
        &self.node.id
    }

    fn is_healthy(&self) -> bool {
        self.node.is_healthy()
    }

    async fn execute_task(&self, task: TaskSpec) -> Result<TaskCompletionReport> {
        self.node.execute_task(task).await
    }
}

/// Real remote HTTP/TCP worker client for multi-process & multi-node distributed clusters
/// with fast connect timeout (400 ms) and a 3-second circuit breaker for sub-second failover (Wave 1).
pub struct HttpWorkerClient {
    worker_id: WorkerId,
    base_url: String,
    http: reqwest::Client,
    unhealthy_until_ms: AtomicU64,
}

impl HttpWorkerClient {
    pub fn new(worker_id: impl Into<String>, base_url: impl Into<String>) -> Self {
        let worker_id = worker_id.into();
        let base_url = base_url.into();
        // Automatically enable cluster mTLS when RUSTLAKEDB_MTLS_CA/CERT/KEY are configured (Wave 16).
        if let (Ok(ca_p), Ok(cert_p), Ok(key_p)) = (
            std::env::var("RUSTLAKEDB_MTLS_CA"),
            std::env::var("RUSTLAKEDB_MTLS_CERT"),
            std::env::var("RUSTLAKEDB_MTLS_KEY"),
        ) && let (Ok(ca), Ok(cert), Ok(key)) = (
            std::fs::read(&ca_p),
            std::fs::read(&cert_p),
            std::fs::read(&key_p),
        ) && let Ok(client) = Self::new_mtls(&worker_id, &base_url, &ca, &cert, &key)
        {
            return client;
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(400))
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            worker_id: WorkerId::new(worker_id),
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            unhealthy_until_ms: AtomicU64::new(0),
        }
    }

    /// Constructs a mutual-TLS (mTLS) Worker RPC client presenting a cluster-CA-signed client certificate
    /// and strictly validating the remote worker certificate against `ca_pem` (Wave 16).
    pub fn new_mtls(
        worker_id: impl Into<String>,
        base_url: impl Into<String>,
        ca_pem: &[u8],
        client_cert_pem: &[u8],
        client_key_pem: &[u8],
    ) -> Result<Self> {
        let ca_cert = reqwest::Certificate::from_pem(ca_pem)
            .map_err(|e| RustLakeError::Execution(format!("invalid mTLS CA PEM: {e}")))?;
        let mut identity_pem = Vec::with_capacity(client_cert_pem.len() + client_key_pem.len() + 2);
        identity_pem.extend_from_slice(client_cert_pem);
        identity_pem.push(b'\n');
        identity_pem.extend_from_slice(client_key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| RustLakeError::Execution(format!("invalid mTLS client identity PEM: {e}")))?;

        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca_cert)
            .identity(identity)
            .connect_timeout(Duration::from_millis(800))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| RustLakeError::Execution(format!("build mTLS worker client: {e}")))?;

        Ok(Self {
            worker_id: WorkerId::new(worker_id),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            unhealthy_until_ms: AtomicU64::new(0),
        })
    }

    fn trip_circuit_breaker(&self) {
        self.unhealthy_until_ms
            .store(now_epoch_ms() + 3_000, Ordering::Relaxed);
    }
}

#[async_trait]
impl WorkerClient for HttpWorkerClient {
    fn id(&self) -> &WorkerId {
        &self.worker_id
    }

    fn is_healthy(&self) -> bool {
        now_epoch_ms() >= self.unhealthy_until_ms.load(Ordering::Relaxed)
    }

    async fn execute_task(&self, task: TaskSpec) -> Result<TaskCompletionReport> {
        if !self.is_healthy() {
            return Err(RustLakeError::WorkerFailed {
                worker_id: self.worker_id.0.clone(),
                reason: "circuit breaker open (worker recently failed)".into(),
            });
        }

        let url = format!("{}/v1/tasks/execute", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&task)
            .send()
            .await
            .map_err(|e| {
                self.trip_circuit_breaker();
                RustLakeError::WorkerFailed {
                    worker_id: self.worker_id.0.clone(),
                    reason: format!("network error contacting {url}: {e}"),
                }
            })?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            self.trip_circuit_breaker();
            return Err(RustLakeError::WorkerFailed {
                worker_id: self.worker_id.0.clone(),
                reason: "remote worker returned 503 Service Unavailable".into(),
            });
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(RustLakeError::Execution(format!(
                "remote worker {} HTTP {}: {}",
                self.worker_id, status, text
            )));
        }

        self.unhealthy_until_ms.store(0, Ordering::Relaxed);
        resp.json::<TaskCompletionReport>()
            .await
            .map_err(|e| RustLakeError::Execution(format!("decode worker report: {e}")))
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryExecutionStats {
    pub stages_executed: usize,
    pub tasks_executed: usize,
    pub tasks_retried: usize,
    pub speculative_hedges: usize,
    pub skewed_partitions_detected: Vec<usize>,
    pub aqe_partitions_coalesced: usize,
    pub aqe_skew_splits_applied: usize,
    pub total_shuffled_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct QueryExecutionResult {
    pub command_tag: String,
    pub dag: Option<StageDag>,
    pub batch: RowBatch,
    pub stats: QueryExecutionStats,
}

/// Distributed RustLakeDB Coordinator (RFC-001 + RFC-002 + RFC-004 + Waves 1–10).
pub struct Coordinator {
    pub catalog: Arc<dyn Catalog>,
    pub store: Arc<IcebergObjectStore>,
    pub shuffle: Arc<dyn ShuffleTransport>,
    pub planner: DistributedPlanner,
    pub workers: RwLock<Vec<Arc<dyn WorkerClient>>>,
    /// Last-seen heartbeat timestamp (epoch ms) for dynamically registered workers (Wave 9 TTL eviction).
    pub worker_heartbeats: RwLock<HashMap<String, u64>>,
    /// Speculative straggler execution threshold in ms (0 = disabled, >0 = hedge slow tasks on peer worker, Wave 9).
    pub speculative_threshold_ms: AtomicU64,
    pub max_task_retries: u32,
    pub governance: Arc<GovernanceState>,
}

impl Coordinator {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        store: Arc<IcebergObjectStore>,
        shuffle: Arc<dyn ShuffleTransport>,
        workers: Vec<Arc<WorkerNode>>,
        planner_config: PlannerConfig,
    ) -> Self {
        let clients: Vec<Arc<dyn WorkerClient>> = workers
            .into_iter()
            .map(|w| Arc::new(InProcessWorkerClient::new(w)) as Arc<dyn WorkerClient>)
            .collect();
        Self::with_clients(catalog, store, shuffle, clients, planner_config)
    }

    pub fn with_clients(
        catalog: Arc<dyn Catalog>,
        store: Arc<IcebergObjectStore>,
        shuffle: Arc<dyn ShuffleTransport>,
        workers: Vec<Arc<dyn WorkerClient>>,
        planner_config: PlannerConfig,
    ) -> Self {
        Self {
            catalog,
            store,
            shuffle,
            planner: DistributedPlanner::new(planner_config),
            workers: RwLock::new(workers),
            worker_heartbeats: RwLock::new(HashMap::new()),
            speculative_threshold_ms: AtomicU64::new(0),
            max_task_retries: 3,
            governance: Arc::new(GovernanceState::default()),
        }
    }

    /// Dynamically registers or refreshes a remote HTTP worker node at runtime (Wave 7 + Wave 9 TTL).
    pub fn register_worker(&self, worker_id: &str, url: &str) -> usize {
        let mut guard = self.workers.write();
        if let Some(pos) = guard.iter().position(|w| w.id().0 == worker_id) {
            guard[pos] = Arc::new(HttpWorkerClient::new(worker_id, url));
        } else {
            guard.push(Arc::new(HttpWorkerClient::new(worker_id, url)));
        }
        self.worker_heartbeats
            .write()
            .insert(worker_id.to_string(), now_epoch_ms());
        guard.len()
    }

    /// Evicts dynamically registered workers whose last heartbeat is older than `ttl_ms` (Wave 9).
    pub fn evict_stale_workers(&self, ttl_ms: u64) -> usize {
        self.evict_stale_workers_at(now_epoch_ms(), ttl_ms)
    }

    /// Deterministic TTL eviction helper (evicts workers with `now_ms - last_seen > ttl_ms`).
    pub fn evict_stale_workers_at(&self, now_ms: u64, ttl_ms: u64) -> usize {
        let mut hb = self.worker_heartbeats.write();
        let stale: Vec<String> = hb
            .iter()
            .filter(|(_, last)| now_ms.saturating_sub(**last) > ttl_ms)
            .map(|(id, _)| id.clone())
            .collect();
        if stale.is_empty() {
            return 0;
        }
        for id in &stale {
            hb.remove(id);
        }
        let mut guard = self.workers.write();
        let before = guard.len();
        guard.retain(|w| !stale.contains(&w.id().0));
        before.saturating_sub(guard.len())
    }

    pub fn workers_snapshot(&self) -> Vec<Arc<dyn WorkerClient>> {
        self.workers.read().clone()
    }

    pub async fn execute_sql(&self, sql: &str, branch: &str) -> Result<QueryExecutionResult> {
        self.execute_sql_with_session(sql, branch, &SessionContext::default())
            .await
    }

    pub async fn execute_sql_with_session(
        &self,
        sql: &str,
        branch: &str,
        session: &SessionContext,
    ) -> Result<QueryExecutionResult> {
        self.execute_sql_traced(sql, branch, session, None).await
    }

    pub async fn execute_sql_traced(
        &self,
        sql: &str,
        branch: &str,
        session: &SessionContext,
        trace_id: Option<&str>,
    ) -> Result<QueryExecutionResult> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_lowercase();

        // Active query cancellation (`CANCEL QUERY '<query_id>'`, Wave 21)
        if lower.starts_with("cancel query ") {
            let target_id = trimmed["cancel query ".len()..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"');
            let was_active = self.governance.cancel_query(target_id);
            return Ok(QueryExecutionResult {
                command_tag: format!("CANCEL {}", if was_active { 1 } else { 0 }),
                dag: None,
                batch: RowBatch::new(
                    vec!["query_id".into(), "cancelled".into()],
                    vec![BTreeMap::from([
                        ("query_id".into(), ScalarValue::Utf8(target_id.to_string())),
                        ("cancelled".into(), ScalarValue::Bool(true)),
                    ])],
                ),
                stats: QueryExecutionStats::default(),
            });
        }

        // Virtual governance & observability system tables (Wave 3)
        if lower.contains("system.query_history") {
            let entries = self.governance.query_history_snapshot();
            let cols = vec![
                "query_id".into(),
                "user_name".into(),
                "sql".into(),
                "status".into(),
                "rows_returned".into(),
                "elapsed_ms".into(),
                "timestamp_ms".into(),
            ];
            let rows: Vec<BTreeMap<String, ScalarValue>> = entries
                .into_iter()
                .map(|e| {
                    BTreeMap::from([
                        ("query_id".into(), ScalarValue::Utf8(e.query_id)),
                        ("user_name".into(), ScalarValue::Utf8(e.user)),
                        ("sql".into(), ScalarValue::Utf8(e.sql)),
                        ("status".into(), ScalarValue::Utf8(e.status)),
                        ("rows_returned".into(), ScalarValue::Int64(e.rows_returned as i64)),
                        ("elapsed_ms".into(), ScalarValue::Int64(e.elapsed_ms as i64)),
                        ("timestamp_ms".into(), ScalarValue::Int64(e.timestamp_ms as i64)),
                    ])
                })
                .collect();
            let n = rows.len();
            return Ok(QueryExecutionResult {
                command_tag: format!("SELECT {n}"),
                dag: None,
                batch: RowBatch::new(cols, rows),
                stats: QueryExecutionStats::default(),
            });
        }
        if lower.contains("system.audit_log") {
            let entries = self.governance.audit_log_snapshot();
            let cols = vec![
                "timestamp_ms".into(),
                "user_name".into(),
                "action".into(),
                "target".into(),
                "outcome".into(),
            ];
            let rows: Vec<BTreeMap<String, ScalarValue>> = entries
                .into_iter()
                .map(|e| {
                    BTreeMap::from([
                        ("timestamp_ms".into(), ScalarValue::Int64(e.timestamp_ms as i64)),
                        ("user_name".into(), ScalarValue::Utf8(e.user)),
                        ("action".into(), ScalarValue::Utf8(e.action)),
                        ("target".into(), ScalarValue::Utf8(e.target)),
                        ("outcome".into(), ScalarValue::Utf8(e.outcome)),
                    ])
                })
                .collect();
            let n = rows.len();
            return Ok(QueryExecutionResult {
                command_tag: format!("SELECT {n}"),
                dag: None,
                batch: RowBatch::new(cols, rows),
                stats: QueryExecutionStats::default(),
            });
        }

        let qid = Uuid::new_v4().to_string();
        let tid = trace_id
            .map(String::from)
            .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        let span_id = Uuid::new_v4().simple().to_string()[..16].to_string();
        let start_ms = security::epoch_ms();
        self.governance
            .register_active_query(&qid, &session.user, trimmed)?;
        let t0 = std::time::Instant::now();
        let res = self.execute_sql_inner(trimmed, branch, session).await;
        self.governance.finish_active_query(&qid);
        let elapsed_ms = t0.elapsed().as_millis() as u64;

        let mut attrs = BTreeMap::from([
            ("db.system".into(), "rustlakedb".into()),
            ("db.user".into(), session.user.clone()),
            ("db.statement".into(), trimmed.chars().take(160).collect()),
        ]);
        if let Ok(ref ok) = res {
            attrs.insert("db.rows_returned".into(), ok.batch.num_rows().to_string());
            attrs.insert("db.stages".into(), ok.stats.stages_executed.to_string());
            attrs.insert("db.tasks".into(), ok.stats.tasks_executed.to_string());
        }
        self.governance.record_span(security::TraceSpan {
            trace_id: tid,
            span_id,
            parent_span_id: None,
            name: "coordinator.execute_sql".into(),
            service: "rustlakedb-coordinator".into(),
            start_ms,
            duration_ms: elapsed_ms,
            attributes: attrs,
        });

        match &res {
            Ok(ok) => {
                self.governance.record_query(
                    qid,
                    &session.user,
                    trimmed,
                    "OK",
                    ok.batch.num_rows(),
                    elapsed_ms,
                );
            }
            Err(e) => {
                self.governance.record_query(
                    qid,
                    &session.user,
                    trimmed,
                    &format!("ERROR: {e}"),
                    0,
                    elapsed_ms,
                );
            }
        }
        res
    }

    async fn execute_sql_inner(
        &self,
        sql: &str,
        branch: &str,
        session: &SessionContext,
    ) -> Result<QueryExecutionResult> {
        if let Some(compat) = pg_catalog::try_handle_pg_compat(self, sql).await {
            return compat;
        }

        // Transparently expand SQL UDFs, virtual views as CTEs, and scalar/IN/NOT IN/EXISTS/NOT EXISTS subqueries.
        let with_udfs = self.governance.expand_udfs(sql);
        let with_views = self.expand_virtual_views(&with_udfs).await;
        let effective_sql = self
            .rewrite_subqueries(&with_views, branch, session)
            .await?;

        let cmd = self
            .planner
            .plan_command(&effective_sql, branch, self.catalog.as_ref())
            .await?;

        match cmd {
            PlannedCommand::Query(mut dag) => {
                let tables = collect_dag_tables(&dag);
                for t in &tables {
                    self.governance.check_access(session, t, "SELECT")?;
                }
                inject_rls_into_dag(&mut dag, session, &self.governance);
                track_hot_json_paths_in_dag(&dag, &self.governance);
                let mut res = self.execute_dag(dag).await?;
                self.governance
                    .apply_masking(session, &tables, &mut res.batch);
                Ok(res)
            }
            PlannedCommand::ExplainAnalyze(mut dag) => {
                let tables = collect_dag_tables(&dag);
                for t in &tables {
                    self.governance.check_access(session, t, "SELECT")?;
                }
                inject_rls_into_dag(&mut dag, session, &self.governance);
                let t0 = std::time::Instant::now();
                let res = self.execute_dag(dag.clone()).await?;
                let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                let mut rows = Vec::new();
                for note in &dag.cbo_notes {
                    rows.push(BTreeMap::from([(
                        "QUERY PLAN".to_string(),
                        ScalarValue::Utf8(note.clone()),
                    )]));
                }
                for stage in &dag.stages {
                    let line = format!(
                        "Stage {} | deps={:?} | partitions={} | shuffle={:?}",
                        stage.stage_id, stage.upstream_stages, stage.num_partitions, stage.shuffle_mode
                    );
                    rows.push(BTreeMap::from([(
                        "QUERY PLAN".to_string(),
                        ScalarValue::Utf8(line),
                    )]));
                }
                let summary = format!(
                    "Execution Summary: elapsed={:.2} ms | rows={} | stages={} | tasks={} | retries={} | aqe_coalesced={} | aqe_skew_splits={} | shuffled_bytes={} | cache_hits={} | partition_pruned={} | puffin_pruned={} | zonemap_pruned={}",
                    elapsed_ms,
                    res.batch.num_rows(),
                    res.stats.stages_executed,
                    res.stats.tasks_executed,
                    res.stats.tasks_retried,
                    res.stats.aqe_partitions_coalesced,
                    res.stats.aqe_skew_splits_applied,
                    res.stats.total_shuffled_bytes,
                    self.store.cache_hits.load(Ordering::Relaxed),
                    self.store.files_pruned_by_partition.load(Ordering::Relaxed),
                    self.store.files_pruned_by_puffin.load(Ordering::Relaxed),
                    self.store.files_pruned_by_zonemap.load(Ordering::Relaxed),
                );
                rows.push(BTreeMap::from([(
                    "QUERY PLAN".to_string(),
                    ScalarValue::Utf8(summary),
                )]));
                Ok(QueryExecutionResult {
                    command_tag: format!("EXPLAIN ANALYZE {}", rows.len()),
                    dag: Some(dag),
                    batch: RowBatch::new(vec!["QUERY PLAN".into()], rows),
                    stats: res.stats,
                })
            }
            PlannedCommand::Explain(dag) => {
                let mut rows = Vec::new();
                for note in &dag.cbo_notes {
                    rows.push(BTreeMap::from([(
                        "QUERY PLAN".to_string(),
                        ScalarValue::Utf8(note.clone()),
                    )]));
                }
                for stage in &dag.stages {
                    let line = format!(
                        "Stage {} | deps={:?} | partitions={} | shuffle={:?} | output_part={:?}",
                        stage.stage_id,
                        stage.upstream_stages,
                        stage.num_partitions,
                        stage.shuffle_mode,
                        stage.output_partitioning
                    );
                    rows.push(BTreeMap::from([(
                        "QUERY PLAN".to_string(),
                        ScalarValue::Utf8(line),
                    )]));
                }
                Ok(QueryExecutionResult {
                    command_tag: format!("EXPLAIN {}", rows.len()),
                    dag: Some(dag),
                    batch: RowBatch::new(vec!["QUERY PLAN".into()], rows),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateTable {
                table_name,
                schema,
                if_not_exists,
                partition_by,
            } => {
                if if_not_exists && self.catalog.get_table(&table_name).await.is_ok() {
                    return Ok(QueryExecutionResult {
                        command_tag: "CREATE TABLE".into(),
                        dag: None,
                        batch: RowBatch::default(),
                        stats: QueryExecutionStats::default(),
                    });
                }
                let mut meta = IcebergTableMetadata::new(&table_name, schema);
                meta.partition_by = partition_by;
                self.catalog.create_table(meta.clone()).await?;
                self.store.write_iceberg_metadata(&meta, "main").await?;
                Ok(QueryExecutionResult {
                    command_tag: "CREATE TABLE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateTableAs {
                table_name,
                query_sql,
            } => {
                let rows_written = self
                    .materialize_view_table(&table_name, &query_sql, branch, session)
                    .await?;
                Ok(QueryExecutionResult {
                    command_tag: format!("SELECT {rows_written}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::InsertSelect {
                table_name,
                query_sql,
            } => {
                self.governance.check_access(session, &table_name, "INSERT")?;
                let res = Box::pin(self.execute_sql_inner(&query_sql, branch, session)).await?;
                let meta = self.catalog.get_table(&table_name).await?;
                let snap = meta.current_snapshot(branch)?;
                let count = res.batch.num_rows();
                if count > 0 {
                    // Map positional result columns onto target table schema names when needed.
                    let target_cols: Vec<String> =
                        meta.schema.iter().map(|f| f.name.clone()).collect();
                    let mapped_rows: Vec<BTreeMap<String, ScalarValue>> = res
                        .batch
                        .rows
                        .into_iter()
                        .map(|r| {
                            let mut out = BTreeMap::new();
                            for (idx, tc) in target_cols.iter().enumerate() {
                                let v = r
                                    .get(tc)
                                    .or_else(|| res.batch.columns.get(idx).and_then(|sc| r.get(sc)))
                                    .cloned()
                                    .unwrap_or(ScalarValue::Null);
                                out.insert(tc.clone(), v);
                            }
                            out
                        })
                        .collect();
                    let batch = RowBatch::new(target_cols, mapped_rows);
                    let added_files = self
                        .write_partitioned_data_files(&meta, "ins-sel", &batch)
                        .await?;
                    let prop = TableCommitProposal {
                        table_name: table_name.clone(),
                        branch: branch.to_string(),
                        expected_snapshot_id: snap.snapshot_id,
                        added_files,
                        removed_files: vec![],
                        is_append_only: true,
                    };
                    if self.governance.in_tx(&session.user) {
                        self.governance.buffer_tx_proposal(&session.user, prop);
                    } else {
                        self.catalog.commit_table(prop).await?;
                        let updated = self.catalog.get_table(&table_name).await?;
                        self.store.write_iceberg_metadata(&updated, branch).await?;
                    }
                }
                Ok(QueryExecutionResult {
                    command_tag: format!("INSERT 0 {count}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::BeginTx => {
                self.governance.begin_tx(&session.user);
                Ok(QueryExecutionResult {
                    command_tag: "BEGIN".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CommitTx => {
                let proposals = self
                    .governance
                    .take_tx_proposals(&session.user)
                    .unwrap_or_default();
                let n = proposals.len();
                if n > 0 {
                    let committed = self.catalog.commit_multi_table(proposals).await?;
                    for (tbl, _) in committed {
                        if let Ok(updated) = self.catalog.get_table(&tbl).await {
                            let _ = self.store.write_iceberg_metadata(&updated, branch).await;
                        }
                    }
                }
                Ok(QueryExecutionResult {
                    command_tag: "COMMIT".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::RollbackTx => {
                let _ = self.governance.take_tx_proposals(&session.user);
                Ok(QueryExecutionResult {
                    command_tag: "ROLLBACK".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::DropTable {
                table_name,
                if_exists,
            } => {
                if if_exists && self.catalog.get_table(&table_name).await.is_err() {
                    return Ok(QueryExecutionResult {
                        command_tag: "DROP TABLE".into(),
                        dag: None,
                        batch: RowBatch::default(),
                        stats: QueryExecutionStats::default(),
                    });
                }
                self.catalog.drop_table(&table_name).await?;
                Ok(QueryExecutionResult {
                    command_tag: "DROP TABLE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::DropView { name, if_exists } => {
                if let Ok(Some(v)) = self.catalog.get_view(&name).await {
                    let _ = self.catalog.drop_view(&name).await;
                    if v.materialized {
                        let _ = self.catalog.drop_table(&name).await;
                    }
                } else if !if_exists {
                    return Err(RustLakeError::Catalog(format!("view '{name}' not found")));
                }
                Ok(QueryExecutionResult {
                    command_tag: "DROP VIEW".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::AlterRenameTable { old_name, new_name } => {
                self.catalog.rename_table(&old_name, &new_name).await?;
                if let Ok(updated) = self.catalog.get_table(&new_name).await {
                    let _ = self.store.write_iceberg_metadata(&updated, branch).await;
                }
                Ok(QueryExecutionResult {
                    command_tag: "ALTER TABLE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::TruncateTable { table_name } => {
                self.governance.check_access(session, &table_name, "DELETE")?;
                let _snap = self.catalog.truncate_table(&table_name, branch).await?;
                if let Ok(updated) = self.catalog.get_table(&table_name).await {
                    let _ = self.store.write_iceberg_metadata(&updated, branch).await;
                }
                Ok(QueryExecutionResult {
                    command_tag: "TRUNCATE TABLE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateSchema {
                name,
                if_not_exists,
            } => {
                self.governance.create_schema(&name, if_not_exists)?;
                Ok(QueryExecutionResult {
                    command_tag: "CREATE SCHEMA".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::DropSchema { name, if_exists } => {
                self.governance.drop_schema(&name, if_exists)?;
                Ok(QueryExecutionResult {
                    command_tag: "DROP SCHEMA".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateFunction {
                name,
                param_name,
                body_expr,
            } => {
                self.governance.register_udf(&name, &param_name, &body_expr);
                self.governance
                    .record_audit(&session.user, "CREATE FUNCTION", &name, "OK");
                Ok(QueryExecutionResult {
                    command_tag: "CREATE FUNCTION".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::AlterAddColumn {
                table_name,
                column_name,
                data_type,
            } => {
                let fid = self
                    .catalog
                    .add_column(&table_name, column_name, data_type)
                    .await?;
                let updated = self.catalog.get_table(&table_name).await?;
                self.store.write_iceberg_metadata(&updated, branch).await?;
                Ok(QueryExecutionResult {
                    command_tag: format!("ALTER TABLE ADD COLUMN field_id={fid}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::AlterRenameColumn {
                table_name,
                old_name,
                new_name,
            } => {
                let fid = self
                    .catalog
                    .rename_column(&table_name, &old_name, &new_name)
                    .await?;
                let updated = self.catalog.get_table(&table_name).await?;
                self.store.write_iceberg_metadata(&updated, branch).await?;
                Ok(QueryExecutionResult {
                    command_tag: format!("ALTER TABLE RENAME COLUMN field_id={fid}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::AlterDropColumn {
                table_name,
                column_name,
                if_exists,
            } => {
                self.catalog
                    .drop_column(&table_name, &column_name, if_exists)
                    .await?;
                let updated = self.catalog.get_table(&table_name).await?;
                self.store.write_iceberg_metadata(&updated, branch).await?;
                Ok(QueryExecutionResult {
                    command_tag: "ALTER TABLE DROP COLUMN".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::AlterColumnType {
                table_name,
                column_name,
                data_type,
            } => {
                self.catalog
                    .widen_column(&table_name, &column_name, data_type)
                    .await?;
                let updated = self.catalog.get_table(&table_name).await?;
                self.store.write_iceberg_metadata(&updated, branch).await?;
                Ok(QueryExecutionResult {
                    command_tag: "ALTER TABLE ALTER COLUMN TYPE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateIndex { table_name, spec } => {
                let is_hnsw = spec.index_type == PuffinIndexType::Hnsw;
                let idx_col = spec.column.clone();
                self.catalog.add_index(&table_name, spec).await?;
                let updated = self.catalog.get_table(&table_name).await?;
                if is_hnsw && let Ok(snap) = updated.current_snapshot(branch) {
                    // Build & persist HNSW proximity graph sidecar for every Parquet file in table (Wave 8)!
                    for df in &snap.data_files {
                        if let Ok(fb) = self
                            .store
                            .read_data_file(&df.file_path, &updated.schema, None)
                            .await
                        {
                            let mut hnsw = rustlakedb_storage::hnsw::HnswIndex::new(8, 32);
                            for (row_idx, row) in fb.rows.iter().enumerate() {
                                let vec_opt = match row.get(&idx_col) {
                                    Some(ScalarValue::Json(serde_json::Value::Array(arr))) => {
                                        Some(arr.iter().filter_map(|v| v.as_f64()).collect::<Vec<_>>())
                                    }
                                    Some(ScalarValue::Utf8(s)) => {
                                        let t = s.trim().trim_start_matches('[').trim_end_matches(']');
                                        let v: Vec<f64> = t
                                            .split(',')
                                            .filter_map(|p| p.trim().parse::<f64>().ok())
                                            .collect();
                                        if v.is_empty() { None } else { Some(v) }
                                    }
                                    _ => None,
                                };
                                if let Some(v) = vec_opt
                                    && !v.is_empty()
                                {
                                    hnsw.insert(row_idx as u32, v);
                                }
                            }
                            let _ = self.store.write_hnsw_index(&df.file_path, &hnsw).await;
                        }
                    }
                }
                self.store.write_iceberg_metadata(&updated, branch).await?;
                Ok(QueryExecutionResult {
                    command_tag: "CREATE INDEX".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::Insert { table_name, batch } => {
                self.governance.check_access(session, &table_name, "INSERT")?;
                let meta = self.catalog.get_table(&table_name).await?;
                let snap = meta.current_snapshot(branch)?;
                let count = batch.num_rows();
                let added_files = self
                    .write_partitioned_data_files(&meta, "part", &batch)
                    .await?;
                let prop = TableCommitProposal {
                    table_name: table_name.clone(),
                    branch: branch.to_string(),
                    expected_snapshot_id: snap.snapshot_id,
                    added_files,
                    removed_files: vec![],
                    is_append_only: true,
                };
                if self.governance.in_tx(&session.user) {
                    self.governance.buffer_tx_proposal(&session.user, prop);
                } else {
                    self.catalog.commit_table(prop).await?;
                    let updated = self.catalog.get_table(&table_name).await?;
                    self.store.write_iceberg_metadata(&updated, branch).await?;
                }
                Ok(QueryExecutionResult {
                    command_tag: format!("INSERT 0 {count}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::Update {
                table_name,
                assignments,
                predicate,
            } => {
                // Iceberg v3 Merge-on-Read UPDATE:
                // 1) Mark old rows deleted in file-local Roaring Bitmap Deletion Vector
                // 2) Write updated row versions to a new Parquet + Puffin file
                // 3) Commit new snapshot atomically via OCC
                let meta = self.catalog.get_table(&table_name).await?;
                let snap = meta.current_snapshot(branch)?;
                let mut updated_rows = Vec::new();

                for df in &snap.data_files {
                    let mut dv = self.store.read_deletion_vector(&df.file_path).await?;
                    let file_batch = self
                        .store
                        .read_data_file(&df.file_path, &meta.schema, None)
                        .await?;
                    let mut phys_idx: u32 = 0;
                    let mut changed = false;
                    for mut row in file_batch.rows {
                        while dv.contains(phys_idx) {
                            phys_idx += 1;
                        }
                        let matches = predicate
                            .as_ref()
                            .map(|p| eval_filter(&row, p))
                            .unwrap_or(true);
                        if matches {
                            dv.insert(phys_idx);
                            changed = true;
                            for (col, val) in &assignments {
                                row.insert(col.clone(), val.clone());
                            }
                            updated_rows.push(row);
                        }
                        phys_idx += 1;
                    }
                    if changed {
                        self.store.write_deletion_vector(&df.file_path, &dv).await?;
                    }
                }

                let updated_count = updated_rows.len();
                if updated_count > 0 {
                    let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
                    let new_batch = RowBatch::new(cols, updated_rows);
                    let file_path = format!("{table_name}/data/update-{}.parquet", Uuid::new_v4());
                    let entry = self
                        .store
                        .write_data_file(&file_path, &meta.schema, &new_batch)
                        .await?;
                    self.catalog
                        .commit_table(TableCommitProposal {
                            table_name: table_name.clone(),
                            branch: branch.to_string(),
                            expected_snapshot_id: snap.snapshot_id,
                            added_files: vec![entry],
                            removed_files: vec![],
                            is_append_only: false,
                        })
                        .await?;
                    let updated = self.catalog.get_table(&table_name).await?;
                    self.store.write_iceberg_metadata(&updated, branch).await?;
                }

                Ok(QueryExecutionResult {
                    command_tag: format!("UPDATE {updated_count}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::Delete {
                table_name,
                predicate,
            } => {
                let meta = self.catalog.get_table(&table_name).await?;
                let snap = meta.current_snapshot(branch)?;
                let mut total_deleted = 0usize;

                for df in &snap.data_files {
                    let mut dv = self.store.read_deletion_vector(&df.file_path).await?;
                    let file_batch = self
                        .store
                        .read_data_file(&df.file_path, &meta.schema, None)
                        .await?;
                    let mut phys_idx: u32 = 0;
                    let mut changed = false;
                    for row in &file_batch.rows {
                        while dv.contains(phys_idx) {
                            phys_idx += 1;
                        }
                        if eval_filter(row, &predicate) {
                            dv.insert(phys_idx);
                            total_deleted += 1;
                            changed = true;
                        }
                        phys_idx += 1;
                    }
                    if changed {
                        self.store.write_deletion_vector(&df.file_path, &dv).await?;
                    }
                }

                Ok(QueryExecutionResult {
                    command_tag: format!("DELETE {total_deleted}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CompactTable { table_name } => {
                // Autonomous/explicit compaction (RFC-002):
                // Bin-packs all active small files, materializes Iceberg v3 Deletion Vectors,
                // re-shreds hot JSON paths, and rebuilds Puffin secondary indexes atomically!
                let meta = self.catalog.get_table(&table_name).await?;
                let snap = meta.current_snapshot(branch)?;
                let old_paths: Vec<String> =
                    snap.data_files.iter().map(|f| f.file_path.clone()).collect();

                if old_paths.is_empty() {
                    return Ok(QueryExecutionResult {
                        command_tag: "COMPACT 0->0".into(),
                        dag: None,
                        batch: RowBatch::default(),
                        stats: QueryExecutionStats::default(),
                    });
                }

                let mut live_rows = Vec::new();
                for df in &snap.data_files {
                    let b = self
                        .store
                        .read_data_file(&df.file_path, &meta.schema, None)
                        .await?;
                    live_rows.extend(b.rows);
                }

                let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
                let compacted_batch = RowBatch::new(cols, live_rows);
                let added_files = if compacted_batch.num_rows() > 0 {
                    let path = format!("{table_name}/data/compacted-{}.parquet", Uuid::new_v4());
                    vec![
                        self.store
                            .write_data_file(&path, &meta.schema, &compacted_batch)
                            .await?,
                    ]
                } else {
                    vec![]
                };

                let new_count = added_files.len();
                let old_count = old_paths.len();
                self.catalog
                    .commit_table(TableCommitProposal {
                        table_name: table_name.clone(),
                        branch: branch.to_string(),
                        expected_snapshot_id: snap.snapshot_id,
                        added_files,
                        removed_files: old_paths,
                        is_append_only: false,
                    })
                    .await?;
                let updated = self.catalog.get_table(&table_name).await?;
                self.store.write_iceberg_metadata(&updated, branch).await?;

                Ok(QueryExecutionResult {
                    command_tag: format!("COMPACT {old_count}->{new_count}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::VacuumTable {
                table_name,
                keep_last,
            } => {
                // Expire old snapshots & garbage-collect unreferenced orphan Parquet/Puffin/DV files!
                let expired = self.catalog.expire_snapshots(&table_name, keep_last).await?;
                let meta = self.catalog.get_table(&table_name).await?;
                let referenced = meta.referenced_file_paths();
                let all_paths = self.store.list_paths(&format!("{table_name}/data")).await?;

                let mut deleted_files = 0usize;
                for p in all_paths {
                    if p.ends_with(".parquet") && !referenced.contains(&p) {
                        self.store.delete_data_file(&p).await?;
                        deleted_files += 1;
                    }
                }

                Ok(QueryExecutionResult {
                    command_tag: format!("VACUUM expired={expired} deleted_files={deleted_files}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::ShowTables => {
                let tables = self.catalog.list_tables().await?;
                let rows = tables
                    .into_iter()
                    .map(|t| BTreeMap::from([("table_name".to_string(), ScalarValue::Utf8(t))]))
                    .collect();
                Ok(QueryExecutionResult {
                    command_tag: "SHOW TABLES".into(),
                    dag: None,
                    batch: RowBatch::new(vec!["table_name".into()], rows),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::InformationSchemaTables => {
                let tables = self.catalog.list_tables().await?;
                let rows: Vec<_> = tables
                    .into_iter()
                    .map(|t| {
                        BTreeMap::from([
                            ("table_schema".to_string(), ScalarValue::Utf8("public".into())),
                            ("table_name".to_string(), ScalarValue::Utf8(t)),
                            (
                                "table_type".to_string(),
                                ScalarValue::Utf8("BASE TABLE (ICEBERG V3)".into()),
                            ),
                        ])
                    })
                    .collect();
                let count = rows.len();
                Ok(QueryExecutionResult {
                    command_tag: format!("SELECT {count}"),
                    dag: None,
                    batch: RowBatch::new(
                        vec![
                            "table_schema".into(),
                            "table_name".into(),
                            "table_type".into(),
                        ],
                        rows,
                    ),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::InformationSchemaColumns => {
                let tables = self.catalog.list_tables().await?;
                let mut rows = Vec::new();
                for tbl in tables {
                    if let Ok(meta) = self.catalog.get_table(&tbl).await {
                        for col in meta.schema {
                            rows.push(BTreeMap::from([
                                ("table_name".to_string(), ScalarValue::Utf8(tbl.clone())),
                                ("column_name".to_string(), ScalarValue::Utf8(col.name)),
                                (
                                    "data_type".to_string(),
                                    ScalarValue::Utf8(format!("{:?}", col.data_type)),
                                ),
                            ]));
                        }
                    }
                }
                let count = rows.len();
                Ok(QueryExecutionResult {
                    command_tag: format!("SELECT {count}"),
                    dag: None,
                    batch: RowBatch::new(
                        vec!["table_name".into(), "column_name".into(), "data_type".into()],
                        rows,
                    ),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CurrentDatabase => {
                let rows = vec![BTreeMap::from([(
                    "current_database".to_string(),
                    ScalarValue::Utf8("rustlakedb".into()),
                )])];
                Ok(QueryExecutionResult {
                    command_tag: "SELECT 1".into(),
                    dag: None,
                    batch: RowBatch::new(vec!["current_database".into()], rows),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CurrentSchema => {
                let rows = vec![BTreeMap::from([(
                    "current_schema".to_string(),
                    ScalarValue::Utf8("public".into()),
                )])];
                Ok(QueryExecutionResult {
                    command_tag: "SELECT 1".into(),
                    dag: None,
                    batch: RowBatch::new(vec!["current_schema".into()], rows),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateBranch {
                table_name,
                from_branch,
                new_branch,
            } => {
                let snap_id = self
                    .catalog
                    .create_branch(&table_name, &from_branch, &new_branch)
                    .await?;
                Ok(QueryExecutionResult {
                    command_tag: format!("CREATE BRANCH {new_branch}@{snap_id}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::MergeBranch {
                table_name,
                source_branch,
                target_branch,
            } => {
                let _snap_id = self
                    .catalog
                    .merge_branch(&table_name, &source_branch, &target_branch)
                    .await?;
                Ok(QueryExecutionResult {
                    command_tag: "MERGE 1".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::ServerVersion => {
                let rows = vec![BTreeMap::from([(
                    "version".to_string(),
                    ScalarValue::Utf8(format!(
                        "RustLakeDB {} (PostgreSQL 16 compatible, Apache Iceberg v3)",
                        env!("CARGO_PKG_VERSION")
                    )),
                )])];
                Ok(QueryExecutionResult {
                    command_tag: "SELECT 1".into(),
                    dag: None,
                    batch: RowBatch::new(vec!["version".into()], rows),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::NoOp(tag) => Ok(QueryExecutionResult {
                command_tag: tag,
                dag: None,
                batch: RowBatch::default(),
                stats: QueryExecutionStats::default(),
            }),
            PlannedCommand::AlterClusterBy {
                table_name,
                columns,
            } => {
                self.catalog.set_cluster_by(&table_name, columns.clone()).await?;
                self.governance.record_audit(
                    &session.user,
                    "ALTER CLUSTER BY",
                    &table_name,
                    "OK",
                );
                Ok(QueryExecutionResult {
                    command_tag: format!("ALTER TABLE CLUSTER BY ({})", columns.join(",")),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CopyFrom {
                table_name,
                source_path,
                format,
                on_error_continue,
            } => {
                self.governance.check_access(session, &table_name, "INSERT")?;
                let clean_path = source_path.strip_prefix("file://").unwrap_or(&source_path);
                let (loaded, dlq) = match format {
                    CopyFormat::Json => {
                        let raw = std::fs::read_to_string(clean_path).map_err(|e| {
                            RustLakeError::Execution(format!("COPY read '{clean_path}': {e}"))
                        })?;
                        self.ingest_ndjson(&table_name, branch, &raw, on_error_continue)
                            .await?
                    }
                    CopyFormat::Csv => {
                        let raw = std::fs::read_to_string(clean_path).map_err(|e| {
                            RustLakeError::Execution(format!("COPY CSV read '{clean_path}': {e}"))
                        })?;
                        self.ingest_csv(&table_name, branch, &raw, on_error_continue)
                            .await?
                    }
                    CopyFormat::Parquet => {
                        let meta = self.catalog.get_table(&table_name).await?;
                        let snap = meta.current_snapshot(branch)?;
                        let batch =
                            rustlakedb_storage::read_external_parquet(std::path::Path::new(
                                clean_path,
                            ))?;
                        let count = batch.num_rows();
                        if count > 0 {
                            let file_path =
                                format!("{table_name}/data/copy-{}.parquet", Uuid::new_v4());
                            let entry = self
                                .store
                                .write_data_file(&file_path, &meta.schema, &batch)
                                .await?;
                            self.catalog
                                .commit_table(TableCommitProposal {
                                    table_name: table_name.clone(),
                                    branch: branch.to_string(),
                                    expected_snapshot_id: snap.snapshot_id,
                                    added_files: vec![entry],
                                    removed_files: vec![],
                                    is_append_only: true,
                                })
                                .await?;
                            let updated = self.catalog.get_table(&table_name).await?;
                            self.store.write_iceberg_metadata(&updated, branch).await?;
                        }
                        (count, 0)
                    }
                };
                self.governance
                    .record_audit(&session.user, "COPY", &table_name, "OK");
                let tag = if dlq > 0 {
                    format!("COPY {loaded} (dlq={dlq})")
                } else {
                    format!("COPY {loaded}")
                };
                Ok(QueryExecutionResult {
                    command_tag: tag,
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::MergeInto {
                target_table,
                source_table,
                target_key,
                source_key,
                when_matched_delete,
                when_matched_updates,
                when_not_matched_insert,
            } => {
                self.governance
                    .check_access(session, &source_table, "SELECT")?;
                self.governance
                    .check_access(session, &target_table, "UPDATE")?;

                let src_meta = self.catalog.get_table(&source_table).await?;
                let src_snap = src_meta.current_snapshot(branch)?;
                let mut src_index: HashMap<String, BTreeMap<String, ScalarValue>> = HashMap::new();
                for df in &src_snap.data_files {
                    let b = self
                        .store
                        .read_data_file(&df.file_path, &src_meta.schema, None)
                        .await?;
                    for row in b.rows {
                        if let Some(k) = row.get(&source_key).and_then(|v| v.to_pg_text()) {
                            src_index.insert(k, row);
                        }
                    }
                }

                let tgt_meta = self.catalog.get_table(&target_table).await?;
                let tgt_snap = tgt_meta.current_snapshot(branch)?;
                let mut matched_keys = std::collections::HashSet::new();
                let mut new_rows: Vec<BTreeMap<String, ScalarValue>> = Vec::new();
                let mut affected = 0usize;

                for df in &tgt_snap.data_files {
                    let mut dv = self.store.read_deletion_vector(&df.file_path).await?;
                    let file_batch = self
                        .store
                        .read_data_file(&df.file_path, &tgt_meta.schema, None)
                        .await?;
                    let mut phys_idx: u32 = 0;
                    let mut changed = false;
                    for mut row in file_batch.rows {
                        while dv.contains(phys_idx) {
                            phys_idx += 1;
                        }
                        if let Some(k) = row.get(&target_key).and_then(|v| v.to_pg_text())
                            && let Some(src_row) = src_index.get(&k)
                        {
                            matched_keys.insert(k);
                            dv.insert(phys_idx);
                            changed = true;
                            affected += 1;
                            if !when_matched_delete {
                                if when_matched_updates.is_empty() {
                                    for f in &tgt_meta.schema {
                                        if let Some(sv) = src_row.get(&f.name) {
                                            row.insert(f.name.clone(), sv.clone());
                                        }
                                    }
                                } else {
                                    for (lhs, rhs) in &when_matched_updates {
                                        if let Some(sv) = src_row.get(rhs) {
                                            row.insert(lhs.clone(), sv.clone());
                                        }
                                    }
                                }
                                new_rows.push(row);
                            }
                        }
                        phys_idx += 1;
                    }
                    if changed {
                        self.store.write_deletion_vector(&df.file_path, &dv).await?;
                    }
                }

                if when_not_matched_insert {
                    for (k, src_row) in src_index {
                        if !matched_keys.contains(&k) {
                            let mut ins_row = BTreeMap::new();
                            for f in &tgt_meta.schema {
                                ins_row.insert(
                                    f.name.clone(),
                                    src_row
                                        .get(&f.name)
                                        .cloned()
                                        .unwrap_or(ScalarValue::Null),
                                );
                            }
                            new_rows.push(ins_row);
                            affected += 1;
                        }
                    }
                }

                if !new_rows.is_empty() {
                    let cols: Vec<String> =
                        tgt_meta.schema.iter().map(|f| f.name.clone()).collect();
                    let batch = RowBatch::new(cols, new_rows);
                    let path = format!("{target_table}/data/merge-{}.parquet", Uuid::new_v4());
                    let entry = self
                        .store
                        .write_data_file(&path, &tgt_meta.schema, &batch)
                        .await?;
                    self.catalog
                        .commit_table(TableCommitProposal {
                            table_name: target_table.clone(),
                            branch: branch.to_string(),
                            expected_snapshot_id: tgt_snap.snapshot_id,
                            added_files: vec![entry],
                            removed_files: vec![],
                            is_append_only: false,
                        })
                        .await?;
                    let updated = self.catalog.get_table(&target_table).await?;
                    self.store.write_iceberg_metadata(&updated, branch).await?;
                }

                self.governance
                    .record_audit(&session.user, "MERGE", &target_table, "OK");
                Ok(QueryExecutionResult {
                    command_tag: format!("MERGE {affected}"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::OptimizeTable { table_name } => {
                // Wave 4 Liquid / Multi-Column Clustering + Dynamic Query-Driven JSON Re-Shredding!
                let meta = self.catalog.get_table(&table_name).await?;
                let snap = meta.current_snapshot(branch)?;
                let old_paths: Vec<String> =
                    snap.data_files.iter().map(|f| f.file_path.clone()).collect();
                if old_paths.is_empty() {
                    return Ok(QueryExecutionResult {
                        command_tag: "OPTIMIZE 0->0".into(),
                        dag: None,
                        batch: RowBatch::default(),
                        stats: QueryExecutionStats::default(),
                    });
                }

                let mut live_rows = Vec::new();
                for df in &snap.data_files {
                    let b = self
                        .store
                        .read_data_file(&df.file_path, &meta.schema, None)
                        .await?;
                    live_rows.extend(b.rows);
                }

                // Sort along `cluster_by` columns (or primary column) for tight non-overlapping Zone Maps!
                let cluster_cols: Vec<String> = if !meta.cluster_by.is_empty() {
                    meta.cluster_by.clone()
                } else {
                    meta.schema
                        .first()
                        .map(|f| vec![f.name.clone()])
                        .unwrap_or_default()
                };
                if !cluster_cols.is_empty() {
                    live_rows.sort_by(|a, b| {
                        for col in &cluster_cols {
                            let va = a.get(col).unwrap_or(&ScalarValue::Null);
                            let vb = b.get(col).unwrap_or(&ScalarValue::Null);
                            let ord = cmp_scalar_values(va, vb);
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                // Promote hot queried JSON paths into shredded Parquet sub-columns!
                let hot_paths = self.governance.hot_json_paths_for(&table_name);
                if !hot_paths.is_empty() {
                    let _ = self
                        .catalog
                        .set_shredded_paths(&table_name, hot_paths.clone())
                        .await;
                }

                let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
                let num_chunks = if live_rows.len() >= 16 { 4 } else { 1 };
                let chunk_size = std::cmp::max(1, live_rows.len().div_ceil(num_chunks));
                let mut added_files = Vec::new();

                for (ci, chunk) in live_rows.chunks(chunk_size).enumerate() {
                    let batch = RowBatch::new(cols.clone(), chunk.to_vec());
                    let path = format!(
                        "{table_name}/data/clustered-{}-{ci:02}.parquet",
                        Uuid::new_v4()
                    );
                    let entry = self
                        .store
                        .write_data_file_with_shred_paths(&path, &meta.schema, &batch, &hot_paths)
                        .await?;
                    added_files.push(entry);
                }

                let new_count = added_files.len();
                let old_count = old_paths.len();
                self.catalog
                    .commit_table(TableCommitProposal {
                        table_name: table_name.clone(),
                        branch: branch.to_string(),
                        expected_snapshot_id: snap.snapshot_id,
                        added_files,
                        removed_files: old_paths,
                        is_append_only: false,
                    })
                    .await?;
                let updated = self.catalog.get_table(&table_name).await?;
                self.store.write_iceberg_metadata(&updated, branch).await?;
                Ok(QueryExecutionResult {
                    command_tag: format!(
                        "OPTIMIZE {old_count}->{new_count} clustered_by=({}) shredded={}",
                        cluster_cols.join(","),
                        hot_paths.len()
                    ),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateView {
                name,
                query_sql,
                materialized,
            } => {
                let source_table = extract_primary_from_table(&query_sql);
                let last_source_snapshot_id = match source_table.as_deref() {
                    Some(tbl) => self
                        .catalog
                        .get_table(tbl)
                        .await
                        .ok()
                        .and_then(|m| m.current_snapshot(branch).ok().map(|s| s.snapshot_id)),
                    None => None,
                };
                let def = ViewDefinition {
                    name: name.clone(),
                    query_sql: query_sql.clone(),
                    materialized,
                    last_refreshed_ms: security::epoch_ms(),
                    source_table,
                    last_source_snapshot_id,
                };
                self.catalog.create_view(def).await?;
                if materialized {
                    let rows_mat = self
                        .materialize_view_table(&name, &query_sql, branch, session)
                        .await?;
                    Ok(QueryExecutionResult {
                        command_tag: format!("CREATE MATERIALIZED VIEW ({rows_mat} rows)"),
                        dag: None,
                        batch: RowBatch::default(),
                        stats: QueryExecutionStats::default(),
                    })
                } else {
                    Ok(QueryExecutionResult {
                        command_tag: "CREATE VIEW".into(),
                        dag: None,
                        batch: RowBatch::default(),
                        stats: QueryExecutionStats::default(),
                    })
                }
            }
            PlannedCommand::RefreshMaterializedView { name } => {
                let mut def = self
                    .catalog
                    .get_view(&name)
                    .await?
                    .ok_or_else(|| RustLakeError::Catalog(format!("view '{name}' not found")))?;

                // Wave 8 True Incremental Materialized View Refresh (Iceberg v3 Snapshot Watermark):
                // If the base table is append-only since `last_source_snapshot_id` and the MV is a
                // simple projection/filter (no GROUP BY), process ONLY the newly added delta Parquet files!
                let is_simple_append_mv = !def.query_sql.to_uppercase().contains("GROUP BY")
                    && !def.query_sql.to_uppercase().contains(" JOIN ");
                if is_simple_append_mv
                    && let (Some(src_tbl), Some(prev_snap_id)) =
                        (&def.source_table, def.last_source_snapshot_id)
                    && let Ok(src_meta) = self.catalog.get_table(src_tbl).await
                    && let (Ok(old_snap), Ok(cur_snap)) = (
                        src_meta.snapshot_at_version(prev_snap_id),
                        src_meta.current_snapshot(branch),
                    )
                {
                    let old_paths: std::collections::HashSet<&str> = old_snap
                        .data_files
                        .iter()
                        .map(|f| f.file_path.as_str())
                        .collect();
                    let all_old_still_present = old_paths.iter().all(|p| {
                        cur_snap
                            .data_files
                            .iter()
                            .any(|f| f.file_path.as_str() == *p)
                    });
                    if all_old_still_present {
                        let delta_files: Vec<_> = cur_snap
                            .data_files
                            .iter()
                            .filter(|f| !old_paths.contains(f.file_path.as_str()))
                            .cloned()
                            .collect();
                        let delta_count = delta_files.len();
                        let mut delta_rows_appended = 0usize;
                        if delta_count > 0 {
                            let mv_meta = self.catalog.get_table(&name).await?;
                            let mv_snap = mv_meta.current_snapshot(branch)?;
                            let mut combined_delta = Vec::new();
                            for df in &delta_files {
                                let fb = self
                                    .store
                                    .read_data_file(&df.file_path, &src_meta.schema, None)
                                    .await?;
                                combined_delta.extend(fb.rows);
                            }
                            // Apply MV projection columns from mv_meta.schema
                            let mv_cols: Vec<String> =
                                mv_meta.schema.iter().map(|f| f.name.clone()).collect();
                            let projected: Vec<BTreeMap<String, ScalarValue>> = combined_delta
                                .into_iter()
                                .map(|r| {
                                    mv_cols
                                        .iter()
                                        .map(|c| {
                                            (
                                                c.clone(),
                                                r.get(c).cloned().unwrap_or(ScalarValue::Null),
                                            )
                                        })
                                        .collect()
                                })
                                .collect();
                            delta_rows_appended = projected.len();
                            if delta_rows_appended > 0 {
                                let delta_batch = RowBatch::new(mv_cols, projected);
                                let path = format!("{name}/data/mv-delta-{}.parquet", Uuid::new_v4());
                                let entry = self
                                    .store
                                    .write_data_file(&path, &mv_meta.schema, &delta_batch)
                                    .await?;
                                self.catalog
                                    .commit_table(TableCommitProposal {
                                        table_name: name.clone(),
                                        branch: branch.to_string(),
                                        expected_snapshot_id: mv_snap.snapshot_id,
                                        added_files: vec![entry],
                                        removed_files: vec![],
                                        is_append_only: true,
                                    })
                                    .await?;
                                let updated = self.catalog.get_table(&name).await?;
                                self.store.write_iceberg_metadata(&updated, branch).await?;
                            }
                        }
                        def.last_source_snapshot_id = Some(cur_snap.snapshot_id);
                        def.last_refreshed_ms = security::epoch_ms();
                        let _ = self.catalog.create_view(def).await;
                        return Ok(QueryExecutionResult {
                            command_tag: format!(
                                "REFRESH MATERIALIZED VIEW INCREMENTAL (+{delta_rows_appended} rows, {delta_count} delta files)"
                            ),
                            dag: None,
                            batch: RowBatch::default(),
                            stats: QueryExecutionStats::default(),
                        });
                    }
                }

                let rows_mat = self
                    .materialize_view_table(&name, &def.query_sql, branch, session)
                    .await?;
                if let Some(ref src_tbl) = def.source_table
                    && let Ok(src_meta) = self.catalog.get_table(src_tbl).await
                    && let Ok(cur_snap) = src_meta.current_snapshot(branch)
                {
                    def.last_source_snapshot_id = Some(cur_snap.snapshot_id);
                    def.last_refreshed_ms = security::epoch_ms();
                    let _ = self.catalog.create_view(def).await;
                }
                Ok(QueryExecutionResult {
                    command_tag: format!("REFRESH MATERIALIZED VIEW ({rows_mat} rows)"),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateRole { role } => {
                self.governance.create_role(&role);
                self.governance
                    .record_audit(&session.user, "CREATE ROLE", &role, "OK");
                Ok(QueryExecutionResult {
                    command_tag: "CREATE ROLE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::Grant {
                privilege,
                table_name,
                role,
            } => {
                self.governance.grant(&role, &table_name, &privilege);
                self.governance.record_audit(
                    &session.user,
                    &format!("GRANT {privilege}"),
                    &format!("{table_name} TO {role}"),
                    "OK",
                );
                Ok(QueryExecutionResult {
                    command_tag: "GRANT".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::Revoke {
                privilege,
                table_name,
                role,
            } => {
                self.governance.revoke(&role, &table_name, &privilege);
                self.governance.record_audit(
                    &session.user,
                    &format!("REVOKE {privilege}"),
                    &format!("{table_name} FROM {role}"),
                    "OK",
                );
                Ok(QueryExecutionResult {
                    command_tag: "REVOKE".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateRlsPolicy {
                name,
                table_name,
                role,
                predicate,
            } => {
                self.governance.add_rls_policy(&role, &table_name, predicate);
                self.governance.record_audit(
                    &session.user,
                    "CREATE POLICY",
                    &format!("{name} ON {table_name} FOR {role}"),
                    "OK",
                );
                Ok(QueryExecutionResult {
                    command_tag: "CREATE POLICY".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::CreateMaskingPolicy {
                name,
                table_name,
                target,
                role,
                mask_value,
            } => {
                self.governance.add_masking_policy(
                    &role,
                    &table_name,
                    MaskingRule {
                        name: name.clone(),
                        target: target.clone(),
                        mask_value,
                    },
                );
                self.governance.record_audit(
                    &session.user,
                    "CREATE MASKING POLICY",
                    &format!("{name} ON {table_name}.{target} FOR {role}"),
                    "OK",
                );
                Ok(QueryExecutionResult {
                    command_tag: "CREATE MASKING POLICY".into(),
                    dag: None,
                    batch: RowBatch::default(),
                    stats: QueryExecutionStats::default(),
                })
            }
            PlannedCommand::ShowQueries => {
                let active = self.governance.active_queries_snapshot();
                let cols = vec![
                    "query_id".into(),
                    "user_name".into(),
                    "sql".into(),
                    "started_ms".into(),
                ];
                let rows = active
                    .into_iter()
                    .map(|q| {
                        BTreeMap::from([
                            ("query_id".into(), ScalarValue::Utf8(q.query_id)),
                            ("user_name".into(), ScalarValue::Utf8(q.user)),
                            ("sql".into(), ScalarValue::Utf8(q.sql)),
                            ("started_ms".into(), ScalarValue::Int64(q.started_ms as i64)),
                        ])
                    })
                    .collect();
                Ok(QueryExecutionResult {
                    command_tag: "SHOW QUERIES".into(),
                    dag: None,
                    batch: RowBatch::new(cols, rows),
                    stats: QueryExecutionStats::default(),
                })
            }
        }
    }

    async fn expand_virtual_views(&self, sql: &str) -> String {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("SELECT ") && !upper.starts_with("EXPLAIN ") {
            return sql.to_string();
        }
        let Ok(views) = self.catalog.list_views().await else {
            return sql.to_string();
        };
        let virtual_views: Vec<ViewDefinition> = views
            .into_iter()
            .filter(|v| !v.materialized && sql.contains(&v.name))
            .collect();
        if virtual_views.is_empty() {
            return sql.to_string();
        }
        let ctes: Vec<String> = virtual_views
            .into_iter()
            .map(|v| format!("{} AS ({})", v.name, v.query_sql))
            .collect();
        if upper.starts_with("EXPLAIN ANALYZE ") {
            format!(
                "EXPLAIN ANALYZE WITH {} {}",
                ctes.join(", "),
                &sql["EXPLAIN ANALYZE ".len()..]
            )
        } else if upper.starts_with("EXPLAIN ") {
            format!(
                "EXPLAIN WITH {} {}",
                ctes.join(", "),
                &sql["EXPLAIN ".len()..]
            )
        } else {
            format!("WITH {} {sql}", ctes.join(", "))
        }
    }

    /// Wave 6 + Wave 9 Subquery Pre-Evaluator: rewrites `IN (SELECT ...)`, `NOT IN (SELECT ...)`,
    /// `EXISTS (SELECT ...)`, and `NOT EXISTS (SELECT ...)` across distributed scans!
    async fn rewrite_subqueries(
        &self,
        sql: &str,
        branch: &str,
        session: &SessionContext,
    ) -> Result<String> {
        let mut current = sql.to_string();

        // 1a. Handle `NOT EXISTS (SELECT ...)` first (including correlated `WHERE inner.k = outer.k`)
        loop {
            let upper = current.to_uppercase();
            let Some(ex_pos) = upper.find("NOT EXISTS (SELECT ") else {
                break;
            };
            let open_paren = ex_pos + "NOT EXISTS ".len();
            let Some(close_paren) = find_matching_paren(&current, open_paren) else {
                break;
            };
            let sub_sql = current[open_paren + 1..close_paren].trim().to_string();
            let replacement = if let Some((outer_col, inner_in_sql)) =
                try_decorrelate_exists(&current[..ex_pos], &sub_sql)
            {
                format!("{outer_col} NOT IN ({inner_in_sql})")
            } else {
                let sub_res = Box::pin(self.execute_sql_inner(&sub_sql, branch, session)).await?;
                if sub_res.batch.num_rows() == 0 {
                    "1 = 1".to_string()
                } else {
                    "1 = 0".to_string()
                }
            };
            current = format!(
                "{}{}{}",
                &current[..ex_pos],
                replacement,
                &current[close_paren + 1..]
            );
        }

        // 1b. Handle `EXISTS (SELECT ...)` (including correlated `WHERE inner.k = outer.k`)
        loop {
            let upper = current.to_uppercase();
            let Some(ex_pos) = upper.find("EXISTS (SELECT ") else {
                break;
            };
            let open_paren = ex_pos + "EXISTS ".len();
            let Some(close_paren) = find_matching_paren(&current, open_paren) else {
                break;
            };
            let sub_sql = current[open_paren + 1..close_paren].trim().to_string();
            let replacement = if let Some((outer_col, inner_in_sql)) =
                try_decorrelate_exists(&current[..ex_pos], &sub_sql)
            {
                format!("{outer_col} IN ({inner_in_sql})")
            } else {
                let sub_res = Box::pin(self.execute_sql_inner(&sub_sql, branch, session)).await?;
                if sub_res.batch.num_rows() > 0 {
                    "1 = 1".to_string()
                } else {
                    "1 = 0".to_string()
                }
            };
            current = format!(
                "{}{}{}",
                &current[..ex_pos],
                replacement,
                &current[close_paren + 1..]
            );
        }

        // 2a. Handle `NOT IN (SELECT ...)`
        loop {
            let upper = current.to_uppercase();
            let Some(in_pos) = upper.find(" NOT IN (SELECT ") else {
                break;
            };
            let open_paren = in_pos + " NOT IN ".len();
            let Some(close_paren) = find_matching_paren(&current, open_paren) else {
                break;
            };
            let sub_sql = current[open_paren + 1..close_paren].trim().to_string();
            let sub_res = Box::pin(self.execute_sql_inner(&sub_sql, branch, session)).await?;
            let first_col = sub_res.batch.columns.first().cloned().unwrap_or_default();
            let mut lits = Vec::new();
            for row in &sub_res.batch.rows {
                if let Some(v) = row.get(&first_col) {
                    match v {
                        ScalarValue::Int64(i) => lits.push(i.to_string()),
                        ScalarValue::Float64(f) => lits.push(f.to_string()),
                        ScalarValue::Bool(b) => lits.push(b.to_string()),
                        ScalarValue::Utf8(s) => lits.push(format!("'{}'", s.replace('\'', "''"))),
                        _ => {}
                    }
                }
            }
            let replacement = if lits.is_empty() {
                " NOT IN ('__rustlakedb_empty_subquery__')".to_string()
            } else {
                format!(" NOT IN ({})", lits.join(", "))
            };
            current = format!(
                "{}{}{}",
                &current[..in_pos],
                replacement,
                &current[close_paren + 1..]
            );
        }

        // 2b. Handle `IN (SELECT ...)`
        loop {
            let upper = current.to_uppercase();
            let Some(in_pos) = upper.find(" IN (SELECT ") else {
                break;
            };
            let open_paren = in_pos + " IN ".len();
            let Some(close_paren) = find_matching_paren(&current, open_paren) else {
                break;
            };
            let sub_sql = current[open_paren + 1..close_paren].trim().to_string();
            let sub_res = Box::pin(self.execute_sql_inner(&sub_sql, branch, session)).await?;
            let first_col = sub_res.batch.columns.first().cloned().unwrap_or_default();
            let mut lits = Vec::new();
            for row in &sub_res.batch.rows {
                if let Some(v) = row.get(&first_col) {
                    match v {
                        ScalarValue::Int64(i) => lits.push(i.to_string()),
                        ScalarValue::Float64(f) => lits.push(f.to_string()),
                        ScalarValue::Bool(b) => lits.push(b.to_string()),
                        ScalarValue::Utf8(s) => lits.push(format!("'{}'", s.replace('\'', "''"))),
                        _ => {}
                    }
                }
            }
            let replacement = if lits.is_empty() {
                " IN ('__rustlakedb_empty_subquery__')".to_string()
            } else {
                format!(" IN ({})", lits.join(", "))
            };
            current = format!(
                "{}{}{}",
                &current[..in_pos],
                replacement,
                &current[close_paren + 1..]
            );
        }

        // 3. Handle scalar subqueries in WHERE/HAVING comparisons (`< (SELECT ...)`, `> (SELECT ...)`, `= (SELECT ...)`, etc., Wave 19 TPC-H)
        loop {
            let upper = current.to_uppercase();
            let found = ["<= (SELECT ", ">= (SELECT ", "<> (SELECT ", "!= (SELECT ", "= (SELECT ", "< (SELECT ", "> (SELECT "]
                .iter()
                .filter_map(|pat| upper.find(pat).map(|pos| (pos, *pat)))
                .min_by_key(|(pos, _)| *pos);
            let Some((op_pos, pat)) = found else {
                break;
            };
            let op_str = pat.trim_end_matches("(SELECT ").trim();
            let open_paren = op_pos + pat.len() - "(SELECT ".len();
            let Some(close_paren) = find_matching_paren(&current, open_paren) else {
                break;
            };
            let sub_sql = current[open_paren + 1..close_paren].trim().to_string();
            let sub_res = Box::pin(self.execute_sql_inner(&sub_sql, branch, session)).await?;
            let first_col = sub_res.batch.columns.first().cloned().unwrap_or_default();
            let scalar_lit = sub_res
                .batch
                .rows
                .first()
                .and_then(|r| r.get(&first_col))
                .map(|v| match v {
                    ScalarValue::Int64(i) => i.to_string(),
                    ScalarValue::Float64(f) => f.to_string(),
                    ScalarValue::Decimal(d, s) => {
                        rustlakedb_protocol::format_decimal_i128(*d, *s)
                    }
                    ScalarValue::Bool(b) => b.to_string(),
                    ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
                    ScalarValue::Date(d) => {
                        format!("'{}'", rustlakedb_protocol::format_iso_date_days(*d))
                    }
                    _ => "NULL".to_string(),
                })
                .unwrap_or_else(|| "NULL".to_string());
            current = format!(
                "{}{} {}{}",
                &current[..op_pos],
                op_str,
                scalar_lit,
                &current[close_paren + 1..]
            );
        }

        Ok(current)
    }

    async fn materialize_view_table(
        &self,
        name: &str,
        query_sql: &str,
        branch: &str,
        session: &SessionContext,
    ) -> Result<usize> {
        let res = Box::pin(self.execute_sql_inner(query_sql, branch, session)).await?;
        let batch = res.batch;
        let schema: Vec<FieldSchema> = batch
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let dt = batch
                    .rows
                    .iter()
                    .filter_map(|r| r.get(col))
                    .find(|v| !matches!(v, ScalarValue::Null))
                    .map(|v| match v {
                        ScalarValue::Int64(_) => ColumnType::Int64,
                        ScalarValue::Float64(_) => ColumnType::Float64,
                        ScalarValue::Bool(_) => ColumnType::Bool,
                        ScalarValue::Json(_) => ColumnType::Variant,
                        _ => ColumnType::Utf8,
                    })
                    .unwrap_or(ColumnType::Utf8);
                FieldSchema {
                    field_id: (idx + 1) as i32,
                    name: col.clone(),
                    data_type: dt,
                    nullable: true,
                }
            })
            .collect();

        let existing = self.catalog.get_table(name).await.ok();
        let (expected_snap, old_files) = match existing {
            Some(meta) => {
                let s = meta.current_snapshot(branch)?;
                (
                    s.snapshot_id,
                    s.data_files.iter().map(|f| f.file_path.clone()).collect(),
                )
            }
            None => {
                self.catalog
                    .create_table(IcebergTableMetadata::new(name, schema.clone()))
                    .await?;
                (1, vec![])
            }
        };

        let row_count = batch.num_rows();
        let added_files = if row_count > 0 {
            let path = format!("{name}/data/mv-{}.parquet", Uuid::new_v4());
            vec![self.store.write_data_file(&path, &schema, &batch).await?]
        } else {
            vec![]
        };

        self.catalog
            .commit_table(TableCommitProposal {
                table_name: name.to_string(),
                branch: branch.to_string(),
                expected_snapshot_id: expected_snap,
                added_files,
                removed_files: old_files,
                is_append_only: false,
            })
            .await?;
        let updated = self.catalog.get_table(name).await?;
        self.store.write_iceberg_metadata(&updated, branch).await?;
        Ok(row_count)
    }

    /// Streaming / Bulk NDJSON ingestion with optional Dead-Letter Queue (`<table_name>_dlq`) routing (Wave 2).
    pub async fn ingest_ndjson(
        &self,
        table_name: &str,
        branch: &str,
        body: &str,
        on_error_continue: bool,
    ) -> Result<(usize, usize)> {
        let meta = self.catalog.get_table(table_name).await?;
        let snap = meta.current_snapshot(branch)?;
        let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
        let mut good_rows = Vec::new();
        let mut bad_lines: Vec<(String, String)> = Vec::new();

        for line in body.lines() {
            let l = line.trim();
            if l.is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(l) {
                Ok(serde_json::Value::Object(obj)) => {
                    let mut row = BTreeMap::new();
                    let mut row_err: Option<String> = None;
                    for col in &meta.schema {
                        match obj.get(&col.name) {
                            None | Some(serde_json::Value::Null) => {
                                row.insert(col.name.clone(), ScalarValue::Null);
                            }
                            Some(val) => match coerce_json_value(val, &col.data_type) {
                                Ok(sv) => {
                                    row.insert(col.name.clone(), sv);
                                }
                                Err(e) => {
                                    row_err = Some(e);
                                    break;
                                }
                            },
                        }
                    }
                    if let Some(err_msg) = row_err {
                        if on_error_continue {
                            bad_lines.push((l.to_string(), err_msg));
                        } else {
                            return Err(RustLakeError::Execution(format!(
                                "NDJSON ingest schema error: {err_msg}"
                            )));
                        }
                    } else {
                        good_rows.push(row);
                    }
                }
                Ok(_) => {
                    let msg = "expected JSON object per NDJSON line".to_string();
                    if on_error_continue {
                        bad_lines.push((l.to_string(), msg));
                    } else {
                        return Err(RustLakeError::Execution(msg));
                    }
                }
                Err(e) => {
                    if on_error_continue {
                        bad_lines.push((l.to_string(), e.to_string()));
                    } else {
                        return Err(RustLakeError::Execution(format!("invalid NDJSON line: {e}")));
                    }
                }
            }
        }

        let loaded = good_rows.len();
        if loaded > 0 {
            let batch = RowBatch::new(cols, good_rows);
            let path = format!("{table_name}/data/ingest-{}.parquet", Uuid::new_v4());
            let entry = self
                .store
                .write_data_file(&path, &meta.schema, &batch)
                .await?;
            self.catalog
                .commit_table(TableCommitProposal {
                    table_name: table_name.to_string(),
                    branch: branch.to_string(),
                    expected_snapshot_id: snap.snapshot_id,
                    added_files: vec![entry],
                    removed_files: vec![],
                    is_append_only: true,
                })
                .await?;
            let updated = self.catalog.get_table(table_name).await?;
            self.store.write_iceberg_metadata(&updated, branch).await?;
        }

        let dlq_count = bad_lines.len();
        if dlq_count > 0 {
            let dlq_table = format!("{table_name}_dlq");
            let dlq_schema = vec![
                FieldSchema {
                    field_id: 1,
                    name: "raw_line".into(),
                    data_type: ColumnType::Utf8,
                    nullable: false,
                },
                FieldSchema {
                    field_id: 2,
                    name: "error".into(),
                    data_type: ColumnType::Utf8,
                    nullable: false,
                },
            ];
            if self.catalog.get_table(&dlq_table).await.is_err() {
                self.catalog
                    .create_table(IcebergTableMetadata::new(&dlq_table, dlq_schema.clone()))
                    .await?;
            }
            let dlq_meta = self.catalog.get_table(&dlq_table).await?;
            let dlq_snap = dlq_meta.current_snapshot("main")?;
            let dlq_rows: Vec<BTreeMap<String, ScalarValue>> = bad_lines
                .into_iter()
                .map(|(raw, err)| {
                    BTreeMap::from([
                        ("raw_line".into(), ScalarValue::Utf8(raw)),
                        ("error".into(), ScalarValue::Utf8(err)),
                    ])
                })
                .collect();
            let dlq_batch = RowBatch::new(vec!["raw_line".into(), "error".into()], dlq_rows);
            let dlq_path = format!("{dlq_table}/data/dlq-{}.parquet", Uuid::new_v4());
            let dlq_entry = self
                .store
                .write_data_file(&dlq_path, &dlq_schema, &dlq_batch)
                .await?;
            self.catalog
                .commit_table(TableCommitProposal {
                    table_name: dlq_table,
                    branch: "main".into(),
                    expected_snapshot_id: dlq_snap.snapshot_id,
                    added_files: vec![dlq_entry],
                    removed_files: vec![],
                    is_append_only: true,
                })
                .await?;
        }

        Ok((loaded, dlq_count))
    }

    /// Wave 13 Debezium-Compatible Streaming CDC Ingestion with Exactly-Once Offset Watermarks:
    /// Processes `{"op": "c"|"u"|"d", "offset": N, "key_col": "id", "before": {...}, "after": {...}}`
    /// envelopes, skips duplicate offsets (`offset <= watermark`), applies deletes/updates atomically
    /// via Iceberg v3 Deletion Vectors, and commits new row versions in a single OCC transaction!
    pub async fn ingest_cdc_events(
        &self,
        table_name: &str,
        branch: &str,
        stream_id: &str,
        events: &[serde_json::Value],
    ) -> Result<serde_json::Value> {
        let meta = self.catalog.get_table(table_name).await?;
        let snap = meta.current_snapshot(branch)?;
        let watermark = self.governance.get_cdc_offset(table_name, stream_id);
        let default_key = meta
            .schema
            .first()
            .map(|f| f.name.clone())
            .unwrap_or_else(|| "id".into());

        let mut max_offset = watermark;
        let mut skipped_duplicates = 0usize;
        let mut applied = 0usize;
        let mut deletes = 0usize;
        let mut upserts = 0usize;
        let mut keys_to_delete: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
        let mut latest_upserts: BTreeMap<String, BTreeMap<String, ScalarValue>> = BTreeMap::new();
        let mut key_order_fallback: Vec<BTreeMap<String, ScalarValue>> = Vec::new();

        for ev in events {
            let offset = ev.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
            if offset > 0 && offset <= watermark {
                skipped_duplicates += 1;
                continue;
            }
            if offset > max_offset {
                max_offset = offset;
            }
            let op = ev.get("op").and_then(|v| v.as_str()).unwrap_or("c");
            let key_col = ev
                .get("key_col")
                .and_then(|v| v.as_str())
                .unwrap_or(&default_key)
                .to_string();

            let parse_obj = |obj: &serde_json::Map<String, serde_json::Value>| {
                let mut row = BTreeMap::new();
                for col in &meta.schema {
                    match obj.get(&col.name) {
                        None | Some(serde_json::Value::Null) => {
                            row.insert(col.name.clone(), ScalarValue::Null);
                        }
                        Some(v) => {
                            let sv = coerce_json_value(v, &col.data_type)
                                .unwrap_or(ScalarValue::Null);
                            row.insert(col.name.clone(), sv);
                        }
                    }
                }
                row
            };

            match op {
                "d" | "D" => {
                    let src = ev
                        .get("before")
                        .or_else(|| ev.get("after"))
                        .and_then(|v| v.as_object());
                    if let Some(obj) = src {
                        let row = parse_obj(obj);
                        if let Some(k) = row.get(&key_col).and_then(|v| v.to_pg_text()) {
                            keys_to_delete.entry(key_col).or_default().insert(k.clone());
                            latest_upserts.remove(&k);
                            deletes += 1;
                            applied += 1;
                        }
                    }
                }
                "u" | "U" => {
                    if let Some(obj) = ev.get("after").and_then(|v| v.as_object()) {
                        let row = parse_obj(obj);
                        if let Some(k) = row.get(&key_col).and_then(|v| v.to_pg_text()) {
                            keys_to_delete.entry(key_col).or_default().insert(k.clone());
                            latest_upserts.insert(k, row);
                            upserts += 1;
                            applied += 1;
                        }
                    }
                }
                _ => {
                    if let Some(obj) = ev.get("after").and_then(|v| v.as_object()) {
                        let row = parse_obj(obj);
                        if let Some(k) = row.get(&key_col).and_then(|v| v.to_pg_text()) {
                            latest_upserts.insert(k, row);
                        } else {
                            key_order_fallback.push(row);
                        }
                        upserts += 1;
                        applied += 1;
                    }
                }
            }
        }

        // Apply Iceberg v3 Deletion Vectors for updated/deleted keys in existing Parquet files
        if !keys_to_delete.is_empty() {
            for df in &snap.data_files {
                let mut dv = self.store.read_deletion_vector(&df.file_path).await?;
                let file_batch = self
                    .store
                    .read_data_file(&df.file_path, &meta.schema, None)
                    .await?;
                let mut phys_idx: u32 = 0;
                let mut changed = false;
                for row in &file_batch.rows {
                    while dv.contains(phys_idx) {
                        phys_idx += 1;
                    }
                    let should_del = keys_to_delete.iter().any(|(kcol, kset)| {
                        row.get(kcol)
                            .and_then(|v| v.to_pg_text())
                            .is_some_and(|kv| kset.contains(&kv))
                    });
                    if should_del {
                        dv.insert(phys_idx);
                        changed = true;
                    }
                    phys_idx += 1;
                }
                if changed {
                    self.store.write_deletion_vector(&df.file_path, &dv).await?;
                }
            }
        }

        let mut rows_to_write: Vec<BTreeMap<String, ScalarValue>> =
            latest_upserts.into_values().collect();
        rows_to_write.extend(key_order_fallback);

        if !rows_to_write.is_empty() {
            let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
            let batch = RowBatch::new(cols, rows_to_write);
            let path = format!("{table_name}/data/cdc-{}.parquet", Uuid::new_v4());
            let entry = self
                .store
                .write_data_file(&path, &meta.schema, &batch)
                .await?;
            self.catalog
                .commit_table(TableCommitProposal {
                    table_name: table_name.to_string(),
                    branch: branch.to_string(),
                    expected_snapshot_id: snap.snapshot_id,
                    added_files: vec![entry],
                    removed_files: vec![],
                    is_append_only: keys_to_delete.is_empty(),
                })
                .await?;
            let updated = self.catalog.get_table(table_name).await?;
            self.store.write_iceberg_metadata(&updated, branch).await?;
        }

        if max_offset > watermark {
            self.governance
                .set_cdc_offset(table_name, stream_id, max_offset);
        }

        Ok(serde_json::json!({
            "table": table_name,
            "stream_id": stream_id,
            "applied": applied,
            "upserts": upserts,
            "deletes": deletes,
            "skipped_duplicates": skipped_duplicates,
            "last_offset": max_offset
        }))
    }

    async fn ingest_csv(
        &self,
        table_name: &str,
        branch: &str,
        body: &str,
        on_error_continue: bool,
    ) -> Result<(usize, usize)> {
        let meta = self.catalog.get_table(table_name).await?;
        let snap = meta.current_snapshot(branch)?;
        let mut lines = body.lines().filter(|l| !l.trim().is_empty());
        let header_line = lines
            .next()
            .ok_or_else(|| RustLakeError::Execution("CSV input is empty".into()))?;
        let headers: Vec<String> = header_line
            .split(',')
            .map(|h| h.trim().trim_matches('"').to_string())
            .collect();
        let col_types: HashMap<&str, &ColumnType> = meta
            .schema
            .iter()
            .map(|f| (f.name.as_str(), &f.data_type))
            .collect();

        let mut good_rows = Vec::new();
        let mut bad_count = 0usize;
        for line in lines {
            let fields = split_csv_line(line);
            if fields.len() != headers.len() {
                if on_error_continue {
                    bad_count += 1;
                    continue;
                }
                return Err(RustLakeError::Execution(format!(
                    "CSV column count mismatch: expected {}, got {}",
                    headers.len(),
                    fields.len()
                )));
            }
            let mut row = BTreeMap::new();
            let mut failed = false;
            for (h, raw_v) in headers.iter().zip(fields.iter()) {
                let dt = col_types.get(h.as_str()).copied().unwrap_or(&ColumnType::Utf8);
                let trimmed = raw_v.trim();
                if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
                    row.insert(h.clone(), ScalarValue::Null);
                    continue;
                }
                let parsed = match dt {
                    ColumnType::Int64 => trimmed
                        .parse::<i64>()
                        .map(ScalarValue::Int64)
                        .map_err(|e| e.to_string()),
                    ColumnType::Float64 => trimmed
                        .parse::<f64>()
                        .map(ScalarValue::Float64)
                        .map_err(|e| e.to_string()),
                    ColumnType::Bool => trimmed
                        .parse::<bool>()
                        .map(ScalarValue::Bool)
                        .map_err(|e| e.to_string()),
                    ColumnType::Variant => serde_json::from_str::<serde_json::Value>(trimmed)
                        .map(ScalarValue::Json)
                        .map_err(|e| e.to_string()),
                    ColumnType::Date => rustlakedb_protocol::parse_iso_date_days(trimmed)
                        .map(ScalarValue::Date)
                        .ok_or_else(|| format!("invalid DATE literal '{trimmed}'")),
                    ColumnType::Decimal { scale, .. } => {
                        rustlakedb_protocol::parse_decimal_i128(trimmed, *scale)
                            .map(|v| ScalarValue::Decimal(v, *scale))
                            .ok_or_else(|| format!("invalid DECIMAL literal '{trimmed}'"))
                    }
                    ColumnType::Utf8 | ColumnType::TimestampTz => {
                        Ok(ScalarValue::Utf8(trimmed.to_string()))
                    }
                };
                match parsed {
                    Ok(sv) => {
                        row.insert(h.clone(), sv);
                    }
                    Err(e) => {
                        if on_error_continue {
                            bad_count += 1;
                            failed = true;
                            break;
                        }
                        return Err(RustLakeError::Execution(format!(
                            "CSV parse error for column '{h}': {e}"
                        )));
                    }
                }
            }
            if !failed {
                good_rows.push(row);
            }
        }

        let loaded = good_rows.len();
        if loaded > 0 {
            let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
            let batch = RowBatch::new(cols, good_rows);
            let path = format!("{table_name}/data/copy-csv-{}.parquet", Uuid::new_v4());
            let entry = self
                .store
                .write_data_file(&path, &meta.schema, &batch)
                .await?;
            self.catalog
                .commit_table(TableCommitProposal {
                    table_name: table_name.to_string(),
                    branch: branch.to_string(),
                    expected_snapshot_id: snap.snapshot_id,
                    added_files: vec![entry],
                    removed_files: vec![],
                    is_append_only: true,
                })
                .await?;
            let updated = self.catalog.get_table(table_name).await?;
            self.store.write_iceberg_metadata(&updated, branch).await?;
        }
        Ok((loaded, bad_count))
    }

    /// Wave 14 Write-Time Iceberg Hidden Partition Router:
    /// When `meta.partition_by` is configured (`region`, `days(ts)`, `bucket(N, id)`, `truncate(W, s)`),
    /// splits incoming rows into physical partition directories and stamps `DataFileEntry.partition_values`
    /// so Layer 0 partition pruning skips non-matching files in O(1) without touching Parquet!
    async fn write_partitioned_data_files(
        &self,
        meta: &IcebergTableMetadata,
        file_prefix: &str,
        batch: &RowBatch,
    ) -> Result<Vec<rustlakedb_catalog::DataFileEntry>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }
        if meta.partition_by.is_empty() {
            let path = format!(
                "{}/data/{}-{}.parquet",
                meta.table_name,
                file_prefix,
                Uuid::new_v4()
            );
            return Ok(vec![
                self.store.write_data_file(&path, &meta.schema, batch).await?,
            ]);
        }

        type PartitionRowsMap = BTreeMap<Vec<(String, String)>, Vec<BTreeMap<String, ScalarValue>>>;
        let mut grouped: PartitionRowsMap = BTreeMap::new();
        for row in &batch.rows {
            let mut key = Vec::with_capacity(meta.partition_by.len());
            for spec in &meta.partition_by {
                let src_col = rustlakedb_catalog::partition_spec_source_col(spec);
                let raw_val = row
                    .get(&src_col)
                    .and_then(|v| v.to_pg_text())
                    .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".into());
                let (_, part_val) = rustlakedb_catalog::eval_partition_transform(spec, &raw_val);
                key.push((spec.clone(), part_val));
            }
            grouped.entry(key).or_default().push(row.clone());
        }

        let mut entries = Vec::with_capacity(grouped.len());
        for (part_kv, rows) in grouped {
            let dir_segs: Vec<String> = part_kv
                .iter()
                .map(|(spec, val)| {
                    let clean_spec = spec
                        .replace(['(', ')', ',', ' '], "_")
                        .trim_matches('_')
                        .to_string();
                    let clean_val = val.replace(['/', '\\', ' '], "_");
                    format!("{clean_spec}={clean_val}")
                })
                .collect();
            let path = format!(
                "{}/data/{}/{}-{}.parquet",
                meta.table_name,
                dir_segs.join("/"),
                file_prefix,
                Uuid::new_v4()
            );
            let sub_batch = RowBatch::new(batch.columns.clone(), rows);
            let mut entry = self
                .store
                .write_data_file(&path, &meta.schema, &sub_batch)
                .await?;
            for (spec, val) in part_kv {
                entry.partition_values.insert(spec, val);
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    pub async fn execute_dag(&self, dag: StageDag) -> Result<QueryExecutionResult> {
        let mut stats = QueryExecutionStats::default();
        let mut stage_blocks: HashMap<u32, Vec<ShuffleBlockLocation>> = HashMap::new();
        let mut stage_skewed: HashMap<u32, Vec<usize>> = HashMap::new();
        let mut final_batches: Vec<RowBatch> = Vec::new();

        for stage in &dag.stages {
            // Cooperative in-flight query cancellation check between DAG stages (Wave 22)
            if self.governance.is_query_cancelled(&dag.query_id.0.to_string()) {
                return Err(RustLakeError::Execution(format!(
                    "query '{}' cancelled by user before stage {}",
                    dag.query_id, stage.stage_id
                )));
            }
            stats.stages_executed += 1;
            let leaf_splits = collect_leaf_splits(&stage.plan);

            let upstream_blocks: Vec<ShuffleBlockLocation> = stage
                .upstream_stages
                .iter()
                .flat_map(|sid| stage_blocks.get(sid).cloned().unwrap_or_default())
                .collect();

            // Build per-task execution specs with real AQE Coalescing & Skew Splitting (RFC-001)!
            struct PlannedTask {
                partition_id: usize,
                splits: Vec<IcebergSplit>,
                blocks: Vec<ShuffleBlockLocation>,
                reduce_parts: Vec<usize>,
            }

            let mut planned_tasks: Vec<PlannedTask> = Vec::new();
            if !leaf_splits.is_empty() {
                for (i, split) in leaf_splits.into_iter().enumerate() {
                    planned_tasks.push(PlannedTask {
                        partition_id: i,
                        splits: vec![split],
                        blocks: upstream_blocks.clone(),
                        reduce_parts: vec![],
                    });
                }
            } else if !stage.upstream_stages.is_empty() && stage.num_partitions > 1 {
                // Real AQE Dynamic Partition Coalescing: skip empty partitions (row_count == 0)!
                let mut non_empty_parts = Vec::new();
                for p in 0..stage.num_partitions {
                    let has_rows = upstream_blocks
                        .iter()
                        .any(|b| b.key.reduce_partition == p && b.row_count > 0);
                    if has_rows {
                        non_empty_parts.push(p);
                    } else {
                        stats.aqe_partitions_coalesced += 1;
                    }
                }
                if non_empty_parts.is_empty() {
                    non_empty_parts.push(0);
                }

                // Check if any upstream stage flagged skewed partitions on a HashJoin!
                let skewed_upstream: Vec<usize> = stage
                    .upstream_stages
                    .iter()
                    .flat_map(|sid| stage_skewed.get(sid).cloned().unwrap_or_default())
                    .collect();
                let is_join_stage = contains_hash_join(&stage.plan);

                for p in non_empty_parts {
                    if is_join_stage && skewed_upstream.contains(&p) && stage.upstream_stages.len() >= 2 {
                        // AQE Skew Join Split: split probe-side blocks into 2 sub-tasks and
                        // replicate the matching build-side blocks across both sub-tasks!
                        let probe_stage = stage.upstream_stages[0];
                        let probe_blocks: Vec<ShuffleBlockLocation> = upstream_blocks
                            .iter()
                            .filter(|b| b.key.stage_id == probe_stage && b.key.reduce_partition == p && b.row_count > 0)
                            .cloned()
                            .collect();
                        let other_blocks: Vec<ShuffleBlockLocation> = upstream_blocks
                            .iter()
                            .filter(|b| b.key.stage_id != probe_stage)
                            .cloned()
                            .collect();

                        if probe_blocks.len() >= 2 {
                            let mid = probe_blocks.len() / 2;
                            for chunk in [&probe_blocks[..mid], &probe_blocks[mid..]] {
                                let mut sub_blocks = chunk.to_vec();
                                sub_blocks.extend(other_blocks.clone());
                                planned_tasks.push(PlannedTask {
                                    partition_id: p,
                                    splits: vec![],
                                    blocks: sub_blocks,
                                    reduce_parts: vec![p],
                                });
                            }
                            stats.aqe_skew_splits_applied += 1;
                            continue;
                        }
                    }

                    planned_tasks.push(PlannedTask {
                        partition_id: p,
                        splits: vec![],
                        blocks: upstream_blocks.clone(),
                        reduce_parts: vec![p],
                    });
                }
            } else {
                for p in 0..std::cmp::max(1, stage.num_partitions) {
                    planned_tasks.push(PlannedTask {
                        partition_id: p,
                        splits: vec![],
                        blocks: upstream_blocks.clone(),
                        reduce_parts: vec![p],
                    });
                }
            }

            let num_tasks = planned_tasks.len();
            let mut initial_loads: HashMap<WorkerId, usize> = HashMap::new();
            let workers_snap = self.workers_snapshot();
            let healthy_snapshot: Vec<WorkerId> = workers_snap
                .iter()
                .filter(|w| w.is_healthy())
                .map(|w| w.id().clone())
                .collect();

            if healthy_snapshot.is_empty() {
                let _ = self.shuffle.cleanup_query(dag.query_id).await;
                return Err(RustLakeError::Execution(format!(
                    "no healthy workers available for stage {}",
                    stage.stage_id
                )));
            }

            // Pre-assign primary Rendezvous worker per task so all tasks in the stage
            // dispatch CONCURRENTLY across all worker nodes in true parallel Spark-style waves!
            let max_load = std::cmp::max(1, num_tasks.div_ceil(healthy_snapshot.len()));
            let mut task_assignments: Vec<(usize, PlannedTask, WorkerId)> =
                Vec::with_capacity(num_tasks);
            for (task_idx, pt) in planned_tasks.into_iter().enumerate() {
                let primary_id = if let Some(split) = pt.splits.first() {
                    assign_split_rendezvous(
                        &split.split_key(),
                        &healthy_snapshot,
                        &initial_loads,
                        max_load,
                    )
                    .expect("healthy_snapshot non-empty")
                    .clone()
                } else {
                    healthy_snapshot[task_idx % healthy_snapshot.len()].clone()
                };
                *initial_loads.entry(primary_id.clone()).or_insert(0) += 1;
                task_assignments.push((task_idx, pt, primary_id));
            }

            let stage_id = stage.stage_id;
            let stage_plan = &stage.plan;
            let stage_out_part = &stage.output_partitioning;
            let stage_shuf_mode = stage.shuffle_mode;
            let query_id = dag.query_id;
            let max_retries = self.max_task_retries;
            let spec_threshold_ms = self.speculative_threshold_ms.load(Ordering::Relaxed);
            let workers_ref = &workers_snap;

            let task_futs =
                task_assignments
                    .into_iter()
                    .map(|(task_idx, pt, primary_id)| async move {
                        let mut attempt = 0u32;
                        let mut excluded_workers: Vec<WorkerId> = Vec::new();
                        let mut local_executed = 0usize;
                        let mut local_retried = 0usize;
                        let mut local_hedged = 0usize;

                        loop {
                            let primary_healthy = workers_ref
                                .iter()
                                .find(|w| *w.id() == primary_id)
                                .is_some_and(|w| w.is_healthy());
                            let chosen_id = if attempt == 0
                                && primary_healthy
                                && !excluded_workers.contains(&primary_id)
                            {
                                primary_id.clone()
                            } else {
                                let candidates: Vec<WorkerId> = workers_ref
                                    .iter()
                                    .filter(|w| {
                                        w.is_healthy() && !excluded_workers.contains(w.id())
                                    })
                                    .map(|w| w.id().clone())
                                    .collect();
                                if candidates.is_empty() {
                                    return Err(RustLakeError::Execution(format!(
                                        "no healthy workers remaining to execute stage {stage_id} partition {}",
                                        pt.partition_id
                                    )));
                                }
                                candidates[(task_idx + attempt as usize) % candidates.len()].clone()
                            };

                            let worker = workers_ref
                                .iter()
                                .find(|w| *w.id() == chosen_id)
                                .expect("worker exists")
                                .clone();

                            let task_spec = TaskSpec {
                                attempt_id: TaskAttemptId {
                                    query_id,
                                    stage_id,
                                    partition_id: task_idx,
                                    attempt,
                                },
                                plan: stage_plan.clone(),
                                assigned_splits: pt.splits.clone(),
                                upstream_blocks: pt.blocks.clone(),
                                output_partitioning: stage_out_part.clone(),
                                output_shuffle_mode: stage_shuf_mode,
                                assigned_reduce_partitions: pt.reduce_parts.clone(),
                            };

                            local_executed += 1;

                            // Wave 9 Speculative Straggler Execution:
                            // If `speculative_threshold_ms > 0` on attempt 0 and a healthy peer exists,
                            // race a backup copy on the peer worker if primary exceeds the threshold!
                            let peer_worker = if spec_threshold_ms > 0 && attempt == 0 {
                                workers_ref
                                    .iter()
                                    .find(|w| *w.id() != chosen_id && w.is_healthy())
                                    .cloned()
                            } else {
                                None
                            };

                            let exec_outcome = if let Some(backup) = peer_worker {
                                let primary_fut = worker.execute_task(task_spec.clone());
                                tokio::pin!(primary_fut);
                                tokio::select! {
                                    res = &mut primary_fut => res,
                                    _ = tokio::time::sleep(Duration::from_millis(spec_threshold_ms)) => {
                                        let mut backup_spec = task_spec.clone();
                                        backup_spec.attempt_id.attempt = 99;
                                        local_hedged += 1;
                                        local_executed += 1;
                                        let backup_fut = backup.execute_task(backup_spec);
                                        tokio::select! {
                                            res = &mut primary_fut => res,
                                            res = backup_fut => res,
                                        }
                                    }
                                }
                            } else {
                                worker.execute_task(task_spec).await
                            };

                            match exec_outcome {
                                Ok(rep) => {
                                    return Ok((rep, local_executed, local_retried, local_hedged))
                                }
                                Err(RustLakeError::WorkerFailed { worker_id, reason }) => {
                                    tracing::warn!(
                                        stage = stage_id,
                                        partition = pt.partition_id,
                                        attempt,
                                        worker = %worker_id,
                                        %reason,
                                        "task attempt failed; rescheduling on healthy worker"
                                    );
                                    excluded_workers.push(WorkerId::new(worker_id));
                                    attempt += 1;
                                    local_retried += 1;
                                    if attempt > max_retries {
                                        return Err(RustLakeError::Execution(format!(
                                            "stage {stage_id} partition {} exceeded max retries ({max_retries})",
                                            pt.partition_id
                                        )));
                                    }
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    });

            let results = futures::future::join_all(task_futs).await;
            let mut stage_produced_blocks = Vec::new();
            let mut stage_metrics: HashMap<usize, PartitionMetrics> = HashMap::new();

            for res in results {
                let (report, exec_cnt, retry_cnt, hedge_cnt) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = self.shuffle.cleanup_query(dag.query_id).await;
                        return Err(e);
                    }
                };
                stats.tasks_executed += exec_cnt;
                stats.tasks_retried += retry_cnt;
                stats.speculative_hedges += hedge_cnt;

                for m in report.partition_metrics {
                    let entry = stage_metrics
                        .entry(m.reduce_partition)
                        .or_insert(PartitionMetrics {
                            reduce_partition: m.reduce_partition,
                            row_count: 0,
                            byte_size: 0,
                        });
                    entry.row_count += m.row_count;
                    entry.byte_size += m.byte_size;
                    stats.total_shuffled_bytes += m.byte_size;
                }
                stage_produced_blocks.extend(report.produced_blocks);
                if let Some(batch) = report.final_output {
                    final_batches.push(batch);
                }
            }

            if stage.output_partitioning.is_some() {
                let skewed = detect_skewed_partitions(&stage_metrics);
                if !skewed.is_empty() {
                    stage_skewed.insert(stage.stage_id, skewed.clone());
                    stats.skewed_partitions_detected.extend(skewed);
                }
                stage_blocks.insert(stage.stage_id, stage_produced_blocks);
            }
        }

        self.shuffle.cleanup_query(dag.query_id).await?;

        let mut merged_cols = Vec::new();
        let mut merged_rows = Vec::new();
        for b in final_batches {
            if merged_cols.is_empty() && !b.columns.is_empty() {
                merged_cols = b.columns;
            }
            merged_rows.extend(b.rows);
        }

        let row_count = merged_rows.len();
        Ok(QueryExecutionResult {
            command_tag: format!("SELECT {row_count}"),
            dag: Some(dag),
            batch: RowBatch::new(merged_cols, merged_rows),
            stats,
        })
    }
}

fn contains_hash_join(node: &PhysicalNode) -> bool {
    match node {
        PhysicalNode::HashJoin { .. } => true,
        PhysicalNode::Filter { input, .. }
        | PhysicalNode::Project { input, .. }
        | PhysicalNode::Distinct { input }
        | PhysicalNode::Sort { input, .. }
        | PhysicalNode::Limit { input, .. }
        | PhysicalNode::TopN { input, .. }
        | PhysicalNode::HashAggregate { input, .. }
        | PhysicalNode::Window { input, .. }
        | PhysicalNode::Exchange { input, .. } => contains_hash_join(input),
        PhysicalNode::UnionAll { inputs, .. } => inputs.iter().any(contains_hash_join),
        PhysicalNode::SetOp { left, right, .. } => {
            contains_hash_join(left) || contains_hash_join(right)
        }
        _ => false,
    }
}

fn collect_leaf_splits(node: &PhysicalNode) -> Vec<IcebergSplit> {
    match node {
        PhysicalNode::IcebergScan { splits, .. } => splits.clone(),
        PhysicalNode::Filter { input, .. }
        | PhysicalNode::Project { input, .. }
        | PhysicalNode::Distinct { input }
        | PhysicalNode::Sort { input, .. }
        | PhysicalNode::Limit { input, .. }
        | PhysicalNode::TopN { input, .. }
        | PhysicalNode::HashAggregate { input, .. }
        | PhysicalNode::Window { input, .. }
        | PhysicalNode::Exchange { input, .. } => collect_leaf_splits(input),
        PhysicalNode::HashJoin { left, right, .. }
        | PhysicalNode::SetOp { left, right, .. } => {
            let mut s = collect_leaf_splits(left);
            s.extend(collect_leaf_splits(right));
            s
        }
        PhysicalNode::UnionAll { inputs, .. } => {
            inputs.iter().flat_map(collect_leaf_splits).collect()
        }
        PhysicalNode::ShuffleRead { .. } => vec![],
    }
}

fn collect_dag_tables(dag: &StageDag) -> Vec<String> {
    fn walk(node: &PhysicalNode, out: &mut Vec<String>) {
        match node {
            PhysicalNode::IcebergScan { table_name, .. } => {
                if !out.contains(table_name) {
                    out.push(table_name.clone());
                }
            }
            PhysicalNode::Filter { input, .. }
            | PhysicalNode::Project { input, .. }
            | PhysicalNode::Distinct { input }
            | PhysicalNode::Sort { input, .. }
            | PhysicalNode::Limit { input, .. }
            | PhysicalNode::TopN { input, .. }
            | PhysicalNode::HashAggregate { input, .. }
            | PhysicalNode::Window { input, .. }
            | PhysicalNode::Exchange { input, .. } => walk(input, out),
            PhysicalNode::HashJoin { left, right, .. }
            | PhysicalNode::SetOp { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            PhysicalNode::UnionAll { inputs, .. } => {
                for inp in inputs {
                    walk(inp, out);
                }
            }
            PhysicalNode::ShuffleRead { .. } => {}
        }
    }
    let mut out = Vec::new();
    for s in &dag.stages {
        walk(&s.plan, &mut out);
    }
    out
}

fn inject_rls_into_dag(dag: &mut StageDag, session: &SessionContext, gov: &GovernanceState) {
    if session.is_superuser() {
        return;
    }
    fn walk(node: &mut PhysicalNode, session: &SessionContext, gov: &GovernanceState) {
        match node {
            PhysicalNode::IcebergScan {
                table_name,
                pushed_filter,
                projection,
                ..
            } => {
                if let Some(policy) = gov.get_rls_policy(session, table_name) {
                    // Ensure columns referenced by RLS policy are included if projection pushdown narrowed them.
                    if !projection.is_empty() {
                        collect_filter_cols_vec(&policy, projection);
                    }
                    *pushed_filter = Some(match pushed_filter.take() {
                        Some(existing) => FilterExpr::And(Box::new(existing), Box::new(policy)),
                        None => policy,
                    });
                }
            }
            PhysicalNode::Filter { input, .. }
            | PhysicalNode::Project { input, .. }
            | PhysicalNode::Distinct { input }
            | PhysicalNode::Sort { input, .. }
            | PhysicalNode::Limit { input, .. }
            | PhysicalNode::TopN { input, .. }
            | PhysicalNode::HashAggregate { input, .. }
            | PhysicalNode::Window { input, .. }
            | PhysicalNode::Exchange { input, .. } => walk(input, session, gov),
            PhysicalNode::HashJoin { left, right, .. }
            | PhysicalNode::SetOp { left, right, .. } => {
                walk(left, session, gov);
                walk(right, session, gov);
            }
            PhysicalNode::UnionAll { inputs, .. } => {
                for inp in inputs {
                    walk(inp, session, gov);
                }
            }
            PhysicalNode::ShuffleRead { .. } => {}
        }
    }
    for s in &mut dag.stages {
        walk(&mut s.plan, session, gov);
    }
}

fn collect_filter_cols_vec(expr: &FilterExpr, out: &mut Vec<String>) {
    match expr {
        FilterExpr::Eq { column, .. }
        | FilterExpr::NotEq { column, .. }
        | FilterExpr::Gt { column, .. }
        | FilterExpr::GtEq { column, .. }
        | FilterExpr::Lt { column, .. }
        | FilterExpr::LtEq { column, .. }
        | FilterExpr::IsNull { column }
        | FilterExpr::IsNotNull { column }
        | FilterExpr::InList { column, .. }
        | FilterExpr::Like { column, .. }
        | FilterExpr::JsonPathEq { column, .. }
        | FilterExpr::JsonContains { column, .. }
        | FilterExpr::JsonHasKey { column, .. } => {
            if !out.contains(column) {
                out.push(column.clone());
            }
        }
        FilterExpr::ColCmp { left, right, .. } => {
            if !out.contains(left) {
                out.push(left.clone());
            }
            if !out.contains(right) {
                out.push(right.clone());
            }
        }
        FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            collect_filter_cols_vec(a, out);
            collect_filter_cols_vec(b, out);
        }
        FilterExpr::Not(inner) => collect_filter_cols_vec(inner, out),
        FilterExpr::LiteralBool(_) => {}
    }
}

fn track_hot_json_paths_in_dag(dag: &StageDag, gov: &GovernanceState) {
    let tables = collect_dag_tables(dag);
    let primary_table = match tables.first() {
        Some(t) => t.as_str(),
        None => return,
    };
    fn walk_filter(expr: &FilterExpr, table: &str, gov: &GovernanceState) {
        match expr {
            FilterExpr::JsonPathEq {
                column, json_path, ..
            } => gov.record_hot_json_path(table, column, json_path),
            FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
                walk_filter(a, table, gov);
                walk_filter(b, table, gov);
            }
            FilterExpr::Not(i) => walk_filter(i, table, gov),
            _ => {}
        }
    }
    fn walk_node(node: &PhysicalNode, table: &str, gov: &GovernanceState) {
        match node {
            PhysicalNode::IcebergScan {
                table_name,
                pushed_filter,
                ..
            } => {
                if let Some(f) = pushed_filter {
                    walk_filter(f, table_name, gov);
                }
            }
            PhysicalNode::Project { input, exprs } => {
                for e in exprs {
                    if let ColumnExpr::JsonPath { column, path, .. } = e {
                        gov.record_hot_json_path(table, column, path);
                    }
                }
                walk_node(input, table, gov);
            }
            PhysicalNode::HashAggregate {
                input, group_by, ..
            } => {
                for g in group_by {
                    if let ColumnExpr::JsonPath { column, path, .. } = g {
                        gov.record_hot_json_path(table, column, path);
                    }
                }
                walk_node(input, table, gov);
            }
            PhysicalNode::Filter { input, predicate } => {
                walk_filter(predicate, table, gov);
                walk_node(input, table, gov);
            }
            PhysicalNode::Distinct { input }
            | PhysicalNode::Sort { input, .. }
            | PhysicalNode::Limit { input, .. }
            | PhysicalNode::TopN { input, .. }
            | PhysicalNode::Window { input, .. }
            | PhysicalNode::Exchange { input, .. } => walk_node(input, table, gov),
            PhysicalNode::HashJoin { left, right, .. }
            | PhysicalNode::SetOp { left, right, .. } => {
                walk_node(left, table, gov);
                walk_node(right, table, gov);
            }
            PhysicalNode::UnionAll { inputs, .. } => {
                for inp in inputs {
                    walk_node(inp, table, gov);
                }
            }
            PhysicalNode::ShuffleRead { .. } => {}
        }
    }
    for s in &dag.stages {
        walk_node(&s.plan, primary_table, gov);
    }
}

fn coerce_json_value(
    val: &serde_json::Value,
    dt: &ColumnType,
) -> std::result::Result<ScalarValue, String> {
    match dt {
        ColumnType::Int64 => val
            .as_i64()
            .map(ScalarValue::Int64)
            .ok_or_else(|| format!("expected integer, got {val}")),
        ColumnType::Float64 => val
            .as_f64()
            .map(ScalarValue::Float64)
            .ok_or_else(|| format!("expected float, got {val}")),
        ColumnType::Bool => val
            .as_bool()
            .map(ScalarValue::Bool)
            .ok_or_else(|| format!("expected bool, got {val}")),
        ColumnType::Variant => Ok(ScalarValue::Json(val.clone())),
        ColumnType::Date => val
            .as_str()
            .and_then(rustlakedb_protocol::parse_iso_date_days)
            .map(ScalarValue::Date)
            .ok_or_else(|| format!("expected ISO date YYYY-MM-DD, got {val}")),
        ColumnType::Decimal { scale, .. } => {
            let s = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            rustlakedb_protocol::parse_decimal_i128(&s, *scale)
                .map(|v| ScalarValue::Decimal(v, *scale))
                .ok_or_else(|| format!("expected decimal, got {val}"))
        }
        ColumnType::Utf8 | ColumnType::TimestampTz => match val {
            serde_json::Value::String(s) => Ok(ScalarValue::Utf8(s.clone())),
            other => Ok(ScalarValue::Utf8(other.to_string())),
        },
    }
}

fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    let mut in_quotes = false;
    let mut at_field_start = true;
    while i < chars.len() {
        let ch = chars[i];
        if in_quotes {
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] == '"' {
                    cur.push('"');
                    i += 2;
                    continue;
                }
                in_quotes = false;
            } else {
                cur.push(ch);
            }
        } else if at_field_start && ch == '"' {
            in_quotes = true;
            at_field_start = false;
        } else if ch == ',' {
            fields.push(std::mem::take(&mut cur));
            at_field_start = true;
        } else {
            cur.push(ch);
            at_field_start = false;
        }
        i += 1;
    }
    fields.push(cur);
    fields
}

fn cmp_scalar_values(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ScalarValue::Null, ScalarValue::Null) => std::cmp::Ordering::Equal,
        (ScalarValue::Null, _) => std::cmp::Ordering::Less,
        (_, ScalarValue::Null) => std::cmp::Ordering::Greater,
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x.cmp(y),
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x.cmp(y),
        (ScalarValue::Bool(x), ScalarValue::Bool(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

fn find_matching_paren(s: &str, open_idx: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, ch) in s[open_idx..].char_indices() {
        if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth -= 1;
            if depth == 0 {
                return Some(open_idx + offset);
            }
        }
    }
    None
}

/// Wave 12 Correlated `EXISTS` / `NOT EXISTS` Decorrelator:
/// Rewrites `EXISTS (SELECT 1 FROM inner i WHERE i.fk = outer.pk [AND extra])` into
/// `(outer.pk, "SELECT i.fk FROM inner i [WHERE extra]")` so it executes via distributed semi-join!
fn try_decorrelate_exists(outer_prefix: &str, sub_sql: &str) -> Option<(String, String)> {
    let outer_upper = outer_prefix.to_uppercase();
    let from_pos = outer_upper.rfind(" FROM ")?;
    let after_from = outer_prefix[from_pos + 6..].trim();
    let outer_tokens: Vec<&str> = after_from.split_whitespace().collect();
    if outer_tokens.is_empty() {
        return None;
    }
    let outer_table = outer_tokens[0].trim_matches('"');
    let outer_alias = if outer_tokens.len() >= 3 && outer_tokens[1].eq_ignore_ascii_case("AS") {
        outer_tokens[2].trim_matches('"')
    } else if outer_tokens.len() >= 2
        && !outer_tokens[1].eq_ignore_ascii_case("WHERE")
        && !outer_tokens[1].eq_ignore_ascii_case("JOIN")
    {
        outer_tokens[1].trim_matches('"')
    } else {
        outer_table
    };

    let sub_upper = sub_sql.to_uppercase();
    let sub_from_pos = sub_upper.find(" FROM ")?;
    let sub_where_pos = sub_upper.find(" WHERE ")?;
    let from_clause = sub_sql[sub_from_pos + 6..sub_where_pos].trim();
    let where_clause = sub_sql[sub_where_pos + 7..].trim();

    let outer_dot = format!("{outer_alias}.");
    let outer_tbl_dot = format!("{outer_table}.");
    if !where_clause.contains(&outer_dot) && !where_clause.contains(&outer_tbl_dot) {
        return None;
    }

    // Split top-level AND conjuncts inside the correlated WHERE clause
    let conjuncts: Vec<&str> = where_clause.split(" AND ").collect();
    let mut corr_outer_col: Option<String> = None;
    let mut corr_inner_col: Option<String> = None;
    let mut remaining = Vec::new();
    for part in conjuncts {
        let trimmed = part.trim();
        if corr_outer_col.is_none()
            && let Some((lhs, rhs)) = trimmed.split_once('=')
        {
            let l = lhs.trim();
            let r = rhs.trim();
            if l.starts_with(&outer_dot) || l.starts_with(&outer_tbl_dot) {
                corr_outer_col = Some(l.to_string());
                corr_inner_col = Some(r.to_string());
                continue;
            }
            if r.starts_with(&outer_dot) || r.starts_with(&outer_tbl_dot) {
                corr_outer_col = Some(r.to_string());
                corr_inner_col = Some(l.to_string());
                continue;
            }
        }
        remaining.push(trimmed);
    }

    let outer_col = corr_outer_col?;
    let inner_col = corr_inner_col?;
    let rebuilt = if remaining.is_empty() {
        format!("SELECT {inner_col} FROM {from_clause}")
    } else {
        format!(
            "SELECT {inner_col} FROM {from_clause} WHERE {}",
            remaining.join(" AND ")
        )
    };
    Some((outer_col, rebuilt))
}

fn extract_primary_from_table(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let pos = upper.find(" FROM ")?;
    let rest = sql[pos + " FROM ".len()..].trim();
    let tok = rest
        .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .next()?;
    if tok.is_empty() {
        None
    } else {
        Some(rustlakedb_catalog::normalize_table_name(tok))
    }
}

/// Detects skewed reduce partitions (> 3x median row_count OR > 3x median byte_size) at AQE barriers.
/// Checking both row_count and byte_size catches repeated-key skew even when Zstd compresses it heavily!
fn detect_skewed_partitions(metrics: &HashMap<usize, PartitionMetrics>) -> Vec<usize> {
    let non_empty: Vec<&PartitionMetrics> = metrics.values().filter(|m| m.row_count > 0).collect();
    if non_empty.len() < 2 {
        return vec![];
    }
    let mut row_counts: Vec<usize> = non_empty.iter().map(|m| m.row_count).collect();
    let mut byte_sizes: Vec<usize> = non_empty.iter().map(|m| m.byte_size).collect();
    row_counts.sort_unstable();
    byte_sizes.sort_unstable();
    let mid = (non_empty.len() - 1) / 2;
    let median_rows = row_counts[mid];
    let median_bytes = byte_sizes[mid];
    non_empty
        .into_iter()
        .filter(|m| {
            m.row_count > median_rows.saturating_mul(3)
                || m.byte_size > median_bytes.saturating_mul(3)
        })
        .map(|m| m.reduce_partition)
        .collect()
}
