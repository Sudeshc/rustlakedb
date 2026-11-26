// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::security::{verify_jwt_hs256, SessionContext};
use crate::Coordinator;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_storage::row_batch_to_arrow_ipc;
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Unified Lakehouse Control Plane HTTP Server (Waves 2–4):
/// - Embedded **Web Admin & SQL Workbench UI** (`GET /`)
/// - **Prometheus Metrics** (`GET /metrics`)
/// - **JSON SQL API** (`POST /v1/sql`)
/// - **Zero-Copy Arrow IPC Stream SQL Endpoint** (`POST /v1/sql/arrow`)
/// - **Streaming Micro-Batch / CDC NDJSON Ingest** (`POST /v1/ingest/{table}`)
/// - **Apache Iceberg v3 REST Catalog** (`/v1/config`, `/v1/namespaces`, `/v1/namespaces/{ns}/tables/...`)
pub struct ControlPlaneServer {
    listener: TcpListener,
    coord: Arc<Coordinator>,
}

impl ControlPlaneServer {
    pub async fn bind(addr: &str, coord: Arc<Coordinator>) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, coord })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn serve(self) -> Result<()> {
        // Automatic streaming micro-batch flusher (coalesces `?buffered=true` record-by-record streams)
        let flush_coord = self.coord.clone();
        let flush_ms: u64 = std::env::var("RUSTLAKEDB_INGEST_FLUSH_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000)
            .max(100);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(flush_ms)).await;
                let _ = flush_coord.flush_all_ingest_buffers().await;
            }
        });
        let app = build_control_plane_router(self.coord);
        axum::serve(self.listener, app)
            .await
            .map_err(|e| RustLakeError::Execution(format!("control plane server: {e}")))
    }
}

pub fn build_control_plane_router(coord: Arc<Coordinator>) -> Router {
    let iceberg_rest = rustlakedb_catalog::rest::build_router(coord.catalog.clone()).layer(
        axum::middleware::from_fn_with_state(coord.clone(), enforce_iceberg_rest_rbac),
    );
    Router::new()
        .route("/", get(web_ui))
        .route("/readyz", get(healthz_http))
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/cluster/status", get(cluster_status))
        .route("/v1/workers/register", post(register_worker_http))
        .route("/v1/workers/heartbeat", post(register_worker_http))
        .route("/v1/sql", post(sql_json))
        .route("/v1/sql/arrow", post(sql_arrow))
        .route("/v1/queries/active", get(active_queries_http))
        .route("/v1/queries/{query_id}/cancel", post(cancel_query_http))
        .route("/v1/ingest/{table}", post(ingest_ndjson_http))
        .route("/v1/cdc/{table}", post(ingest_cdc_http))
        .route("/v1/traces", get(get_traces_http))
        .route("/v1/governance/overview", get(get_governance_overview_http))
        .route("/v1/governance/lineage", get(get_lineage_http))
        .route("/v1/governance/tags", get(get_tags_http))
        .route("/v1/governance/audit", get(get_audit_http))
        .route("/v1/governance/access-history", get(get_access_history_http))
        .route("/v1/governance/query-history", get(get_query_history_http))
        .route("/v1/federation/catalogs", get(get_external_catalogs_http))
        .route("/v1/federation/export/{table}", get(export_federation_descriptors_http))
        .route("/v1/tables/{table}/snapshots", get(get_table_snapshots_http))
        .route("/v1/tables/{table}/health", get(get_table_health_http))
        .route("/v1/admin/drain", post(admin_drain_http))
        .route("/v1/admin/undrain", post(admin_undrain_http))
        .route("/v1/admin/backup-catalog", post(admin_backup_catalog_http))
        .route("/v1/ml/models", get(get_ml_models_http))
        .route("/v1/ml/feature-views", get(get_ml_feature_views_http))
        .route("/v1/mcp", post(mcp_jsonrpc_http))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(coord)
        .merge(iceberg_rest)
}

/// Wave 40: Enforces OIDC/JWT AuthN + Table RBAC + Access/Audit Logging on External Iceberg REST
/// Catalog table endpoints (`/v1/namespaces/{ns}/tables/{table}` and `/credentials`) so Spark,
/// Trino, Snowflake, Databricks, and PyIceberg obey identical governance as SQL!
async fn enforce_iceberg_rest_rbac(
    State(coord): State<Arc<Coordinator>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    // Parse `/v1/namespaces/{ns}/tables/{table}[/credentials]`
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segs.len() >= 5 && segs[0] == "v1" && segs[1] == "namespaces" && segs[3] == "tables" {
        let table = segs[4];
        let has_bearer = req.headers().contains_key("authorization");
        let auth_required = std::env::var("RUSTLAKEDB_REST_AUTH_REQUIRED")
            .is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1");
        if has_bearer || auth_required {
            let session = match resolve_http_session(&coord, req.headers(), "anonymous").await {
                Ok(s) => s,
                Err(e) => {
                    return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))).into_response();
                }
            };
            if !session.is_superuser()
                && let Ok(gov) = coord.catalog.get_governance().await
            {
                coord.governance.hydrate_from_catalog(gov);
            }
            let priv_needed = match method.as_str() {
                "POST" => "INSERT",
                "DELETE" => "DELETE",
                _ => "SELECT",
            };
            if let Err(e) = coord.governance.check_access(&session, table, priv_needed) {
                let _ = coord
                    .catalog
                    .apply_governance(rustlakedb_catalog::GovernanceOp::RecordAudit(
                        crate::security::make_persisted_audit(
                            &session,
                            priv_needed,
                            table,
                            "DENIED (42501)",
                            "ICEBERG_REST",
                        ),
                    ))
                    .await;
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": { "message": e.to_string(), "type": "ForbiddenException", "code": 403 } })),
                )
                    .into_response();
            }
            let entry = coord.governance.record_access(
                "iceberg-rest",
                &session,
                table,
                vec!["*".into()],
                priv_needed,
                "ICEBERG_REST",
            );
            let _ = coord
                .catalog
                .apply_governance(rustlakedb_catalog::GovernanceOp::RecordAccess(entry))
                .await;
        }
    }
    next.run(req).await
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default = "default_user")]
    user: String,
    #[serde(default = "default_branch")]
    branch: String,
}

fn default_user() -> String {
    "postgres".into()
}
fn default_branch() -> String {
    "main".into()
}

#[derive(Deserialize)]
struct IngestQuery {
    #[serde(default)]
    on_error: Option<String>,
    #[serde(default = "default_branch")]
    branch: String,
    /// When `true`, coalesces record-by-record streaming writes into micro-batched Parquet flushes
    /// (every `RUSTLAKEDB_INGEST_FLUSH_MS` or `RUSTLAKEDB_INGEST_FLUSH_ROWS`), eliminating tiny files!
    #[serde(default)]
    buffered: bool,
    /// When `true`, forces immediate flush of the micro-batch buffer.
    #[serde(default)]
    flush: bool,
}

#[derive(Deserialize)]
struct WorkerRegisterRequest {
    worker_id: String,
    url: String,
}

async fn register_worker_http(
    State(coord): State<Arc<Coordinator>>,
    Json(req): Json<WorkerRegisterRequest>,
) -> (StatusCode, Json<Value>) {
    let total = coord.register_worker(&req.worker_id, &req.url);
    (
        StatusCode::OK,
        Json(json!({
            "status": "registered",
            "worker_id": req.worker_id,
            "url": req.url,
            "total_workers": total
        })),
    )
}

async fn resolve_http_session(
    coord: &Coordinator,
    headers: &HeaderMap,
    fallback_user: &str,
) -> std::result::Result<SessionContext, String> {
    let has_jwks = coord.governance.has_jwks();
    let hs_secret = coord.governance.jwt_secret();
    if has_jwks || hs_secret.is_some() {
        let auth = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| "missing Authorization: Bearer <jwt> header".to_string())?;
        let token = auth
            .strip_prefix("Bearer ")
            .ok_or_else(|| "Authorization header must start with 'Bearer '".to_string())?;
        if has_jwks {
            match coord.governance.verify_jwt_rs256_auto_rotate(token).await {
                Ok(sess) => return Ok(sess),
                Err(e) if hs_secret.is_none() => return Err(e.to_string()),
                Err(_) => {}
            }
        }
        if let Some(secret) = hs_secret {
            return verify_jwt_hs256(&secret, token).map_err(|e| e.to_string());
        }
    }
    Ok(SessionContext::for_user(fallback_user))
}

async fn active_queries_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    Json(json!({
        "active_queries": coord.governance.active_queries_snapshot()
    }))
}

async fn cancel_query_http(
    Path(query_id): Path<String>,
    State(coord): State<Arc<Coordinator>>,
) -> (StatusCode, Json<Value>) {
    let was_active = coord.governance.cancel_query(&query_id);
    (
        StatusCode::OK,
        Json(json!({
            "query_id": query_id,
            "cancelled": true,
            "was_active": was_active
        })),
    )
}

#[derive(Deserialize)]
struct CdcQuery {
    #[serde(default = "default_branch")]
    branch: String,
    #[serde(default = "default_stream")]
    stream_id: String,
}

fn default_stream() -> String {
    "default".into()
}

async fn ingest_cdc_http(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    Path(table): Path<String>,
    Query(q): Query<CdcQuery>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(e) = resolve_http_session(&coord, &headers, "postgres").await {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e })));
    }
    let events: Vec<Value> = match payload {
        Value::Array(arr) => arr,
        Value::Object(ref map) if map.contains_key("events") => map["events"]
            .as_array()
            .cloned()
            .unwrap_or_default(),
        single => vec![single],
    };
    match coord
        .ingest_cdc_events(&table, &q.branch, &q.stream_id, &events)
        .await
    {
        Ok(report) => (StatusCode::OK, Json(report)),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn get_traces_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    let spans = coord.governance.traces_snapshot();
    Json(json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{ "key": "service.name", "value": { "stringValue": "rustlakedb" } }]
            },
            "scopeSpans": [{ "spans": spans }]
        }],
        "spans": spans
    }))
}

async fn sql_json(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> (StatusCode, Json<Value>) {
    if coord.is_draining() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "coordinator node is draining; retry request on a healthy peer" })),
        );
    }
    let session = match resolve_http_session(&coord, &headers, &req.user).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))),
    };
    let trace_id = headers
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(|tp| tp.split('-').nth(1))
        .filter(|s| s.len() >= 16)
        .map(String::from);
    let t0 = std::time::Instant::now();
    match coord
        .execute_sql_traced(&req.sql, &req.branch, &session, trace_id.as_deref())
        .await
    {
        Ok(res) => {
            let elapsed_ms = t0.elapsed().as_millis() as u64;
            let rows_json: Vec<Vec<Value>> = res
                .batch
                .rows
                .iter()
                .map(|r| {
                    res.batch
                        .columns
                        .iter()
                        .map(|c| {
                            r.get(c)
                                .and_then(|v| serde_json::to_value(v).ok())
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect();
            let dag_stages: Vec<Value> = res
                .dag
                .as_ref()
                .map(|d| {
                    d.stages
                        .iter()
                        .map(|s| {
                            json!({
                                "stage_id": s.stage_id,
                                "shuffle_mode": format!("{:?}", s.shuffle_mode),
                                "partitions": s.num_partitions,
                                "dependencies": s.upstream_stages,
                                "operators": vec![format!("{:?}", s.plan)],
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            (
                StatusCode::OK,
                Json(json!({
                    "command_tag": res.command_tag,
                    "columns": res.batch.columns,
                    "rows": rows_json,
                    "row_count": res.batch.num_rows(),
                    "elapsed_ms": elapsed_ms,
                    "stages_executed": res.stats.stages_executed,
                    "tasks_executed": res.stats.tasks_executed,
                    "dag_stages": dag_stages
                })),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn sql_arrow(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Response {
    let session = match resolve_http_session(&coord, &headers, &req.user).await {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))).into_response();
        }
    };
    match coord
        .execute_sql_with_session(&req.sql, &req.branch, &session)
        .await
    {
        Ok(res) => match row_batch_to_arrow_ipc(&res.batch) {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")],
                bytes,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response(),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn ingest_ndjson_http(
    Path(table): Path<String>,
    Query(q): Query<IngestQuery>,
    State(coord): State<Arc<Coordinator>>,
    body: String,
) -> (StatusCode, Json<Value>) {
    let on_err = q
        .on_error
        .as_deref()
        .is_some_and(|s| s.eq_ignore_ascii_case("continue"));
    if q.buffered || q.flush {
        return match coord
            .ingest_ndjson_buffered(&table, &q.branch, &body, on_err, q.flush)
            .await
        {
            Ok((loaded, dlq, flushed)) => (
                StatusCode::OK,
                Json(json!({
                    "status": if flushed { "committed" } else { "buffered" },
                    "table": table,
                    "rows_loaded": loaded,
                    "dlq_rows": dlq,
                    "flushed": flushed
                })),
            ),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            ),
        };
    }
    match coord.ingest_ndjson(&table, &q.branch, &body, on_err).await {
        Ok((loaded, dlq)) => (
            StatusCode::OK,
            Json(json!({
                "status": "committed",
                "table": table,
                "rows_loaded": loaded,
                "dlq_rows": dlq
            })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn cluster_status(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    let tables = coord.catalog.list_tables().await.unwrap_or_default();
    let views = coord.catalog.list_views().await.unwrap_or_default();
    let workers: Vec<Value> = coord
        .workers_snapshot()
        .iter()
        .map(|w| json!({ "id": w.id().0, "healthy": w.is_healthy() }))
        .collect();
    let history = coord.governance.query_history_snapshot();
    let (p50_ms, p95_ms, p99_ms) = coord.governance.latency_percentiles_ms();
    let policy = crate::CompactionPolicy::from_env();
    let (small_files, total_files, total_bytes) =
        coord.catalog.catalog_file_health_stats(policy.min_file_bytes);
    let (_tables_total, max_snapshots) = coord.catalog.catalog_stats();
    let raft = coord.catalog.raft_telemetry();
    Json(json!({
        "engine": "RustLakeDB",
        "version": env!("CARGO_PKG_VERSION"),
        "draining": coord.is_draining(),
        "storage_uri": coord.store.base_uri().unwrap_or("memory://"),
        "raft": raft,
        "workers": workers,
        "tables": tables,
        "views": views,
        "active_queries": coord.governance.active_queries_snapshot(),
        "recent_queries": history.into_iter().rev().take(20).collect::<Vec<_>>(),
        "ops": {
            "queries_total": coord.governance.queries_total.load(Ordering::Relaxed),
            "query_errors_total": coord.governance.query_errors_total.load(Ordering::Relaxed),
            "worker_rpc_timeouts_total": coord.governance.worker_rpc_timeouts_total.load(Ordering::Relaxed),
            "auth_denied_total": coord.governance.auth_denied_total.load(Ordering::Relaxed),
            "compaction_runs_total": coord.governance.compaction_runs_total.load(Ordering::Relaxed),
            "compaction_files_rewritten_total": coord.governance.compaction_files_rewritten_total.load(Ordering::Relaxed),
            "otlp_spans_exported_total": coord.governance.otlp_spans_exported_total.load(Ordering::Relaxed),
            "ingest_buffer_rows": coord.ingest_buffer_rows(),
            "max_snapshots": max_snapshots,
            "latency_ms": { "p50": p50_ms, "p95": p95_ms, "p99": p99_ms },
            "latency_histogram": {
                "bounds_ms": crate::security::LATENCY_BUCKET_BOUNDS_MS,
                "buckets": coord.governance.latency_histogram_snapshot(),
            }
        },
        "storage": {
            "parquet_files_written": coord.store.parquet_files_written.load(Ordering::Relaxed),
            "parquet_files_read": coord.store.parquet_files_read.load(Ordering::Relaxed),
            "cache_hits": coord.store.cache_hits.load(Ordering::Relaxed),
            "files_pruned_by_puffin": coord.store.files_pruned_by_puffin.load(Ordering::Relaxed),
            "files_pruned_by_zonemap": coord.store.files_pruned_by_zonemap.load(Ordering::Relaxed),
            "hnsw_index_hits": coord.store.hnsw_index_hits.load(Ordering::Relaxed),
            "small_files_total": small_files,
            "total_data_files": total_files,
            "total_data_bytes": total_bytes
        }
    }))
}

async fn healthz_http(State(coord): State<Arc<Coordinator>>) -> (StatusCode, Json<Value>) {
    let wsnap = coord.workers_snapshot();
    let healthy_workers = wsnap.iter().filter(|w| w.is_healthy()).count();
    let raft = coord.catalog.raft_telemetry();
    let draining = coord.is_draining();
    let code = if draining {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (
        code,
        Json(json!({
            "status": if draining { "draining" } else { "ok" },
            "draining": draining,
            "healthy_workers": healthy_workers,
            "total_workers": wsnap.len(),
            "raft": raft,
        })),
    )
}

async fn prometheus_metrics(State(coord): State<Arc<Coordinator>>) -> Response {
    let wsnap = coord.workers_snapshot();
    let healthy_workers = wsnap.iter().filter(|w| w.is_healthy()).count();
    let total_workers = wsnap.len();
    let queries = coord.governance.queries_total.load(Ordering::Relaxed);
    let errors = coord.governance.query_errors_total.load(Ordering::Relaxed);
    let dur_ms = coord
        .governance
        .query_duration_ms_sum
        .load(Ordering::Relaxed);
    let rpc_timeouts = coord
        .governance
        .worker_rpc_timeouts_total
        .load(Ordering::Relaxed);
    let auth_denied = coord.governance.auth_denied_total.load(Ordering::Relaxed);
    let compaction_runs = coord
        .governance
        .compaction_runs_total
        .load(Ordering::Relaxed);
    let compaction_files = coord
        .governance
        .compaction_files_rewritten_total
        .load(Ordering::Relaxed);
    let otlp_exported = coord
        .governance
        .otlp_spans_exported_total
        .load(Ordering::Relaxed);
    let ingest_buf_rows = coord.ingest_buffer_rows();
    let draining_flag = if coord.is_draining() { 1 } else { 0 };
    let hist = coord.governance.latency_histogram_snapshot();
    let pq_read = coord.store.parquet_files_read.load(Ordering::Relaxed);
    let pq_written = coord.store.parquet_files_written.load(Ordering::Relaxed);
    let cache_hits = coord.store.cache_hits.load(Ordering::Relaxed);
    let puffin_pruned = coord.store.files_pruned_by_puffin.load(Ordering::Relaxed);
    let zm_pruned = coord.store.files_pruned_by_zonemap.load(Ordering::Relaxed);
    let hnsw_hits = coord.store.hnsw_index_hits.load(Ordering::Relaxed);

    let (raft_is_leader, raft_term, raft_commit_idx, raft_leader_changes) =
        match coord.catalog.raft_telemetry() {
            Some(rt) => (
                if rt.is_leader { 1 } else { 0 },
                rt.term,
                rt.commit_index,
                rt.leader_changes,
            ),
            None => (1, 1, 0, 0),
        };

    let (tables_total, max_snapshots) = coord.catalog.catalog_stats();
    let policy = crate::CompactionPolicy::from_env();
    let (small_files_total, _total_files, _total_bytes) =
        coord.catalog.catalog_file_health_stats(policy.min_file_bytes);

    let mut spill_bytes: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
        for ent in entries.flatten() {
            let name = ent.file_name().to_string_lossy().to_string();
            if name.starts_with("arrow_") && name.ends_with(".arw1")
                && let Ok(md) = ent.metadata()
            {
                spill_bytes = spill_bytes.saturating_add(md.len());
            }
        }
    }

    let mut hist_lines = String::new();
    for (i, bound) in crate::security::LATENCY_BUCKET_BOUNDS_MS.iter().enumerate() {
        hist_lines.push_str(&format!(
            "rustlakedb_query_duration_ms_bucket{{le=\"{bound}\"}} {}\n",
            hist[i]
        ));
    }
    hist_lines.push_str(&format!(
        "rustlakedb_query_duration_ms_bucket{{le=\"+Inf\"}} {}\n\
         rustlakedb_query_duration_ms_count {}\n",
        hist[12], hist[12]
    ));

    let body = format!(
        "# HELP rustlakedb_queries_total Total SQL queries executed\n\
         # TYPE rustlakedb_queries_total counter\n\
         rustlakedb_queries_total {queries}\n\
         # HELP rustlakedb_query_errors_total Total failed SQL queries\n\
         # TYPE rustlakedb_query_errors_total counter\n\
         rustlakedb_query_errors_total {errors}\n\
         # HELP rustlakedb_query_duration_ms Query latency histogram in milliseconds\n\
         # TYPE rustlakedb_query_duration_ms histogram\n\
         {hist_lines}\
         rustlakedb_query_duration_ms_sum {dur_ms}\n\
         # HELP rustlakedb_worker_rpc_timeouts_total Total worker RPC failures/timeouts triggering retry\n\
         # TYPE rustlakedb_worker_rpc_timeouts_total counter\n\
         rustlakedb_worker_rpc_timeouts_total {rpc_timeouts}\n\
         # HELP rustlakedb_auth_denied_total Total RBAC/column-grant/REST authorization denials (42501/403)\n\
         # TYPE rustlakedb_auth_denied_total counter\n\
         rustlakedb_auth_denied_total {auth_denied}\n\
         # HELP rustlakedb_compaction_runs_total Total table compaction and optimize runs completed\n\
         # TYPE rustlakedb_compaction_runs_total counter\n\
         rustlakedb_compaction_runs_total {compaction_runs}\n\
         # HELP rustlakedb_compaction_files_rewritten_total Total Parquet files rewritten by compaction/optimize\n\
         # TYPE rustlakedb_compaction_files_rewritten_total counter\n\
         rustlakedb_compaction_files_rewritten_total {compaction_files}\n\
         # HELP rustlakedb_small_files_total Active Parquet data files smaller than min_file_bytes compaction threshold\n\
         # TYPE rustlakedb_small_files_total gauge\n\
         rustlakedb_small_files_total {small_files_total}\n\
         # HELP rustlakedb_ingest_buffer_rows Streaming micro-batch rows currently buffered awaiting flush\n\
         # TYPE rustlakedb_ingest_buffer_rows gauge\n\
         rustlakedb_ingest_buffer_rows {ingest_buf_rows}\n\
         # HELP rustlakedb_otlp_spans_exported_total Total OpenTelemetry spans pushed to configured OTLP endpoint\n\
         # TYPE rustlakedb_otlp_spans_exported_total counter\n\
         rustlakedb_otlp_spans_exported_total {otlp_exported}\n\
         # HELP rustlakedb_node_draining Whether this coordinator is draining for maintenance (1=draining, 0=active)\n\
         # TYPE rustlakedb_node_draining gauge\n\
         rustlakedb_node_draining {draining_flag}\n\
         # HELP rustlakedb_workers_healthy Number of healthy distributed workers\n\
         # TYPE rustlakedb_workers_healthy gauge\n\
         rustlakedb_workers_healthy {healthy_workers}\n\
         # HELP rustlakedb_workers_total Total registered workers\n\
         # TYPE rustlakedb_workers_total gauge\n\
         rustlakedb_workers_total {total_workers}\n\
         # HELP rustlakedb_raft_is_leader Whether this coordinator is the current Raft quorum leader (1=leader, 0=follower)\n\
         # TYPE rustlakedb_raft_is_leader gauge\n\
         rustlakedb_raft_is_leader {raft_is_leader}\n\
         # HELP rustlakedb_raft_term Current Raft consensus term\n\
         # TYPE rustlakedb_raft_term gauge\n\
         rustlakedb_raft_term {raft_term}\n\
         # HELP rustlakedb_raft_commit_index Highest Raft log index committed by quorum\n\
         # TYPE rustlakedb_raft_commit_index gauge\n\
         rustlakedb_raft_commit_index {raft_commit_idx}\n\
         # HELP rustlakedb_raft_leader_changes_total Total Raft leadership changes observed on this coordinator\n\
         # TYPE rustlakedb_raft_leader_changes_total counter\n\
         rustlakedb_raft_leader_changes_total {raft_leader_changes}\n\
         # HELP rustlakedb_catalog_tables_total Total Iceberg tables registered in catalog\n\
         # TYPE rustlakedb_catalog_tables_total gauge\n\
         rustlakedb_catalog_tables_total {tables_total}\n\
         # HELP rustlakedb_catalog_max_snapshots Maximum retained snapshots on any table (detects VACUUM backlog)\n\
         # TYPE rustlakedb_catalog_max_snapshots gauge\n\
         rustlakedb_catalog_max_snapshots {max_snapshots}\n\
         # HELP rustlakedb_nvme_spill_bytes Active NVMe out-of-core Arrow spill bytes on disk\n\
         # TYPE rustlakedb_nvme_spill_bytes gauge\n\
         rustlakedb_nvme_spill_bytes {spill_bytes}\n\
         # HELP rustlakedb_parquet_files_read_total Parquet files read\n\
         # TYPE rustlakedb_parquet_files_read_total counter\n\
         rustlakedb_parquet_files_read_total {pq_read}\n\
         # HELP rustlakedb_parquet_files_written_total Parquet files written\n\
         # TYPE rustlakedb_parquet_files_written_total counter\n\
         rustlakedb_parquet_files_written_total {pq_written}\n\
         # HELP rustlakedb_cache_hits_total Hybrid RAM+NVMe cache hits\n\
         # TYPE rustlakedb_cache_hits_total counter\n\
         rustlakedb_cache_hits_total {cache_hits}\n\
         # HELP rustlakedb_puffin_pruned_total Files pruned by Puffin JSON indexes\n\
         # TYPE rustlakedb_puffin_pruned_total counter\n\
         rustlakedb_puffin_pruned_total {puffin_pruned}\n\
         # HELP rustlakedb_zonemap_pruned_total Files pruned by Liquid Clustering zone-maps\n\
         # TYPE rustlakedb_zonemap_pruned_total counter\n\
         rustlakedb_zonemap_pruned_total {zm_pruned}\n\
         # HELP rustlakedb_hnsw_index_hits_total Worker-side HNSW ANN graph pushdown hits\n\
         # TYPE rustlakedb_hnsw_index_hits_total counter\n\
         rustlakedb_hnsw_index_hits_total {hnsw_hits}\n"
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn get_lineage_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    let edges = coord.governance.lineage_snapshot();
    Json(json!({ "count": edges.len(), "lineage": edges }))
}

async fn get_tags_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    let tables = coord.catalog.list_tables().await.unwrap_or_default();
    let mut out = serde_json::Map::new();
    for t in tables {
        if let Ok(meta) = coord.catalog.get_table(&t).await
            && !meta.tags.is_empty()
        {
            out.insert(meta.table_name, json!(meta.tags));
        }
    }
    Json(json!({ "tags": out }))
}

async fn get_audit_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    if let Ok(gov) = coord.catalog.get_governance().await {
        coord.governance.hydrate_from_catalog(gov);
    }
    let entries = coord.governance.audit_log_snapshot();
    Json(json!({ "count": entries.len(), "audit_log": entries }))
}

async fn get_access_history_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    if let Ok(gov) = coord.catalog.get_governance().await {
        coord.governance.hydrate_from_catalog(gov);
    }
    let entries = coord.governance.access_history_snapshot();
    Json(json!({ "count": entries.len(), "access_history": entries }))
}

async fn get_query_history_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    let entries = coord.governance.query_history_snapshot();
    Json(json!({ "count": entries.len(), "query_history": entries }))
}

async fn get_external_catalogs_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    if let Ok(gov) = coord.catalog.get_governance().await {
        coord.governance.hydrate_from_catalog(gov);
    }
    let catalogs = coord.governance.list_external_catalogs();
    Json(json!({ "count": catalogs.len(), "external_catalogs": catalogs }))
}

/// Outbound Multi-Engine Catalog Federation Descriptor (Wave 40):
/// Generates turnkey Iceberg v3 federation snippets so Snowflake, Databricks Unity Catalog,
/// AWS Glue, and Google BigQuery BigLake can query this RustLakeDB table with ZERO data copying!
async fn export_federation_descriptors_http(
    Path(table): Path<String>,
    State(coord): State<Arc<Coordinator>>,
) -> (StatusCode, Json<Value>) {
    match coord.catalog.get_table(&table).await {
        Ok(meta) => {
            let snap_id = meta.current_snapshot("main").map(|s| s.snapshot_id).unwrap_or(1);
            let base_uri = coord
                .store
                .base_uri()
                .unwrap_or("s3://rustlakedb-warehouse");
            let rest_uri = std::env::var("RUSTLAKEDB_PUBLIC_REST_URI")
                .unwrap_or_else(|_| "https://rustlakedb-coordinator:8181".into());
            let metadata_location = format!(
                "{}/{}/metadata/v{snap_id}.metadata.json",
                base_uri.trim_end_matches('/'),
                meta.table_name
            );
            (
                StatusCode::OK,
                Json(json!({
                    "table": meta.table_name,
                    "format": "apache-iceberg-v3",
                    "snapshot_id": snap_id,
                    "metadata_location": metadata_location,
                    "iceberg_rest_uri": rest_uri,
                    "tags": meta.tags,
                    "federation": {
                        "snowflake": {
                            "catalog_integration_sql": format!(
                                "CREATE OR REPLACE CATALOG INTEGRATION rustlakedb_rest_int CATALOG_SOURCE = ICEBERG_REST TABLE_FORMAT = ICEBERG REST_CONFIG = (CATALOG_URI = '{rest_uri}', WAREHOUSE = 'default') ENABLED = TRUE;"
                            ),
                            "create_table_sql": format!(
                                "CREATE OR REPLACE ICEBERG TABLE {} CATALOG = 'rustlakedb_rest_int' CATALOG_NAMESPACE = 'default' CATALOG_TABLE_NAME = '{}' AUTO_REFRESH = TRUE;",
                                meta.table_name, meta.table_name
                            )
                        },
                        "databricks_unity_catalog": {
                            "create_connection_sql": format!(
                                "CREATE CONNECTION rustlakedb_conn TYPE ICEBERG_REST OPTIONS (host '{rest_uri}', credential '<oidc_jwt>');"
                            ),
                            "create_foreign_catalog_sql": "CREATE FOREIGN CATALOG rustlakedb_fed USING CONNECTION rustlakedb_conn;".to_string(),
                            "query_sql": format!("SELECT * FROM rustlakedb_fed.default.{};", meta.table_name)
                        },
                        "aws_glue": {
                            "table_input": {
                                "Name": meta.table_name,
                                "TableType": "EXTERNAL_TABLE",
                                "Parameters": {
                                    "table_type": "ICEBERG",
                                    "metadata_location": metadata_location
                                }
                            }
                        },
                        "google_bigquery": {
                            "create_external_table_sql": format!(
                                "CREATE EXTERNAL TABLE `project.dataset.{}` WITH CONNECTION `us.rustlakedb_biglake` OPTIONS (format = 'ICEBERG', uris = ['{metadata_location}']);",
                                meta.table_name
                            )
                        }
                    }
                })),
            )
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn get_table_snapshots_http(
    State(coord): State<Arc<Coordinator>>,
    Path(table): Path<String>,
) -> (StatusCode, Json<Value>) {
    match coord.catalog.get_table(&table).await {
        Ok(meta) => {
            let snaps: Vec<Value> = meta
                .snapshots
                .values()
                .map(|s| {
                    json!({
                        "snapshot_id": s.snapshot_id,
                        "parent_snapshot_id": s.parent_snapshot_id,
                        "timestamp_ms": s.timestamp_ms,
                        "file_count": s.data_files.len(),
                        "row_count": s.data_files.iter().map(|f| f.row_count).sum::<u64>(),
                        "summary": s.summary,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "table": meta.table_name,
                    "refs": meta.refs,
                    "snapshots": snaps,
                })),
            )
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn get_governance_overview_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    if let Ok(gov) = coord.catalog.get_governance().await {
        coord.governance.hydrate_from_catalog(gov);
    }
    Json(coord.governance.governance_overview_snapshot())
}

async fn get_table_health_http(
    State(coord): State<Arc<Coordinator>>,
    Path(table): Path<String>,
) -> (StatusCode, Json<Value>) {
    match coord.catalog.get_table(&table).await {
        Ok(meta) => {
            let policy = crate::CompactionPolicy::from_env();
            let snap = meta.current_snapshot("main").ok();
            let mut b_lt_1m = 0usize;
            let mut b_1m_32m = 0usize;
            let mut b_32m_128m = 0usize;
            let mut b_128m_256m = 0usize;
            let mut b_gt_256m = 0usize;
            let mut small_files = 0usize;
            let mut oversized_files = 0usize;
            let mut files_with_dv = 0usize;
            let mut total_rows = 0u64;
            let mut deleted_rows = 0u64;
            let mut total_bytes = 0u64;
            let mut file_count = 0usize;
            let mut min_bytes = u64::MAX;
            let mut max_bytes = 0u64;

            if let Some(s) = snap {
                file_count = s.data_files.len();
                for df in &s.data_files {
                    total_rows = total_rows.saturating_add(df.row_count);
                    deleted_rows = deleted_rows.saturating_add(df.deleted_row_count);
                    total_bytes = total_bytes.saturating_add(df.byte_size);
                    min_bytes = min_bytes.min(df.byte_size);
                    max_bytes = max_bytes.max(df.byte_size);
                    if df.byte_size < 1024 * 1024 {
                        b_lt_1m += 1;
                    } else if df.byte_size < 32 * 1024 * 1024 {
                        b_1m_32m += 1;
                    } else if df.byte_size < 128 * 1024 * 1024 {
                        b_32m_128m += 1;
                    } else if df.byte_size <= 256 * 1024 * 1024 {
                        b_128m_256m += 1;
                    } else {
                        b_gt_256m += 1;
                    }
                    if df.byte_size < policy.min_file_bytes {
                        small_files += 1;
                    }
                    if df.byte_size > policy.max_file_bytes {
                        oversized_files += 1;
                    }
                    if df.dv_path.is_some() {
                        files_with_dv += 1;
                    }
                }
            }
            if file_count == 0 {
                min_bytes = 0;
            }
            let delete_ratio = if total_rows > 0 {
                deleted_rows as f64 / total_rows as f64
            } else {
                0.0
            };
            let avg_bytes = if file_count > 0 {
                total_bytes / file_count as u64
            } else {
                0
            };
            let needs_compaction = small_files >= policy.min_small_files_per_partition
                || oversized_files > 0
                || delete_ratio >= policy.delete_ratio_threshold;
            let schema: Vec<Value> = meta
                .schema
                .iter()
                .map(|f| {
                    json!({
                        "field_id": f.field_id,
                        "name": f.name,
                        "data_type": format!("{:?}", f.data_type),
                        "nullable": f.nullable,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "table": meta.table_name,
                    "schema": schema,
                    "tags": meta.tags,
                    "cluster_by": meta.cluster_by,
                    "refs": meta.refs,
                    "snapshot_count": meta.snapshots.len(),
                    "file_count": file_count,
                    "total_rows": total_rows,
                    "deleted_rows": deleted_rows,
                    "delete_ratio": delete_ratio,
                    "total_bytes": total_bytes,
                    "avg_file_bytes": avg_bytes,
                    "min_file_bytes": min_bytes,
                    "max_file_bytes": max_bytes,
                    "small_files": small_files,
                    "oversized_files": oversized_files,
                    "files_with_dv": files_with_dv,
                    "recommendation": if needs_compaction { "COMPACT_RECOMMENDED" } else { "HEALTHY" },
                    "file_size_distribution": {
                        "lt_1mb": b_lt_1m,
                        "1mb_to_32mb": b_1m_32m,
                        "32mb_to_128mb": b_32m_128m,
                        "128mb_to_256mb": b_128m_256m,
                        "gt_256mb": b_gt_256m,
                    }
                })),
            )
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

#[derive(Deserialize, Default)]
struct DrainRequest {
    #[serde(default = "default_drain_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_user")]
    user: String,
}
fn default_drain_timeout_ms() -> u64 {
    10_000
}

async fn admin_drain_http(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    body: Option<Json<DrainRequest>>,
) -> (StatusCode, Json<Value>) {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let session = match resolve_http_session(&coord, &headers, &req.user).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))),
    };
    if let Err(e) = coord
        .governance
        .require_security_admin(&session, "ADMIN DRAIN", "coordinator")
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": e.to_string() })));
    }
    let report = coord
        .drain_gracefully(std::time::Duration::from_millis(req.timeout_ms.min(60_000)))
        .await;
    coord
        .governance
        .record_audit(&session.user, "ADMIN DRAIN", "coordinator", "OK");
    (StatusCode::OK, Json(report))
}

async fn admin_undrain_http(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    body: Option<Json<DrainRequest>>,
) -> (StatusCode, Json<Value>) {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let session = match resolve_http_session(&coord, &headers, &req.user).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))),
    };
    if let Err(e) = coord
        .governance
        .require_security_admin(&session, "ADMIN UNDRAIN", "coordinator")
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": e.to_string() })));
    }
    coord.set_draining(false);
    coord
        .governance
        .record_audit(&session.user, "ADMIN UNDRAIN", "coordinator", "OK");
    (
        StatusCode::OK,
        Json(json!({ "status": "active", "draining": false })),
    )
}

#[derive(Deserialize, Default)]
struct BackupCatalogRequest {
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default = "default_user")]
    user: String,
}

async fn admin_backup_catalog_http(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    body: Option<Json<BackupCatalogRequest>>,
) -> (StatusCode, Json<Value>) {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let session = match resolve_http_session(&coord, &headers, &req.user).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))),
    };
    if let Err(e) = coord
        .governance
        .require_security_admin(&session, "BACKUP CATALOG", "catalog")
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": e.to_string() })));
    }
    match coord
        .backup_catalog_snapshot(&session, req.prefix.as_deref())
        .await
    {
        Ok(report) => (StatusCode::OK, Json(report)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn get_ml_models_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    if let Ok(gov) = coord.catalog.get_governance().await {
        coord.governance.hydrate_from_catalog(gov);
    }
    let models = coord.governance.list_models();
    Json(json!({ "count": models.len(), "models": models }))
}

async fn get_ml_feature_views_http(State(coord): State<Arc<Coordinator>>) -> Json<Value> {
    if let Ok(gov) = coord.catalog.get_governance().await {
        coord.governance.hydrate_from_catalog(gov);
    }
    let fvs = coord.governance.list_feature_views();
    Json(json!({ "count": fvs.len(), "feature_views": fvs }))
}

/// Governed Model Context Protocol (MCP) JSON-RPC 2.0 Endpoint (`POST /v1/mcp`, Wave 42):
/// Lets AI agents (Gemini, Claude, LangGraph, CrewAI) discover schemas, run SQL, execute
/// hybrid BM25+Vector RAG search, and inspect registered ML models while automatically
/// inheriting the caller's OIDC/JWT RBAC, RLS, Tag-ABAC PII masking, and durable audit trail!
async fn mcp_jsonrpc_http(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    Json(req): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let id = req.get("id").cloned().unwrap_or(json!(1));
    let fallback_user = headers
        .get("x-rustlakedb-user")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("postgres");
    let session = match resolve_http_session(&coord, &headers, fallback_user).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32001, "message": e }
                })),
            );
        }
    };

    let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or_else(|| json!({}));

    match method {
        "initialize" => (
            StatusCode::OK,
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": { "tools": {} },
                    "serverInfo": {
                        "name": "rustlakedb-mcp",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }
            })),
        ),
        "tools/list" => (
            StatusCode::OK,
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "tools": [
                        {
                            "name": "list_tables",
                            "description": "List all Apache Iceberg v3 tables and views in the RustLakeDB catalog.",
                            "inputSchema": { "type": "object", "properties": {} }
                        },
                        {
                            "name": "describe_table",
                            "description": "Inspect Iceberg schema, governance tags, file health, and snapshot count for a table.",
                            "inputSchema": {
                                "type": "object",
                                "properties": { "table": { "type": "string" } },
                                "required": ["table"]
                            }
                        },
                        {
                            "name": "query_sql",
                            "description": "Execute a governed SQL query inheriting caller RBAC, RLS, column grants, and Tag-ABAC PII masking.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "sql": { "type": "string" },
                                    "branch": { "type": "string" }
                                },
                                "required": ["sql"]
                            }
                        },
                        {
                            "name": "hybrid_search",
                            "description": "Run Reciprocal Rank Fusion (RRF) hybrid BM25 + Vector Cosine RAG search over a table.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "table": { "type": "string" },
                                    "text_column": { "type": "string" },
                                    "vector_column": { "type": "string" },
                                    "query_text": { "type": "string" },
                                    "query_vector": { "type": "string" },
                                    "limit": { "type": "integer" }
                                },
                                "required": ["table", "text_column", "vector_column", "query_text", "query_vector"]
                            }
                        },
                        {
                            "name": "list_models",
                            "description": "List registered ML models, lifecycle stages, metrics, and Iceberg training snapshot lineage.",
                            "inputSchema": { "type": "object", "properties": {} }
                        }
                    ]
                }
            })),
        ),
        "tools/call" => {
            let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            let tool_result: std::result::Result<Value, String> = match tool_name {
                "list_tables" => {
                    let tables = coord.catalog.list_tables().await.unwrap_or_default();
                    let views: Vec<String> = coord
                        .catalog
                        .list_views()
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .map(|v| v.name)
                        .collect();
                    Ok(json!({ "tables": tables, "views": views }))
                }
                "describe_table" => {
                    let tbl = args.get("table").and_then(|v| v.as_str()).unwrap_or("");
                    match coord.governance.check_access(&session, tbl, "SELECT") {
                        Err(e) => Err(e.to_string()),
                        Ok(()) => match coord.catalog.get_table(tbl).await {
                            Ok(meta) => Ok(json!({
                                "table": meta.table_name,
                                "tags": meta.tags,
                                "snapshots": meta.snapshots.len(),
                                "schema": meta.schema.iter().map(|f| json!({
                                    "name": f.name, "type": format!("{:?}", f.data_type), "nullable": f.nullable
                                })).collect::<Vec<_>>()
                            })),
                            Err(e) => Err(e.to_string()),
                        },
                    }
                }
                "query_sql" => {
                    let sql = args.get("sql").and_then(|v| v.as_str()).unwrap_or("");
                    let branch = args.get("branch").and_then(|v| v.as_str()).unwrap_or("main");
                    match coord.execute_sql_with_session(sql, branch, &session).await {
                        Ok(res) => {
                            let rows: Vec<Vec<Value>> = res
                                .batch
                                .rows
                                .iter()
                                .map(|r| {
                                    res.batch
                                        .columns
                                        .iter()
                                        .map(|c| {
                                            r.get(c)
                                                .and_then(|v| serde_json::to_value(v).ok())
                                                .unwrap_or(Value::Null)
                                        })
                                        .collect()
                                })
                                .collect();
                            Ok(json!({
                                "command_tag": res.command_tag,
                                "columns": res.batch.columns,
                                "rows": rows
                            }))
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
                "hybrid_search" => {
                    let tbl = args.get("table").and_then(|v| v.as_str()).unwrap_or("");
                    let tcol = args.get("text_column").and_then(|v| v.as_str()).unwrap_or("body");
                    let vcol = args.get("vector_column").and_then(|v| v.as_str()).unwrap_or("embedding");
                    let qtxt = args.get("query_text").and_then(|v| v.as_str()).unwrap_or("").replace('\'', "''");
                    let qvec = args.get("query_vector").and_then(|v| v.as_str()).unwrap_or("[0.0]");
                    let lim = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(5).clamp(1, 100);
                    let sql = format!(
                        "SELECT *, RRF(BM25_SCORE({tcol}, '{qtxt}'), VECTOR_COSINE({vcol}, '{qvec}')) AS rrf_score FROM {tbl} ORDER BY rrf_score DESC LIMIT {lim}"
                    );
                    match coord.execute_sql_with_session(&sql, "main", &session).await {
                        Ok(res) => Ok(json!({
                            "columns": res.batch.columns,
                            "row_count": res.batch.num_rows(),
                            "rows": res.batch.rows
                        })),
                        Err(e) => Err(e.to_string()),
                    }
                }
                "list_models" => {
                    if let Ok(gov) = coord.catalog.get_governance().await {
                        coord.governance.hydrate_from_catalog(gov);
                    }
                    Ok(json!({ "models": coord.governance.list_models() }))
                }
                other => Err(format!("unknown MCP tool: '{other}'")),
            };

            let outcome = if tool_result.is_ok() { "OK" } else { "DENIED/ERROR" };
            coord.governance.record_audit(
                &session.user,
                &format!("MCP:{tool_name}"),
                "mcp",
                outcome,
            );
            let _ = coord
                .catalog
                .apply_governance(rustlakedb_catalog::GovernanceOp::RecordAudit(
                    crate::security::make_persisted_audit(
                        &session,
                        &format!("MCP:{tool_name}"),
                        "mcp",
                        outcome,
                        "MCP",
                    ),
                ))
                .await;

            match tool_result {
                Ok(data) => (
                    StatusCode::OK,
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "content": [{ "type": "text", "text": serde_json::to_string(&data).unwrap_or_default() }],
                            "data": data,
                            "isError": false
                        }
                    })),
                ),
                Err(msg) => (
                    StatusCode::OK,
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "content": [{ "type": "text", "text": msg }],
                            "isError": true
                        }
                    })),
                ),
            }
        }
        other => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": format!("unsupported MCP method: '{other}'") }
            })),
        ),
    }
}

async fn web_ui() -> Html<&'static str> {
    Html(include_str!("../ui/console.html"))
}
