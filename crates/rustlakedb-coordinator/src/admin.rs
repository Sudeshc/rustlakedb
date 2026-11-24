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
        let app = build_control_plane_router(self.coord);
        axum::serve(self.listener, app)
            .await
            .map_err(|e| RustLakeError::Execution(format!("control plane server: {e}")))
    }
}

pub fn build_control_plane_router(coord: Arc<Coordinator>) -> Router {
    let iceberg_rest = rustlakedb_catalog::rest::build_router(coord.catalog.clone());
    Router::new()
        .route("/", get(web_ui))
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/cluster/status", get(cluster_status))
        .route("/v1/workers/register", post(register_worker_http))
        .route("/v1/workers/heartbeat", post(register_worker_http))
        .route("/v1/sql", post(sql_json))
        .route("/v1/sql/arrow", post(sql_arrow))
        .route("/v1/ingest/{table}", post(ingest_ndjson_http))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(coord)
        .merge(iceberg_rest)
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

fn resolve_http_session(
    coord: &Coordinator,
    headers: &HeaderMap,
    fallback_user: &str,
) -> std::result::Result<SessionContext, String> {
    if let Some(secret) = coord.governance.jwt_secret() {
        let auth = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| "missing Authorization: Bearer <jwt> header".to_string())?;
        let token = auth
            .strip_prefix("Bearer ")
            .ok_or_else(|| "Authorization header must start with 'Bearer '".to_string())?;
        return verify_jwt_hs256(&secret, token).map_err(|e| e.to_string());
    }
    Ok(SessionContext::for_user(fallback_user))
}

async fn sql_json(
    State(coord): State<Arc<Coordinator>>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> (StatusCode, Json<Value>) {
    let session = match resolve_http_session(&coord, &headers, &req.user) {
        Ok(s) => s,
        Err(e) => return (StatusCode::UNAUTHORIZED, Json(json!({ "error": e }))),
    };
    let t0 = std::time::Instant::now();
    match coord
        .execute_sql_with_session(&req.sql, &req.branch, &session)
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
            (
                StatusCode::OK,
                Json(json!({
                    "command_tag": res.command_tag,
                    "columns": res.batch.columns,
                    "rows": rows_json,
                    "row_count": res.batch.num_rows(),
                    "elapsed_ms": elapsed_ms,
                    "stages_executed": res.stats.stages_executed,
                    "tasks_executed": res.stats.tasks_executed
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
    let session = match resolve_http_session(&coord, &headers, &req.user) {
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
    Json(json!({
        "engine": "RustLakeDB",
        "version": env!("CARGO_PKG_VERSION"),
        "workers": workers,
        "tables": tables,
        "views": views,
        "active_queries": coord.governance.active_queries_snapshot(),
        "recent_queries": history.into_iter().rev().take(20).collect::<Vec<_>>(),
        "storage": {
            "parquet_files_written": coord.store.parquet_files_written.load(Ordering::Relaxed),
            "parquet_files_read": coord.store.parquet_files_read.load(Ordering::Relaxed),
            "cache_hits": coord.store.cache_hits.load(Ordering::Relaxed),
            "files_pruned_by_puffin": coord.store.files_pruned_by_puffin.load(Ordering::Relaxed),
            "files_pruned_by_zonemap": coord.store.files_pruned_by_zonemap.load(Ordering::Relaxed)
        }
    }))
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
    let pq_read = coord.store.parquet_files_read.load(Ordering::Relaxed);
    let pq_written = coord.store.parquet_files_written.load(Ordering::Relaxed);
    let cache_hits = coord.store.cache_hits.load(Ordering::Relaxed);
    let puffin_pruned = coord.store.files_pruned_by_puffin.load(Ordering::Relaxed);
    let zm_pruned = coord.store.files_pruned_by_zonemap.load(Ordering::Relaxed);

    let body = format!(
        "# HELP rustlakedb_queries_total Total SQL queries executed\n\
         # TYPE rustlakedb_queries_total counter\n\
         rustlakedb_queries_total {queries}\n\
         # HELP rustlakedb_query_errors_total Total failed SQL queries\n\
         # TYPE rustlakedb_query_errors_total counter\n\
         rustlakedb_query_errors_total {errors}\n\
         # HELP rustlakedb_query_duration_ms_sum Cumulative query latency in milliseconds\n\
         # TYPE rustlakedb_query_duration_ms_sum counter\n\
         rustlakedb_query_duration_ms_sum {dur_ms}\n\
         # HELP rustlakedb_workers_healthy Number of healthy distributed workers\n\
         # TYPE rustlakedb_workers_healthy gauge\n\
         rustlakedb_workers_healthy {healthy_workers}\n\
         # HELP rustlakedb_workers_total Total registered workers\n\
         # TYPE rustlakedb_workers_total gauge\n\
         rustlakedb_workers_total {total_workers}\n\
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
         rustlakedb_zonemap_pruned_total {zm_pruned}\n"
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn web_ui() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"/><title>RustLakeDB — Distributed Lakehouse Console</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:24px}
h1{margin:0 0 8px;color:#58a6ff} .sub{color:#8b949e;margin-bottom:20px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
textarea{width:100%;height:90px;background:#0d1117;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:8px;font-family:monospace;font-size:13px}
button{background:#238636;color:#fff;border:none;border-radius:6px;padding:8px 16px;cursor:pointer;margin-top:8px;font-weight:600}
table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px}
th,td{border:1px solid #30363d;padding:6px 8px;text-align:left} th{background:#21262d}
pre{background:#0d1117;padding:10px;border-radius:6px;overflow:auto;font-size:12px}
</style></head>
<body>
<h1>RustLakeDB Lakehouse Console</h1>
<div class="sub">100% Rust Distributed SQL &amp; Apache Iceberg v3 Engine &bull; PG v3 Wire &bull; Arrow IPC &bull; Prometheus <code>/metrics</code></div>
<div class="grid">
  <div class="card"><h3>Cluster &amp; Catalog Status</h3><pre id="status">Loading...</pre></div>
  <div class="card"><h3>Interactive SQL Workbench</h3>
    <textarea id="sql">SELECT region, COUNT(*) AS orders, SUM(amount) AS total_rev FROM public.big_orders GROUP BY region ORDER BY region;</textarea><br/>
    <button onclick="runSql()">Run Distributed SQL</button>
    <div id="meta" style="margin-top:8px;color:#8b949e"></div>
  </div>
</div>
<div class="card"><h3>Query Result</h3><div id="out">Run a query above to inspect results.</div></div>
<script>
async function refresh(){const r=await fetch('/v1/cluster/status');document.getElementById('status').textContent=JSON.stringify(await r.json(),null,2);}
async function runSql(){
  const sql=document.getElementById('sql').value;
  const r=await fetch('/v1/sql',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql})});
  const d=await r.json();
  if(d.error){document.getElementById('meta').textContent='ERROR: '+d.error;return;}
  document.getElementById('meta').textContent=d.command_tag+' ('+d.elapsed_ms+' ms, stages='+d.stages_executed+', tasks='+d.tasks_executed+')';
  let h='<table><thead><tr>'+d.columns.map(c=>'<th>'+c+'</th>').join('')+'</tr></thead><tbody>';
  for(const row of d.rows){h+='<tr>'+row.map(v=>'<td>'+(v===null?'NULL':typeof v==='object'?JSON.stringify(v):v)+'</td>').join('')+'</tr>';}
  document.getElementById('out').innerHTML=h+'</tbody></table>';
  refresh();
}
refresh();
</script></body></html>"#,
    )
}
