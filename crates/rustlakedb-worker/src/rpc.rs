// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::WorkerNode;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{RowBatch, ShuffleBlockLocation, TaskCompletionReport, TaskSpec};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Networked HTTP/JSON RPC Server wrapping a stateless `WorkerNode` for true multi-node clusters.
pub struct WorkerHttpServer {
    listener: TcpListener,
    worker: Arc<WorkerNode>,
}

impl WorkerHttpServer {
    pub async fn bind(addr: &str, worker: Arc<WorkerNode>) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, worker })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn serve(self) -> Result<()> {
        let app = Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/tasks/execute", post(execute_task))
            .route("/v1/shuffle/read", post(read_shuffle))
            .route("/v1/shuffle/read_bin", post(read_shuffle_bin))
            .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
            .with_state(self.worker);
        axum::serve(self.listener, app)
            .await
            .map_err(|e| RustLakeError::Execution(format!("worker RPC server: {e}")))
    }
}

async fn healthz(State(worker): State<Arc<WorkerNode>>) -> (StatusCode, Json<Value>) {
    let tasks = worker
        .tasks_executed
        .load(std::sync::atomic::Ordering::Relaxed);
    let spills = worker
        .memory_pool
        .spill_events
        .load(std::sync::atomic::Ordering::Relaxed);
    let mem_used = worker.memory_pool.used_bytes();
    let vec_rows = worker
        .vectorized_rows_processed
        .load(std::sync::atomic::Ordering::Relaxed);
    let cache_hits = worker
        .store()
        .cache_hits
        .load(std::sync::atomic::Ordering::Relaxed);
    let cache_misses = worker
        .store()
        .cache_misses
        .load(std::sync::atomic::Ordering::Relaxed);
    let obj_gets = worker
        .store()
        .object_store_gets
        .load(std::sync::atomic::Ordering::Relaxed);
    let zm_pruned = worker
        .store()
        .files_pruned_by_zonemap
        .load(std::sync::atomic::Ordering::Relaxed);
    if worker.is_healthy() {
        (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "worker_id": worker.id.0,
                "tasks_executed": tasks,
                "vectorized_rows_processed": vec_rows,
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "object_store_gets": obj_gets,
                "files_pruned_by_zonemap": zm_pruned,
                "spill_events": spills,
                "memory_used_bytes": mem_used
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unhealthy",
                "worker_id": worker.id.0,
                "tasks_executed": tasks
            })),
        )
    }
}

async fn execute_task(
    State(worker): State<Arc<WorkerNode>>,
    Json(task): Json<TaskSpec>,
) -> (StatusCode, Json<Value>) {
    match worker.execute_task(task).await {
        Ok(report) => (
            StatusCode::OK,
            Json(serde_json::to_value::<TaskCompletionReport>(report).unwrap()),
        ),
        Err(RustLakeError::WorkerFailed { worker_id, reason }) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "worker_failed", "worker_id": worker_id, "reason": reason })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "execution_error", "message": e.to_string() })),
        ),
    }
}

async fn read_shuffle(
    State(worker): State<Arc<WorkerNode>>,
    Json(loc): Json<ShuffleBlockLocation>,
) -> (StatusCode, Json<Value>) {
    match worker.read_shuffle_block(&loc).await {
        Ok(batch) => (
            StatusCode::OK,
            Json(serde_json::to_value::<RowBatch>(batch).unwrap()),
        ),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// Binary Arrow IPC + Zstd cross-node shuffle fetch endpoint (zero JSON serialization on the wire!).
async fn read_shuffle_bin(
    State(worker): State<Arc<WorkerNode>>,
    Json(loc): Json<ShuffleBlockLocation>,
) -> Response {
    match worker.read_shuffle_block_raw(&loc).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
