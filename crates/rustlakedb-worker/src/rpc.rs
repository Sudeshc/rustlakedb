// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::WorkerNode;
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{RowBatch, ShuffleBlockLocation, TaskCompletionReport, TaskSpec};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use serde_json::{json, Value};
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::Service;

/// Builds a strict mutual-TLS (mTLS) `TlsAcceptor` requiring client certificates signed by `ca_pem` (Wave 16).
pub fn build_mtls_server_acceptor(
    ca_pem: &[u8],
    server_cert_pem: &[u8],
    server_key_pem: &[u8],
) -> Result<TlsAcceptor> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let mut ca_reader = BufReader::new(ca_pem);
    let ca_certs: Vec<_> = rustls_pemfile::certs(&mut ca_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| RustLakeError::Execution(format!("parse mTLS CA PEM: {e}")))?;
    if ca_certs.is_empty() {
        return Err(RustLakeError::Execution(
            "mTLS CA PEM contains no certificates".into(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for cert in ca_certs {
        roots
            .add(cert)
            .map_err(|e| RustLakeError::Execution(format!("add mTLS CA root: {e}")))?;
    }

    let client_verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| RustLakeError::Execution(format!("build mTLS client verifier: {e}")))?;

    let mut cert_reader = BufReader::new(server_cert_pem);
    let cert_chain: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| RustLakeError::Execution(format!("parse mTLS server cert PEM: {e}")))?;
    if cert_chain.is_empty() {
        return Err(RustLakeError::Execution(
            "mTLS server cert PEM contains no certificates".into(),
        ));
    }

    let mut key_reader = BufReader::new(server_key_pem);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| RustLakeError::Execution(format!("parse mTLS server key PEM: {e}")))?
        .ok_or_else(|| RustLakeError::Execution("mTLS server key PEM has no private key".into()))?;

    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| RustLakeError::Execution(format!("mTLS TLS versions: {e}")))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, key)
        .map_err(|e| RustLakeError::Execution(format!("mTLS server config: {e}")))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Builds a strict mTLS `TlsAcceptor` from PEM files on disk (`RUSTLAKEDB_MTLS_CA`, `CERT`, `KEY`).
pub fn build_mtls_server_acceptor_from_files(
    ca_path: impl AsRef<Path>,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<TlsAcceptor> {
    let ca = std::fs::read(ca_path.as_ref())
        .map_err(|e| RustLakeError::Execution(format!("read mTLS CA file: {e}")))?;
    let cert = std::fs::read(cert_path.as_ref())
        .map_err(|e| RustLakeError::Execution(format!("read mTLS server cert file: {e}")))?;
    let key = std::fs::read(key_path.as_ref())
        .map_err(|e| RustLakeError::Execution(format!("read mTLS server key file: {e}")))?;
    build_mtls_server_acceptor(&ca, &cert, &key)
}

/// Networked HTTP/HTTPS(mTLS) RPC Server wrapping a stateless `WorkerNode` for multi-node clusters.
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

    fn router(&self) -> Router {
        Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/tasks/execute", post(execute_task))
            .route("/v1/shuffle/read", post(read_shuffle))
            .route("/v1/shuffle/read_bin", post(read_shuffle_bin))
            .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
            .with_state(self.worker.clone())
    }

    pub async fn serve(self) -> Result<()> {
        let app = self.router();
        axum::serve(self.listener, app)
            .await
            .map_err(|e| RustLakeError::Execution(format!("worker RPC server: {e}")))
    }

    /// Serves the Worker RPC router over strict mutual TLS (mTLS), rejecting any peer without
    /// a valid client certificate signed by the cluster CA (Wave 16).
    pub async fn serve_mtls(self, acceptor: TlsAcceptor) -> Result<()> {
        let app = self.router();
        loop {
            let (tcp_stream, peer_addr) = self.listener.accept().await?;
            let acceptor = acceptor.clone();
            let app = app.clone();
            tokio::spawn(async move {
                let tls_stream = match acceptor.accept(tcp_stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(%peer_addr, error = %e, "rejected worker mTLS connection");
                        return;
                    }
                };
                let io = TokioIo::new(tls_stream);
                let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    let mut router = app.clone();
                    async move { router.call(req.map(Body::new)).await }
                });
                if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                    tracing::debug!(%peer_addr, error = %e, "worker mTLS HTTP connection closed");
                }
            });
        }
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
