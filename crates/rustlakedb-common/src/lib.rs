// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Error)]
pub enum RustLakeError {
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("Planning error: {0}")]
    Plan(String),
    #[error("Catalog error: {0}")]
    Catalog(String),
    #[error("OCC conflict on table {table}: {reason}")]
    OccConflict { table: String, reason: String },
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Shuffle error: {0}")]
    Shuffle(String),
    #[error("Worker {worker_id} unavailable: {reason}")]
    WorkerFailed { worker_id: String, reason: String },
    #[error("Memory budget exceeded: requested {requested} bytes, available {available} bytes")]
    MemoryExhausted { requested: usize, available: usize },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Hierarchical bounded MemoryPool enforcing OOM protection and triggering operator NVMe spill (RFC-001).
#[derive(Debug)]
pub struct MemoryPool {
    limit_bytes: usize,
    used_bytes: std::sync::atomic::AtomicUsize,
    pub spill_events: std::sync::atomic::AtomicU64,
}

impl MemoryPool {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            limit_bytes,
            used_bytes: std::sync::atomic::AtomicUsize::new(0),
            spill_events: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation<'_>> {
        let mut cur = self.used_bytes.load(std::sync::atomic::Ordering::SeqCst);
        loop {
            if cur.saturating_add(bytes) > self.limit_bytes {
                return Err(RustLakeError::MemoryExhausted {
                    requested: bytes,
                    available: self.limit_bytes.saturating_sub(cur),
                });
            }
            match self.used_bytes.compare_exchange_weak(
                cur,
                cur + bytes,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Ok(MemoryReservation {
                        pool: self,
                        bytes,
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn record_spill(&self) {
        self.spill_events
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct MemoryReservation<'a> {
    pool: &'a MemoryPool,
    bytes: usize,
}

impl Drop for MemoryReservation<'_> {
    fn drop(&mut self) {
        self.pool
            .used_bytes
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

pub type Result<T> = std::result::Result<T, RustLakeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRole {
    Standalone,
    Local,
    Coordinator,
    Worker,
    Gateway,
    Maintainer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(pub Uuid);

impl QueryId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for QueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(pub String);

impl WorkerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskAttemptId {
    pub query_id: QueryId,
    pub stage_id: u32,
    pub partition_id: usize,
    pub attempt: u32,
}

impl fmt::Display for TaskAttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "q:{}/s:{}/p:{}/a:{}",
            self.query_id, self.stage_id, self.partition_id, self.attempt
        )
    }
}

/// Deterministic Rendezvous (Highest Random Weight) hashing for locality-aware
/// Iceberg Parquet split placement with optional load-cap spillover (RFC-001).
pub fn assign_split_rendezvous<'a>(
    split_key: &str,
    healthy_workers: &'a [WorkerId],
    current_loads: &HashMap<WorkerId, usize>,
    max_load_per_worker: usize,
) -> Option<&'a WorkerId> {
    if healthy_workers.is_empty() {
        return None;
    }

    let mut scored: Vec<(u64, &'a WorkerId)> = healthy_workers
        .iter()
        .map(|w| {
            let mut buf = Vec::with_capacity(w.0.len() + split_key.len() + 1);
            buf.extend_from_slice(w.0.as_bytes());
            buf.push(b':');
            buf.extend_from_slice(split_key.as_bytes());
            (xxh3_64(&buf), w)
        })
        .collect();

    scored.sort_by_key(|a| std::cmp::Reverse(a.0));

    for (_, worker) in &scored {
        let load = current_loads.get(*worker).copied().unwrap_or(0);
        if load < max_load_per_worker {
            return Some(*worker);
        }
    }

    Some(scored[0].1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendezvous_is_deterministic_and_respects_load_cap() {
        let workers = vec![
            WorkerId::new("w1"),
            WorkerId::new("w2"),
            WorkerId::new("w3"),
        ];
        let loads = HashMap::new();
        let first = assign_split_rendezvous("s3://lake/orders/part-0001.parquet#0", &workers, &loads, 10).unwrap();
        let second = assign_split_rendezvous("s3://lake/orders/part-0001.parquet#0", &workers, &loads, 10).unwrap();
        assert_eq!(first, second);

        let mut saturated = HashMap::new();
        saturated.insert(first.clone(), 10);
        let fallback = assign_split_rendezvous("s3://lake/orders/part-0001.parquet#0", &workers, &saturated, 10).unwrap();
        assert_ne!(first, fallback);
    }
}
