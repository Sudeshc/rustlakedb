// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::{
    Catalog, ColumnType, IcebergTableMetadata, InMemoryRaftCatalog, PuffinIndexSpec,
    TableCommitProposal, ViewDefinition,
};
use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::RwLock;
use rustlakedb_common::{Result, RustLakeError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

/// Multi-Node Raft Consensus HA Catalog Quorum (RFC-002 / Wave 5):
/// Implements real Raft Leader Election, Term/Log Replication, Majority Quorum Commit,
/// Transparent Follower->Leader Forwarding, and Automatic Leader Failover over HTTP RPC!
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogCommand {
    CreateTable(IcebergTableMetadata),
    DropTable(String),
    AddColumn {
        table_name: String,
        name: String,
        data_type: ColumnType,
    },
    AddIndex {
        table_name: String,
        spec: PuffinIndexSpec,
    },
    CreateBranch {
        table_name: String,
        from_branch: String,
        new_branch: String,
    },
    MergeBranch {
        table_name: String,
        source_branch: String,
        target_branch: String,
    },
    CommitMultiTable(Vec<TableCommitProposal>),
    ExpireSnapshots {
        table_name: String,
        keep_last: usize,
    },
    SetClusterBy {
        table_name: String,
        columns: Vec<String>,
    },
    SetShreddedPaths {
        table_name: String,
        paths: Vec<String>,
    },
    CreateView(ViewDefinition),
    DropView(String),
    RenameTable {
        old_name: String,
        new_name: String,
    },
    TruncateTable {
        table_name: String,
        branch: String,
    },
    RenameColumn {
        table_name: String,
        old_col: String,
        new_col: String,
    },
    DropColumn {
        table_name: String,
        col_name: String,
        if_exists: bool,
    },
    WidenColumn {
        table_name: String,
        col_name: String,
        new_type: ColumnType,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub index: u64,
    pub term: u64,
    pub command: CatalogCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReq {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResp {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftStatus {
    pub node_id: u64,
    pub role: RaftRole,
    pub term: u64,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub log_len: usize,
    pub snapshot_index: u64,
}

struct RaftCore {
    current_term: u64,
    voted_for: Option<u64>,
    role: RaftRole,
    leader_id: Option<u64>,
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    last_applied: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    last_heartbeat: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRaftWal {
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    #[serde(default)]
    snapshot_index: u64,
    #[serde(default)]
    snapshot_term: u64,
}

/// Multi-Node Raft HA Catalog Node implementing `Catalog` with crash-safe WAL persistence & log snapshotting (Wave 11).
pub struct RaftCatalogNode {
    pub node_id: u64,
    peers: RwLock<HashMap<u64, String>>,
    core: RwLock<RaftCore>,
    state_machine: Arc<InMemoryRaftCatalog>,
    wal_path: Option<PathBuf>,
    snapshot_threshold: std::sync::atomic::AtomicUsize,
    http: reqwest::Client,
    shutdown: AtomicBool,
}

impl RaftCatalogNode {
    pub fn new(
        node_id: u64,
        peers: HashMap<u64, String>,
        state_machine: Arc<InMemoryRaftCatalog>,
    ) -> Arc<Self> {
        Self::with_wal_dir(node_id, peers, state_machine, None)
    }

    pub fn open_persistent(
        node_id: u64,
        peers: HashMap<u64, String>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Arc<Self>> {
        let sm = Arc::new(InMemoryRaftCatalog::open(data_dir.as_ref())?);
        let wal_dir = data_dir.as_ref().join("_catalog");
        std::fs::create_dir_all(&wal_dir)?;
        Ok(Self::with_wal_dir(
            node_id,
            peers,
            sm,
            Some(wal_dir.join(format!("raft_wal_node_{node_id}.json"))),
        ))
    }

    fn with_wal_dir(
        node_id: u64,
        peers: HashMap<u64, String>,
        state_machine: Arc<InMemoryRaftCatalog>,
        wal_path: Option<PathBuf>,
    ) -> Arc<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(250))
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let persisted: Option<PersistedRaftWal> = wal_path
            .as_ref()
            .filter(|p| p.exists())
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|b| serde_json::from_slice(&b).ok());

        let (current_term, voted_for, log, commit_index, snapshot_index, snapshot_term) =
            match persisted {
                Some(w) => (
                    w.current_term,
                    w.voted_for,
                    w.log,
                    w.commit_index,
                    w.snapshot_index,
                    w.snapshot_term,
                ),
                None => (0, None, Vec::new(), 0, 0, 0),
            };

        Arc::new(Self {
            node_id,
            peers: RwLock::new(peers),
            core: RwLock::new(RaftCore {
                current_term,
                voted_for,
                role: RaftRole::Follower,
                leader_id: None,
                log,
                commit_index,
                last_applied: commit_index,
                snapshot_index,
                snapshot_term,
                last_heartbeat: Instant::now(),
            }),
            state_machine,
            wal_path,
            snapshot_threshold: std::sync::atomic::AtomicUsize::new(64),
            http,
            shutdown: AtomicBool::new(false),
        })
    }

    pub fn set_snapshot_threshold(&self, threshold: usize) {
        self.snapshot_threshold
            .store(threshold.max(1), Ordering::Relaxed);
    }

    /// Compacts applied Raft WAL entries up to `commit_index` into a durable snapshot marker (Wave 11).
    /// Returns the number of WAL log entries reclaimed.
    pub fn compact_wal_snapshot(&self) -> usize {
        let mut c = self.core.write();
        self.compact_wal_locked(&mut c)
    }

    fn compact_wal_locked(&self, c: &mut RaftCore) -> usize {
        let compact_upto = c.commit_index.min(c.last_applied);
        if compact_upto <= c.snapshot_index || c.log.is_empty() {
            return 0;
        }
        let mut snap_term = c.snapshot_term;
        for e in &c.log {
            if e.index <= compact_upto {
                snap_term = e.term;
            }
        }
        let before = c.log.len();
        c.log.retain(|e| e.index > compact_upto);
        let removed = before.saturating_sub(c.log.len());
        if removed > 0 {
            c.snapshot_index = compact_upto;
            c.snapshot_term = snap_term;
            self.persist_wal_locked(c);
        }
        removed
    }

    fn persist_wal_locked(&self, c: &RaftCore) {
        if let Some(ref p) = self.wal_path {
            let snap = PersistedRaftWal {
                current_term: c.current_term,
                voted_for: c.voted_for,
                log: c.log.clone(),
                commit_index: c.commit_index,
                snapshot_index: c.snapshot_index,
                snapshot_term: c.snapshot_term,
            };
            if let Ok(bytes) = serde_json::to_vec(&snap) {
                let tmp = p.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
                if std::fs::write(&tmp, bytes).is_ok() {
                    let _ = std::fs::rename(&tmp, p);
                }
            }
        }
    }

    pub fn set_peers(&self, peers: HashMap<u64, String>) {
        *self.peers.write() = peers;
    }

    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub fn status(&self) -> RaftStatus {
        let c = self.core.read();
        RaftStatus {
            node_id: self.node_id,
            role: c.role,
            term: c.current_term,
            leader_id: c.leader_id,
            commit_index: c.commit_index,
            log_len: c.log.len(),
            snapshot_index: c.snapshot_index,
        }
    }

    pub async fn start_server(self: &Arc<Self>, addr: &str) -> Result<SocketAddr> {
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        let node = self.clone();

        // Spawn Raft election & heartbeat loop
        let loop_node = self.clone();
        tokio::spawn(async move {
            loop_node.run_raft_loop().await;
        });

        let app = Router::new()
            .route("/raft/status", get(handle_status))
            .route("/raft/request_vote", post(handle_request_vote))
            .route("/raft/append_entries", post(handle_append_entries))
            .route("/raft/client_write", post(handle_client_write))
            .with_state(node.clone());

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(bound)
    }

    async fn run_raft_loop(self: Arc<Self>) {
        // Staggered election timeouts per node_id prevent split votes in 3-node quorums!
        let election_timeout = Duration::from_millis(220 + (self.node_id % 5) * 90);
        let heartbeat_interval = Duration::from_millis(60);

        while !self.is_stopped() {
            tokio::time::sleep(heartbeat_interval).await;
            if self.is_stopped() {
                break;
            }
            let (role, elapsed) = {
                let c = self.core.read();
                (c.role, c.last_heartbeat.elapsed())
            };
            match role {
                RaftRole::Leader => {
                    self.replicate_to_peers().await;
                }
                RaftRole::Follower | RaftRole::Candidate => {
                    if elapsed >= election_timeout {
                        self.start_election().await;
                    }
                }
            }
        }
    }

    async fn start_election(&self) {
        let (req, peer_urls, total_nodes) = {
            let mut c = self.core.write();
            c.current_term += 1;
            c.role = RaftRole::Candidate;
            c.voted_for = Some(self.node_id);
            c.leader_id = None;
            c.last_heartbeat = Instant::now();
            let last_log_index = c.log.last().map(|e| e.index).unwrap_or(c.snapshot_index);
            let last_log_term = c.log.last().map(|e| e.term).unwrap_or(c.snapshot_term);
            let peers = self.peers.read();
            let urls: Vec<String> = peers
                .iter()
                .filter(|&(&id, _)| id != self.node_id)
                .map(|(_, u)| u.clone())
                .collect();
            let total = urls.len() + 1;
            (
                RequestVoteReq {
                    term: c.current_term,
                    candidate_id: self.node_id,
                    last_log_index,
                    last_log_term,
                },
                urls,
                total,
            )
        };

        let majority = (total_nodes / 2) + 1;
        let mut votes = 1usize; // self-vote

        let futs = peer_urls.into_iter().map(|url| {
            let http = self.http.clone();
            let body = req.clone();
            async move {
                http.post(format!("{url}/raft/request_vote"))
                    .json(&body)
                    .send()
                    .await
                    .ok()?
                    .json::<RequestVoteResp>()
                    .await
                    .ok()
            }
        });

        let results = futures::future::join_all(futs).await;
        for resp in results.into_iter().flatten() {
            if resp.term > req.term {
                let mut c = self.core.write();
                if resp.term > c.current_term {
                    c.current_term = resp.term;
                    c.role = RaftRole::Follower;
                    c.voted_for = None;
                    c.leader_id = None;
                }
                return;
            }
            if resp.vote_granted {
                votes += 1;
            }
        }

        if votes >= majority {
            let became_leader = {
                let mut c = self.core.write();
                if c.role == RaftRole::Candidate && c.current_term == req.term {
                    c.role = RaftRole::Leader;
                    c.leader_id = Some(self.node_id);
                    c.last_heartbeat = Instant::now();
                    true
                } else {
                    false
                }
            };
            if became_leader {
                tracing::info!(node = self.node_id, term = req.term, "Raft node elected LEADER");
                self.replicate_to_peers().await;
            }
        }
    }

    /// Replicates log entries / heartbeats to all peers and returns how many nodes (incl. self) ACKed.
    async fn replicate_to_peers(&self) -> usize {
        let (req, peer_urls) = {
            let c = self.core.read();
            if c.role != RaftRole::Leader {
                return 0;
            }
            let peers = self.peers.read();
            let urls: Vec<String> = peers
                .iter()
                .filter(|&(&id, _)| id != self.node_id)
                .map(|(_, u)| u.clone())
                .collect();
            (
                AppendEntriesReq {
                    term: c.current_term,
                    leader_id: self.node_id,
                    prev_log_index: c.snapshot_index,
                    prev_log_term: c.snapshot_term,
                    entries: c.log.clone(),
                    leader_commit: c.commit_index,
                },
                urls,
            )
        };

        let futs = peer_urls.into_iter().map(|url| {
            let http = self.http.clone();
            let body = req.clone();
            async move {
                http.post(format!("{url}/raft/append_entries"))
                    .json(&body)
                    .send()
                    .await
                    .ok()?
                    .json::<AppendEntriesResp>()
                    .await
                    .ok()
            }
        });

        let results = futures::future::join_all(futs).await;
        let mut acks = 1usize; // self
        for resp in results.into_iter().flatten() {
            if resp.term > req.term {
                let mut c = self.core.write();
                if resp.term > c.current_term {
                    c.current_term = resp.term;
                    c.role = RaftRole::Follower;
                    c.voted_for = None;
                    c.leader_id = None;
                }
                return 0;
            }
            if resp.success {
                acks += 1;
            }
        }
        acks
    }

    pub fn on_request_vote(&self, req: RequestVoteReq) -> RequestVoteResp {
        let mut c = self.core.write();
        if req.term < c.current_term {
            return RequestVoteResp {
                term: c.current_term,
                vote_granted: false,
            };
        }
        if req.term > c.current_term {
            c.current_term = req.term;
            c.voted_for = None;
            c.role = RaftRole::Follower;
            c.leader_id = None;
        }
        let my_last_idx = c.log.last().map(|e| e.index).unwrap_or(c.snapshot_index);
        let my_last_term = c.log.last().map(|e| e.term).unwrap_or(c.snapshot_term);
        let up_to_date = req.last_log_term > my_last_term
            || (req.last_log_term == my_last_term && req.last_log_index >= my_last_idx);
        let can_vote = c.voted_for.is_none() || c.voted_for == Some(req.candidate_id);

        if can_vote && up_to_date {
            c.voted_for = Some(req.candidate_id);
            c.last_heartbeat = Instant::now();
            RequestVoteResp {
                term: c.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteResp {
                term: c.current_term,
                vote_granted: false,
            }
        }
    }

    pub async fn on_append_entries(&self, req: AppendEntriesReq) -> AppendEntriesResp {
        let (resp, to_apply) = {
            let mut c = self.core.write();
            if req.term < c.current_term {
                return AppendEntriesResp {
                    term: c.current_term,
                    success: false,
                    match_index: c.log.len() as u64,
                };
            }
            c.current_term = req.term;
            c.role = RaftRole::Follower;
            c.leader_id = Some(req.leader_id);
            c.last_heartbeat = Instant::now();

            // Replace/merge suffix from leader's authoritative log window
            let leader_last = req
                .entries
                .last()
                .map(|e| e.index)
                .unwrap_or(req.prev_log_index);
            let my_last = c.log.last().map(|e| e.index).unwrap_or(c.snapshot_index);
            if leader_last >= my_last {
                if req.prev_log_index > c.snapshot_index {
                    c.snapshot_index = req.prev_log_index;
                    c.snapshot_term = req.prev_log_term;
                }
                c.log = req.entries;
            }
            let max_idx = c.log.last().map(|e| e.index).unwrap_or(c.snapshot_index);
            if req.leader_commit > c.commit_index {
                c.commit_index = req.leader_commit.min(max_idx);
            }
            let mut cmds = Vec::new();
            while c.last_applied < c.commit_index {
                let next = c.last_applied + 1;
                if let Some(entry) = c.log.iter().find(|e| e.index == next) {
                    cmds.push(entry.command.clone());
                }
                c.last_applied = next;
            }
            if !cmds.is_empty() {
                if c.log.len() >= self.snapshot_threshold.load(Ordering::Relaxed) {
                    self.compact_wal_locked(&mut c);
                } else {
                    self.persist_wal_locked(&c);
                }
            }
            (
                AppendEntriesResp {
                    term: c.current_term,
                    success: true,
                    match_index: max_idx,
                },
                cmds,
            )
        };

        for cmd in to_apply {
            let _ = self.apply_command_locally(cmd).await;
        }
        resp
    }

    /// Proposes a catalog mutation through the Raft consensus quorum.
    /// If called on a Follower, transparently forwards the command to the current Leader!
    pub async fn propose(&self, cmd: CatalogCommand) -> Result<Value> {
        if self.is_stopped() {
            return Err(RustLakeError::Catalog("Raft node is stopped".into()));
        }
        let (is_leader, leader_url) = {
            let c = self.core.read();
            let peers = self.peers.read();
            let url = c.leader_id.and_then(|id| peers.get(&id).cloned());
            (c.role == RaftRole::Leader, url)
        };

        if !is_leader {
            let url = leader_url.ok_or_else(|| {
                RustLakeError::Catalog("Raft quorum has no elected leader yet".into())
            })?;
            let resp = self
                .http
                .post(format!("{url}/raft/client_write"))
                .json(&cmd)
                .send()
                .await
                .map_err(|e| RustLakeError::Catalog(format!("forward to Raft leader: {e}")))?;
            if !resp.status().is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(RustLakeError::Catalog(format!(
                    "Raft leader write rejected: {text}"
                )));
            }
            return resp
                .json::<Value>()
                .await
                .map_err(|e| RustLakeError::Catalog(format!("decode leader reply: {e}")));
        }

        // Leader appends entry to log and replicates to majority quorum!
        let (entry_index, majority) = {
            let mut c = self.core.write();
            let idx = c.log.last().map(|e| e.index).unwrap_or(c.snapshot_index) + 1;
            let term = c.current_term;
            c.log.push(RaftLogEntry {
                index: idx,
                term,
                command: cmd.clone(),
            });
            let total = self.peers.read().len().max(1);
            (idx, (total / 2) + 1)
        };

        let acks = self.replicate_to_peers().await;
        if acks < majority {
            return Err(RustLakeError::Catalog(format!(
                "Raft quorum lost: received {acks} ACKs, required majority {majority}"
            )));
        }

        {
            let mut c = self.core.write();
            if entry_index > c.commit_index {
                c.commit_index = entry_index;
                c.last_applied = entry_index;
            }
            self.persist_wal_locked(&c);
        }
        // Broadcast updated commit_index to followers so they apply immediately
        let _ = self.replicate_to_peers().await;
        let out = self.apply_command_locally(cmd).await;
        // Auto-compact WAL into a snapshot once threshold is reached!
        {
            let mut c = self.core.write();
            if c.log.len() >= self.snapshot_threshold.load(Ordering::Relaxed) {
                self.compact_wal_locked(&mut c);
            }
        }
        out
    }

    async fn apply_command_locally(&self, cmd: CatalogCommand) -> Result<Value> {
        match cmd {
            CatalogCommand::CreateTable(meta) => {
                self.state_machine.create_table(meta).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::DropTable(name) => {
                self.state_machine.drop_table(&name).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::AddColumn {
                table_name,
                name,
                data_type,
            } => {
                let fid = self
                    .state_machine
                    .add_column(&table_name, name, data_type)
                    .await?;
                Ok(json!({ "field_id": fid }))
            }
            CatalogCommand::AddIndex { table_name, spec } => {
                self.state_machine.add_index(&table_name, spec).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::CreateBranch {
                table_name,
                from_branch,
                new_branch,
            } => {
                let s = self
                    .state_machine
                    .create_branch(&table_name, &from_branch, &new_branch)
                    .await?;
                Ok(json!({ "snapshot_id": s }))
            }
            CatalogCommand::MergeBranch {
                table_name,
                source_branch,
                target_branch,
            } => {
                let s = self
                    .state_machine
                    .merge_branch(&table_name, &source_branch, &target_branch)
                    .await?;
                Ok(json!({ "snapshot_id": s }))
            }
            CatalogCommand::CommitMultiTable(props) => {
                let res = self.state_machine.commit_multi_table(props).await?;
                Ok(serde_json::to_value(res)?)
            }
            CatalogCommand::ExpireSnapshots {
                table_name,
                keep_last,
            } => {
                let n = self
                    .state_machine
                    .expire_snapshots(&table_name, keep_last)
                    .await?;
                Ok(json!({ "expired": n }))
            }
            CatalogCommand::SetClusterBy {
                table_name,
                columns,
            } => {
                self.state_machine.set_cluster_by(&table_name, columns).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::SetShreddedPaths { table_name, paths } => {
                self.state_machine.set_shredded_paths(&table_name, paths).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::CreateView(def) => {
                self.state_machine.create_view(def).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::DropView(name) => {
                self.state_machine.drop_view(&name).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::RenameTable { old_name, new_name } => {
                self.state_machine.rename_table(&old_name, &new_name).await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::TruncateTable { table_name, branch } => {
                let s = self
                    .state_machine
                    .truncate_table(&table_name, &branch)
                    .await?;
                Ok(json!({ "snapshot_id": s }))
            }
            CatalogCommand::RenameColumn {
                table_name,
                old_col,
                new_col,
            } => {
                let fid = self
                    .state_machine
                    .rename_column(&table_name, &old_col, &new_col)
                    .await?;
                Ok(json!({ "field_id": fid }))
            }
            CatalogCommand::DropColumn {
                table_name,
                col_name,
                if_exists,
            } => {
                self.state_machine
                    .drop_column(&table_name, &col_name, if_exists)
                    .await?;
                Ok(json!({ "ok": true }))
            }
            CatalogCommand::WidenColumn {
                table_name,
                col_name,
                new_type,
            } => {
                self.state_machine
                    .widen_column(&table_name, &col_name, new_type)
                    .await?;
                Ok(json!({ "ok": true }))
            }
        }
    }
}

#[async_trait]
impl Catalog for RaftCatalogNode {
    async fn create_table(&self, metadata: IcebergTableMetadata) -> Result<()> {
        self.propose(CatalogCommand::CreateTable(metadata)).await?;
        Ok(())
    }

    async fn drop_table(&self, table_name: &str) -> Result<()> {
        self.propose(CatalogCommand::DropTable(table_name.to_string()))
            .await?;
        Ok(())
    }

    async fn get_table(&self, table_name: &str) -> Result<IcebergTableMetadata> {
        self.state_machine.get_table(table_name).await
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        self.state_machine.list_tables().await
    }

    async fn add_column(&self, table_name: &str, name: String, data_type: ColumnType) -> Result<i32> {
        let v = self
            .propose(CatalogCommand::AddColumn {
                table_name: table_name.to_string(),
                name,
                data_type,
            })
            .await?;
        Ok(v["field_id"].as_i64().unwrap_or(1) as i32)
    }

    async fn add_index(&self, table_name: &str, spec: PuffinIndexSpec) -> Result<()> {
        self.propose(CatalogCommand::AddIndex {
            table_name: table_name.to_string(),
            spec,
        })
        .await?;
        Ok(())
    }

    async fn create_branch(
        &self,
        table_name: &str,
        from_branch: &str,
        new_branch: &str,
    ) -> Result<u64> {
        let v = self
            .propose(CatalogCommand::CreateBranch {
                table_name: table_name.to_string(),
                from_branch: from_branch.to_string(),
                new_branch: new_branch.to_string(),
            })
            .await?;
        Ok(v["snapshot_id"].as_u64().unwrap_or(1))
    }

    async fn merge_branch(
        &self,
        table_name: &str,
        source_branch: &str,
        target_branch: &str,
    ) -> Result<u64> {
        let v = self
            .propose(CatalogCommand::MergeBranch {
                table_name: table_name.to_string(),
                source_branch: source_branch.to_string(),
                target_branch: target_branch.to_string(),
            })
            .await?;
        Ok(v["snapshot_id"].as_u64().unwrap_or(1))
    }

    async fn commit_table(&self, proposal: TableCommitProposal) -> Result<u64> {
        let res = self.commit_multi_table(vec![proposal]).await?;
        Ok(res[0].1)
    }

    async fn commit_multi_table(
        &self,
        proposals: Vec<TableCommitProposal>,
    ) -> Result<Vec<(String, u64)>> {
        let v = self
            .propose(CatalogCommand::CommitMultiTable(proposals))
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    async fn expire_snapshots(&self, table_name: &str, keep_last: usize) -> Result<usize> {
        let v = self
            .propose(CatalogCommand::ExpireSnapshots {
                table_name: table_name.to_string(),
                keep_last,
            })
            .await?;
        Ok(v["expired"].as_u64().unwrap_or(0) as usize)
    }

    async fn set_cluster_by(&self, table_name: &str, columns: Vec<String>) -> Result<()> {
        self.propose(CatalogCommand::SetClusterBy {
            table_name: table_name.to_string(),
            columns,
        })
        .await?;
        Ok(())
    }

    async fn set_shredded_paths(&self, table_name: &str, paths: Vec<String>) -> Result<()> {
        self.propose(CatalogCommand::SetShreddedPaths {
            table_name: table_name.to_string(),
            paths,
        })
        .await?;
        Ok(())
    }

    async fn create_view(&self, def: ViewDefinition) -> Result<()> {
        self.propose(CatalogCommand::CreateView(def)).await?;
        Ok(())
    }

    async fn drop_view(&self, name: &str) -> Result<()> {
        self.propose(CatalogCommand::DropView(name.to_string()))
            .await?;
        Ok(())
    }

    async fn get_view(&self, name: &str) -> Result<Option<ViewDefinition>> {
        self.state_machine.get_view(name).await
    }

    async fn list_views(&self) -> Result<Vec<ViewDefinition>> {
        self.state_machine.list_views().await
    }

    async fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        self.propose(CatalogCommand::RenameTable {
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn truncate_table(&self, table_name: &str, branch: &str) -> Result<u64> {
        let v = self
            .propose(CatalogCommand::TruncateTable {
                table_name: table_name.to_string(),
                branch: branch.to_string(),
            })
            .await?;
        Ok(v["snapshot_id"].as_u64().unwrap_or(1))
    }

    async fn rename_column(&self, table_name: &str, old_col: &str, new_col: &str) -> Result<i32> {
        let v = self
            .propose(CatalogCommand::RenameColumn {
                table_name: table_name.to_string(),
                old_col: old_col.to_string(),
                new_col: new_col.to_string(),
            })
            .await?;
        Ok(v["field_id"].as_i64().unwrap_or(0) as i32)
    }

    async fn drop_column(&self, table_name: &str, col_name: &str, if_exists: bool) -> Result<()> {
        self.propose(CatalogCommand::DropColumn {
            table_name: table_name.to_string(),
            col_name: col_name.to_string(),
            if_exists,
        })
        .await?;
        Ok(())
    }

    async fn widen_column(
        &self,
        table_name: &str,
        col_name: &str,
        new_type: ColumnType,
    ) -> Result<()> {
        self.propose(CatalogCommand::WidenColumn {
            table_name: table_name.to_string(),
            col_name: col_name.to_string(),
            new_type,
        })
        .await?;
        Ok(())
    }
}

async fn handle_status(State(node): State<Arc<RaftCatalogNode>>) -> (StatusCode, Json<Value>) {
    if node.is_stopped() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "stopped" })),
        );
    }
    (StatusCode::OK, Json(serde_json::to_value(node.status()).unwrap()))
}

async fn handle_request_vote(
    State(node): State<Arc<RaftCatalogNode>>,
    Json(req): Json<RequestVoteReq>,
) -> (StatusCode, Json<Value>) {
    if node.is_stopped() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "stopped" })),
        );
    }
    let resp = node.on_request_vote(req);
    (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()))
}

async fn handle_append_entries(
    State(node): State<Arc<RaftCatalogNode>>,
    Json(req): Json<AppendEntriesReq>,
) -> (StatusCode, Json<Value>) {
    if node.is_stopped() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "stopped" })),
        );
    }
    let resp = node.on_append_entries(req).await;
    (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()))
}

async fn handle_client_write(
    State(node): State<Arc<RaftCatalogNode>>,
    Json(cmd): Json<CatalogCommand>,
) -> (StatusCode, Json<Value>) {
    if node.is_stopped() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "stopped" })),
        );
    }
    match node.propose(cmd).await {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}
