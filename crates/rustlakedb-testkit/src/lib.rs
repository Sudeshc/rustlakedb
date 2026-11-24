// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use bytes::{BufMut, BytesMut};
use rustlakedb_catalog::rest::IcebergRestServer;
use rustlakedb_catalog::InMemoryRaftCatalog;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_coordinator::{Coordinator, HttpWorkerClient, QueryExecutionResult, WorkerClient};
use rustlakedb_gateway::PgGateway;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::ShuffleMode;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::rpc::WorkerHttpServer;
use rustlakedb_worker::WorkerNode;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Isolated, self-cleaning distributed test cluster backed by real Parquet + Puffin + NVMe shuffle
/// and a crash-safe persistent Iceberg v3 catalog.
pub struct TestCluster {
    pub coordinator: Arc<Coordinator>,
    pub catalog: Arc<InMemoryRaftCatalog>,
    pub store: Arc<IcebergObjectStore>,
    pub shuffle: Arc<dyn ShuffleTransport>,
    pub workers: Vec<Arc<WorkerNode>>,
    pub data_dir: Option<TempDir>,
    pub data_path: PathBuf,
    pub shuffle_dir: TempDir,
}

impl TestCluster {
    pub fn start(num_workers: usize, shuffle_mode: ShuffleMode) -> Result<Self> {
        Self::start_with_config(
            num_workers,
            PlannerConfig {
                default_shuffle_partitions: num_workers,
                shuffle_mode,
                broadcast_join_threshold_bytes: 0,
                ..Default::default()
            },
        )
    }

    pub fn start_with_config(num_workers: usize, planner_config: PlannerConfig) -> Result<Self> {
        let data_dir = tempfile::tempdir()?;
        let data_path = data_dir.path().to_path_buf();
        let mut c = Self::open_persistent(&data_path, num_workers, planner_config)?;
        c.data_dir = Some(data_dir);
        Ok(c)
    }

    /// Opens a persistent cluster over an existing directory (used for restart / crash-recovery tests).
    pub fn open_persistent(
        data_path: impl AsRef<Path>,
        num_workers: usize,
        planner_config: PlannerConfig,
    ) -> Result<Self> {
        let shuffle_dir = tempfile::tempdir()?;
        let catalog = Arc::new(InMemoryRaftCatalog::open(data_path.as_ref())?);
        let store = Arc::new(IcebergObjectStore::local_fs(data_path.as_ref())?);
        let shuffle: Arc<dyn ShuffleTransport> =
            Arc::new(HybridShuffleManager::new(shuffle_dir.path())?);

        let workers: Vec<Arc<WorkerNode>> = (1..=num_workers)
            .map(|i| {
                Arc::new(WorkerNode::new(
                    format!("worker-{i}"),
                    catalog.clone(),
                    shuffle.clone(),
                    store.clone(),
                ))
            })
            .collect();

        let coordinator = Arc::new(Coordinator::new(
            catalog.clone(),
            store.clone(),
            shuffle.clone(),
            workers.clone(),
            planner_config,
        ));

        Ok(Self {
            coordinator,
            catalog,
            store,
            shuffle,
            workers,
            data_dir: None,
            data_path: data_path.as_ref().to_path_buf(),
            shuffle_dir,
        })
    }

    /// Opens a persistent cluster with custom per-worker MemoryPool limit (for Grace Hash Join spill tests).
    pub fn start_with_memory_limit(
        num_workers: usize,
        memory_limit_bytes: usize,
    ) -> Result<Self> {
        let data_dir = tempfile::tempdir()?;
        let data_path = data_dir.path().to_path_buf();
        let shuffle_dir = tempfile::tempdir()?;
        let catalog = Arc::new(InMemoryRaftCatalog::open(&data_path)?);
        let store = Arc::new(IcebergObjectStore::local_fs(&data_path)?);
        let shuffle: Arc<dyn ShuffleTransport> =
            Arc::new(HybridShuffleManager::new(shuffle_dir.path())?);

        let workers: Vec<Arc<WorkerNode>> = (1..=num_workers)
            .map(|i| {
                Arc::new(WorkerNode::with_memory_limit(
                    format!("worker-{i}"),
                    catalog.clone(),
                    shuffle.clone(),
                    store.clone(),
                    memory_limit_bytes,
                ))
            })
            .collect();

        let coordinator = Arc::new(Coordinator::new(
            catalog.clone(),
            store.clone(),
            shuffle.clone(),
            workers.clone(),
            PlannerConfig {
                default_shuffle_partitions: num_workers,
                shuffle_mode: ShuffleMode::Staged,
                broadcast_join_threshold_bytes: 0,
                ..Default::default()
            },
        ));

        Ok(Self {
            coordinator,
            catalog,
            store,
            shuffle,
            workers,
            data_dir: Some(data_dir),
            data_path,
            shuffle_dir,
        })
    }

    /// Spins up a real disaggregated multi-node networked cluster:
    /// Each `WorkerHttpServer` owns its own private local shuffle directory and advertises
    /// its HTTP URL so workers fetch Zstd shuffle blocks from each other over TCP!
    pub async fn start_networked(num_workers: usize) -> Result<Self> {
        Self::start_networked_with_config(
            num_workers,
            PlannerConfig {
                default_shuffle_partitions: num_workers,
                shuffle_mode: ShuffleMode::Staged,
                broadcast_join_threshold_bytes: 0,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn start_networked_with_config(
        num_workers: usize,
        planner_config: PlannerConfig,
    ) -> Result<Self> {
        let data_dir = tempfile::tempdir()?;
        let data_path = data_dir.path().to_path_buf();
        let shuffle_dir = tempfile::tempdir()?;
        let catalog = Arc::new(InMemoryRaftCatalog::open(&data_path)?);
        let store = Arc::new(IcebergObjectStore::local_fs(&data_path)?);
        let coord_shuffle: Arc<dyn ShuffleTransport> =
            Arc::new(HybridShuffleManager::new(shuffle_dir.path())?);

        let mut workers = Vec::with_capacity(num_workers);
        let mut clients: Vec<Arc<dyn WorkerClient>> = Vec::with_capacity(num_workers);

        for i in 1..=num_workers {
            let private_dir = shuffle_dir.path().join(format!("private_w{i}"));
            let worker_shuffle_mgr = Arc::new(HybridShuffleManager::new(private_dir)?);
            let worker = Arc::new(WorkerNode::new(
                format!("net-worker-{i}"),
                catalog.clone(),
                worker_shuffle_mgr.clone(),
                store.clone(),
            ));
            let server = WorkerHttpServer::bind("127.0.0.1:0", worker.clone()).await?;
            let addr = server.local_addr()?;
            worker_shuffle_mgr.set_advertise_url(format!("http://{addr}"));
            tokio::spawn(async move {
                let _ = server.serve().await;
            });
            clients.push(Arc::new(HttpWorkerClient::new(
                format!("net-worker-{i}"),
                format!("http://{addr}"),
            )));
            workers.push(worker);
        }

        let coordinator = Arc::new(Coordinator::with_clients(
            catalog.clone(),
            store.clone(),
            coord_shuffle.clone(),
            clients,
            planner_config,
        ));

        Ok(Self {
            coordinator,
            catalog,
            store,
            shuffle: coord_shuffle,
            workers,
            data_dir: Some(data_dir),
            data_path,
            shuffle_dir,
        })
    }

    pub fn data_dir_path(&self) -> &Path {
        &self.data_path
    }

    pub async fn sql(&self, query: &str) -> Result<QueryExecutionResult> {
        self.coordinator.execute_sql(query, "main").await
    }

    pub async fn sql_as(&self, user: &str, query: &str) -> Result<QueryExecutionResult> {
        let session = rustlakedb_coordinator::security::SessionContext::for_user(user);
        self.coordinator
            .execute_sql_with_session(query, "main", &session)
            .await
    }

    pub async fn sql_on_branch(&self, query: &str, branch: &str) -> Result<QueryExecutionResult> {
        self.coordinator.execute_sql(query, branch).await
    }

    pub async fn start_control_plane(&self) -> Result<SocketAddr> {
        let cp = rustlakedb_coordinator::admin::ControlPlaneServer::bind(
            "127.0.0.1:0",
            self.coordinator.clone(),
        )
        .await?;
        let addr = cp.local_addr()?;
        tokio::spawn(async move {
            let _ = cp.serve().await;
        });
        Ok(addr)
    }

    pub async fn start_pg_gateway(&self) -> Result<SocketAddr> {
        let gw = PgGateway::bind("127.0.0.1:0", self.coordinator.clone()).await?;
        let addr = gw.local_addr()?;
        tokio::spawn(async move {
            let _ = gw.serve().await;
        });
        Ok(addr)
    }

    pub async fn start_pg_gateway_scram(
        &self,
        users: std::collections::HashMap<String, String>,
    ) -> Result<SocketAddr> {
        let gw = PgGateway::bind_with_auth(
            "127.0.0.1:0",
            self.coordinator.clone(),
            rustlakedb_gateway::PgAuthConfig::ScramSha256(users),
        )
        .await?;
        let addr = gw.local_addr()?;
        tokio::spawn(async move {
            let _ = gw.serve().await;
        });
        Ok(addr)
    }

    pub async fn start_iceberg_rest(&self) -> Result<SocketAddr> {
        let rest = IcebergRestServer::bind("127.0.0.1:0", self.catalog.clone()).await?;
        let addr = rest.local_addr()?;
        tokio::spawn(async move {
            let _ = rest.serve().await;
        });
        Ok(addr)
    }

    pub async fn seed_ecommerce(&self) -> Result<()> {
        self.sql("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, region TEXT, amount DOUBLE, payload JSONB)").await?;
        self.sql("CREATE TABLE customers (customer_id BIGINT, tier TEXT)").await?;

        self.sql(
            r#"INSERT INTO orders VALUES
               (1, 10, 'us', 100.0, '{"event_type": "purchase", "channel": "web"}'),
               (2, 20, 'eu', 200.0, '{"event_type": "refund", "channel": "app"}'),
               (3, 10, 'us', 999.0, '{"event_type": "purchase", "channel": "fraud"}')"#,
        )
        .await?;
        self.sql(
            r#"INSERT INTO orders VALUES
               (4, 10, 'us', 150.0, '{"event_type": "purchase", "channel": "app"}'),
               (5, 30, 'apac', 300.0, '{"event_type": "purchase", "channel": "web"}')"#,
        )
        .await?;
        self.sql(
            r#"INSERT INTO orders VALUES
               (6, 20, 'eu', 50.0, '{"event_type": "view", "channel": "web"}')"#,
        )
        .await?;

        self.sql(
            r#"INSERT INTO customers VALUES
               (10, 'enterprise'),
               (20, 'pro'),
               (30, 'enterprise')"#,
        )
        .await?;
        Ok(())
    }
}

/// Real PostgreSQL Wire Protocol v3.0 TCP test client supporting BOTH Simple Query
/// and Extended Query (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`).
pub struct PgTestClient {
    stream: TcpStream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgQueryOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: String,
}

impl PgTestClient {
    pub async fn connect(addr: SocketAddr, user: &str, db: &str) -> Result<Self> {
        Self::connect_with_password(addr, user, None, db).await
    }

    pub async fn connect_scram(
        addr: SocketAddr,
        user: &str,
        password: &str,
        db: &str,
    ) -> Result<Self> {
        Self::connect_with_password(addr, user, Some(password), db).await
    }

    async fn connect_with_password(
        addr: SocketAddr,
        user: &str,
        password: Option<&str>,
        db: &str,
    ) -> Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;

        let mut ssl_req = BytesMut::new();
        ssl_req.put_i32(8);
        ssl_req.put_i32(80877103);
        stream.write_all(&ssl_req).await?;
        let ssl_resp = stream.read_u8().await?;
        if ssl_resp != b'N' {
            return Err(RustLakeError::Execution(format!(
                "expected SSL decline 'N', got '{}'",
                ssl_resp as char
            )));
        }

        let mut params = BytesMut::new();
        params.put_i32(196608);
        params.put_slice(b"user\0");
        params.put_slice(user.as_bytes());
        params.put_u8(0);
        params.put_slice(b"database\0");
        params.put_slice(db.as_bytes());
        params.put_u8(0);
        params.put_u8(0);

        let mut startup = BytesMut::new();
        startup.put_i32((4 + params.len()) as i32);
        startup.put_slice(&params);
        stream.write_all(&startup).await?;

        let client_first_bare = format!("n={user},r=rustlakedb_nonce_42");
        let mut expected_server_sig: Option<String> = None;

        loop {
            let tag = stream.read_u8().await?;
            let len = stream.read_i32().await?;
            let mut body = vec![0u8; (len - 4).max(0) as usize];
            if !body.is_empty() {
                stream.read_exact(&mut body).await?;
            }
            match tag {
                b'R' if body.len() >= 4 => {
                    let auth_code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                    if auth_code == 10 {
                        // AuthenticationSASL -> send SASLInitialResponse ('p')
                        let client_first = format!("n,,{client_first_bare}");
                        let mut p = BytesMut::new();
                        let mech = b"SCRAM-SHA-256\0";
                        let mut inner = BytesMut::new();
                        inner.put_slice(mech);
                        inner.put_i32(client_first.len() as i32);
                        inner.put_slice(client_first.as_bytes());
                        p.put_u8(b'p');
                        p.put_i32((4 + inner.len()) as i32);
                        p.put_slice(&inner);
                        stream.write_all(&p).await?;
                    } else if auth_code == 11 {
                        // AuthenticationSASLContinue -> compute & send client-final-message
                        let server_first = String::from_utf8_lossy(&body[4..]).to_string();
                        let pw = password.unwrap_or("");
                        let (client_final, exp_sig) = rustlakedb_gateway::scram::scram_client_final(
                            &client_first_bare,
                            &server_first,
                            pw,
                        )?;
                        expected_server_sig = Some(exp_sig);
                        let mut p = BytesMut::new();
                        p.put_u8(b'p');
                        p.put_i32((4 + client_final.len()) as i32);
                        p.put_slice(client_final.as_bytes());
                        stream.write_all(&p).await?;
                    } else if auth_code == 12 {
                        // AuthenticationSASLFinal -> verify server signature!
                        let server_final = String::from_utf8_lossy(&body[4..]).to_string();
                        if let Some(ref exp) = expected_server_sig
                            && server_final != *exp
                        {
                            return Err(RustLakeError::Execution(format!(
                                "SCRAM server signature mismatch: expected {exp}, got {server_final}"
                            )));
                        }
                    }
                }
                b'E' => {
                    let msg = String::from_utf8_lossy(&body).to_string();
                    return Err(RustLakeError::Execution(format!("pgwire auth error: {msg}")));
                }
                b'Z' => break,
                _ => {}
            }
        }

        Ok(Self { stream })
    }

    pub async fn query(&mut self, sql: &str) -> Result<PgQueryOutput> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'Q');
        msg.put_i32((4 + sql.len() + 1) as i32);
        msg.put_slice(sql.as_bytes());
        msg.put_u8(0);
        self.stream.write_all(&msg).await?;
        self.read_until_ready().await
    }

    /// Executes a parameterized Extended Query (`Parse -> Bind -> Describe -> Execute -> Sync`).
    pub async fn query_extended(
        &mut self,
        sql: &str,
        params: &[Option<&str>],
    ) -> Result<PgQueryOutput> {
        let mut buf = BytesMut::new();

        // Parse ('P'): stmt="", sql, num_oids=0
        let mut p_body = BytesMut::new();
        p_body.put_u8(0);
        p_body.put_slice(sql.as_bytes());
        p_body.put_u8(0);
        p_body.put_i16(0);
        buf.put_u8(b'P');
        buf.put_i32((4 + p_body.len()) as i32);
        buf.put_slice(&p_body);

        // Bind ('B'): portal="", stmt="", 0 pfmt, N params, 0 rfmt
        let mut b_body = BytesMut::new();
        b_body.put_u8(0);
        b_body.put_u8(0);
        b_body.put_i16(0);
        b_body.put_i16(params.len() as i16);
        for p in params {
            match p {
                Some(s) => {
                    b_body.put_i32(s.len() as i32);
                    b_body.put_slice(s.as_bytes());
                }
                None => b_body.put_i32(-1),
            }
        }
        b_body.put_i16(0);
        buf.put_u8(b'B');
        buf.put_i32((4 + b_body.len()) as i32);
        buf.put_slice(&b_body);

        // Describe ('D'): portal ""
        buf.put_u8(b'D');
        buf.put_i32(6);
        buf.put_u8(b'P');
        buf.put_u8(0);

        // Execute ('E'): portal "", max_rows=0
        buf.put_u8(b'E');
        buf.put_i32(9);
        buf.put_u8(0);
        buf.put_i32(0);

        // Sync ('S')
        buf.put_u8(b'S');
        buf.put_i32(4);

        self.stream.write_all(&buf).await?;
        self.read_until_ready().await
    }

    async fn read_until_ready(&mut self) -> Result<PgQueryOutput> {
        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut command_tag = String::new();
        let mut error_msg: Option<String> = None;

        loop {
            let tag = self.stream.read_u8().await?;
            let len = self.stream.read_i32().await?;
            let mut body = vec![0u8; (len - 4).max(0) as usize];
            if !body.is_empty() {
                self.stream.read_exact(&mut body).await?;
            }

            match tag {
                b'T' => {
                    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut pos = 2;
                    for _ in 0..count {
                        let end = body[pos..].iter().position(|&b| b == 0).unwrap();
                        let name = String::from_utf8_lossy(&body[pos..pos + end]).to_string();
                        columns.push(name);
                        pos += end + 1 + 18;
                    }
                }
                b'D' => {
                    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut pos = 2;
                    let mut row = Vec::with_capacity(count);
                    for _ in 0..count {
                        let flen = i32::from_be_bytes([
                            body[pos],
                            body[pos + 1],
                            body[pos + 2],
                            body[pos + 3],
                        ]);
                        pos += 4;
                        if flen < 0 {
                            row.push(None);
                        } else {
                            let flen = flen as usize;
                            let val = String::from_utf8_lossy(&body[pos..pos + flen]).to_string();
                            pos += flen;
                            row.push(Some(val));
                        }
                    }
                    rows.push(row);
                }
                b'C' => {
                    command_tag = String::from_utf8_lossy(&body)
                        .trim_end_matches('\0')
                        .to_string();
                }
                b'E' => {
                    error_msg = Some(String::from_utf8_lossy(&body).to_string());
                }
                b'Z' => break,
                _ => {}
            }
        }

        if let Some(err) = error_msg {
            return Err(RustLakeError::Execution(format!("pgwire error: {err}")));
        }

        Ok(PgQueryOutput {
            columns,
            rows,
            command_tag,
        })
    }

    pub async fn close(mut self) -> Result<()> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'X');
        msg.put_i32(4);
        self.stream.write_all(&msg).await?;
        Ok(())
    }
}

pub async fn run_slt_file(cluster: &TestCluster, path: impl AsRef<Path>) -> Result<usize> {
    let content = std::fs::read_to_string(path)?;
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;
    let mut executed = 0;

    while i < lines.len() {
        let line = lines[i].trim();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        if line == "statement ok" {
            i += 1;
            let sql = lines[i].trim();
            cluster.sql(sql).await?;
            executed += 1;
            i += 1;
        } else if line == "query" {
            i += 1;
            let sql = lines[i].trim();
            i += 1;
            if lines.get(i).map(|s| s.trim()) != Some("----") {
                return Err(RustLakeError::Execution(format!(
                    "expected '----' separator after SQL: {sql}"
                )));
            }
            i += 1;
            let mut expected = Vec::new();
            while i < lines.len() && !lines[i].trim().is_empty() {
                expected.push(lines[i].trim().to_string());
                i += 1;
            }
            let res = cluster.sql(sql).await?;
            let actual: Vec<String> = res
                .batch
                .rows
                .iter()
                .map(|r| {
                    res.batch
                        .columns
                        .iter()
                        .map(|c| {
                            r.get(c)
                                .and_then(|v| v.to_pg_text())
                                .unwrap_or_else(|| "NULL".into())
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            if actual != expected {
                return Err(RustLakeError::Execution(format!(
                    "SLT mismatch for `{sql}`:\n  expected: {expected:?}\n  actual:   {actual:?}"
                )));
            }
            executed += 1;
        } else {
            return Err(RustLakeError::Execution(format!(
                "unknown SLT directive: '{line}'"
            )));
        }
    }

    Ok(executed)
}
