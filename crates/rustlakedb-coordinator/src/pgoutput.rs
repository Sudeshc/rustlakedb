// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Native PostgreSQL Logical Replication (`pgoutput` v1/v2) Binary Protocol Decoder & Streaming
//! CDC Consumer (Wave 35):
//! Decodes real PostgreSQL WAL `pgoutput` binary frames (`Begin`, `Relation`, `Insert`, `Update`,
//! `Delete`, `Truncate`, `Commit`) inside `CopyData ('d')` / `XLogData ('w')` replication streams,
//! commits transactions atomically into Apache Iceberg v3 using zero-read Equality Delete Parquet
//! files (`Coordinator::ingest_cdc_equality_deletes`), and checkpoints durable LSN watermarks for
//! exactly-once delivery across restarts.

use crate::Coordinator;
use bytes::{Buf, BufMut, BytesMut};
use rustlakedb_catalog::ColumnType;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{RowBatch, ScalarValue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRelationColumn {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgTupleColumn {
    Null,
    UnchangedToast,
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputMessage {
    Begin {
        final_lsn: u64,
        commit_time_us: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_time_us: i64,
    },
    Relation {
        rel_id: u32,
        namespace: String,
        name: String,
        replica_identity: u8,
        columns: Vec<PgRelationColumn>,
    },
    Insert {
        rel_id: u32,
        tuple: Vec<PgTupleColumn>,
    },
    Update {
        rel_id: u32,
        old_tuple: Option<Vec<PgTupleColumn>>,
        new_tuple: Vec<PgTupleColumn>,
    },
    Delete {
        rel_id: u32,
        key_tuple: Vec<PgTupleColumn>,
    },
    Truncate {
        options: u8,
        rel_ids: Vec<u32>,
    },
}

fn read_cstr(cur: &mut &[u8]) -> Result<String> {
    let nul = cur
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| RustLakeError::Execution("pgoutput: unterminated c-string".into()))?;
    let s = String::from_utf8_lossy(&cur[..nul]).to_string();
    cur.advance(nul + 1);
    Ok(s)
}

fn decode_tuple_data(cur: &mut &[u8]) -> Result<Vec<PgTupleColumn>> {
    if cur.remaining() < 2 {
        return Err(RustLakeError::Execution("pgoutput: truncated tuple count".into()));
    }
    let ncols = cur.get_i16().max(0) as usize;
    let mut cols = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        if cur.remaining() < 1 {
            return Err(RustLakeError::Execution("pgoutput: truncated tuple column tag".into()));
        }
        match cur.get_u8() {
            b'n' => cols.push(PgTupleColumn::Null),
            b'u' => cols.push(PgTupleColumn::UnchangedToast),
            b't' => {
                if cur.remaining() < 4 {
                    return Err(RustLakeError::Execution("pgoutput: truncated text len".into()));
                }
                let len = cur.get_i32().max(0) as usize;
                if cur.remaining() < len {
                    return Err(RustLakeError::Execution("pgoutput: truncated text body".into()));
                }
                let s = String::from_utf8_lossy(&cur[..len]).to_string();
                cur.advance(len);
                cols.push(PgTupleColumn::Text(s));
            }
            b'b' => {
                if cur.remaining() < 4 {
                    return Err(RustLakeError::Execution("pgoutput: truncated binary len".into()));
                }
                let len = cur.get_i32().max(0) as usize;
                if cur.remaining() < len {
                    return Err(RustLakeError::Execution("pgoutput: truncated binary body".into()));
                }
                let bytes = cur[..len].to_vec();
                cur.advance(len);
                cols.push(PgTupleColumn::Binary(bytes));
            }
            other => {
                return Err(RustLakeError::Execution(format!(
                    "pgoutput: unknown tuple column tag 0x{other:02x}"
                )));
            }
        }
    }
    Ok(cols)
}

/// Decodes a single PostgreSQL `pgoutput` binary logical replication message.
pub fn decode_pgoutput_message(buf: &[u8]) -> Result<PgOutputMessage> {
    if buf.is_empty() {
        return Err(RustLakeError::Execution("pgoutput: empty message".into()));
    }
    let mut cur = buf;
    let tag = cur.get_u8();
    match tag {
        b'B' => {
            if cur.remaining() < 20 {
                return Err(RustLakeError::Execution("pgoutput: truncated Begin".into()));
            }
            Ok(PgOutputMessage::Begin {
                final_lsn: cur.get_u64(),
                commit_time_us: cur.get_i64(),
                xid: cur.get_u32(),
            })
        }
        b'C' => {
            if cur.remaining() < 25 {
                return Err(RustLakeError::Execution("pgoutput: truncated Commit".into()));
            }
            Ok(PgOutputMessage::Commit {
                flags: cur.get_u8(),
                commit_lsn: cur.get_u64(),
                end_lsn: cur.get_u64(),
                commit_time_us: cur.get_i64(),
            })
        }
        b'R' => {
            if cur.remaining() < 4 {
                return Err(RustLakeError::Execution("pgoutput: truncated Relation".into()));
            }
            let rel_id = cur.get_u32();
            let namespace = read_cstr(&mut cur)?;
            let name = read_cstr(&mut cur)?;
            if cur.remaining() < 3 {
                return Err(RustLakeError::Execution("pgoutput: truncated Relation header".into()));
            }
            let replica_identity = cur.get_u8();
            let ncols = cur.get_i16().max(0) as usize;
            let mut columns = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                if cur.remaining() < 1 {
                    return Err(RustLakeError::Execution("pgoutput: truncated Relation col".into()));
                }
                let flags = cur.get_u8();
                let col_name = read_cstr(&mut cur)?;
                if cur.remaining() < 8 {
                    return Err(RustLakeError::Execution("pgoutput: truncated Relation oid".into()));
                }
                let type_oid = cur.get_u32();
                let type_modifier = cur.get_i32();
                columns.push(PgRelationColumn {
                    flags,
                    name: col_name,
                    type_oid,
                    type_modifier,
                });
            }
            Ok(PgOutputMessage::Relation {
                rel_id,
                namespace,
                name,
                replica_identity,
                columns,
            })
        }
        b'I' => {
            if cur.remaining() < 5 {
                return Err(RustLakeError::Execution("pgoutput: truncated Insert".into()));
            }
            let rel_id = cur.get_u32();
            let marker = cur.get_u8();
            if marker != b'N' {
                return Err(RustLakeError::Execution(format!(
                    "pgoutput Insert: expected 'N', got 0x{marker:02x}"
                )));
            }
            let tuple = decode_tuple_data(&mut cur)?;
            Ok(PgOutputMessage::Insert { rel_id, tuple })
        }
        b'U' => {
            if cur.remaining() < 5 {
                return Err(RustLakeError::Execution("pgoutput: truncated Update".into()));
            }
            let rel_id = cur.get_u32();
            let marker = cur.get_u8();
            let (old_tuple, new_tuple) = if marker == b'K' || marker == b'O' {
                let old = decode_tuple_data(&mut cur)?;
                if cur.remaining() < 1 || cur.get_u8() != b'N' {
                    return Err(RustLakeError::Execution("pgoutput Update: missing 'N'".into()));
                }
                let new_t = decode_tuple_data(&mut cur)?;
                (Some(old), new_t)
            } else if marker == b'N' {
                (None, decode_tuple_data(&mut cur)?)
            } else {
                return Err(RustLakeError::Execution(format!(
                    "pgoutput Update: unexpected marker 0x{marker:02x}"
                )));
            };
            Ok(PgOutputMessage::Update {
                rel_id,
                old_tuple,
                new_tuple,
            })
        }
        b'D' => {
            if cur.remaining() < 5 {
                return Err(RustLakeError::Execution("pgoutput: truncated Delete".into()));
            }
            let rel_id = cur.get_u32();
            let marker = cur.get_u8();
            if marker != b'K' && marker != b'O' {
                return Err(RustLakeError::Execution(format!(
                    "pgoutput Delete: expected 'K'/'O', got 0x{marker:02x}"
                )));
            }
            let key_tuple = decode_tuple_data(&mut cur)?;
            Ok(PgOutputMessage::Delete { rel_id, key_tuple })
        }
        b'T' => {
            if cur.remaining() < 5 {
                return Err(RustLakeError::Execution("pgoutput: truncated Truncate".into()));
            }
            let nrels = cur.get_i32().max(0) as usize;
            let options = cur.get_u8();
            let mut rel_ids = Vec::with_capacity(nrels);
            for _ in 0..nrels {
                if cur.remaining() < 4 {
                    break;
                }
                rel_ids.push(cur.get_u32());
            }
            Ok(PgOutputMessage::Truncate { options, rel_ids })
        }
        other => Err(RustLakeError::Execution(format!(
            "pgoutput: unsupported message type 0x{other:02x}"
        ))),
    }
}

fn encode_tuple(buf: &mut BytesMut, cols: &[PgTupleColumn]) {
    buf.put_i16(cols.len() as i16);
    for c in cols {
        match c {
            PgTupleColumn::Null => buf.put_u8(b'n'),
            PgTupleColumn::UnchangedToast => buf.put_u8(b'u'),
            PgTupleColumn::Text(s) => {
                buf.put_u8(b't');
                buf.put_i32(s.len() as i32);
                buf.put_slice(s.as_bytes());
            }
            PgTupleColumn::Binary(b) => {
                buf.put_u8(b'b');
                buf.put_i32(b.len() as i32);
                buf.put_slice(b);
            }
        }
    }
}

/// Encodes a `PgOutputMessage` into wire-format bytes (useful for PostgreSQL WAL simulators & tests).
pub fn encode_pgoutput_message(msg: &PgOutputMessage) -> Vec<u8> {
    let mut buf = BytesMut::new();
    match msg {
        PgOutputMessage::Begin {
            final_lsn,
            commit_time_us,
            xid,
        } => {
            buf.put_u8(b'B');
            buf.put_u64(*final_lsn);
            buf.put_i64(*commit_time_us);
            buf.put_u32(*xid);
        }
        PgOutputMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            commit_time_us,
        } => {
            buf.put_u8(b'C');
            buf.put_u8(*flags);
            buf.put_u64(*commit_lsn);
            buf.put_u64(*end_lsn);
            buf.put_i64(*commit_time_us);
        }
        PgOutputMessage::Relation {
            rel_id,
            namespace,
            name,
            replica_identity,
            columns,
        } => {
            buf.put_u8(b'R');
            buf.put_u32(*rel_id);
            buf.put_slice(namespace.as_bytes());
            buf.put_u8(0);
            buf.put_slice(name.as_bytes());
            buf.put_u8(0);
            buf.put_u8(*replica_identity);
            buf.put_i16(columns.len() as i16);
            for col in columns {
                buf.put_u8(col.flags);
                buf.put_slice(col.name.as_bytes());
                buf.put_u8(0);
                buf.put_u32(col.type_oid);
                buf.put_i32(col.type_modifier);
            }
        }
        PgOutputMessage::Insert { rel_id, tuple } => {
            buf.put_u8(b'I');
            buf.put_u32(*rel_id);
            buf.put_u8(b'N');
            encode_tuple(&mut buf, tuple);
        }
        PgOutputMessage::Update {
            rel_id,
            old_tuple,
            new_tuple,
        } => {
            buf.put_u8(b'U');
            buf.put_u32(*rel_id);
            if let Some(old) = old_tuple {
                buf.put_u8(b'O');
                encode_tuple(&mut buf, old);
            }
            buf.put_u8(b'N');
            encode_tuple(&mut buf, new_tuple);
        }
        PgOutputMessage::Delete { rel_id, key_tuple } => {
            buf.put_u8(b'D');
            buf.put_u32(*rel_id);
            buf.put_u8(b'K');
            encode_tuple(&mut buf, key_tuple);
        }
        PgOutputMessage::Truncate { options, rel_ids } => {
            buf.put_u8(b'T');
            buf.put_i32(rel_ids.len() as i32);
            buf.put_u8(*options);
            for id in rel_ids {
                buf.put_u32(*id);
            }
        }
    }
    buf.to_vec()
}

/// Wraps a `pgoutput` payload inside a real PostgreSQL `CopyData ('d')` + `XLogData ('w')` frame.
pub fn encode_pg_xlog_copy_data_frame(wal_start: u64, wal_end: u64, server_time_us: i64, pgoutput_payload: &[u8]) -> Vec<u8> {
    let inner_len = 1 + 8 + 8 + 8 + pgoutput_payload.len();
    let mut out = BytesMut::with_capacity(1 + 4 + inner_len);
    out.put_u8(b'd'); // CopyData
    out.put_i32((4 + inner_len) as i32);
    out.put_u8(b'w'); // XLogData
    out.put_u64(wal_start);
    out.put_u64(wal_end);
    out.put_i64(server_time_us);
    out.put_slice(pgoutput_payload);
    out.to_vec()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LsnCheckpoint {
    committed_lsn: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PgOutputIngestReport {
    pub transactions_committed: usize,
    pub inserts_applied: usize,
    pub updates_applied: usize,
    pub deletes_applied: usize,
    pub skipped_replayed_txs: usize,
    pub committed_lsn: u64,
}

/// Native PostgreSQL Logical Replication (`pgoutput`) Streaming CDC Consumer (Wave 35):
/// Reads real `CopyData ('d')` / `XLogData ('w')` frames over TCP (or any `AsyncRead`), buffers
/// each transaction from `Begin` to `Commit`, commits inserts/updates/deletes atomically via
/// zero-read Iceberg v3 Equality Delete Parquet files, and checkpoints LSNs durably on disk.
pub struct PgOutputCdcConsumer {
    coord: Arc<Coordinator>,
    table_name: String,
    branch: String,
    key_col: String,
    checkpoint_file: PathBuf,
}

impl PgOutputCdcConsumer {
    pub fn new(
        coord: Arc<Coordinator>,
        table_name: impl Into<String>,
        key_col: impl Into<String>,
        slot_name: impl AsRef<str>,
        checkpoint_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        std::fs::create_dir_all(checkpoint_dir.as_ref())?;
        let table_s = table_name.into();
        let safe_slot = slot_name.as_ref().replace(['/', '\\', ':', '.'], "_");
        let checkpoint_file = checkpoint_dir
            .as_ref()
            .join(format!("pgoutput_{table_s}_{safe_slot}.json"));
        Ok(Self {
            coord,
            table_name: table_s,
            branch: "main".into(),
            key_col: key_col.into(),
            checkpoint_file,
        })
    }

    pub fn load_committed_lsn(&self) -> u64 {
        if self.checkpoint_file.exists()
            && let Ok(bytes) = std::fs::read(&self.checkpoint_file)
            && let Ok(cp) = serde_json::from_slice::<LsnCheckpoint>(&bytes)
        {
            return cp.committed_lsn;
        }
        0
    }

    fn save_committed_lsn(&self, committed_lsn: u64) -> Result<()> {
        let cp = LsnCheckpoint { committed_lsn };
        let tmp = self
            .checkpoint_file
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, serde_json::to_vec_pretty(&cp)?)?;
        std::fs::rename(&tmp, &self.checkpoint_file)?;
        Ok(())
    }

    /// Connects to a TCP PostgreSQL logical-replication stream endpoint and consumes all available
    /// `CopyData ('d')` / `XLogData ('w')` frames until EOF or `CopyDone ('c')`.
    pub async fn consume_tcp(&self, addr: &str) -> Result<PgOutputIngestReport> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RustLakeError::Execution(format!("pgoutput tcp connect '{addr}': {e}")))?;
        self.consume_stream(stream).await
    }

    /// Consumes PostgreSQL `CopyData ('d')` -> `XLogData ('w')` frames from any async stream.
    pub async fn consume_stream<R: AsyncRead + Unpin>(
        &self,
        mut reader: R,
    ) -> Result<PgOutputIngestReport> {
        let mut committed_lsn = self.load_committed_lsn();
        let meta = self.coord.catalog.get_table(&self.table_name).await?;
        let col_types: HashMap<String, ColumnType> = meta
            .schema
            .iter()
            .map(|f| (f.name.clone(), f.data_type.clone()))
            .collect();
        let col_names: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();

        let mut relations: HashMap<u32, Vec<PgRelationColumn>> = HashMap::new();
        let mut pending_upserts: Vec<BTreeMap<String, ScalarValue>> = Vec::new();
        let mut pending_deletes: Vec<ScalarValue> = Vec::new();
        let mut tx_inserts = 0usize;
        let mut tx_updates = 0usize;
        let mut tx_deletes = 0usize;

        let mut report = PgOutputIngestReport {
            committed_lsn,
            ..Default::default()
        };

        while let Ok(msg_type) = reader.read_u8().await {
            let len = match reader.read_i32().await {
                Ok(l) => l,
                Err(_) => break,
            };
            let body_len = (len - 4).max(0) as usize;
            let mut body = vec![0u8; body_len];
            if body_len > 0 && reader.read_exact(&mut body).await.is_err() {
                break;
            }

            if msg_type == b'c' {
                // CopyDone
                break;
            }
            if msg_type != b'd' || body.is_empty() {
                continue;
            }
            // Inside CopyData: 'w' = XLogData (25-byte header + pgoutput message), 'k' = keepalive.
            if body[0] != b'w' || body.len() <= 25 {
                continue;
            }
            let pg_msg = decode_pgoutput_message(&body[25..])?;
            match pg_msg {
                PgOutputMessage::Relation { rel_id, columns, .. } => {
                    relations.insert(rel_id, columns);
                }
                PgOutputMessage::Begin { .. } => {
                    pending_upserts.clear();
                    pending_deletes.clear();
                    tx_inserts = 0;
                    tx_updates = 0;
                    tx_deletes = 0;
                }
                PgOutputMessage::Insert { rel_id, tuple } => {
                    if let Some(rel_cols) = relations.get(&rel_id) {
                        let row = tuple_to_row(rel_cols, &tuple, &col_types);
                        pending_upserts.push(row);
                        tx_inserts += 1;
                    }
                }
                PgOutputMessage::Update {
                    rel_id,
                    old_tuple,
                    new_tuple,
                } => {
                    if let Some(rel_cols) = relations.get(&rel_id) {
                        let new_row = tuple_to_row(rel_cols, &new_tuple, &col_types);
                        let key_val = old_tuple
                            .as_ref()
                            .and_then(|ot| tuple_to_row(rel_cols, ot, &col_types).get(&self.key_col).cloned())
                            .or_else(|| new_row.get(&self.key_col).cloned());
                        if let Some(kv) = key_val
                            && !matches!(kv, ScalarValue::Null)
                        {
                            pending_deletes.push(kv);
                        }
                        pending_upserts.push(new_row);
                        tx_updates += 1;
                    }
                }
                PgOutputMessage::Delete { rel_id, key_tuple } => {
                    if let Some(rel_cols) = relations.get(&rel_id) {
                        let old_row = tuple_to_row(rel_cols, &key_tuple, &col_types);
                        if let Some(kv) = old_row.get(&self.key_col)
                            && !matches!(kv, ScalarValue::Null)
                        {
                            pending_deletes.push(kv.clone());
                            tx_deletes += 1;
                        }
                    }
                }
                PgOutputMessage::Commit { end_lsn, .. } => {
                    if end_lsn <= committed_lsn {
                        report.skipped_replayed_txs += 1;
                        pending_upserts.clear();
                        pending_deletes.clear();
                        continue;
                    }
                    if !pending_upserts.is_empty() || !pending_deletes.is_empty() {
                        let batch = RowBatch::new(col_names.clone(), std::mem::take(&mut pending_upserts));
                        let del_keys = std::mem::take(&mut pending_deletes);
                        self.coord
                            .ingest_cdc_equality_deletes(
                                &self.table_name,
                                &self.branch,
                                &self.key_col,
                                del_keys,
                                batch,
                            )
                            .await?;
                    }
                    committed_lsn = end_lsn;
                    self.save_committed_lsn(committed_lsn)?;
                    report.transactions_committed += 1;
                    report.inserts_applied += tx_inserts;
                    report.updates_applied += tx_updates;
                    report.deletes_applied += tx_deletes;
                    report.committed_lsn = committed_lsn;
                }
                PgOutputMessage::Truncate { .. } => {}
            }
        }

        Ok(report)
    }
}

fn tuple_to_row(
    rel_cols: &[PgRelationColumn],
    tuple: &[PgTupleColumn],
    col_types: &HashMap<String, ColumnType>,
) -> BTreeMap<String, ScalarValue> {
    let mut row = BTreeMap::new();
    for (col_def, cell) in rel_cols.iter().zip(tuple.iter()) {
        let dt = col_types.get(&col_def.name).unwrap_or(&ColumnType::Utf8);
        let sv = match cell {
            PgTupleColumn::Null | PgTupleColumn::UnchangedToast => ScalarValue::Null,
            PgTupleColumn::Text(s) => parse_text_scalar(s, dt),
            PgTupleColumn::Binary(b) => parse_text_scalar(&String::from_utf8_lossy(b), dt),
        };
        row.insert(col_def.name.clone(), sv);
    }
    row
}

fn parse_text_scalar(s: &str, dt: &ColumnType) -> ScalarValue {
    match dt {
        ColumnType::Int64 => s
            .parse::<i64>()
            .map(ScalarValue::Int64)
            .unwrap_or_else(|_| ScalarValue::Utf8(s.to_string())),
        ColumnType::Float64 => s
            .parse::<f64>()
            .map(ScalarValue::Float64)
            .unwrap_or_else(|_| ScalarValue::Utf8(s.to_string())),
        ColumnType::Bool => ScalarValue::Bool(matches!(s, "t" | "true" | "TRUE" | "1")),
        ColumnType::Variant | ColumnType::Vector(_) => ScalarValue::Json(
            serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string())),
        ),
        _ => ScalarValue::Utf8(s.to_string()),
    }
}
