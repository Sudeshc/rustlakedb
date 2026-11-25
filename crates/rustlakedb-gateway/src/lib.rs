// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod flight;
pub mod scram;

use bytes::{BufMut, BytesMut};
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_coordinator::security::SessionContext;
use rustlakedb_coordinator::{Coordinator, QueryExecutionResult};
use rustlakedb_protocol::RowBatch;
use rustls::ServerConfig;
use std::collections::HashMap;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

const SSL_REQUEST_CODE: i32 = 80877103;
const PG_TEXT_OID: i32 = 25;

/// PostgreSQL Wire Protocol authentication mode (Wave 5).
#[derive(Debug, Clone, Default)]
pub enum PgAuthConfig {
    /// Trust local/dev connections without password challenge (default).
    #[default]
    Trust,
    /// Full RFC 5802 / PostgreSQL 16 **SCRAM-SHA-256** authentication (`user -> password`).
    ScramSha256(HashMap<String, String>),
}

/// Builds a `tokio_rustls::TlsAcceptor` from PEM certificate chain and private key bytes (Wave 10).
pub fn build_tls_acceptor_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<TlsAcceptor> {
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert_pem))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| RustLakeError::Execution(format!("parse TLS cert PEM: {e}")))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_pem))
        .map_err(|e| RustLakeError::Execution(format!("parse TLS key PEM: {e}")))?
        .ok_or_else(|| RustLakeError::Execution("no private key found in TLS key PEM".into()))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| RustLakeError::Execution(format!("TLS protocol versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| RustLakeError::Execution(format!("TLS server config: {e}")))?;
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

pub fn load_tls_acceptor_from_files(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<TlsAcceptor> {
    let cert = std::fs::read(cert_path)?;
    let key = std::fs::read(key_path)?;
    build_tls_acceptor_from_pem(&cert, &key)
}

/// PostgreSQL Wire Protocol v3.0 TCP Gateway (`psql`, `pgjdbc`, DBeaver, DataGrip, SQLx, psycopg compatible).
/// Supports Simple Query (`Q`), Extended Query (`P/B/D/E/C/H/S`), **SCRAM-SHA-256** AuthN, and **TLS 1.3** (`SSLRequest -> 'S'`)!
pub struct PgGateway {
    listener: TcpListener,
    coordinator: Arc<Coordinator>,
    auth: Arc<PgAuthConfig>,
    tls: Option<TlsAcceptor>,
}

impl PgGateway {
    pub async fn bind(addr: &str, coordinator: Arc<Coordinator>) -> Result<Self> {
        let auth = match std::env::var("RUSTLAKEDB_PASSWORD") {
            Ok(pw) if !pw.is_empty() => {
                let mut map = HashMap::new();
                map.insert("postgres".to_string(), pw);
                PgAuthConfig::ScramSha256(map)
            }
            _ => PgAuthConfig::Trust,
        };
        let tls = match (
            std::env::var("RUSTLAKEDB_TLS_CERT"),
            std::env::var("RUSTLAKEDB_TLS_KEY"),
        ) {
            (Ok(c), Ok(k)) if !c.is_empty() && !k.is_empty() => {
                Some(load_tls_acceptor_from_files(c, k)?)
            }
            _ => None,
        };
        Self::bind_full(addr, coordinator, auth, tls).await
    }

    pub async fn bind_with_auth(
        addr: &str,
        coordinator: Arc<Coordinator>,
        auth: PgAuthConfig,
    ) -> Result<Self> {
        Self::bind_full(addr, coordinator, auth, None).await
    }

    pub async fn bind_full(
        addr: &str,
        coordinator: Arc<Coordinator>,
        auth: PgAuthConfig,
        tls: Option<TlsAcceptor>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            coordinator,
            auth: Arc::new(auth),
            tls,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn serve(self) -> Result<()> {
        loop {
            let (stream, peer) = self.listener.accept().await?;
            let coord = self.coordinator.clone();
            let auth = self.auth.clone();
            let tls = self.tls.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, coord, auth, tls).await {
                    tracing::debug!(%peer, error = %e, "pgwire connection closed");
                }
            });
        }
    }
}

struct PortalState {
    sql: String,
    described: bool,
    cached_result: Option<QueryExecutionResult>,
}

async fn handle_connection(
    mut stream: TcpStream,
    coord: Arc<Coordinator>,
    auth: Arc<PgAuthConfig>,
    tls: Option<TlsAcceptor>,
) -> Result<()> {
    // 1. Handle optional SSLRequest (80877103); if TLS is configured, reply 'S' and upgrade to TLS 1.3!
    let first_len = match stream.read_i32().await {
        Ok(l) => l,
        Err(_) => return Ok(()),
    };
    if first_len < 8 {
        return Ok(());
    }
    let first_code = stream.read_i32().await?;
    let remaining = (first_len - 8) as usize;
    let mut first_payload = vec![0u8; remaining];
    if remaining > 0 {
        stream.read_exact(&mut first_payload).await?;
    }

    if first_code == SSL_REQUEST_CODE {
        if let Some(acceptor) = tls {
            stream.write_all(b"S").await?;
            let tls_stream = acceptor
                .accept(stream)
                .await
                .map_err(|e| RustLakeError::Execution(format!("TLS handshake failed: {e}")))?;
            return handle_pg_stream(tls_stream, coord, auth, None).await;
        } else {
            stream.write_all(b"N").await?;
            return handle_pg_stream(stream, coord, auth, None).await;
        }
    }

    handle_pg_stream(stream, coord, auth, Some(first_payload)).await
}

async fn handle_pg_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    coord: Arc<Coordinator>,
    auth: Arc<PgAuthConfig>,
    initial_startup_payload: Option<Vec<u8>>,
) -> Result<()> {
    let payload = match initial_startup_payload {
        Some(p) => p,
        None => {
            let len = match stream.read_i32().await {
                Ok(l) => l,
                Err(_) => return Ok(()),
            };
            if len < 8 {
                return Ok(());
            }
            let _proto = stream.read_i32().await?;
            let rem = (len - 8) as usize;
            let mut buf = vec![0u8; rem];
            if rem > 0 {
                stream.read_exact(&mut buf).await?;
            }
            buf
        }
    };

    let mut session_user = "postgres".to_string();
    let mut idx = 0;
    while idx < payload.len() && payload[idx] != 0 {
        let (k, next) = read_cstr(&payload, idx);
        let (v, after) = read_cstr(&payload, next);
        if k == "user" && !v.is_empty() {
            session_user = v;
        }
        idx = after;
    }
    let session = SessionContext::for_user(session_user.clone());

    // 1b. Optional SCRAM-SHA-256 Authentication Challenge (RFC 5802 / PostgreSQL 16)
    if let PgAuthConfig::ScramSha256(ref users) = *auth
        && let Err(e) = perform_scram_auth(&mut stream, &session_user, users).await
    {
        let mut err_buf = BytesMut::new();
        write_error_response(&mut err_buf, "28P01", &e.to_string());
        let _ = stream.write_all(&err_buf).await;
        return Ok(());
    }

    // 2. Send AuthenticationOk, full pgjdbc/DBeaver ParameterStatus set, BackendKeyData, ReadyForQuery.
    let mut handshake = BytesMut::new();
    write_auth_ok(&mut handshake);
    write_param_status(&mut handshake, "server_version", "16.0 (RustLakeDB 2.0.0)");
    write_param_status(&mut handshake, "server_version_num", "160000");
    write_param_status(&mut handshake, "server_encoding", "UTF8");
    write_param_status(&mut handshake, "client_encoding", "UTF8");
    write_param_status(&mut handshake, "standard_conforming_strings", "on");
    write_param_status(&mut handshake, "integer_datetimes", "on");
    write_param_status(&mut handshake, "DateStyle", "ISO, MDY");
    write_param_status(&mut handshake, "TimeZone", "UTC");
    write_param_status(&mut handshake, "is_superuser", "on");
    write_param_status(&mut handshake, "session_authorization", &session_user);
    write_backend_key(&mut handshake, 1, 42);
    write_ready_for_query(&mut handshake, b'I');
    stream.write_all(&handshake).await?;

    // Session state for Extended Query Protocol (prepared statements + OIDs, bound portals, error barrier).
    let mut prepared_stmts: HashMap<String, (String, Vec<u32>)> = HashMap::new();
    let mut portals: HashMap<String, PortalState> = HashMap::new();
    let mut extended_error = false;

    // 3. Main PostgreSQL v3 message loop.
    loop {
        let msg_type = match stream.read_u8().await {
            Ok(b) => b,
            Err(_) => return Ok(()),
        };
        let len = stream.read_i32().await?;
        let body_len = (len - 4).max(0) as usize;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            stream.read_exact(&mut body).await?;
        }

        // PostgreSQL v3 spec: after an error in Extended Query mode, ignore P/B/D/E/C/H until Sync ('S').
        if extended_error && !matches!(msg_type, b'S' | b'Q' | b'X') {
            continue;
        }

        let mut out = BytesMut::new();
        match msg_type {
            b'Q' => {
                extended_error = false;
                // Simple Query protocol (supports multi-statement SQL separated by `;`).
                let raw_sql = String::from_utf8_lossy(&body)
                    .trim_end_matches('\0')
                    .trim()
                    .to_string();
                let statements: Vec<String> = split_sql_statements(&raw_sql);
                if statements.is_empty() {
                    write_empty_query(&mut out);
                } else {
                    for sql in statements {
                        tracing::debug!(sql = %sql, "pgwire simple query");
                        match coord.execute_sql_with_session(&sql, "main", &session).await {
                            Ok(res) => {
                                if !res.batch.columns.is_empty() {
                                    write_row_description(&mut out, &res.batch);
                                    write_data_rows(&mut out, &res.batch);
                                }
                                write_command_complete(&mut out, &res.command_tag);
                            }
                            Err(e) => {
                                tracing::warn!(sql = %sql, error = %e, "pgwire query error");
                                write_error_response(&mut out, "42000", &e.to_string());
                                break;
                            }
                        }
                    }
                }
                write_ready_for_query(&mut out, b'I');
                stream.write_all(&out).await?;
            }
            b'P' => {
                // Extended Query: Parse(stmt_name\0, query\0, int16 num_params, int32 param_oids[])
                let (stmt_name, pos) = read_cstr(&body, 0);
                let (sql, mut pos) = read_cstr(&body, pos);
                let num_oids = read_i16(&body, &mut pos).max(0) as usize;
                let mut oids = Vec::with_capacity(num_oids);
                for _ in 0..num_oids {
                    oids.push(read_i32(&body, &mut pos) as u32);
                }
                tracing::debug!(stmt = %stmt_name, sql = %sql, "pgwire parse");
                prepared_stmts.insert(stmt_name, (sql, oids));
                out.put_u8(b'1'); // ParseComplete
                out.put_i32(4);
                stream.write_all(&out).await?;
            }
            b'B' => {
                // Extended Query: Bind(portal\0, stmt\0, format_codes, param_values, result_formats)
                // Supports BOTH text (fmt=0) AND binary (fmt=1: int2/int4/int8/float4/float8/bool) PGWire params (Wave 32)!
                let (portal_name, pos) = read_cstr(&body, 0);
                let (stmt_name, mut pos) = read_cstr(&body, pos);
                let num_pfmt = read_i16(&body, &mut pos).max(0) as usize;
                let mut pfmts = Vec::with_capacity(num_pfmt);
                for _ in 0..num_pfmt {
                    pfmts.push(read_i16(&body, &mut pos));
                }
                let num_params = read_i16(&body, &mut pos).max(0) as usize;
                let stmt_oids = prepared_stmts
                    .get(&stmt_name)
                    .map(|(_, oids)| oids.as_slice())
                    .unwrap_or(&[]);
                let mut params: Vec<Option<String>> = Vec::with_capacity(num_params);
                for i in 0..num_params {
                    let plen = read_i32(&body, &mut pos);
                    if plen < 0 {
                        params.push(None);
                    } else {
                        let plen = plen as usize;
                        let raw = &body[pos..pos + plen];
                        pos += plen;
                        let fmt = if pfmts.len() == 1 {
                            pfmts[0]
                        } else {
                            pfmts.get(i).copied().unwrap_or(0)
                        };
                        let oid = stmt_oids.get(i).copied().unwrap_or(0);
                        let decoded = if fmt == 1 {
                            decode_pg_binary_param(raw, oid)
                        } else {
                            String::from_utf8_lossy(raw).to_string()
                        };
                        params.push(Some(decoded));
                    }
                }
                if let Some((template, _)) = prepared_stmts.get(&stmt_name) {
                    let bound_sql = substitute_params(template, &params);
                    portals.insert(
                        portal_name,
                        PortalState {
                            sql: bound_sql,
                            described: false,
                            cached_result: None,
                        },
                    );
                    out.put_u8(b'2'); // BindComplete
                    out.put_i32(4);
                } else {
                    write_error_response(
                        &mut out,
                        "26000",
                        &format!("prepared statement '{stmt_name}' does not exist"),
                    );
                }
                stream.write_all(&out).await?;
            }
            b'D' => {
                // Extended Query: Describe ('S' statement or 'P' portal)
                // Critical for pgjdbc/DBeaver: Portal Describe MUST emit RowDescription ('T') before Execute!
                let kind = body.first().copied().unwrap_or(b'P');
                let (name, _) = read_cstr(&body, 1);
                if kind == b'S' {
                    out.put_u8(b't'); // ParameterDescription
                    out.put_i32(6);
                    out.put_i16(0);
                    out.put_u8(b'n'); // NoData
                    out.put_i32(4);
                } else if let Some(portal) = portals.get_mut(&name) {
                    tracing::debug!(portal = %name, sql = %portal.sql, "pgwire describe portal");
                    match coord
                        .execute_sql_with_session(&portal.sql, "main", &session)
                        .await
                    {
                        Ok(res) => {
                            if !res.batch.columns.is_empty() {
                                write_row_description(&mut out, &res.batch);
                            } else {
                                out.put_u8(b'n'); // NoData
                                out.put_i32(4);
                            }
                            portal.described = true;
                            portal.cached_result = Some(res);
                        }
                        Err(e) => {
                            tracing::warn!(sql = %portal.sql, error = %e, "pgwire describe error");
                            extended_error = true;
                            write_error_response(&mut out, "42000", &e.to_string());
                        }
                    }
                } else {
                    out.put_u8(b'n');
                    out.put_i32(4);
                }
                stream.write_all(&out).await?;
            }
            b'E' => {
                // Extended Query: Execute(portal\0, max_rows: i32)
                let (portal_name, _) = read_cstr(&body, 0);
                if let Some(portal) = portals.get_mut(&portal_name) {
                    let already_described = portal.described;
                    let result = if let Some(cached) = portal.cached_result.take() {
                        Ok(cached)
                    } else {
                        coord
                            .execute_sql_with_session(&portal.sql, "main", &session)
                            .await
                    };
                    match result {
                        Ok(res) => {
                            if !res.batch.columns.is_empty() {
                                if !already_described {
                                    write_row_description(&mut out, &res.batch);
                                }
                                write_data_rows(&mut out, &res.batch);
                            }
                            write_command_complete(&mut out, &res.command_tag);
                        }
                        Err(e) => {
                            tracing::warn!(sql = %portal.sql, error = %e, "pgwire execute error");
                            extended_error = true;
                            write_error_response(&mut out, "42000", &e.to_string());
                        }
                    }
                } else {
                    write_error_response(
                        &mut out,
                        "34000",
                        &format!("portal '{portal_name}' does not exist"),
                    );
                }
                stream.write_all(&out).await?;
            }
            b'C' => {
                let kind = body.first().copied().unwrap_or(b'P');
                let (name, _) = read_cstr(&body, 1);
                if kind == b'S' {
                    prepared_stmts.remove(&name);
                } else {
                    portals.remove(&name);
                }
                out.put_u8(b'3'); // CloseComplete
                out.put_i32(4);
                stream.write_all(&out).await?;
            }
            b'H' => {
                // Flush: no-op (we already flush after each message)
            }
            b'S' => {
                extended_error = false;
                write_ready_for_query(&mut out, b'I');
                stream.write_all(&out).await?;
            }
            b'X' => return Ok(()),
            _ => {
                write_error_response(
                    &mut out,
                    "0A000",
                    &format!("unsupported pgwire message type '{}'", msg_type as char),
                );
                write_ready_for_query(&mut out, b'I');
                stream.write_all(&out).await?;
            }
        }
    }
}

fn split_sql_statements(input: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut cur = String::new();
    let mut in_single = false;
    for ch in input.chars() {
        if ch == '\'' {
            in_single = !in_single;
            cur.push(ch);
        } else if ch == ';' && !in_single {
            let trimmed = cur.trim();
            if !trimmed.is_empty() {
                stmts.push(trimmed.to_string());
            }
            cur.clear();
        } else {
            cur.push(ch);
        }
    }
    let tail = cur.trim();
    if !tail.is_empty() {
        stmts.push(tail.to_string());
    }
    stmts
}

fn read_cstr(buf: &[u8], start: usize) -> (String, usize) {
    if start >= buf.len() {
        return (String::new(), buf.len());
    }
    match buf[start..].iter().position(|&b| b == 0) {
        Some(rel) => (
            String::from_utf8_lossy(&buf[start..start + rel]).to_string(),
            start + rel + 1,
        ),
        None => (
            String::from_utf8_lossy(&buf[start..]).to_string(),
            buf.len(),
        ),
    }
}

fn read_i16(buf: &[u8], pos: &mut usize) -> i16 {
    if *pos + 2 > buf.len() {
        return 0;
    }
    let v = i16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    v
}

fn read_i32(buf: &[u8], pos: &mut usize) -> i32 {
    if *pos + 4 > buf.len() {
        return 0;
    }
    let v = i32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    v
}

fn decode_pg_binary_param(raw: &[u8], oid: u32) -> String {
    match (oid, raw.len()) {
        (16, 1) => (raw[0] != 0).to_string(),
        (21, 2) => i16::from_be_bytes([raw[0], raw[1]]).to_string(),
        (23, 4) => i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]).to_string(),
        (20, 8) => i64::from_be_bytes(raw.try_into().unwrap_or([0; 8])).to_string(),
        (700, 4) => f32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]).to_string(),
        (701, 8) => f64::from_be_bytes(raw.try_into().unwrap_or([0; 8])).to_string(),
        // When client passes OID 0 (unspecified) with binary format, infer standard numeric widths
        (0, 1) if raw[0] <= 1 => (raw[0] != 0).to_string(),
        (0, 4) => i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]).to_string(),
        (0, 8) => i64::from_be_bytes(raw.try_into().unwrap_or([0; 8])).to_string(),
        _ => String::from_utf8_lossy(raw).to_string(),
    }
}

fn substitute_params(template: &str, params: &[Option<String>]) -> String {
    let mut sql = template.to_string();
    for idx in (1..=params.len()).rev() {
        let placeholder = format!("${idx}");
        let replacement = match &params[idx - 1] {
            None => "NULL".to_string(),
            Some(v) => {
                if v.parse::<i64>().is_ok()
                    || v.parse::<f64>().is_ok()
                    || v.eq_ignore_ascii_case("true")
                    || v.eq_ignore_ascii_case("false")
                {
                    v.clone()
                } else {
                    format!("'{}'", v.replace('\'', "''"))
                }
            }
        };
        sql = sql.replace(&placeholder, &replacement);
    }
    sql
}

fn write_auth_ok(buf: &mut BytesMut) {
    buf.put_u8(b'R');
    buf.put_i32(8);
    buf.put_i32(0);
}

async fn perform_scram_auth<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    user: &str,
    users: &HashMap<String, String>,
) -> Result<()> {
    // 1. Send AuthenticationSASL (auth_type = 10, mechanisms = ["SCRAM-SHA-256"])
    let mech = b"SCRAM-SHA-256\0\0";
    let mut sasl_init = BytesMut::new();
    sasl_init.put_u8(b'R');
    sasl_init.put_i32((8 + mech.len()) as i32);
    sasl_init.put_i32(10);
    sasl_init.put_slice(mech);
    stream.write_all(&sasl_init).await?;

    // 2. Read SASLInitialResponse ('p')
    let tag = stream.read_u8().await?;
    if tag != b'p' {
        return Err(RustLakeError::Execution(
            "expected SASLInitialResponse ('p')".into(),
        ));
    }
    let len = stream.read_i32().await?;
    let mut body = vec![0u8; (len - 4).max(0) as usize];
    stream.read_exact(&mut body).await?;
    let (chosen_mech, mut pos) = read_cstr(&body, 0);
    if chosen_mech != "SCRAM-SHA-256" {
        return Err(RustLakeError::Execution(format!(
            "unsupported SASL mechanism '{chosen_mech}'"
        )));
    }
    let resp_len = read_i32(&body, &mut pos).max(0) as usize;
    let client_first = String::from_utf8_lossy(&body[pos..pos + resp_len]).to_string();

    // 3. Send AuthenticationSASLContinue (auth_type = 11, server-first-message)
    let challenge = scram::scram_server_first(&client_first)?;
    let mut cont = BytesMut::new();
    cont.put_u8(b'R');
    cont.put_i32((8 + challenge.server_first_message.len()) as i32);
    cont.put_i32(11);
    cont.put_slice(challenge.server_first_message.as_bytes());
    stream.write_all(&cont).await?;

    // 4. Read SASLResponse ('p', client-final-message)
    let tag2 = stream.read_u8().await?;
    if tag2 != b'p' {
        return Err(RustLakeError::Execution(
            "expected SASLResponse ('p')".into(),
        ));
    }
    let len2 = stream.read_i32().await?;
    let mut body2 = vec![0u8; (len2 - 4).max(0) as usize];
    stream.read_exact(&mut body2).await?;
    let client_final = String::from_utf8_lossy(&body2).to_string();

    // 5. Verify cryptographic ClientProof against expected password
    let expected_pw = users.get(user).ok_or_else(|| {
        RustLakeError::Execution(format!("password authentication failed for user \"{user}\""))
    })?;
    let server_final = scram::scram_server_verify(&challenge, &client_final, expected_pw)?;

    // 6. Send AuthenticationSASLFinal (auth_type = 12, v=<server_signature>)
    let mut fin = BytesMut::new();
    fin.put_u8(b'R');
    fin.put_i32((8 + server_final.len()) as i32);
    fin.put_i32(12);
    fin.put_slice(server_final.as_bytes());
    stream.write_all(&fin).await?;
    Ok(())
}

fn write_param_status(buf: &mut BytesMut, key: &str, val: &str) {
    let len = 4 + key.len() + 1 + val.len() + 1;
    buf.put_u8(b'S');
    buf.put_i32(len as i32);
    buf.put_slice(key.as_bytes());
    buf.put_u8(0);
    buf.put_slice(val.as_bytes());
    buf.put_u8(0);
}

fn write_backend_key(buf: &mut BytesMut, pid: i32, secret: i32) {
    buf.put_u8(b'K');
    buf.put_i32(12);
    buf.put_i32(pid);
    buf.put_i32(secret);
}

fn write_ready_for_query(buf: &mut BytesMut, status: u8) {
    buf.put_u8(b'Z');
    buf.put_i32(5);
    buf.put_u8(status);
}

fn write_empty_query(buf: &mut BytesMut) {
    buf.put_u8(b'I');
    buf.put_i32(4);
}

fn write_row_description(buf: &mut BytesMut, batch: &RowBatch) {
    let mut inner = BytesMut::new();
    inner.put_i16(batch.columns.len() as i16);
    for col in &batch.columns {
        inner.put_slice(col.as_bytes());
        inner.put_u8(0);
        inner.put_i32(0);
        inner.put_i16(0);
        inner.put_i32(PG_TEXT_OID);
        inner.put_i16(-1);
        inner.put_i32(-1);
        inner.put_i16(0);
    }
    buf.put_u8(b'T');
    buf.put_i32((4 + inner.len()) as i32);
    buf.put_slice(&inner);
}

fn write_data_rows(buf: &mut BytesMut, batch: &RowBatch) {
    for row in &batch.rows {
        let mut inner = BytesMut::new();
        inner.put_i16(batch.columns.len() as i16);
        for col in &batch.columns {
            match row.get(col).and_then(|v| v.to_pg_text()) {
                Some(txt) => {
                    inner.put_i32(txt.len() as i32);
                    inner.put_slice(txt.as_bytes());
                }
                None => {
                    inner.put_i32(-1);
                }
            }
        }
        buf.put_u8(b'D');
        buf.put_i32((4 + inner.len()) as i32);
        buf.put_slice(&inner);
    }
}

fn write_command_complete(buf: &mut BytesMut, tag: &str) {
    buf.put_u8(b'C');
    buf.put_i32((4 + tag.len() + 1) as i32);
    buf.put_slice(tag.as_bytes());
    buf.put_u8(0);
}

fn write_error_response(buf: &mut BytesMut, sqlstate: &str, msg: &str) {
    let mut inner = BytesMut::new();
    inner.put_u8(b'S');
    inner.put_slice(b"ERROR\0");
    inner.put_u8(b'C');
    inner.put_slice(sqlstate.as_bytes());
    inner.put_u8(0);
    inner.put_u8(b'M');
    inner.put_slice(msg.as_bytes());
    inner.put_u8(0);
    inner.put_u8(0);

    buf.put_u8(b'E');
    buf.put_i32((4 + inner.len()) as i32);
    buf.put_slice(&inner);
}
