// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use base64::Engine;
use parking_lot::RwLock;
use ring::hmac;
use rustlakedb_catalog::TableCommitProposal;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{FilterExpr, RowBatch, ScalarValue};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Authenticated session context propagated from PostgreSQL v3 Wire Protocol or HTTP API.
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub user: String,
    pub role: String,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            user: "postgres".into(),
            role: "postgres".into(),
        }
    }
}

impl SessionContext {
    pub fn for_user(user: impl Into<String>) -> Self {
        let u = user.into();
        Self {
            role: u.clone(),
            user: u,
        }
    }

    pub fn is_superuser(&self) -> bool {
        self.role.eq_ignore_ascii_case("postgres") || self.role.eq_ignore_ascii_case("admin")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskingRule {
    pub name: String,
    /// Direct column name (`email`) or JSON path (`payload.ssn` / `payload->>'ssn'`).
    pub target: String,
    pub mask_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHistoryEntry {
    pub query_id: String,
    pub user: String,
    pub sql: String,
    pub status: String,
    pub rows_returned: usize,
    pub elapsed_ms: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub timestamp_ms: u64,
    pub user: String,
    pub action: String,
    pub target: String,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveQueryInfo {
    pub query_id: String,
    pub user: String,
    pub sql: String,
    pub started_ms: u64,
}

/// Enterprise Governance, RBAC, RLS, Dynamic Masking, WLM & Observability Engine (Waves 3–4).
pub struct GovernanceState {
    roles: RwLock<HashSet<String>>,
    /// (role, table) -> granted privileges ("SELECT", "INSERT", "UPDATE", "DELETE", "ALL")
    grants: RwLock<HashMap<(String, String), HashSet<String>>>,
    /// (role, table) -> RLS filter predicate injected automatically onto table scans
    rls_policies: RwLock<HashMap<(String, String), FilterExpr>>,
    /// (role, table) -> Dynamic column / JSON-path masking rules
    masking_policies: RwLock<HashMap<(String, String), Vec<MaskingRule>>>,
    /// Ring buffer for `system.query_history`
    query_history: RwLock<VecDeque<QueryHistoryEntry>>,
    /// Ring buffer for `system.audit_log`
    audit_log: RwLock<VecDeque<AuditLogEntry>>,
    /// Active queries for WLM & `SHOW QUERIES`
    active_queries: RwLock<HashMap<String, ActiveQueryInfo>>,
    /// Query-driven JSON path hotness tracker: table -> (col.path -> hit_count)
    hot_json_paths: RwLock<HashMap<String, HashMap<String, u64>>>,
    /// Open multi-statement ACID transactions keyed by session user (`BEGIN` -> `COMMIT`/`ROLLBACK`, Wave 6)
    open_txs: RwLock<HashMap<String, Vec<TableCommitProposal>>>,
    /// Optional HMAC-SHA256 JWT secret for Control Plane Bearer token AuthN (Wave 7)
    jwt_secret: RwLock<Option<String>>,
    /// Registered SQL Scalar UDFs: lowercase_name -> (param_name, body_expr) (Wave 10)
    udfs: RwLock<HashMap<String, (String, String)>>,
    /// Logical schemas created via `CREATE SCHEMA` (Wave 9 dbt compat)
    schemas: RwLock<HashSet<String>>,
    /// Prometheus metrics counters
    pub queries_total: AtomicU64,
    pub query_errors_total: AtomicU64,
    pub query_duration_ms_sum: AtomicU64,
    pub max_concurrent_queries: AtomicUsize,
}

impl Default for GovernanceState {
    fn default() -> Self {
        let mut roles = HashSet::new();
        roles.insert("postgres".into());
        roles.insert("admin".into());
        Self {
            roles: RwLock::new(roles),
            grants: RwLock::new(HashMap::new()),
            rls_policies: RwLock::new(HashMap::new()),
            masking_policies: RwLock::new(HashMap::new()),
            query_history: RwLock::new(VecDeque::with_capacity(512)),
            audit_log: RwLock::new(VecDeque::with_capacity(512)),
            active_queries: RwLock::new(HashMap::new()),
            hot_json_paths: RwLock::new(HashMap::new()),
            open_txs: RwLock::new(HashMap::new()),
            jwt_secret: RwLock::new(
                std::env::var("RUSTLAKEDB_JWT_SECRET")
                    .ok()
                    .filter(|s| !s.is_empty()),
            ),
            udfs: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::from(["public".to_string(), "default".to_string()])),
            queries_total: AtomicU64::new(0),
            query_errors_total: AtomicU64::new(0),
            query_duration_ms_sum: AtomicU64::new(0),
            max_concurrent_queries: AtomicUsize::new(64),
        }
    }
}

impl GovernanceState {
    pub fn create_role(&self, role: &str) {
        self.roles.write().insert(role.to_string());
    }

    pub fn grant(&self, role: &str, table: &str, privilege: &str) {
        self.roles.write().insert(role.to_string());
        self.grants
            .write()
            .entry((role.to_string(), table.to_string()))
            .or_default()
            .insert(privilege.to_uppercase());
    }

    pub fn revoke(&self, role: &str, table: &str, privilege: &str) {
        let mut guard = self.grants.write();
        if let Some(set) = guard.get_mut(&(role.to_string(), table.to_string())) {
            let p = privilege.to_uppercase();
            if p == "ALL" {
                set.clear();
            } else {
                set.remove(&p);
            }
        }
    }

    pub fn check_access(&self, session: &SessionContext, table: &str, privilege: &str) -> Result<()> {
        if session.is_superuser() {
            return Ok(());
        }
        let guard = self.grants.read();
        if let Some(privs) = guard.get(&(session.role.clone(), table.to_string()))
            && (privs.contains("ALL") || privs.contains(&privilege.to_uppercase()))
        {
            return Ok(());
        }
        self.record_audit(
            &session.user,
            privilege,
            table,
            "DENIED (42501)",
        );
        Err(RustLakeError::Execution(format!(
            "permission denied (42501): role '{}' lacks {} privilege on table '{}'",
            session.role, privilege, table
        )))
    }

    pub fn add_rls_policy(&self, role: &str, table: &str, predicate: FilterExpr) {
        self.rls_policies
            .write()
            .insert((role.to_string(), table.to_string()), predicate);
    }

    pub fn get_rls_policy(&self, session: &SessionContext, table: &str) -> Option<FilterExpr> {
        if session.is_superuser() {
            return None;
        }
        self.rls_policies
            .read()
            .get(&(session.role.clone(), table.to_string()))
            .cloned()
    }

    pub fn add_masking_policy(&self, role: &str, table: &str, rule: MaskingRule) {
        self.masking_policies
            .write()
            .entry((role.to_string(), table.to_string()))
            .or_default()
            .push(rule);
    }

    pub fn apply_masking(&self, session: &SessionContext, tables: &[String], batch: &mut RowBatch) {
        if session.is_superuser() || tables.is_empty() {
            return;
        }
        let guard = self.masking_policies.read();
        let mut rules: Vec<MaskingRule> = Vec::new();
        for t in tables {
            if let Some(list) = guard.get(&(session.role.clone(), t.clone())) {
                rules.extend(list.clone());
            }
        }
        if rules.is_empty() {
            return;
        }

        for row in &mut batch.rows {
            for rule in &rules {
                // Support both direct column (`email`) and JSON path (`payload.ssn` or `payload->>ssn`).
                if let Some((json_col, json_path)) = rule
                    .target
                    .split_once('.')
                    .or_else(|| rule.target.split_once("->>"))
                {
                    let clean_path = json_path.trim_matches('\'');
                    if let Some(ScalarValue::Json(doc)) = row.get_mut(json_col) {
                        mask_json_path(doc, clean_path, &rule.mask_value);
                    }
                    // Also mask any projected column named `<col>_<path>` or `<path>`!
                    let default_alias = format!("{json_col}_{clean_path}");
                    if row.contains_key(&default_alias) {
                        row.insert(default_alias, ScalarValue::Utf8(rule.mask_value.clone()));
                    }
                    if row.contains_key(clean_path) {
                        row.insert(
                            clean_path.to_string(),
                            ScalarValue::Utf8(rule.mask_value.clone()),
                        );
                    }
                } else if row.contains_key(&rule.target) {
                    row.insert(
                        rule.target.clone(),
                        ScalarValue::Utf8(rule.mask_value.clone()),
                    );
                }
            }
        }
    }

    pub fn record_query(
        &self,
        query_id: String,
        user: &str,
        sql: &str,
        status: &str,
        rows_returned: usize,
        elapsed_ms: u64,
    ) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.query_duration_ms_sum
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        if status != "OK" {
            self.query_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        let mut hist = self.query_history.write();
        if hist.len() >= 500 {
            hist.pop_front();
        }
        hist.push_back(QueryHistoryEntry {
            query_id,
            user: user.to_string(),
            sql: sql.to_string(),
            status: status.to_string(),
            rows_returned,
            elapsed_ms,
            timestamp_ms: epoch_ms(),
        });
    }

    pub fn query_history_snapshot(&self) -> Vec<QueryHistoryEntry> {
        self.query_history.read().iter().cloned().collect()
    }

    pub fn record_audit(&self, user: &str, action: &str, target: &str, outcome: &str) {
        let mut log = self.audit_log.write();
        if log.len() >= 500 {
            log.pop_front();
        }
        log.push_back(AuditLogEntry {
            timestamp_ms: epoch_ms(),
            user: user.to_string(),
            action: action.to_string(),
            target: target.to_string(),
            outcome: outcome.to_string(),
        });
    }

    pub fn audit_log_snapshot(&self) -> Vec<AuditLogEntry> {
        self.audit_log.read().iter().cloned().collect()
    }

    pub fn register_active_query(&self, query_id: &str, user: &str, sql: &str) -> Result<()> {
        let mut active = self.active_queries.write();
        let cap = self.max_concurrent_queries.load(Ordering::Relaxed);
        if active.len() >= cap {
            return Err(RustLakeError::Execution(format!(
                "WLM resource limit exceeded: {cap} concurrent queries active"
            )));
        }
        active.insert(
            query_id.to_string(),
            ActiveQueryInfo {
                query_id: query_id.to_string(),
                user: user.to_string(),
                sql: sql.to_string(),
                started_ms: epoch_ms(),
            },
        );
        Ok(())
    }

    pub fn finish_active_query(&self, query_id: &str) {
        self.active_queries.write().remove(query_id);
    }

    pub fn active_queries_snapshot(&self) -> Vec<ActiveQueryInfo> {
        self.active_queries.read().values().cloned().collect()
    }

    pub fn record_hot_json_path(&self, table: &str, column: &str, path: &str) {
        let key = format!("{column}.{path}");
        *self
            .hot_json_paths
            .write()
            .entry(table.to_string())
            .or_default()
            .entry(key)
            .or_insert(0) += 1;
    }

    pub fn hot_json_paths_for(&self, table: &str) -> Vec<String> {
        let guard = self.hot_json_paths.read();
        let Some(map) = guard.get(table) else {
            return vec![];
        };
        let mut pairs: Vec<(&String, &u64)> = map.iter().collect();
        pairs.sort_by(|a, b| b.1.cmp(a.1));
        pairs.into_iter().take(16).map(|(k, _)| k.clone()).collect()
    }

    // ---- Wave 6: Multi-Statement ACID Transaction Buffer ----
    pub fn begin_tx(&self, user: &str) {
        self.open_txs.write().insert(user.to_string(), Vec::new());
    }

    pub fn in_tx(&self, user: &str) -> bool {
        self.open_txs.read().contains_key(user)
    }

    pub fn buffer_tx_proposal(&self, user: &str, proposal: TableCommitProposal) {
        if let Some(list) = self.open_txs.write().get_mut(user) {
            list.push(proposal);
        }
    }

    pub fn take_tx_proposals(&self, user: &str) -> Option<Vec<TableCommitProposal>> {
        self.open_txs.write().remove(user)
    }

    // ---- Wave 7: HMAC-SHA256 JWT Bearer Authentication ----
    pub fn set_jwt_secret(&self, secret: Option<String>) {
        *self.jwt_secret.write() = secret;
    }

    pub fn jwt_secret(&self) -> Option<String> {
        self.jwt_secret.read().clone()
    }

    // ---- Wave 9: Logical Schema Registry (`CREATE/DROP SCHEMA`) ----
    pub fn create_schema(&self, name: &str, if_not_exists: bool) -> Result<()> {
        let mut guard = self.schemas.write();
        let key = name.to_lowercase();
        if guard.contains(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(RustLakeError::Catalog(format!(
                "schema '{name}' already exists"
            )));
        }
        guard.insert(key);
        Ok(())
    }

    pub fn drop_schema(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut guard = self.schemas.write();
        let key = name.to_lowercase();
        if !guard.remove(&key) && !if_exists {
            return Err(RustLakeError::Catalog(format!(
                "schema '{name}' does not exist"
            )));
        }
        Ok(())
    }

    pub fn list_schemas(&self) -> Vec<String> {
        let mut out: Vec<String> = self.schemas.read().iter().cloned().collect();
        out.sort();
        out
    }

    // ---- Wave 10: SQL Scalar User-Defined Functions (`CREATE FUNCTION`) ----
    pub fn register_udf(&self, name: &str, param_name: &str, body_expr: &str) {
        self.udfs.write().insert(
            name.to_lowercase(),
            (param_name.to_string(), body_expr.to_string()),
        );
    }

    /// Expands calls to registered SQL UDFs inline (e.g. `net_price(amount)` -> `(ROUND(amount * 0.9, 2))`).
    pub fn expand_udfs(&self, sql: &str) -> String {
        let upper = sql.trim().to_uppercase();
        if upper.starts_with("CREATE FUNCTION ") || upper.starts_with("CREATE OR REPLACE FUNCTION ")
        {
            return sql.to_string();
        }
        let defs: Vec<(String, String, String)> = self
            .udfs
            .read()
            .iter()
            .map(|(k, (p, b))| (k.clone(), p.clone(), b.clone()))
            .collect();
        if defs.is_empty() {
            return sql.to_string();
        }
        let mut cur = sql.to_string();
        for (name, param, body) in defs {
            let needle = format!("{name}(");
            loop {
                let lower = cur.to_lowercase();
                let mut found_pos = None;
                let mut search_from = 0usize;
                while let Some(rel) = lower[search_from..].find(&needle) {
                    let abs = search_from + rel;
                    let boundary_ok = abs == 0
                        || !cur.as_bytes()[abs - 1].is_ascii_alphanumeric()
                            && cur.as_bytes()[abs - 1] != b'_';
                    if boundary_ok {
                        found_pos = Some(abs);
                        break;
                    }
                    search_from = abs + 1;
                }
                let Some(pos) = found_pos else {
                    break;
                };
                let open_p = pos + name.len();
                let Some(close_p) = find_matching_paren_str(&cur, open_p) else {
                    break;
                };
                let arg_expr = cur[open_p + 1..close_p].trim();
                let expanded_body = substitute_ident(&body, &param, arg_expr);
                cur = format!("{cur_pre}({expanded_body}){cur_post}",
                    cur_pre = &cur[..pos],
                    cur_post = &cur[close_p + 1..]
                );
            }
        }
        cur
    }
}

fn find_matching_paren_str(s: &str, open_idx: usize) -> Option<usize> {
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

fn substitute_ident(body: &str, ident: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(body.len() + replacement.len());
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let tok = &body[start..i];
            if tok.eq_ignore_ascii_case(ident) {
                out.push_str(replacement);
            } else {
                out.push_str(tok);
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Signs an RFC 7519 HS256 JWT token for Control Plane authentication (Wave 7).
pub fn sign_jwt_hs256(secret: &str, user: &str, role: &str) -> String {
    let header = B64URL.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let claims = serde_json::json!({
        "sub": user,
        "role": role,
        "iat": epoch_ms() / 1000
    });
    let payload = B64URL.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let sig = hmac::sign(&key, signing_input.as_bytes());
    format!("{signing_input}.{}", B64URL.encode(sig.as_ref()))
}

/// Verifies an HS256 JWT token and extracts `SessionContext { user, role }`.
pub fn verify_jwt_hs256(secret: &str, token: &str) -> Result<SessionContext> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(RustLakeError::Execution("invalid JWT format".into()));
    }
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let sig_bytes = B64URL
        .decode(parts[2])
        .map_err(|_| RustLakeError::Execution("invalid JWT signature encoding".into()))?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::verify(&key, signing_input.as_bytes(), &sig_bytes)
        .map_err(|_| RustLakeError::Execution("JWT signature verification failed".into()))?;
    let payload_bytes = B64URL
        .decode(parts[1])
        .map_err(|_| RustLakeError::Execution("invalid JWT payload encoding".into()))?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| RustLakeError::Execution("invalid JWT JSON claims".into()))?;
    let user = claims["sub"]
        .as_str()
        .ok_or_else(|| RustLakeError::Execution("JWT missing 'sub' claim".into()))?
        .to_string();
    let role = claims["role"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| user.clone());
    Ok(SessionContext { user, role })
}

fn mask_json_path(doc: &mut serde_json::Value, path: &str, mask: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    if segs.is_empty() {
        return;
    }
    let mut cur = doc;
    for seg in &segs[..segs.len() - 1] {
        match cur.get_mut(*seg) {
            Some(next) => cur = next,
            None => return,
        }
    }
    if let Some(obj) = cur.as_object_mut()
        && let Some(last) = segs.last()
        && obj.contains_key(*last)
    {
        obj.insert((*last).to_string(), serde_json::Value::String(mask.to_string()));
    }
}
