// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use base64::Engine;
use parking_lot::RwLock;
use ring::hmac;
use ring::rand::SystemRandom;
use ring::signature::{
    KeyPair, RsaKeyPair, RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_SHA256,
};
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

/// Centralized data lineage graph edge captured automatically on CTAS / INSERT SELECT / VIEW / COPY (Wave 24).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub source_table: String,
    pub target_table: String,
    pub operation: String,
    pub user: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveQueryInfo {
    pub query_id: String,
    pub user: String,
    pub sql: String,
    pub started_ms: u64,
}

/// OpenTelemetry-compatible distributed execution trace span (Wave 13).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub service: String,
    pub start_ms: u64,
    pub duration_ms: u64,
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// RSA public key extracted from an RFC 7517 OIDC JWKS document (Wave 21).
#[derive(Debug, Clone)]
pub struct JwkRsaKey {
    pub kid: Option<String>,
    pub n: Vec<u8>,
    pub e: Vec<u8>,
}

/// Enterprise Governance, RBAC, RLS, Dynamic Masking, WLM & Observability Engine (Waves 3–21).
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
    /// Automatic table-level data lineage graph for `system.lineage` (Wave 24)
    lineage: RwLock<VecDeque<LineageEdge>>,
    /// Active queries for WLM & `SHOW QUERIES`
    active_queries: RwLock<HashMap<String, ActiveQueryInfo>>,
    /// Query IDs explicitly cancelled via `CANCEL QUERY` or `POST /v1/queries/{id}/cancel` (Wave 21)
    cancelled_queries: RwLock<HashSet<String>>,
    /// Query-driven JSON path hotness tracker: table -> (col.path -> hit_count)
    hot_json_paths: RwLock<HashMap<String, HashMap<String, u64>>>,
    /// Open multi-statement ACID transactions keyed by session user (`BEGIN` -> `COMMIT`/`ROLLBACK`, Wave 6)
    open_txs: RwLock<HashMap<String, Vec<TableCommitProposal>>>,
    /// Optional HMAC-SHA256 JWT secret for Control Plane Bearer token AuthN (Wave 7)
    jwt_secret: RwLock<Option<String>>,
    /// Optional OIDC/JWKS RSA public keys for RS256 Bearer token AuthN (Okta/Auth0/Entra/Cognito, Wave 21)
    jwks_keys: RwLock<Vec<JwkRsaKey>>,
    /// OIDC Discovery metadata for automatic JWKS key rotation & iss/aud enforcement (Wave 36)
    oidc_issuer: RwLock<Option<String>>,
    oidc_audience: RwLock<Option<String>>,
    oidc_jwks_uri: RwLock<Option<String>>,
    oidc_last_refresh_ms: AtomicU64,
    oidc_refresh_ttl_ms: AtomicU64,
    /// Registered SQL Scalar UDFs: lowercase_name -> (param_name, body_expr) (Wave 10)
    udfs: RwLock<HashMap<String, (String, String)>>,
    /// Logical schemas created via `CREATE SCHEMA` (Wave 9 dbt compat)
    schemas: RwLock<HashSet<String>>,
    /// OpenTelemetry-compatible distributed trace spans ring buffer (Wave 13)
    trace_spans: RwLock<VecDeque<TraceSpan>>,
    /// Exactly-once CDC stream watermarks: `table::stream_id` -> last committed offset (Wave 13)
    cdc_offsets: RwLock<HashMap<String, u64>>,
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
            lineage: RwLock::new(VecDeque::with_capacity(512)),
            active_queries: RwLock::new(HashMap::new()),
            cancelled_queries: RwLock::new(HashSet::new()),
            hot_json_paths: RwLock::new(HashMap::new()),
            open_txs: RwLock::new(HashMap::new()),
            jwt_secret: RwLock::new(
                std::env::var("RUSTLAKEDB_JWT_SECRET")
                    .ok()
                    .filter(|s| !s.is_empty()),
            ),
            jwks_keys: RwLock::new(Vec::new()),
            oidc_issuer: RwLock::new(None),
            oidc_audience: RwLock::new(None),
            oidc_jwks_uri: RwLock::new(None),
            oidc_last_refresh_ms: AtomicU64::new(0),
            oidc_refresh_ttl_ms: AtomicU64::new(300_000),
            udfs: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::from(["public".to_string(), "default".to_string()])),
            trace_spans: RwLock::new(VecDeque::with_capacity(512)),
            cdc_offsets: RwLock::new(HashMap::new()),
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

    pub fn record_lineage(
        &self,
        source_table: &str,
        target_table: &str,
        operation: &str,
        user: &str,
    ) {
        let mut guard = self.lineage.write();
        if guard.len() >= 1000 {
            guard.pop_front();
        }
        guard.push_back(LineageEdge {
            source_table: source_table.to_string(),
            target_table: target_table.to_string(),
            operation: operation.to_string(),
            user: user.to_string(),
            timestamp_ms: epoch_ms(),
        });
    }

    pub fn lineage_snapshot(&self) -> Vec<LineageEdge> {
        self.lineage.read().iter().cloned().collect()
    }

    /// Hydrates in-memory RBAC, RLS, masking policies, and lineage from the Raft-replicated
    /// Catalog governance state so Coordinator crashes & HA failovers lose zero security rules (Wave 29).
    pub fn hydrate_from_catalog(&self, snap: rustlakedb_catalog::PersistedGovernanceState) {
        if !snap.roles.is_empty() {
            let mut roles = self.roles.write();
            for r in snap.roles {
                roles.insert(r);
            }
        }
        {
            let mut grants = self.grants.write();
            if !snap.grants.is_empty() {
                grants.clear();
                for (r, t, p) in snap.grants {
                    grants.entry((r, t)).or_default().insert(p);
                }
            }
        }
        if !snap.rls_policies.is_empty() {
            let mut rls = self.rls_policies.write();
            for (r, t, pred) in snap.rls_policies {
                rls.insert((r, t), pred);
            }
        }
        if !snap.masking_policies.is_empty() {
            let mut m = self.masking_policies.write();
            m.clear();
            for (r, t, rule) in snap.masking_policies {
                m.entry((r, t)).or_default().push(MaskingRule {
                    name: rule.name,
                    target: rule.target,
                    mask_value: rule.mask_value,
                });
            }
        }
        if !snap.lineage.is_empty() {
            let mut lin = self.lineage.write();
            lin.clear();
            for e in snap.lineage {
                lin.push_back(LineageEdge {
                    source_table: e.source_table,
                    target_table: e.target_table,
                    operation: e.operation,
                    user: e.user,
                    timestamp_ms: e.timestamp_ms,
                });
            }
        }
    }

    /// Loads RFC 7517 OIDC JWKS RSA public keys (`{"keys":[{"kty":"RSA","kid":"...","n":"...","e":"..."}]}`) (Wave 21).
    pub fn set_jwks_json(&self, jwks_json: &str) -> Result<usize> {
        let doc: serde_json::Value = serde_json::from_str(jwks_json)
            .map_err(|e| RustLakeError::Execution(format!("invalid JWKS JSON: {e}")))?;
        let arr = doc
            .get("keys")
            .and_then(|v| v.as_array())
            .ok_or_else(|| RustLakeError::Execution("JWKS missing 'keys' array".into()))?;
        let mut parsed = Vec::new();
        for k in arr {
            if k.get("kty").and_then(|v| v.as_str()) != Some("RSA") {
                continue;
            }
            let Some(n_b64) = k.get("n").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(e_b64) = k.get("e").and_then(|v| v.as_str()) else {
                continue;
            };
            let n = B64URL
                .decode(n_b64)
                .map_err(|e| RustLakeError::Execution(format!("invalid JWK 'n': {e}")))?;
            let e = B64URL
                .decode(e_b64)
                .map_err(|e| RustLakeError::Execution(format!("invalid JWK 'e': {e}")))?;
            parsed.push(JwkRsaKey {
                kid: k.get("kid").and_then(|v| v.as_str()).map(String::from),
                n,
                e,
            });
        }
        let count = parsed.len();
        *self.jwks_keys.write() = parsed;
        Ok(count)
    }

    pub fn has_jwks(&self) -> bool {
        !self.jwks_keys.read().is_empty()
    }

    /// Fetches and atomically installs OIDC JWKS RSA public keys from an HTTPS/HTTP endpoint (Wave 22).
    pub async fn refresh_jwks_from_url(&self, jwks_url: &str) -> Result<usize> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| RustLakeError::Execution(format!("OIDC JWKS HTTP client: {e}")))?;
        let resp = client
            .get(jwks_url)
            .send()
            .await
            .map_err(|e| RustLakeError::Execution(format!("fetch OIDC JWKS '{jwks_url}': {e}")))?;
        if !resp.status().is_success() {
            return Err(RustLakeError::Execution(format!(
                "OIDC JWKS HTTP {}: {jwks_url}",
                resp.status()
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| RustLakeError::Execution(format!("read OIDC JWKS body: {e}")))?;
        let count = self.set_jwks_json(&body)?;
        self.oidc_last_refresh_ms
            .store(epoch_ms(), Ordering::Relaxed);
        Ok(count)
    }

    /// Discovers `jwks_uri` and `issuer` from `{issuer_url}/.well-known/openid-configuration`,
    /// installs the initial RSA keys, and enables automatic JWKS key rotation (Wave 36).
    pub async fn configure_oidc_discovery(
        &self,
        issuer_url: &str,
        expected_audience: Option<String>,
    ) -> Result<usize> {
        let well_known = format!(
            "{}/.well-known/openid-configuration",
            issuer_url.trim_end_matches('/')
        );
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| RustLakeError::Execution(format!("OIDC discovery HTTP client: {e}")))?;
        let resp = client
            .get(&well_known)
            .send()
            .await
            .map_err(|e| RustLakeError::Execution(format!("fetch '{well_known}': {e}")))?;
        if !resp.status().is_success() {
            return Err(RustLakeError::Execution(format!(
                "OIDC discovery HTTP {}: {well_known}",
                resp.status()
            )));
        }
        let doc: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RustLakeError::Execution(format!("parse OIDC discovery JSON: {e}")))?;
        let jwks_uri = doc
            .get("jwks_uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RustLakeError::Execution("OIDC discovery missing 'jwks_uri'".into()))?
            .to_string();
        let issuer = doc
            .get("issuer")
            .and_then(|v| v.as_str())
            .unwrap_or(issuer_url)
            .to_string();
        let count = self.refresh_jwks_from_url(&jwks_uri).await?;
        *self.oidc_issuer.write() = Some(issuer);
        *self.oidc_audience.write() = expected_audience;
        *self.oidc_jwks_uri.write() = Some(jwks_uri);
        Ok(count)
    }

    /// Verifies an OIDC RS256 JWT against cached JWKS keys, validating `iss`/`aud` claims and
    /// automatically refreshing JWKS from `oidc_jwks_uri` on `kid` rotation or TTL expiry (Wave 36).
    pub async fn verify_jwt_rs256_auto_rotate(&self, token: &str) -> Result<SessionContext> {
        let jwks_uri = self.oidc_jwks_uri.read().clone();
        let ttl = self.oidc_refresh_ttl_ms.load(Ordering::Relaxed);
        let last = self.oidc_last_refresh_ms.load(Ordering::Relaxed);
        if let Some(ref uri) = jwks_uri
            && epoch_ms().saturating_sub(last) > ttl
        {
            let _ = self.refresh_jwks_from_url(uri).await;
        }
        let expected_iss = self.oidc_issuer.read().clone();
        let expected_aud = self.oidc_audience.read().clone();
        let first_attempt = {
            let keys = self.jwks_keys.read().clone();
            verify_jwt_rs256_with_keys_and_claims(
                &keys,
                token,
                expected_iss.as_deref(),
                expected_aud.as_deref(),
            )
        };
        if first_attempt.is_ok() {
            return first_attempt;
        }
        if let Some(uri) = jwks_uri {
            let _ = self.refresh_jwks_from_url(&uri).await;
            let keys = self.jwks_keys.read().clone();
            return verify_jwt_rs256_with_keys_and_claims(
                &keys,
                token,
                expected_iss.as_deref(),
                expected_aud.as_deref(),
            );
        }
        first_attempt
    }

    /// Spawns a background OIDC JWKS auto-refresh poller for zero-downtime key rotation (Wave 22).
    pub fn spawn_oidc_jwks_poller(
        self: std::sync::Arc<Self>,
        jwks_url: String,
        interval: std::time::Duration,
    ) {
        tokio::spawn(async move {
            loop {
                match self.refresh_jwks_from_url(&jwks_url).await {
                    Ok(n) => {
                        tracing::info!(url = %jwks_url, keys = n, "refreshed OIDC JWKS keys");
                    }
                    Err(e) => {
                        tracing::warn!(url = %jwks_url, error = %e, "OIDC JWKS refresh failed");
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    /// Verifies an OIDC `RS256` JWT against the configured JWKS key set (Wave 21).
    pub fn verify_jwt_rs256(&self, token: &str) -> Result<SessionContext> {
        let keys = self.jwks_keys.read().clone();
        verify_jwt_rs256_with_keys(&keys, token)
    }

    /// Marks an active query as cancelled and removes it from active WLM tracking (Wave 21).
    pub fn cancel_query(&self, query_id: &str) -> bool {
        self.cancelled_queries.write().insert(query_id.to_string());
        self.active_queries.write().remove(query_id).is_some()
    }

    pub fn is_query_cancelled(&self, query_id: &str) -> bool {
        self.cancelled_queries.read().contains(query_id)
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

    // ---- Wave 13: OpenTelemetry Trace Spans & Exactly-Once CDC Offsets ----
    pub fn record_span(&self, span: TraceSpan) {
        let mut q = self.trace_spans.write();
        if q.len() >= 500 {
            q.pop_front();
        }
        q.push_back(span);
    }

    pub fn traces_snapshot(&self) -> Vec<TraceSpan> {
        self.trace_spans.read().iter().cloned().collect()
    }

    pub fn get_cdc_offset(&self, table: &str, stream_id: &str) -> u64 {
        self.cdc_offsets
            .read()
            .get(&format!("{table}::{stream_id}"))
            .copied()
            .unwrap_or(0)
    }

    pub fn set_cdc_offset(&self, table: &str, stream_id: &str, offset: u64) {
        self.cdc_offsets
            .write()
            .insert(format!("{table}::{stream_id}"), offset);
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

/// Signs an RFC 7519 **RS256** (RSA-SHA256) JWT using a PKCS#8 DER RSA private key (Wave 21).
pub fn sign_jwt_rs256_pkcs8(
    pkcs8_der: &[u8],
    kid: &str,
    user: &str,
    role: &str,
    exp_epoch_secs: u64,
) -> Result<(String, serde_json::Value)> {
    sign_jwt_rs256_pkcs8_with_claims(pkcs8_der, kid, user, role, exp_epoch_secs, None, None)
}

/// Signs an RFC 7519 **RS256** JWT with optional OIDC `iss` & `aud` claims (Wave 36).
pub fn sign_jwt_rs256_pkcs8_with_claims(
    pkcs8_der: &[u8],
    kid: &str,
    user: &str,
    role: &str,
    exp_epoch_secs: u64,
    iss: Option<&str>,
    aud: Option<&str>,
) -> Result<(String, serde_json::Value)> {
    let key_pair = RsaKeyPair::from_pkcs8(pkcs8_der)
        .map_err(|e| RustLakeError::Execution(format!("parse RSA PKCS8 key: {e}")))?;
    let (n_bytes, e_bytes) = parse_der_rsa_public_key(key_pair.public_key().as_ref())?;
    let n_b64 = B64URL.encode(n_bytes);
    let e_b64 = B64URL.encode(e_bytes);
    let jwks = serde_json::json!({
        "keys": [{
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "kid": kid,
            "n": n_b64,
            "e": e_b64
        }]
    });

    let header = B64URL.encode(
        serde_json::json!({ "alg": "RS256", "typ": "JWT", "kid": kid })
            .to_string()
            .as_bytes(),
    );
    let mut claims = serde_json::json!({
        "sub": user,
        "role": role,
        "iat": epoch_ms() / 1000,
        "exp": exp_epoch_secs
    });
    if let Some(i) = iss {
        claims["iss"] = serde_json::Value::String(i.to_string());
    }
    if let Some(a) = aud {
        claims["aud"] = serde_json::Value::String(a.to_string());
    }
    let payload = B64URL.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let rng = SystemRandom::new();
    let mut sig = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(&RSA_PKCS1_SHA256, &rng, signing_input.as_bytes(), &mut sig)
        .map_err(|e| RustLakeError::Execution(format!("RS256 sign error: {e}")))?;
    let token = format!("{signing_input}.{}", B64URL.encode(&sig));
    Ok((token, jwks))
}

/// Cryptographically verifies an OIDC **RS256** JWT against RFC 7517 JWKS RSA keys using `ring` (Wave 21).
pub fn verify_jwt_rs256_with_keys(keys: &[JwkRsaKey], token: &str) -> Result<SessionContext> {
    verify_jwt_rs256_with_keys_and_claims(keys, token, None, None)
}

/// Verifies an OIDC **RS256** JWT with optional `iss` and `aud` claim checks (Wave 36).
pub fn verify_jwt_rs256_with_keys_and_claims(
    keys: &[JwkRsaKey],
    token: &str,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
) -> Result<SessionContext> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(RustLakeError::Execution("invalid JWT format".into()));
    }
    let header_bytes = B64URL
        .decode(parts[0])
        .map_err(|_| RustLakeError::Execution("invalid JWT header encoding".into()))?;
    let header: serde_json::Value = serde_json::from_slice(&header_bytes)
        .map_err(|_| RustLakeError::Execution("invalid JWT header JSON".into()))?;
    if header.get("alg").and_then(|v| v.as_str()) != Some("RS256") {
        return Err(RustLakeError::Execution("JWT alg must be RS256".into()));
    }
    let kid = header.get("kid").and_then(|v| v.as_str());
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let sig_bytes = B64URL
        .decode(parts[2])
        .map_err(|_| RustLakeError::Execution("invalid JWT signature encoding".into()))?;

    let mut verified = false;
    for k in keys {
        if let (Some(want), Some(have)) = (kid, k.kid.as_deref())
            && want != have
        {
            continue;
        }
        let pub_key = RsaPublicKeyComponents {
            n: k.n.as_slice(),
            e: k.e.as_slice(),
        };
        if pub_key
            .verify(&RSA_PKCS1_2048_8192_SHA256, signing_input.as_bytes(), &sig_bytes)
            .is_ok()
        {
            verified = true;
            break;
        }
    }
    if !verified {
        return Err(RustLakeError::Execution(
            "OIDC RS256 JWT signature verification failed".into(),
        ));
    }

    let payload_bytes = B64URL
        .decode(parts[1])
        .map_err(|_| RustLakeError::Execution("invalid JWT payload encoding".into()))?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| RustLakeError::Execution("invalid JWT JSON claims".into()))?;

    let now_secs = epoch_ms() / 1000;
    if let Some(exp) = claims.get("exp").and_then(|v| v.as_u64())
        && now_secs > exp
    {
        return Err(RustLakeError::Execution("OIDC JWT token has expired".into()));
    }
    if let Some(nbf) = claims.get("nbf").and_then(|v| v.as_u64())
        && now_secs < nbf
    {
        return Err(RustLakeError::Execution("OIDC JWT token not yet valid (nbf)".into()));
    }
    if let Some(want_iss) = expected_iss {
        let got_iss = claims.get("iss").and_then(|v| v.as_str()).unwrap_or("");
        if got_iss != want_iss {
            return Err(RustLakeError::Execution(format!(
                "OIDC JWT issuer mismatch: expected '{want_iss}', got '{got_iss}'"
            )));
        }
    }
    if let Some(want_aud) = expected_aud {
        let aud_ok = match claims.get("aud") {
            Some(serde_json::Value::String(s)) => s == want_aud,
            Some(serde_json::Value::Array(arr)) => {
                arr.iter().any(|v| v.as_str() == Some(want_aud))
            }
            _ => false,
        };
        if !aud_ok {
            return Err(RustLakeError::Execution(format!(
                "OIDC JWT audience mismatch: expected '{want_aud}'"
            )));
        }
    }

    let user = claims["sub"]
        .as_str()
        .or_else(|| claims["preferred_username"].as_str())
        .ok_or_else(|| RustLakeError::Execution("OIDC JWT missing 'sub' claim".into()))?
        .to_string();
    let role = claims["role"]
        .as_str()
        .or_else(|| {
            claims["groups"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
        })
        .map(String::from)
        .unwrap_or_else(|| user.clone());
    Ok(SessionContext { user, role })
}

/// Extracts `(modulus_n, exponent_e)` big-endian unsigned bytes from an ASN.1 DER `RSAPublicKey`.
fn parse_der_rsa_public_key(der: &[u8]) -> Result<(&[u8], &[u8])> {
    fn read_tlv(buf: &[u8], expected_tag: u8) -> Result<(&[u8], &[u8])> {
        if buf.len() < 2 || buf[0] != expected_tag {
            return Err(RustLakeError::Execution("invalid DER RSA public key".into()));
        }
        let (len, header_len) = if buf[1] & 0x80 == 0 {
            (buf[1] as usize, 2usize)
        } else {
            let nbytes = (buf[1] & 0x7F) as usize;
            if nbytes == 0 || nbytes > 4 || buf.len() < 2 + nbytes {
                return Err(RustLakeError::Execution("invalid DER length".into()));
            }
            let mut l = 0usize;
            for &b in &buf[2..2 + nbytes] {
                l = (l << 8) | (b as usize);
            }
            (l, 2 + nbytes)
        };
        if buf.len() < header_len + len {
            return Err(RustLakeError::Execution("truncated DER TLV".into()));
        }
        Ok((&buf[header_len..header_len + len], &buf[header_len + len..]))
    }

    let (seq_body, _) = read_tlv(der, 0x30)?;
    let (n_raw, rest) = read_tlv(seq_body, 0x02)?;
    let (e_raw, _) = read_tlv(rest, 0x02)?;
    let n = n_raw.strip_prefix(&[0u8]).unwrap_or(n_raw);
    let e = e_raw.strip_prefix(&[0u8]).unwrap_or(e_raw);
    Ok((n, e))
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
