// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{
    normalize_table_name, Catalog, ColumnType, FieldSchema, PuffinIndexSpec, PuffinIndexType,
};
use rustlakedb_common::{QueryId, Result, RustLakeError};
use rustlakedb_protocol::{
    AggExpr, AggFunc, ArithOp, ColumnExpr, Distribution, FilterExpr, JoinType, MathFunc,
    Partitioning, PhysicalNode, QueryStage, RowBatch, ScalarValue, SetOpKind, ShuffleMode, SortKey,
    StageDag, StringFunc, VectorMetric, WindowFunc, WindowSpec,
};
use sqlparser::ast::{
    AlterTableOperation, BinaryOperator, DataType as SqlDataType, DuplicateTreatment, Expr,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, ObjectType,
    OrderByExpr, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor,
    UnaryOperator, Value, WindowType,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub default_shuffle_partitions: usize,
    pub shuffle_mode: ShuffleMode,
    pub enable_single_node_collapse: bool,
    pub single_node_collapse_threshold_bytes: u64,
    pub broadcast_join_threshold_bytes: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            default_shuffle_partitions: 4,
            shuffle_mode: ShuffleMode::Staged,
            enable_single_node_collapse: false,
            single_node_collapse_threshold_bytes: 128 * 1024 * 1024,
            broadcast_join_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Csv,
    Json,
    Parquet,
}

#[derive(Debug, Clone)]
pub enum PlannedCommand {
    Query(StageDag),
    Explain(StageDag),
    ExplainAnalyze(StageDag),
    CreateTable {
        table_name: String,
        schema: Vec<FieldSchema>,
        if_not_exists: bool,
        partition_by: Vec<String>,
    },
    CreateTableAs {
        table_name: String,
        query_sql: String,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    AlterRenameTable {
        old_name: String,
        new_name: String,
    },
    TruncateTable {
        table_name: String,
    },
    CreateSchema {
        name: String,
        if_not_exists: bool,
    },
    DropSchema {
        name: String,
        if_exists: bool,
    },
    CreateFunction {
        name: String,
        param_name: String,
        body_expr: String,
    },
    AlterAddColumn {
        table_name: String,
        column_name: String,
        data_type: ColumnType,
    },
    AlterClusterBy {
        table_name: String,
        columns: Vec<String>,
    },
    CreateIndex {
        table_name: String,
        spec: PuffinIndexSpec,
    },
    Insert {
        table_name: String,
        batch: RowBatch,
    },
    InsertSelect {
        table_name: String,
        query_sql: String,
    },
    BeginTx,
    CommitTx,
    RollbackTx,
    CopyFrom {
        table_name: String,
        source_path: String,
        format: CopyFormat,
        on_error_continue: bool,
    },
    MergeInto {
        target_table: String,
        source_table: String,
        target_key: String,
        source_key: String,
        when_matched_delete: bool,
        /// Vec of (target_col, source_col) assignments when matched.
        when_matched_updates: Vec<(String, String)>,
        when_not_matched_insert: bool,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, ScalarValue)>,
        predicate: Option<FilterExpr>,
    },
    Delete {
        table_name: String,
        predicate: FilterExpr,
    },
    CompactTable {
        table_name: String,
    },
    OptimizeTable {
        table_name: String,
    },
    VacuumTable {
        table_name: String,
        keep_last: usize,
    },
    CreateView {
        name: String,
        query_sql: String,
        materialized: bool,
    },
    RefreshMaterializedView {
        name: String,
    },
    CreateRole {
        role: String,
    },
    Grant {
        privilege: String,
        table_name: String,
        role: String,
    },
    Revoke {
        privilege: String,
        table_name: String,
        role: String,
    },
    CreateRlsPolicy {
        name: String,
        table_name: String,
        role: String,
        predicate: FilterExpr,
    },
    CreateMaskingPolicy {
        name: String,
        table_name: String,
        target: String,
        role: String,
        mask_value: String,
    },
    ShowTables,
    ShowQueries,
    InformationSchemaTables,
    InformationSchemaColumns,
    CurrentDatabase,
    CurrentSchema,
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
    ServerVersion,
    NoOp(String),
}

type PlanQueryFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(PhysicalNode, Vec<String>)>> + Send + 'a>,
>;

pub struct DistributedPlanner {
    pub config: PlannerConfig,
}

impl DistributedPlanner {
    pub fn new(config: PlannerConfig) -> Self {
        Self { config }
    }

    pub async fn plan_command(
        &self,
        sql: &str,
        branch: &str,
        catalog: &dyn Catalog,
    ) -> Result<PlannedCommand> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();

        if trimmed.is_empty() {
            return Ok(PlannedCommand::NoOp("EMPTY".into()));
        }
        if upper == "SHOW TABLES" {
            return Ok(PlannedCommand::ShowTables);
        }
        if upper == "SHOW QUERIES" {
            return Ok(PlannedCommand::ShowQueries);
        }
        if upper.contains("INFORMATION_SCHEMA.TABLES") || upper.contains("PG_CATALOG.PG_TABLES") || upper.contains("FROM PG_TABLES") {
            return Ok(PlannedCommand::InformationSchemaTables);
        }
        if upper.contains("INFORMATION_SCHEMA.COLUMNS") {
            return Ok(PlannedCommand::InformationSchemaColumns);
        }
        if upper.starts_with("SELECT CURRENT_DATABASE()") {
            return Ok(PlannedCommand::CurrentDatabase);
        }
        if upper.starts_with("SELECT CURRENT_SCHEMA()") {
            return Ok(PlannedCommand::CurrentSchema);
        }
        if upper == "SELECT VERSION()" || upper.starts_with("SELECT VERSION()") {
            return Ok(PlannedCommand::ServerVersion);
        }
        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            return Ok(PlannedCommand::BeginTx);
        }
        if upper == "COMMIT" || upper == "COMMIT TRANSACTION" {
            return Ok(PlannedCommand::CommitTx);
        }
        if upper == "ROLLBACK" || upper == "ROLLBACK TRANSACTION" {
            return Ok(PlannedCommand::RollbackTx);
        }
        if upper.starts_with("SET ") || upper.starts_with("RESET ") {
            return Ok(PlannedCommand::NoOp(
                upper
                    .split_whitespace()
                    .next()
                    .unwrap_or("SET")
                    .to_string(),
            ));
        }

        // Custom Iceberg lakehouse DDL & governance extensions (Waves 2-4).
        if upper.starts_with("COMPACT TABLE ") {
            let tbl = normalize_table_name(&trimmed["COMPACT TABLE ".len()..]);
            return Ok(PlannedCommand::CompactTable { table_name: tbl });
        }
        if upper.starts_with("OPTIMIZE TABLE ") || upper.starts_with("OPTIMIZE ") {
            let rest = if upper.starts_with("OPTIMIZE TABLE ") {
                &trimmed["OPTIMIZE TABLE ".len()..]
            } else {
                &trimmed["OPTIMIZE ".len()..]
            };
            return Ok(PlannedCommand::OptimizeTable {
                table_name: normalize_table_name(rest),
            });
        }
        if upper.starts_with("ALTER TABLE ") && upper.contains(" CLUSTER BY ") {
            return parse_alter_cluster_by(trimmed);
        }
        if upper.starts_with("ALTER TABLE ") && upper.contains(" RENAME TO ") {
            return parse_alter_rename_table(trimmed);
        }
        if upper.starts_with("TRUNCATE ") {
            let rest = trimmed["TRUNCATE ".len()..].trim();
            let rest = if rest.to_uppercase().starts_with("TABLE ") {
                rest["TABLE ".len()..].trim()
            } else {
                rest
            };
            return Ok(PlannedCommand::TruncateTable {
                table_name: normalize_table_name(rest),
            });
        }
        if upper.starts_with("CREATE SCHEMA ") {
            let rest = trimmed["CREATE SCHEMA ".len()..].trim();
            let (if_not_exists, name_raw) = if rest.to_uppercase().starts_with("IF NOT EXISTS ") {
                (true, rest["IF NOT EXISTS ".len()..].trim())
            } else {
                (false, rest)
            };
            return Ok(PlannedCommand::CreateSchema {
                name: name_raw.trim_matches('"').to_string(),
                if_not_exists,
            });
        }
        if upper.starts_with("DROP SCHEMA ") {
            let rest = trimmed["DROP SCHEMA ".len()..].trim();
            let (if_exists, name_raw) = if rest.to_uppercase().starts_with("IF EXISTS ") {
                (true, rest["IF EXISTS ".len()..].trim())
            } else {
                (false, rest)
            };
            let name = name_raw
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .to_string();
            return Ok(PlannedCommand::DropSchema { name, if_exists });
        }
        if upper.starts_with("DROP VIEW ") || upper.starts_with("DROP MATERIALIZED VIEW ") {
            let after = if upper.starts_with("DROP MATERIALIZED VIEW ") {
                trimmed["DROP MATERIALIZED VIEW ".len()..].trim()
            } else {
                trimmed["DROP VIEW ".len()..].trim()
            };
            let (if_exists, name_raw) = if after.to_uppercase().starts_with("IF EXISTS ") {
                (true, after["IF EXISTS ".len()..].trim())
            } else {
                (false, after)
            };
            let name = normalize_table_name(name_raw.split_whitespace().next().unwrap_or(""));
            return Ok(PlannedCommand::DropView { name, if_exists });
        }
        if upper.starts_with("CREATE FUNCTION ") || upper.starts_with("CREATE OR REPLACE FUNCTION ")
        {
            return parse_create_function(trimmed);
        }
        if upper.starts_with("COPY ") {
            return parse_copy_from(trimmed);
        }
        if upper.starts_with("MERGE INTO ") {
            return parse_merge_into(trimmed);
        }
        if upper.starts_with("CREATE MATERIALIZED VIEW ") {
            return parse_create_view(trimmed, true);
        }
        if upper.starts_with("CREATE VIEW ") || upper.starts_with("CREATE OR REPLACE VIEW ") {
            return parse_create_view(trimmed, false);
        }
        if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
            let name = normalize_table_name(&trimmed["REFRESH MATERIALIZED VIEW ".len()..]);
            return Ok(PlannedCommand::RefreshMaterializedView { name });
        }
        if upper.starts_with("CREATE ROLE ") {
            let role = trimmed["CREATE ROLE ".len()..]
                .trim()
                .trim_matches('"')
                .to_string();
            return Ok(PlannedCommand::CreateRole { role });
        }
        if upper.starts_with("GRANT ") {
            return parse_grant_or_revoke(trimmed, true);
        }
        if upper.starts_with("REVOKE ") {
            return parse_grant_or_revoke(trimmed, false);
        }
        if upper.starts_with("CREATE MASKING POLICY ") {
            return parse_create_masking_policy(trimmed);
        }
        if upper.starts_with("CREATE POLICY ") {
            return parse_create_rls_policy(trimmed);
        }
        if upper.starts_with("VACUUM TABLE ") {
            let rest = trimmed["VACUUM TABLE ".len()..].trim();
            let parts: Vec<&str> = rest.split_whitespace().collect();
            let tbl = normalize_table_name(parts[0]);
            let keep_last = if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("RETAIN") {
                parts[2].parse::<usize>().unwrap_or(1)
            } else {
                1
            };
            return Ok(PlannedCommand::VacuumTable {
                table_name: tbl,
                keep_last,
            });
        }
        if upper.starts_with("CREATE BRANCH ") {
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            if tokens.len() == 7
                && tokens[3].eq_ignore_ascii_case("FROM")
                && tokens[5].eq_ignore_ascii_case("ON")
            {
                return Ok(PlannedCommand::CreateBranch {
                    new_branch: tokens[2].to_string(),
                    from_branch: tokens[4].to_string(),
                    table_name: normalize_table_name(tokens[6]),
                });
            }
            return Err(RustLakeError::SqlParse(
                "expected: CREATE BRANCH <new> FROM <base> ON <table>".into(),
            ));
        }
        if upper.starts_with("MERGE BRANCH ") {
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            if tokens.len() == 7
                && tokens[3].eq_ignore_ascii_case("INTO")
                && tokens[5].eq_ignore_ascii_case("ON")
            {
                return Ok(PlannedCommand::MergeBranch {
                    source_branch: tokens[2].to_string(),
                    target_branch: tokens[4].to_string(),
                    table_name: normalize_table_name(tokens[6]),
                });
            }
            return Err(RustLakeError::SqlParse(
                "expected: MERGE BRANCH <src> INTO <dst> ON <table>".into(),
            ));
        }

        // Support Iceberg Hidden Partitioning syntax (`CREATE TABLE t (...) PARTITION BY (region, days(ts))`, Wave 12).
        let (sql_no_part, table_partition_by) = extract_create_table_partition_by(trimmed);

        // Support `SELECT ... FROM <table> AT VERSION <snap_id> ...` Iceberg Time Travel syntax!
        let (normalized_sql, time_travel_version) = extract_time_travel_clause(&sql_no_part)?;

        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, &normalized_sql)
            .map_err(|e| RustLakeError::SqlParse(e.to_string()))?;
        let stmt = stmts
            .into_iter()
            .next()
            .ok_or_else(|| RustLakeError::SqlParse("empty SQL statement".into()))?;

        match stmt {
            Statement::Query(query) => {
                let (raw, cbo_notes) = self
                    .plan_query(*query, branch, time_travel_version, catalog)
                    .await?;
                let pruned = push_projections_to_scans(raw, None);
                let dist = self.ensure_requirements(pruned);
                Ok(PlannedCommand::Query(self.cut_stages(
                    QueryId::new(),
                    dist,
                    cbo_notes,
                )))
            }
            Statement::Explain {
                analyze, statement, ..
            } => match *statement {
                Statement::Query(query) => {
                    let (raw, cbo_notes) = self
                        .plan_query(*query, branch, time_travel_version, catalog)
                        .await?;
                    let pruned = push_projections_to_scans(raw, None);
                    let dist = self.ensure_requirements(pruned);
                    let dag = self.cut_stages(QueryId::new(), dist, cbo_notes);
                    if analyze {
                        Ok(PlannedCommand::ExplainAnalyze(dag))
                    } else {
                        Ok(PlannedCommand::Explain(dag))
                    }
                }
                other => Err(RustLakeError::Plan(format!(
                    "EXPLAIN supports SELECT queries, got: {other:?}"
                ))),
            },
            Statement::CreateTable(ct) => {
                let table_name = normalize_table_name(&ct.name.to_string());
                if let Some(q) = ct.query {
                    return Ok(PlannedCommand::CreateTableAs {
                        table_name,
                        query_sql: q.to_string(),
                    });
                }
                let mut schema = Vec::new();
                for (idx, col) in ct.columns.into_iter().enumerate() {
                    let dt = map_sql_data_type(&col.data_type)?;
                    schema.push(FieldSchema {
                        field_id: (idx + 1) as i32,
                        name: col.name.value,
                        data_type: dt,
                        nullable: true,
                    });
                }
                Ok(PlannedCommand::CreateTable {
                    table_name,
                    schema,
                    if_not_exists: ct.if_not_exists,
                    partition_by: table_partition_by,
                })
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                let table_name = names
                    .into_iter()
                    .next()
                    .map(|n| normalize_table_name(&n.to_string()))
                    .ok_or_else(|| RustLakeError::Plan("DROP TABLE requires table name".into()))?;
                Ok(PlannedCommand::DropTable {
                    table_name,
                    if_exists,
                })
            }
            Statement::AlterTable {
                name, operations, ..
            } => {
                let table_name = normalize_table_name(&name.to_string());
                match operations.into_iter().next() {
                    Some(AlterTableOperation::AddColumn { column_def, .. }) => {
                        let data_type = map_sql_data_type(&column_def.data_type)?;
                        Ok(PlannedCommand::AlterAddColumn {
                            table_name,
                            column_name: column_def.name.value,
                            data_type,
                        })
                    }
                    other => Err(RustLakeError::Plan(format!(
                        "unsupported ALTER TABLE operation: {other:?}"
                    ))),
                }
            }
            Statement::CreateIndex(ci) => {
                let table_name = normalize_table_name(&ci.table_name.to_string());
                let name = ci
                    .name
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("idx_{table_name}"));
                let index_type = match ci.using.as_ref().map(|u| u.to_string().to_uppercase()) {
                    Some(ref s) if s == "BLOOM" => PuffinIndexType::Bloom,
                    Some(ref s) if s == "THETA" || s == "THETASKETCH" => {
                        PuffinIndexType::ThetaSketch
                    }
                    Some(ref s) if s == "HNSW" => PuffinIndexType::Hnsw,
                    Some(ref s) if s == "BM25" || s == "FULLTEXT" => PuffinIndexType::Bm25,
                    _ => PuffinIndexType::Inverted,
                };
                let first_col = ci
                    .columns
                    .into_iter()
                    .next()
                    .ok_or_else(|| RustLakeError::Plan("CREATE INDEX requires a column".into()))?;
                let (column, json_path) = match parse_col_expr(&first_col.expr, None)? {
                    ColumnExpr::Column(c) => (c, None),
                    ColumnExpr::JsonPath { column, path, .. } => (column, Some(path)),
                    other => {
                        return Err(RustLakeError::Plan(format!(
                            "CREATE INDEX supports column or JSON path, got {other:?}"
                        )))
                    }
                };
                Ok(PlannedCommand::CreateIndex {
                    table_name,
                    spec: PuffinIndexSpec {
                        name,
                        column,
                        json_path,
                        index_type,
                    },
                })
            }
            Statement::Insert(ins) => {
                let table_name = normalize_table_name(&ins.table_name.to_string());
                let meta = catalog.get_table(&table_name).await?;
                let col_names: Vec<String> = if ins.columns.is_empty() {
                    meta.schema.iter().map(|f| f.name.clone()).collect()
                } else {
                    ins.columns.into_iter().map(|c| c.value).collect()
                };
                let col_types: BTreeMap<String, ColumnType> = meta
                    .schema
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.clone()))
                    .collect();

                let source = ins
                    .source
                    .ok_or_else(|| RustLakeError::Plan("INSERT requires VALUES or SELECT".into()))?;
                let values = match *source.body {
                    SetExpr::Values(v) => v.rows,
                    _ => {
                        return Ok(PlannedCommand::InsertSelect {
                            table_name,
                            query_sql: source.to_string(),
                        });
                    }
                };

                let mut rows = Vec::with_capacity(values.len());
                for val_row in values {
                    if val_row.len() != col_names.len() {
                        return Err(RustLakeError::Plan(format!(
                            "INSERT column count mismatch: expected {}, got {}",
                            col_names.len(),
                            val_row.len()
                        )));
                    }
                    let mut row_map = BTreeMap::new();
                    for (col_name, expr) in col_names.iter().zip(val_row.iter()) {
                        let mut scalar = parse_scalar(expr)?;
                        match col_types.get(col_name) {
                            Some(ColumnType::Variant) => {
                                if let ScalarValue::Utf8(s) = scalar {
                                    let json_val: serde_json::Value = serde_json::from_str(&s)
                                        .map_err(|e| {
                                            RustLakeError::SqlParse(format!(
                                                "invalid JSON for column '{col_name}': {e}"
                                            ))
                                        })?;
                                    scalar = ScalarValue::Json(json_val);
                                }
                            }
                            Some(ColumnType::Date) => {
                                if let ScalarValue::Utf8(ref s) = scalar
                                    && let Some(days) = rustlakedb_protocol::parse_iso_date_days(s)
                                {
                                    scalar = ScalarValue::Date(days);
                                }
                            }
                            Some(ColumnType::Decimal { scale, .. }) => {
                                let sc = *scale;
                                scalar = match scalar {
                                    ScalarValue::Int64(i) => ScalarValue::Decimal(
                                        (i as i128) * 10i128.pow(sc as u32),
                                        sc,
                                    ),
                                    ScalarValue::Float64(f) => {
                                        if let Some(v) = rustlakedb_protocol::parse_decimal_i128(
                                            &format!("{f:.*}", sc as usize),
                                            sc,
                                        ) {
                                            ScalarValue::Decimal(v, sc)
                                        } else {
                                            ScalarValue::Float64(f)
                                        }
                                    }
                                    ScalarValue::Utf8(ref s) => {
                                        if let Some(v) =
                                            rustlakedb_protocol::parse_decimal_i128(s, sc)
                                        {
                                            ScalarValue::Decimal(v, sc)
                                        } else {
                                            scalar
                                        }
                                    }
                                    other => other,
                                };
                            }
                            _ => {}
                        }
                        row_map.insert(col_name.clone(), scalar);
                    }
                    rows.push(row_map);
                }

                Ok(PlannedCommand::Insert {
                    table_name,
                    batch: RowBatch::new(col_names, rows),
                })
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = extract_table_name(&table.relation)?;
                let meta = catalog.get_table(&table_name).await?;
                let col_types: BTreeMap<String, ColumnType> = meta
                    .schema
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.clone()))
                    .collect();

                let mut parsed_assigns = Vec::new();
                for a in assignments {
                    let col_name = match a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(obj) => obj.to_string(),
                        other => {
                            return Err(RustLakeError::Plan(format!(
                                "unsupported UPDATE target: {other:?}"
                            )))
                        }
                    };
                    let mut val = parse_scalar(&a.value)?;
                    if matches!(col_types.get(&col_name), Some(ColumnType::Variant))
                        && let ScalarValue::Utf8(s) = val {
                            let jv: serde_json::Value = serde_json::from_str(&s)
                                .map_err(|e| RustLakeError::SqlParse(e.to_string()))?;
                            val = ScalarValue::Json(jv);
                        }
                    parsed_assigns.push((col_name, val));
                }
                let predicate = selection.map(|e| parse_filter_expr(&e)).transpose()?;
                Ok(PlannedCommand::Update {
                    table_name,
                    assignments: parsed_assigns,
                    predicate,
                })
            }
            Statement::Delete(del) => {
                let tables = match del.from {
                    sqlparser::ast::FromTable::WithFromKeyword(t)
                    | sqlparser::ast::FromTable::WithoutKeyword(t) => t,
                };
                let table_name = tables
                    .into_iter()
                    .next()
                    .map(|t| extract_table_name(&t.relation))
                    .transpose()?
                    .ok_or_else(|| RustLakeError::Plan("DELETE requires table name".into()))?;
                let selection = del.selection.ok_or_else(|| {
                    RustLakeError::Plan("DELETE requires a WHERE clause".into())
                })?;
                let predicate = parse_filter_expr(&selection)?;
                Ok(PlannedCommand::Delete {
                    table_name,
                    predicate,
                })
            }
            other => Err(RustLakeError::Plan(format!(
                "unsupported SQL statement: {other:?}"
            ))),
        }
    }

    pub async fn plan_sql(
        &self,
        sql: &str,
        branch: &str,
        catalog: &dyn Catalog,
    ) -> Result<StageDag> {
        match self.plan_command(sql, branch, catalog).await? {
            PlannedCommand::Query(dag) => Ok(dag),
            other => Err(RustLakeError::Plan(format!(
                "expected SELECT query, got {other:?}"
            ))),
        }
    }

    fn plan_query<'a>(
        &'a self,
        query: sqlparser::ast::Query,
        branch: &'a str,
        time_travel_version: Option<u64>,
        catalog: &'a dyn Catalog,
    ) -> PlanQueryFuture<'a> {
        Box::pin(async move {
            let mut cbo_notes = Vec::new();

            // Support SQL CTEs (`WITH cte_name AS (SELECT ...) SELECT ... FROM cte_name`).
            let mut cte_plans: BTreeMap<String, PhysicalNode> = BTreeMap::new();
            if let Some(with) = query.with {
                for cte in with.cte_tables {
                    let alias = cte.alias.name.value.clone();
                    let (cte_node, notes) = self
                        .plan_query(*cte.query, branch, time_travel_version, catalog)
                        .await?;
                    cbo_notes.extend(notes);
                    cte_plans.insert(alias, cte_node);
                }
            }

            let order_keys: Vec<SortKey> = query
                .order_by
                .map(|ob| {
                    ob.exprs
                        .into_iter()
                        .map(|OrderByExpr { expr, asc, .. }| {
                            Ok(SortKey {
                                column: extract_ident(&expr)?,
                                descending: matches!(asc, Some(false)),
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();

            let limit_val: Option<usize> = query
                .limit
                .map(|l| match parse_scalar(&l)? {
                    ScalarValue::Int64(n) if n >= 0 => Ok(n as usize),
                    _ => Err(RustLakeError::Plan(
                        "LIMIT must be a non-negative integer".into(),
                    )),
                })
                .transpose()?;

            let offset_val: usize = query
                .offset
                .map(|o| match parse_scalar(&o.value)? {
                    ScalarValue::Int64(n) if n >= 0 => Ok(n as usize),
                    _ => Err(RustLakeError::Plan(
                        "OFFSET must be a non-negative integer".into(),
                    )),
                })
                .transpose()?
                .unwrap_or(0);

            let select = match *query.body {
                SetExpr::Select(s) => *s,
                SetExpr::SetOperation {
                    op,
                    set_quantifier,
                    left,
                    right,
                } if matches!(
                    op,
                    SetOperator::Union | SetOperator::Intersect | SetOperator::Except
                ) => {
                    let distinct = !matches!(set_quantifier, SetQuantifier::All);
                    let left_q = sqlparser::ast::Query {
                        with: None,
                        body: left,
                        order_by: None,
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    };
                    let right_q = sqlparser::ast::Query {
                        with: None,
                        body: right,
                        order_by: None,
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    };
                    let (lnode, lnotes) = self
                        .plan_query(left_q, branch, time_travel_version, catalog)
                        .await?;
                    let (rnode, rnotes) = self
                        .plan_query(right_q, branch, time_travel_version, catalog)
                        .await?;
                    cbo_notes.extend(lnotes);
                    cbo_notes.extend(rnotes);
                    let mut node = match op {
                        SetOperator::Union => PhysicalNode::UnionAll {
                            inputs: vec![lnode, rnode],
                            distinct,
                        },
                        SetOperator::Intersect => PhysicalNode::SetOp {
                            op: SetOpKind::Intersect,
                            left: Box::new(lnode),
                            right: Box::new(rnode),
                            distinct,
                        },
                        SetOperator::Except => PhysicalNode::SetOp {
                            op: SetOpKind::Except,
                            left: Box::new(lnode),
                            right: Box::new(rnode),
                            distinct,
                        },
                    };
                    if !order_keys.is_empty() {
                        node = PhysicalNode::Sort {
                            input: Box::new(node),
                            keys: order_keys,
                        };
                    }
                    if limit_val.is_some() || offset_val > 0 {
                        node = PhysicalNode::Limit {
                            input: Box::new(node),
                            limit: limit_val.unwrap_or(usize::MAX),
                            offset: offset_val,
                        };
                    }
                    return Ok((node, cbo_notes));
                }
                _ => {
                    return Err(RustLakeError::Plan(
                        "only SELECT, UNION, INTERSECT, and EXCEPT query bodies supported".into(),
                    ))
                }
            };

            let is_distinct = select.distinct.is_some();
            let from_item = select
                .from
                .into_iter()
                .next()
                .ok_or_else(|| RustLakeError::Plan("SELECT requires a FROM clause".into()))?;

            let pushed_filter = match select.selection {
                Some(expr) => Some(parse_filter_expr(&expr)?),
                None => None,
            };

            let left_table_name = extract_table_name(&from_item.relation)?;
            let (mut node, mut current_bytes) = if let Some(cte_node) = cte_plans.get(&left_table_name) {
                (cte_node.clone(), 4096u64)
            } else {
                let left_meta = catalog.get_table(&left_table_name).await?;
                let left_splits = match time_travel_version {
                    Some(v) => left_meta.splits_at_version(v)?,
                    None => left_meta.splits(branch)?,
                };
                if let Ok(snap) = left_meta.current_snapshot(branch) {
                    let est_rows: u64 = snap.data_files.iter().map(|f| f.row_count).sum();
                    let ndv_cols: usize = snap
                        .data_files
                        .iter()
                        .flat_map(|f| f.ndv_estimates.keys())
                        .collect::<HashSet<_>>()
                        .len();
                    if est_rows > 0 && (ndv_cols > 0 || !left_meta.partition_by.is_empty()) {
                        cbo_notes.push(format!(
                            "CBO: stats({left_table_name} rows={est_rows}, ndv_cols={ndv_cols}, partition_by={:?})",
                            left_meta.partition_by
                        ));
                    }
                }
                let bytes: u64 = left_splits.iter().map(|s| s.estimated_bytes).sum();
                let scan = PhysicalNode::IcebergScan {
                    table_name: left_table_name.clone(),
                    projection: vec![],
                    pushed_filter: if from_item.joins.is_empty() {
                        pushed_filter.clone()
                    } else {
                        None
                    },
                    splits: left_splits,
                };
                (scan, bytes)
            };

            // Wave 9 + Wave 19 Dependency-Aware Multi-Table CBO Join Ordering:
            // When 3+ tables are joined with INNER JOIN, greedily pick the smallest eligible
            // table whose join key is already available in the joined schema (preserving strict
            // foreign-key dependencies on snowflake/chain joins while broadcasting small dims first)!
            let all_inner = from_item.joins.len() >= 2
                && from_item
                    .joins
                    .iter()
                    .all(|j| matches!(j.join_operator, sqlparser::ast::JoinOperator::Inner(_)));
            let ordered_joins = if all_inner {
                let mut available_cols: HashSet<String> = HashSet::new();
                if let Ok(lm) = catalog.get_table(&left_table_name).await {
                    for f in &lm.schema {
                        available_cols.insert(f.name.clone());
                    }
                }
                type JoinCandidate = (
                    usize,
                    u64,
                    HashSet<String>,
                    Option<(String, String)>,
                    sqlparser::ast::Join,
                );
                let mut pool: Vec<JoinCandidate> = Vec::with_capacity(from_item.joins.len());
                for (orig_idx, j) in from_item.joins.into_iter().enumerate() {
                    let rname = extract_table_name(&j.relation)?;
                    let mut rcols = HashSet::new();
                    let rbytes = if cte_plans.contains_key(&rname) {
                        4096u64
                    } else if let Ok(rm) = catalog.get_table(&rname).await {
                        for f in &rm.schema {
                            rcols.insert(f.name.clone());
                        }
                        rm.splits(branch)
                            .map(|sp| sp.iter().map(|s| s.estimated_bytes).sum())
                            .unwrap_or(0)
                    } else {
                        u64::MAX
                    };
                    let keys = match &j.join_operator {
                        sqlparser::ast::JoinOperator::Inner(JoinConstraint::On(expr)) => {
                            extract_join_keys(expr).ok()
                        }
                        _ => None,
                    };
                    pool.push((orig_idx, rbytes, rcols, keys, j));
                }

                let mut chosen: Vec<(usize, sqlparser::ast::Join)> = Vec::with_capacity(pool.len());
                while !pool.is_empty() {
                    let pick_pos = pool
                        .iter()
                        .enumerate()
                        .filter(|(_, (_, _, rcols, keys, _))| {
                            if available_cols.is_empty() {
                                return true;
                            }
                            match keys {
                                Some((lk, rk)) => {
                                    (available_cols.contains(lk) && (rcols.is_empty() || rcols.contains(rk)))
                                        || (available_cols.contains(rk)
                                            && (rcols.is_empty() || rcols.contains(lk)))
                                }
                                None => true,
                            }
                        })
                        .min_by_key(|(_, (_, b, _, _, _))| *b)
                        .map(|(pos, _)| pos)
                        .unwrap_or(0);
                    let (orig_idx, _, rcols, _, j) = pool.remove(pick_pos);
                    available_cols.extend(rcols);
                    chosen.push((orig_idx, j));
                }

                let reordered = chosen.iter().enumerate().any(|(i, (orig, _))| i != *orig);
                if reordered {
                    cbo_notes.push(format!(
                        "CBO: reordered {}-way INNER JOIN by table size (smallest dimensions first)",
                        chosen.len() + 1
                    ));
                }
                chosen.into_iter().map(|(_, j)| j).collect()
            } else {
                from_item.joins
            };

            // Cost-Based Join Strategy Selection & Inner-Join Build-Side Ordering (RFC-004).
            for join in ordered_joins {
                let right_table_name = extract_table_name(&join.relation)?;
                let (right_node, right_bytes) = if let Some(cte_node) = cte_plans.get(&right_table_name) {
                    (cte_node.clone(), 4096u64)
                } else {
                    let right_meta = catalog.get_table(&right_table_name).await?;
                    let right_splits = right_meta.splits(branch)?;
                    let bytes: u64 = right_splits.iter().map(|s| s.estimated_bytes).sum();
                    (
                        PhysicalNode::IcebergScan {
                            table_name: right_table_name.clone(),
                            projection: vec![],
                            pushed_filter: None,
                            splits: right_splits,
                        },
                        bytes,
                    )
                };

                let (join_type, (left_key, right_key)) = match join.join_operator {
                    sqlparser::ast::JoinOperator::Inner(JoinConstraint::On(expr)) => {
                        (JoinType::Inner, extract_join_keys(&expr)?)
                    }
                    sqlparser::ast::JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                        (JoinType::Left, extract_join_keys(&expr)?)
                    }
                    sqlparser::ast::JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
                        (JoinType::Right, extract_join_keys(&expr)?)
                    }
                    sqlparser::ast::JoinOperator::FullOuter(JoinConstraint::On(expr)) => {
                        (JoinType::Full, extract_join_keys(&expr)?)
                    }
                    sqlparser::ast::JoinOperator::CrossJoin => {
                        (JoinType::Cross, (String::new(), String::new()))
                    }
                    other => {
                        return Err(RustLakeError::Plan(format!(
                            "unsupported join operator: {other:?}"
                        )))
                    }
                };

                let (final_left, final_right, final_lkey, final_rkey, build_bytes, probe_bytes) =
                    if join_type == JoinType::Inner && current_bytes < right_bytes {
                        cbo_notes.push(format!(
                            "CBO: swapped INNER JOIN build side ({current_bytes}B < {right_bytes}B)"
                        ));
                        (
                            right_node,
                            node,
                            right_key,
                            left_key,
                            current_bytes,
                            right_bytes,
                        )
                    } else {
                        (
                            node,
                            right_node,
                            left_key,
                            right_key,
                            right_bytes,
                            current_bytes,
                        )
                    };

                let is_broadcast = if join_type == JoinType::Cross {
                    true
                } else if matches!(join_type, JoinType::Right | JoinType::Full) {
                    false
                } else {
                    build_bytes > 0
                        && build_bytes <= self.config.broadcast_join_threshold_bytes
                        && build_bytes <= probe_bytes
                        && self.config.broadcast_join_threshold_bytes > 0
                };

                cbo_notes.push(format!(
                    "CBO: {:?} JOIN {} (build={}B, probe={}B) -> {}",
                    join_type,
                    right_table_name,
                    build_bytes,
                    probe_bytes,
                    if is_broadcast {
                        "BroadcastHashJoin"
                    } else {
                        "PartitionedHashJoin"
                    }
                ));

                current_bytes = current_bytes.saturating_add(right_bytes);
                node = PhysicalNode::HashJoin {
                    left: Box::new(final_left),
                    right: Box::new(final_right),
                    left_key: final_lkey,
                    right_key: final_rkey,
                    join_type,
                    is_broadcast,
                };
            }

            if let Some(pred) = pushed_filter
                && !matches!(node, PhysicalNode::IcebergScan { .. })
            {
                let (pushed_node, remaining, pushed_cnt) =
                    push_filter_through_joins(node, pred, catalog, branch).await;
                if pushed_cnt > 0 {
                    cbo_notes.push(format!(
                        "CBO: pushed {pushed_cnt} predicate conjunct(s) through JOIN onto IcebergScan leaves (histogram selectivity est.)"
                    ));
                }
                node = match remaining {
                    Some(rem) => PhysicalNode::Filter {
                        input: Box::new(pushed_node),
                        predicate: rem,
                    },
                    None => pushed_node,
                };
            }

            let (group_by_exprs, grouping_sets): (Vec<ColumnExpr>, Vec<Vec<String>>) =
                match select.group_by {
                    GroupByExpr::Expressions(exprs, _) => parse_group_by_exprs(exprs)?,
                    GroupByExpr::All(_) => {
                        return Err(RustLakeError::Plan("GROUP BY ALL not yet supported".into()))
                    }
                };

            let mut projected_exprs: Vec<ColumnExpr> = Vec::new();
            let mut pre_agg_exprs: Vec<ColumnExpr> = Vec::new();
            let mut aggs: Vec<AggExpr> = Vec::new();
            let mut windows: Vec<WindowSpec> = Vec::new();
            let mut group_aliases: BTreeMap<String, String> = BTreeMap::new();

            for item in select.projection {
                match item {
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
                    SelectItem::UnnamedExpr(expr) => {
                        if let Some(win) = try_parse_window(&expr, None)? {
                            projected_exprs.push(ColumnExpr::Column(win.alias.clone()));
                            windows.push(win);
                        } else if let Some(agg) =
                            try_parse_agg_with_expr(&expr, None, &mut pre_agg_exprs)?
                        {
                            aggs.push(agg);
                        } else {
                            projected_exprs.push(parse_col_expr(&expr, None)?);
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if let Some(win) = try_parse_window(&expr, Some(alias.value.clone()))? {
                            projected_exprs.push(ColumnExpr::Column(win.alias.clone()));
                            windows.push(win);
                        } else if let Some(agg) = try_parse_agg_with_expr(
                            &expr,
                            Some(alias.value.clone()),
                            &mut pre_agg_exprs,
                        )? {
                            aggs.push(agg);
                        } else {
                            let ce = parse_col_expr(&expr, Some(alias.value.clone()))?;
                            if let ColumnExpr::JsonPath {
                                ref column,
                                ref path,
                                ref alias,
                            } = ce
                            {
                                group_aliases.insert(format!("{column}->>{path}"), alias.clone());
                            }
                            projected_exprs.push(ce);
                        }
                    }
                }
            }

            let aligned_group_by: Vec<ColumnExpr> = group_by_exprs
                .into_iter()
                .map(|g| match g {
                    ColumnExpr::JsonPath { column, path, alias } => {
                        let key = format!("{column}->>{path}");
                        let resolved = group_aliases.get(&key).cloned().unwrap_or(alias);
                        ColumnExpr::JsonPath {
                            column,
                            path,
                            alias: resolved,
                        }
                    }
                    other => other,
                })
                .collect();

            let having_pred = select
                .having
                .map(|e| {
                    let rewritten = rewrite_having_agg_expr(&e, &mut aggs, &mut pre_agg_exprs)?;
                    parse_filter_expr(&rewritten)
                })
                .transpose()?;

            if !aggs.is_empty() || !aligned_group_by.is_empty() {
                // If any aggregate had a complex expression argument (e.g. SUM(l_extendedprice * (1 - l_discount))
                // or SUM(CASE WHEN ... END)), inject a pre-aggregation Project computing those derived columns!
                if !pre_agg_exprs.is_empty() {
                    let mut needed: Vec<ColumnExpr> = Vec::new();
                    let mut seen: HashSet<String> = HashSet::new();
                    let mut raw_cols: HashSet<String> = HashSet::new();
                    for g in &aligned_group_by {
                        collect_col_expr_cols(g, &mut raw_cols);
                    }
                    for a in &aggs {
                        match &a.func {
                            AggFunc::Count => {}
                            AggFunc::CountDistinct(c)
                            | AggFunc::Sum(c)
                            | AggFunc::Avg(c)
                            | AggFunc::Min(c)
                            | AggFunc::Max(c) => {
                                if !pre_agg_exprs.iter().any(|p| p.output_name() == *c) {
                                    raw_cols.insert(c.clone());
                                }
                            }
                        }
                    }
                    for c in raw_cols {
                        if seen.insert(c.clone()) {
                            needed.push(ColumnExpr::Column(c));
                        }
                    }
                    for p in pre_agg_exprs {
                        if seen.insert(p.output_name()) {
                            needed.push(p);
                        }
                    }
                    node = PhysicalNode::Project {
                        input: Box::new(node),
                        exprs: needed,
                    };
                }
                let partial = PhysicalNode::HashAggregate {
                    input: Box::new(node),
                    group_by: aligned_group_by.clone(),
                    aggs: aggs.clone(),
                    is_partial: true,
                    grouping_sets: vec![],
                };
                node = PhysicalNode::HashAggregate {
                    input: Box::new(partial),
                    group_by: aligned_group_by,
                    aggs,
                    is_partial: false,
                    grouping_sets,
                };
                if let Some(hpred) = having_pred {
                    node = PhysicalNode::Filter {
                        input: Box::new(node),
                        predicate: hpred,
                    };
                }
                if is_distinct {
                    node = PhysicalNode::Distinct {
                        input: Box::new(node),
                    };
                }
                if !order_keys.is_empty() {
                    node = PhysicalNode::Sort {
                        input: Box::new(node),
                        keys: order_keys,
                    };
                }
            } else {
                if !windows.is_empty() {
                    node = PhysicalNode::Window {
                        input: Box::new(node),
                        specs: windows,
                    };
                }
                let computed_aliases: HashSet<String> = projected_exprs
                    .iter()
                    .filter(|e| !matches!(e, ColumnExpr::Column(_)))
                    .map(|e| e.output_name())
                    .collect();
                let orders_by_computed_alias = order_keys
                    .iter()
                    .any(|k| computed_aliases.contains(&k.column));
                if orders_by_computed_alias {
                    // Evaluate computed projections BEFORE `Sort` when ORDER BY references a computed alias
                    // (e.g., `ORDER BY sim DESC` or `ORDER BY bm25 DESC`).
                    node = PhysicalNode::Project {
                        input: Box::new(node),
                        exprs: projected_exprs,
                    };
                    if !order_keys.is_empty() {
                        node = PhysicalNode::Sort {
                            input: Box::new(node),
                            keys: order_keys,
                        };
                    }
                } else {
                    // Otherwise sort on raw columns BEFORE `Project` so unprojected ORDER BY columns
                    // (e.g., `SELECT ROUND(amount * 1.15, 2) ORDER BY amount DESC LIMIT 5`) sort accurately
                    // AND unlock Distributed Top-N Pushdown!
                    if !order_keys.is_empty() {
                        node = PhysicalNode::Sort {
                            input: Box::new(node),
                            keys: order_keys,
                        };
                    }
                    if !projected_exprs.is_empty() {
                        node = PhysicalNode::Project {
                            input: Box::new(node),
                            exprs: projected_exprs,
                        };
                    }
                }
                if is_distinct {
                    node = PhysicalNode::Distinct {
                        input: Box::new(node),
                    };
                }
            }

            if limit_val.is_some() || offset_val > 0 {
                let lim = limit_val.unwrap_or(usize::MAX);
                // Wave 8 Distributed Top-N Pushdown: if LIMIT wraps Sort (or Project(Sort)),
                // push a bounded local Top-N heap down to every worker before the final Exchange(Single)!
                if lim < usize::MAX {
                    match node {
                        PhysicalNode::Sort { input, keys } => {
                            let partial_topn = PhysicalNode::TopN {
                                input,
                                keys: keys.clone(),
                                limit: lim,
                                offset: offset_val,
                                is_partial: true,
                            };
                            node = PhysicalNode::TopN {
                                input: Box::new(partial_topn),
                                keys,
                                limit: lim,
                                offset: offset_val,
                                is_partial: false,
                            };
                            cbo_notes.push(format!(
                                "OPT: Distributed Top-N Pushdown (local limit={} before Exchange)",
                                lim.saturating_add(offset_val)
                            ));
                        }
                        PhysicalNode::Project { input, exprs }
                            if matches!(input.as_ref(), PhysicalNode::Sort { .. }) =>
                        {
                            if let PhysicalNode::Sort {
                                input: sort_in,
                                keys,
                            } = *input
                            {
                                let partial_topn = PhysicalNode::TopN {
                                    input: sort_in,
                                    keys: keys.clone(),
                                    limit: lim,
                                    offset: offset_val,
                                    is_partial: true,
                                };
                                let final_topn = PhysicalNode::TopN {
                                    input: Box::new(partial_topn),
                                    keys,
                                    limit: lim,
                                    offset: offset_val,
                                    is_partial: false,
                                };
                                node = PhysicalNode::Project {
                                    input: Box::new(final_topn),
                                    exprs,
                                };
                                cbo_notes.push(format!(
                                    "OPT: Distributed Top-N Pushdown (local limit={} before Exchange)",
                                    lim.saturating_add(offset_val)
                                ));
                            } else {
                                unreachable!()
                            }
                        }
                        other => {
                            node = PhysicalNode::Limit {
                                input: Box::new(other),
                                limit: lim,
                                offset: offset_val,
                            };
                        }
                    }
                } else {
                    node = PhysicalNode::Limit {
                        input: Box::new(node),
                        limit: lim,
                        offset: offset_val,
                    };
                }
            }

            Ok((node, cbo_notes))
        })
    }

    pub fn ensure_requirements(&self, node: PhysicalNode) -> PhysicalNode {
        match node {
            PhysicalNode::IcebergScan { .. } | PhysicalNode::ShuffleRead { .. } => node,
            PhysicalNode::Filter { input, predicate } => PhysicalNode::Filter {
                input: Box::new(self.ensure_requirements(*input)),
                predicate,
            },
            PhysicalNode::Project { input, exprs } => PhysicalNode::Project {
                input: Box::new(self.ensure_requirements(*input)),
                exprs,
            },
            PhysicalNode::Distinct { input } => {
                let child = self.ensure_requirements(*input);
                let aligned =
                    if Distribution::SinglePartition.satisfied_by(&child.output_partitioning()) {
                        child
                    } else {
                        PhysicalNode::Exchange {
                            input: Box::new(child),
                            target_partitioning: Partitioning::Single,
                            mode: self.config.shuffle_mode,
                        }
                    };
                PhysicalNode::Distinct {
                    input: Box::new(aligned),
                }
            }
            PhysicalNode::Sort { input, keys } => {
                let child = self.ensure_requirements(*input);
                let aligned =
                    if Distribution::SinglePartition.satisfied_by(&child.output_partitioning()) {
                        child
                    } else {
                        PhysicalNode::Exchange {
                            input: Box::new(child),
                            target_partitioning: Partitioning::Single,
                            mode: self.config.shuffle_mode,
                        }
                    };
                PhysicalNode::Sort {
                    input: Box::new(aligned),
                    keys,
                }
            }
            PhysicalNode::Limit {
                input,
                limit,
                offset,
            } => {
                let child = self.ensure_requirements(*input);
                let aligned =
                    if Distribution::SinglePartition.satisfied_by(&child.output_partitioning()) {
                        child
                    } else {
                        PhysicalNode::Exchange {
                            input: Box::new(child),
                            target_partitioning: Partitioning::Single,
                            mode: self.config.shuffle_mode,
                        }
                    };
                PhysicalNode::Limit {
                    input: Box::new(aligned),
                    limit,
                    offset,
                }
            }
            PhysicalNode::TopN {
                input,
                keys,
                limit,
                offset,
                is_partial,
            } => {
                let child = self.ensure_requirements(*input);
                if is_partial {
                    PhysicalNode::TopN {
                        input: Box::new(child),
                        keys,
                        limit,
                        offset,
                        is_partial: true,
                    }
                } else {
                    let aligned = if Distribution::SinglePartition
                        .satisfied_by(&child.output_partitioning())
                    {
                        child
                    } else {
                        PhysicalNode::Exchange {
                            input: Box::new(child),
                            target_partitioning: Partitioning::Single,
                            mode: self.config.shuffle_mode,
                        }
                    };
                    PhysicalNode::TopN {
                        input: Box::new(aligned),
                        keys,
                        limit,
                        offset,
                        is_partial: false,
                    }
                }
            }
            PhysicalNode::Window { input, specs } => {
                let child = self.ensure_requirements(*input);
                let aligned =
                    if Distribution::SinglePartition.satisfied_by(&child.output_partitioning()) {
                        child
                    } else {
                        PhysicalNode::Exchange {
                            input: Box::new(child),
                            target_partitioning: Partitioning::Single,
                            mode: self.config.shuffle_mode,
                        }
                    };
                PhysicalNode::Window {
                    input: Box::new(aligned),
                    specs,
                }
            }
            PhysicalNode::UnionAll { inputs, distinct } => {
                let aligned_inputs: Vec<PhysicalNode> = inputs
                    .into_iter()
                    .map(|inp| {
                        let child = self.ensure_requirements(inp);
                        if Distribution::SinglePartition
                            .satisfied_by(&child.output_partitioning())
                        {
                            child
                        } else {
                            PhysicalNode::Exchange {
                                input: Box::new(child),
                                target_partitioning: Partitioning::Single,
                                mode: self.config.shuffle_mode,
                            }
                        }
                    })
                    .collect();
                PhysicalNode::UnionAll {
                    inputs: aligned_inputs,
                    distinct,
                }
            }
            PhysicalNode::SetOp {
                op,
                left,
                right,
                distinct,
            } => {
                let lc = self.ensure_requirements(*left);
                let rc = self.ensure_requirements(*right);
                let al = if Distribution::SinglePartition.satisfied_by(&lc.output_partitioning()) {
                    lc
                } else {
                    PhysicalNode::Exchange {
                        input: Box::new(lc),
                        target_partitioning: Partitioning::Single,
                        mode: self.config.shuffle_mode,
                    }
                };
                let ar = if Distribution::SinglePartition.satisfied_by(&rc.output_partitioning()) {
                    rc
                } else {
                    PhysicalNode::Exchange {
                        input: Box::new(rc),
                        target_partitioning: Partitioning::Single,
                        mode: self.config.shuffle_mode,
                    }
                };
                PhysicalNode::SetOp {
                    op,
                    left: Box::new(al),
                    right: Box::new(ar),
                    distinct,
                }
            }
            PhysicalNode::Exchange {
                input,
                target_partitioning,
                mode,
            } => PhysicalNode::Exchange {
                input: Box::new(self.ensure_requirements(*input)),
                target_partitioning,
                mode,
            },
            PhysicalNode::HashAggregate {
                input,
                group_by,
                aggs,
                is_partial,
                grouping_sets,
            } => {
                let child = self.ensure_requirements(*input);
                if is_partial {
                    PhysicalNode::HashAggregate {
                        input: Box::new(child),
                        group_by,
                        aggs,
                        is_partial: true,
                        grouping_sets: vec![],
                    }
                } else {
                    let g_cols: Vec<String> = group_by.iter().map(|g| g.output_name()).collect();
                    let req = if g_cols.is_empty() || !grouping_sets.is_empty() {
                        Distribution::SinglePartition
                    } else {
                        Distribution::HashPartitioned(g_cols.clone())
                    };
                    let satisfied = req.satisfied_by(&child.output_partitioning());
                    let final_input = if satisfied {
                        child
                    } else {
                        let target = if g_cols.is_empty() || !grouping_sets.is_empty() {
                            Partitioning::Single
                        } else {
                            Partitioning::Hash {
                                columns: g_cols,
                                partitions: self.config.default_shuffle_partitions,
                            }
                        };
                        PhysicalNode::Exchange {
                            input: Box::new(child),
                            target_partitioning: target,
                            mode: self.config.shuffle_mode,
                        }
                    };
                    PhysicalNode::HashAggregate {
                        input: Box::new(final_input),
                        group_by,
                        aggs,
                        is_partial: false,
                        grouping_sets,
                    }
                }
            }
            PhysicalNode::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type,
                is_broadcast,
            } => {
                let left_child = self.ensure_requirements(*left);
                let right_child = self.ensure_requirements(*right);
                if is_broadcast {
                    let right_ex = PhysicalNode::Exchange {
                        input: Box::new(right_child),
                        target_partitioning: Partitioning::Single,
                        mode: ShuffleMode::Pipelined,
                    };
                    PhysicalNode::HashJoin {
                        left: Box::new(left_child),
                        right: Box::new(right_ex),
                        left_key,
                        right_key,
                        join_type,
                        is_broadcast: true,
                    }
                } else {
                    let parts = self.config.default_shuffle_partitions;
                    let left_ex = PhysicalNode::Exchange {
                        input: Box::new(left_child),
                        target_partitioning: Partitioning::Hash {
                            columns: vec![left_key.clone()],
                            partitions: parts,
                        },
                        mode: self.config.shuffle_mode,
                    };
                    let right_ex = PhysicalNode::Exchange {
                        input: Box::new(right_child),
                        target_partitioning: Partitioning::Hash {
                            columns: vec![right_key.clone()],
                            partitions: parts,
                        },
                        mode: self.config.shuffle_mode,
                    };
                    PhysicalNode::HashJoin {
                        left: Box::new(left_ex),
                        right: Box::new(right_ex),
                        left_key,
                        right_key,
                        join_type,
                        is_broadcast: false,
                    }
                }
            }
        }
    }

    pub fn cut_stages(&self, query_id: QueryId, root: PhysicalNode, cbo_notes: Vec<String>) -> StageDag {
        let mut stages = Vec::new();
        let mut next_stage_id: u32 = 1;

        fn cut_rec(
            node: PhysicalNode,
            stages: &mut Vec<QueryStage>,
            next_stage_id: &mut u32,
        ) -> (PhysicalNode, Vec<u32>) {
            match node {
                PhysicalNode::Exchange {
                    input,
                    target_partitioning,
                    mode,
                } => {
                    let (child_plan, child_deps) = cut_rec(*input, stages, next_stage_id);
                    let stage_id = *next_stage_id;
                    *next_stage_id += 1;
                    let num_partitions = child_plan.output_partitioning().partition_count();
                    stages.push(QueryStage {
                        stage_id,
                        upstream_stages: child_deps,
                        plan: child_plan,
                        output_partitioning: Some(target_partitioning.clone()),
                        shuffle_mode: Some(mode),
                        num_partitions,
                    });
                    (
                        PhysicalNode::ShuffleRead {
                            upstream_stage_id: stage_id,
                            output_partitioning: target_partitioning,
                            mode,
                        },
                        vec![stage_id],
                    )
                }
                PhysicalNode::Filter { input, predicate } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::Filter {
                            input: Box::new(p),
                            predicate,
                        },
                        deps,
                    )
                }
                PhysicalNode::Project { input, exprs } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::Project {
                            input: Box::new(p),
                            exprs,
                        },
                        deps,
                    )
                }
                PhysicalNode::Distinct { input } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (PhysicalNode::Distinct { input: Box::new(p) }, deps)
                }
                PhysicalNode::Sort { input, keys } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::Sort {
                            input: Box::new(p),
                            keys,
                        },
                        deps,
                    )
                }
                PhysicalNode::Limit {
                    input,
                    limit,
                    offset,
                } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::Limit {
                            input: Box::new(p),
                            limit,
                            offset,
                        },
                        deps,
                    )
                }
                PhysicalNode::TopN {
                    input,
                    keys,
                    limit,
                    offset,
                    is_partial,
                } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::TopN {
                            input: Box::new(p),
                            keys,
                            limit,
                            offset,
                            is_partial,
                        },
                        deps,
                    )
                }
                PhysicalNode::HashAggregate {
                    input,
                    group_by,
                    aggs,
                    is_partial,
                    grouping_sets,
                } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::HashAggregate {
                            input: Box::new(p),
                            group_by,
                            aggs,
                            is_partial,
                            grouping_sets,
                        },
                        deps,
                    )
                }
                PhysicalNode::HashJoin {
                    left,
                    right,
                    left_key,
                    right_key,
                    join_type,
                    is_broadcast,
                } => {
                    let (lp, mut ldeps) = cut_rec(*left, stages, next_stage_id);
                    let (rp, rdeps) = cut_rec(*right, stages, next_stage_id);
                    ldeps.extend(rdeps);
                    (
                        PhysicalNode::HashJoin {
                            left: Box::new(lp),
                            right: Box::new(rp),
                            left_key,
                            right_key,
                            join_type,
                            is_broadcast,
                        },
                        ldeps,
                    )
                }
                PhysicalNode::Window { input, specs } => {
                    let (p, deps) = cut_rec(*input, stages, next_stage_id);
                    (
                        PhysicalNode::Window {
                            input: Box::new(p),
                            specs,
                        },
                        deps,
                    )
                }
                PhysicalNode::UnionAll { inputs, distinct } => {
                    let mut new_inputs = Vec::with_capacity(inputs.len());
                    let mut all_deps = Vec::new();
                    for inp in inputs {
                        let (p, deps) = cut_rec(inp, stages, next_stage_id);
                        new_inputs.push(p);
                        all_deps.extend(deps);
                    }
                    (
                        PhysicalNode::UnionAll {
                            inputs: new_inputs,
                            distinct,
                        },
                        all_deps,
                    )
                }
                PhysicalNode::SetOp {
                    op,
                    left,
                    right,
                    distinct,
                } => {
                    let (lp, mut ldeps) = cut_rec(*left, stages, next_stage_id);
                    let (rp, rdeps) = cut_rec(*right, stages, next_stage_id);
                    ldeps.extend(rdeps);
                    (
                        PhysicalNode::SetOp {
                            op,
                            left: Box::new(lp),
                            right: Box::new(rp),
                            distinct,
                        },
                        ldeps,
                    )
                }
                leaf => (leaf, vec![]),
            }
        }

        let (final_plan, deps) = cut_rec(root, &mut stages, &mut next_stage_id);
        let final_stage_id = next_stage_id;
        let final_parts = final_plan.output_partitioning().partition_count();
        stages.push(QueryStage {
            stage_id: final_stage_id,
            upstream_stages: deps,
            plan: final_plan,
            output_partitioning: None,
            shuffle_mode: None,
            num_partitions: final_parts,
        });

        StageDag {
            query_id,
            stages,
            collapsed_single_node: false,
            cbo_notes,
        }
    }
}

/// Extracts optional `AT VERSION <snapshot_id>` Iceberg Time Travel clause from a SQL query.
fn extract_create_table_partition_by(sql: &str) -> (String, Vec<String>) {
    let upper = sql.to_uppercase();
    if !upper.starts_with("CREATE TABLE ") {
        return (sql.to_string(), vec![]);
    }
    let Some(pos) = upper.rfind(" PARTITION BY ") else {
        return (sql.to_string(), vec![]);
    };
    let before = sql[..pos].trim().to_string();
    let mut tail = sql[pos + " PARTITION BY ".len()..].trim();
    if let Some(inner) = tail.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        tail = inner;
    }
    // Split on top-level commas only so transforms like `bucket(16, id)` stay intact!
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut depth = 0usize;
    for ch in tail.chars() {
        match ch {
            '(' => {
                depth += 1;
                cur.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                cur.push(ch);
            }
            ',' if depth == 0 => {
                let t = cur.trim();
                if !t.is_empty() {
                    parts.push(t.to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    let last = cur.trim();
    if !last.is_empty() {
        parts.push(last.to_string());
    }
    (before, parts)
}

fn extract_time_travel_clause(sql: &str) -> Result<(String, Option<u64>)> {
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.find(" AT VERSION ") {
        let before = &sql[..pos];
        let after = sql[pos + " AT VERSION ".len()..].trim_start();
        let ver_end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        let ver_str = &after[..ver_end];
        let version = ver_str.parse::<u64>().map_err(|_| {
            RustLakeError::SqlParse(format!("invalid AT VERSION snapshot id: '{ver_str}'"))
        })?;
        let remainder = &after[ver_end..];
        Ok((format!("{before}{remainder}"), Some(version)))
    } else {
        Ok((sql.to_string(), None))
    }
}

fn parse_col_expr(expr: &Expr, alias: Option<String>) -> Result<ColumnExpr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LongArrow,
            right,
        } => {
            let col = extract_ident(left)?;
            let path = match parse_scalar(right)? {
                ScalarValue::Utf8(s) => s,
                _ => return Err(RustLakeError::Plan("JSON path must be string".into())),
            };
            let out = alias.unwrap_or_else(|| format!("{col}_{path}"));
            Ok(ColumnExpr::JsonPath {
                column: col,
                path,
                alias: out,
            })
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Custom(op_str),
            right,
        } if op_str == "<->" => {
            let column = extract_ident(left)?;
            let vec_str = match parse_scalar(right)? {
                ScalarValue::Utf8(s) => s,
                _ => return Err(RustLakeError::Plan("vector literal must be string '[...]'".into())),
            };
            let query_vec = parse_vec_f64(&vec_str)?;
            Ok(ColumnExpr::VectorDistance {
                column,
                query_vec,
                metric: VectorMetric::L2,
                alias: alias.unwrap_or_else(|| "vector_distance".into()),
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut branches = Vec::with_capacity(conditions.len());
            for (cond, res) in conditions.iter().zip(results.iter()) {
                let pred = if let Some(op_expr) = operand {
                    let eq_expr = Expr::BinaryOp {
                        left: op_expr.clone(),
                        op: BinaryOperator::Eq,
                        right: Box::new(cond.clone()),
                    };
                    parse_filter_expr(&eq_expr)?
                } else {
                    parse_filter_expr(cond)?
                };
                branches.push((pred, Box::new(parse_col_expr(res, None)?)));
            }
            let else_expr = match else_result {
                Some(e) => Box::new(parse_col_expr(e, None)?),
                None => Box::new(ColumnExpr::Literal {
                    value: ScalarValue::Null,
                    alias: "null".into(),
                }),
            };
            Ok(ColumnExpr::CaseExpr {
                branches,
                else_expr,
                alias: alias.unwrap_or_else(|| "case_expr".into()),
            })
        }
        Expr::Extract { field, expr, .. } => {
            let inner = parse_col_expr(expr, None)?;
            Ok(ColumnExpr::Extract {
                field: field.to_string().to_uppercase(),
                expr: Box::new(inner),
                alias: alias.unwrap_or_else(|| format!("extract_{}", field.to_string().to_lowercase())),
            })
        }
        Expr::TypedString { value, .. } => Ok(ColumnExpr::Literal {
            value: ScalarValue::Utf8(value.clone()),
            alias: alias.unwrap_or_else(|| "literal".into()),
        }),
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) =>
        {
            let aop = match op {
                BinaryOperator::Plus => ArithOp::Add,
                BinaryOperator::Minus => ArithOp::Sub,
                BinaryOperator::Multiply => ArithOp::Mul,
                BinaryOperator::Divide => ArithOp::Div,
                _ => unreachable!(),
            };
            let l = parse_col_expr(left, None)?;
            let r = parse_col_expr(right, None)?;
            Ok(ColumnExpr::BinaryArith {
                left: Box::new(l),
                op: aop,
                right: Box::new(r),
                alias: alias.unwrap_or_else(|| "arith_expr".into()),
            })
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_ce = parse_col_expr(inner, None)?;
            Ok(ColumnExpr::Cast {
                expr: Box::new(inner_ce),
                target_type: data_type.to_string(),
                alias: alias.unwrap_or_else(|| "cast_expr".into()),
            })
        }
        Expr::Trim { expr: inner, .. } => {
            let inner_ce = parse_col_expr(inner, None)?;
            Ok(ColumnExpr::StringExpr {
                func: StringFunc::Trim,
                args: vec![inner_ce],
                alias: alias.unwrap_or_else(|| "trim".into()),
            })
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![parse_col_expr(inner, None)?];
            if let Some(from_e) = substring_from {
                args.push(parse_col_expr(from_e, None)?);
            }
            if let Some(for_e) = substring_for {
                args.push(parse_col_expr(for_e, None)?);
            }
            Ok(ColumnExpr::StringExpr {
                func: StringFunc::Substring,
                args,
                alias: alias.unwrap_or_else(|| "substring".into()),
            })
        }
        Expr::Nested(inner) => parse_col_expr(inner, alias),
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            let args = match &func.args {
                FunctionArguments::List(list) => list.args.as_slice(),
                _ => &[],
            };
            match name.as_str() {
                "NOW" | "CURRENT_TIMESTAMP" => Ok(ColumnExpr::Now {
                    alias: alias.unwrap_or_else(|| "now".into()),
                }),
                "LOWER" | "UPPER" | "LENGTH" | "TRIM" | "CONCAT" | "SUBSTRING" | "SUBSTR" => {
                    let sf = match name.as_str() {
                        "LOWER" => StringFunc::Lower,
                        "UPPER" => StringFunc::Upper,
                        "LENGTH" => StringFunc::Length,
                        "TRIM" => StringFunc::Trim,
                        "CONCAT" => StringFunc::Concat,
                        _ => StringFunc::Substring,
                    };
                    let mut parsed_args = Vec::with_capacity(args.len());
                    for a in args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                            parsed_args.push(parse_col_expr(e, None)?);
                        }
                    }
                    Ok(ColumnExpr::StringExpr {
                        func: sf,
                        args: parsed_args,
                        alias: alias.unwrap_or_else(|| name.to_lowercase()),
                    })
                }
                "ROUND" | "ABS" => {
                    let mf = if name == "ABS" {
                        MathFunc::Abs
                    } else {
                        MathFunc::Round
                    };
                    let first = match args.first() {
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                            parse_col_expr(e, None)?
                        }
                        _ => return Err(RustLakeError::Plan(format!("{name} requires an argument"))),
                    };
                    let precision = match args.get(1) {
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                            match parse_scalar(e)? {
                                ScalarValue::Int64(n) => Some(n as i32),
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    Ok(ColumnExpr::MathExpr {
                        func: mf,
                        arg: Box::new(first),
                        precision,
                        alias: alias.unwrap_or_else(|| name.to_lowercase()),
                    })
                }
                "BM25_SCORE" => {
                    let column = extract_fn_arg_ident(args.first())?;
                    let query = match args.get(1) {
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                            match parse_scalar(e)? {
                                ScalarValue::Utf8(s) => s,
                                _ => return Err(RustLakeError::Plan("BM25_SCORE query must be string".into())),
                            }
                        }
                        _ => return Err(RustLakeError::Plan("BM25_SCORE requires (column, 'query')".into())),
                    };
                    Ok(ColumnExpr::Bm25Score {
                        column,
                        query,
                        alias: alias.unwrap_or_else(|| "bm25_score".into()),
                    })
                }
                "COALESCE" => {
                    let mut columns = Vec::new();
                    let mut default = ScalarValue::Null;
                    for arg in args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            if let Ok(c) = extract_ident(e) {
                                columns.push(c);
                            } else if let Ok(s) = parse_scalar(e) {
                                default = s;
                            }
                        }
                    }
                    Ok(ColumnExpr::Coalesce {
                        columns,
                        default,
                        alias: alias.unwrap_or_else(|| "coalesce".into()),
                    })
                }
                "DATE_TRUNC" => {
                    let unit = match args.first() {
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                            match parse_scalar(e)? {
                                ScalarValue::Utf8(s) => s,
                                _ => return Err(RustLakeError::Plan("DATE_TRUNC unit must be string".into())),
                            }
                        }
                        _ => return Err(RustLakeError::Plan("DATE_TRUNC requires (unit, column)".into())),
                    };
                    let column = extract_fn_arg_ident(args.get(1))?;
                    Ok(ColumnExpr::DateTrunc {
                        unit,
                        column,
                        alias: alias.unwrap_or_else(|| "date_trunc".into()),
                    })
                }
                "VECTOR_COSINE" | "VECTOR_L2" => {
                    let column = extract_fn_arg_ident(args.first())?;
                    let vec_str = match args.get(1) {
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                            match parse_scalar(e)? {
                                ScalarValue::Utf8(s) => s,
                                _ => return Err(RustLakeError::Plan("vector argument must be string '[...]'".into())),
                            }
                        }
                        _ => return Err(RustLakeError::Plan("vector function requires (col, '[...]')".into())),
                    };
                    let query_vec = parse_vec_f64(&vec_str)?;
                    let metric = if name == "VECTOR_COSINE" {
                        VectorMetric::Cosine
                    } else {
                        VectorMetric::L2
                    };
                    Ok(ColumnExpr::VectorDistance {
                        column,
                        query_vec,
                        metric,
                        alias: alias.unwrap_or_else(|| name.to_lowercase()),
                    })
                }
                _ => Err(RustLakeError::Plan(format!(
                    "unsupported scalar function: {name}"
                ))),
            }
        }
        Expr::Value(_) | Expr::UnaryOp { .. } => Ok(ColumnExpr::Literal {
            value: parse_scalar(expr)?,
            alias: alias.unwrap_or_else(|| "lit".into()),
        }),
        _ => Ok(ColumnExpr::Column(extract_ident(expr)?)),
    }
}

fn parse_vec_f64(s: &str) -> Result<Vec<f64>> {
    let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
    if trimmed.is_empty() {
        return Err(RustLakeError::Plan("vector literal cannot be empty".into()));
    }
    trimmed
        .split(',')
        .map(|p| {
            p.trim()
                .parse::<f64>()
                .map_err(|_| RustLakeError::Plan(format!("invalid float in vector literal: '{p}'")))
        })
        .collect()
}

fn map_sql_data_type(dt: &SqlDataType) -> Result<ColumnType> {
    match dt {
        SqlDataType::Int(_)
        | SqlDataType::Integer(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::Int64 => Ok(ColumnType::Int64),
        SqlDataType::Float(_)
        | SqlDataType::Double
        | SqlDataType::DoublePrecision
        | SqlDataType::Real
        | SqlDataType::Float64 => Ok(ColumnType::Float64),
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => {
            let (precision, scale) = match info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                    ((*p).clamp(1, 38) as u8, (*s).clamp(0, 18) as u8)
                }
                sqlparser::ast::ExactNumberInfo::Precision(p) => ((*p).clamp(1, 38) as u8, 0u8),
                sqlparser::ast::ExactNumberInfo::None => (18u8, 2u8),
            };
            Ok(ColumnType::Decimal { precision, scale })
        }
        SqlDataType::Date => Ok(ColumnType::Date),
        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::String(_) => Ok(ColumnType::Utf8),
        SqlDataType::Boolean | SqlDataType::Bool => Ok(ColumnType::Bool),
        SqlDataType::Timestamp(_, _) | SqlDataType::Datetime(_) => Ok(ColumnType::TimestampTz),
        SqlDataType::JSON | SqlDataType::JSONB => Ok(ColumnType::Variant),
        SqlDataType::Custom(name, _) if name.to_string().eq_ignore_ascii_case("VARIANT") => {
            Ok(ColumnType::Variant)
        }
        other => Err(RustLakeError::Plan(format!(
            "unsupported SQL column type: {other:?}"
        ))),
    }
}

fn extract_table_name(factor: &TableFactor) -> Result<String> {
    match factor {
        TableFactor::Table { name, .. } => Ok(normalize_table_name(&name.to_string())),
        other => Err(RustLakeError::Plan(format!(
            "unsupported table factor: {other:?}"
        ))),
    }
}

fn extract_ident(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => Ok(idents
            .last()
            .map(|i| i.value.clone())
            .unwrap_or_default()),
        Expr::Nested(inner) => extract_ident(inner),
        other => Err(RustLakeError::Plan(format!(
            "expected column identifier, got {other:?}"
        ))),
    }
}

fn extract_join_keys(expr: &Expr) -> Result<(String, String)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => Ok((extract_ident(left)?, extract_ident(right)?)),
        other => Err(RustLakeError::Plan(format!(
            "only equi-join ON a = b supported, got {other:?}"
        ))),
    }
}

fn parse_scalar(expr: &Expr) -> Result<ScalarValue> {
    match expr {
        Expr::Nested(inner) => parse_scalar(inner),
        Expr::TypedString { value, .. } => Ok(ScalarValue::Utf8(value.clone())),
        Expr::Value(v) => match v {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(ScalarValue::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(ScalarValue::Float64(f))
                } else {
                    Err(RustLakeError::Plan(format!("invalid number literal: {n}")))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(ScalarValue::Utf8(s.clone()))
            }
            Value::Boolean(b) => Ok(ScalarValue::Bool(*b)),
            Value::Null => Ok(ScalarValue::Null),
            other => Err(RustLakeError::Plan(format!("unsupported literal: {other:?}"))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match parse_scalar(expr)? {
            ScalarValue::Int64(i) => Ok(ScalarValue::Int64(-i)),
            ScalarValue::Float64(f) => Ok(ScalarValue::Float64(-f)),
            other => Err(RustLakeError::Plan(format!("cannot negate {other:?}"))),
        },
        // Compile-time DATE +/- INTERVAL 'N' DAY|MONTH|YEAR folding and constant arithmetic (Wave 19 TPC-H)!
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) =>
        {
            if let Expr::Interval(iv) = right.as_ref() {
                let base_str = match parse_scalar(left)? {
                    ScalarValue::Utf8(s) => s,
                    other => {
                        return Err(RustLakeError::Plan(format!(
                            "INTERVAL arithmetic expects date string, got {other:?}"
                        )))
                    }
                };
                let sign: i64 = if matches!(op, BinaryOperator::Minus) { -1 } else { 1 };
                let amount_raw = match parse_scalar(&iv.value)? {
                    ScalarValue::Utf8(s) => s.trim().parse::<i64>().unwrap_or(0),
                    ScalarValue::Int64(i) => i,
                    _ => 0,
                };
                let unit = iv
                    .leading_field
                    .as_ref()
                    .map(|f| f.to_string().to_uppercase())
                    .unwrap_or_else(|| "DAY".into());
                let folded = fold_date_interval(&base_str, sign * amount_raw, &unit)?;
                return Ok(ScalarValue::Utf8(folded));
            }
            let lv = parse_scalar(left)?;
            let rv = parse_scalar(right)?;
            if let (Some(a), Some(b)) = (lv.as_f64(), rv.as_f64()) {
                let res = match op {
                    BinaryOperator::Plus => a + b,
                    BinaryOperator::Minus => a - b,
                    BinaryOperator::Multiply => a * b,
                    BinaryOperator::Divide => {
                        if b == 0.0 {
                            0.0
                        } else {
                            a / b
                        }
                    }
                    _ => unreachable!(),
                };
                return Ok(ScalarValue::Float64(res));
            }
            Err(RustLakeError::Plan(format!(
                "unsupported constant binary op: {lv:?} {op:?} {rv:?}"
            )))
        }
        other => Err(RustLakeError::Plan(format!("expected literal, got {other:?}"))),
    }
}

fn fold_date_interval(base_iso: &str, delta: i64, unit: &str) -> Result<String> {
    let days = rustlakedb_protocol::parse_iso_date_days(base_iso).ok_or_else(|| {
        RustLakeError::Plan(format!("invalid ISO date literal '{base_iso}'"))
    })?;
    let new_days = match unit {
        "DAY" | "DAYS" => days + delta as i32,
        "WEEK" | "WEEKS" => days + (delta * 7) as i32,
        "MONTH" | "MONTHS" => {
            let parts: Vec<i64> = base_iso
                .split('-')
                .filter_map(|p| p.parse::<i64>().ok())
                .collect();
            if parts.len() == 3 {
                let total_m = (parts[0] * 12 + (parts[1] - 1)) + delta;
                let ny = total_m.div_euclid(12);
                let nm = total_m.rem_euclid(12) + 1;
                let nd = parts[2].min(28);
                return Ok(format!("{ny:04}-{nm:02}-{nd:02}"));
            }
            days + (delta * 30) as i32
        }
        "YEAR" | "YEARS" => {
            let parts: Vec<i64> = base_iso
                .split('-')
                .filter_map(|p| p.parse::<i64>().ok())
                .collect();
            if parts.len() == 3 {
                let ny = parts[0] + delta;
                return Ok(format!("{ny:04}-{:02}-{:02}", parts[1], parts[2]));
            }
            days + (delta * 365) as i32
        }
        _ => days + delta as i32,
    };
    Ok(rustlakedb_protocol::format_iso_date_days(new_days))
}

fn parse_filter_expr(expr: &Expr) -> Result<FilterExpr> {
    match expr {
        Expr::Nested(inner) => parse_filter_expr(inner),
        Expr::Value(Value::Boolean(b)) => Ok(FilterExpr::LiteralBool(*b)),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(FilterExpr::Not(Box::new(parse_filter_expr(expr)?))),
        Expr::IsNull(e) => Ok(FilterExpr::IsNull {
            column: extract_ident(e)?,
        }),
        Expr::IsNotNull(e) => Ok(FilterExpr::IsNotNull {
            column: extract_ident(e)?,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let column = extract_ident(expr)?;
            let values = list.iter().map(parse_scalar).collect::<Result<Vec<_>>>()?;
            let node = FilterExpr::InList { column, values };
            Ok(if *negated {
                FilterExpr::Not(Box::new(node))
            } else {
                node
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let column = extract_ident(expr)?;
            let lo = parse_scalar(low)?;
            let hi = parse_scalar(high)?;
            let node = FilterExpr::And(
                Box::new(FilterExpr::GtEq {
                    column: column.clone(),
                    value: lo,
                }),
                Box::new(FilterExpr::LtEq { column, value: hi }),
            );
            Ok(if *negated {
                FilterExpr::Not(Box::new(node))
            } else {
                node
            })
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        }
        | Expr::ILike {
            negated,
            expr,
            pattern,
            ..
        } => {
            let column = extract_ident(expr)?;
            let pat = match parse_scalar(pattern)? {
                ScalarValue::Utf8(s) => s,
                _ => return Err(RustLakeError::Plan("LIKE pattern must be string".into())),
            };
            let node = FilterExpr::Like {
                column,
                pattern: pat,
            };
            Ok(if *negated {
                FilterExpr::Not(Box::new(node))
            } else {
                node
            })
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(FilterExpr::And(
                Box::new(parse_filter_expr(left)?),
                Box::new(parse_filter_expr(right)?),
            )),
            BinaryOperator::Or => Ok(FilterExpr::Or(
                Box::new(parse_filter_expr(left)?),
                Box::new(parse_filter_expr(right)?),
            )),
            BinaryOperator::Eq => {
                if let Expr::BinaryOp {
                    left: json_col,
                    op: BinaryOperator::LongArrow,
                    right: json_key,
                } = left.as_ref()
                {
                    let col = extract_ident(json_col)?;
                    let path = match parse_scalar(json_key)? {
                        ScalarValue::Utf8(s) => s,
                        _ => return Err(RustLakeError::Plan("JSON path must be string".into())),
                    };
                    let val = match parse_scalar(right)? {
                        ScalarValue::Utf8(s) => s,
                        ScalarValue::Int64(i) => i.to_string(),
                        ScalarValue::Bool(b) => b.to_string(),
                        _ => {
                            return Err(RustLakeError::Plan(
                                "JSON comparison value must be scalar".into(),
                            ))
                        }
                    };
                    return Ok(FilterExpr::JsonPathEq {
                        column: col,
                        json_path: path,
                        value: val,
                    });
                }
                if let (Ok(lv), Ok(rv)) = (parse_scalar(left), parse_scalar(right)) {
                    return Ok(FilterExpr::LiteralBool(lv == rv));
                }
                let lcol = extract_ident(left)?;
                if let Ok(val) = parse_scalar(right) {
                    Ok(FilterExpr::Eq { column: lcol, value: val })
                } else {
                    Ok(FilterExpr::ColCmp {
                        left: lcol,
                        op: rustlakedb_protocol::CmpOp::Eq,
                        right: extract_ident(right)?,
                    })
                }
            }
            BinaryOperator::AtArrow => {
                // Postgres JSONB `@>` containment (`payload @> '{"env": "prod"}'`).
                let column = extract_ident(left)?;
                let json_str = match parse_scalar(right)? {
                    ScalarValue::Utf8(s) => s,
                    _ => return Err(RustLakeError::Plan("JSON @> operand must be JSON string".into())),
                };
                let subset: serde_json::Value = serde_json::from_str(&json_str)
                    .map_err(|e| RustLakeError::SqlParse(format!("invalid JSON in @>: {e}")))?;
                Ok(FilterExpr::JsonContains { column, subset })
            }
            BinaryOperator::Question => {
                // Postgres JSONB `?` key-exists (`payload ? 'trace_id'`).
                let column = extract_ident(left)?;
                let key = match parse_scalar(right)? {
                    ScalarValue::Utf8(s) => s,
                    _ => return Err(RustLakeError::Plan("JSON ? key must be string".into())),
                };
                Ok(FilterExpr::JsonHasKey { column, key })
            }
            BinaryOperator::NotEq => {
                let lcol = extract_ident(left)?;
                if let Ok(value) = parse_scalar(right) {
                    Ok(FilterExpr::NotEq { column: lcol, value })
                } else {
                    Ok(FilterExpr::ColCmp {
                        left: lcol,
                        op: rustlakedb_protocol::CmpOp::NotEq,
                        right: extract_ident(right)?,
                    })
                }
            }
            BinaryOperator::Gt => {
                let lcol = extract_ident(left)?;
                if let Ok(value) = parse_scalar(right) {
                    Ok(FilterExpr::Gt { column: lcol, value })
                } else {
                    Ok(FilterExpr::ColCmp {
                        left: lcol,
                        op: rustlakedb_protocol::CmpOp::Gt,
                        right: extract_ident(right)?,
                    })
                }
            }
            BinaryOperator::GtEq => {
                let lcol = extract_ident(left)?;
                if let Ok(value) = parse_scalar(right) {
                    Ok(FilterExpr::GtEq { column: lcol, value })
                } else {
                    Ok(FilterExpr::ColCmp {
                        left: lcol,
                        op: rustlakedb_protocol::CmpOp::GtEq,
                        right: extract_ident(right)?,
                    })
                }
            }
            BinaryOperator::Lt => {
                let lcol = extract_ident(left)?;
                if let Ok(value) = parse_scalar(right) {
                    Ok(FilterExpr::Lt { column: lcol, value })
                } else {
                    Ok(FilterExpr::ColCmp {
                        left: lcol,
                        op: rustlakedb_protocol::CmpOp::Lt,
                        right: extract_ident(right)?,
                    })
                }
            }
            BinaryOperator::LtEq => {
                let lcol = extract_ident(left)?;
                if let Ok(value) = parse_scalar(right) {
                    Ok(FilterExpr::LtEq { column: lcol, value })
                } else {
                    Ok(FilterExpr::ColCmp {
                        left: lcol,
                        op: rustlakedb_protocol::CmpOp::LtEq,
                        right: extract_ident(right)?,
                    })
                }
            }
            other => Err(RustLakeError::Plan(format!(
                "unsupported filter operator: {other:?}"
            ))),
        },
        other => Err(RustLakeError::Plan(format!(
            "unsupported filter expression: {other:?}"
        ))),
    }
}

/// Rewrites inline aggregate calls inside `HAVING` (e.g. `HAVING SUM(l_quantity) > 30.0` or
/// `HAVING COUNT(*) >= 2`) by ensuring the aggregate exists in `aggs` and replacing the call
/// with its output alias so `parse_filter_expr` can evaluate it cleanly (Wave 19 TPC-H Q18).
fn rewrite_having_agg_expr(
    expr: &Expr,
    aggs: &mut Vec<AggExpr>,
    pre_agg_exprs: &mut Vec<ColumnExpr>,
) -> Result<Expr> {
    if let Some(parsed_agg) = try_parse_agg_with_expr(expr, None, pre_agg_exprs)? {
        let alias = if let Some(existing) = aggs.iter().find(|a| a.func == parsed_agg.func) {
            existing.alias.clone()
        } else {
            let a = format!("__having_agg_{}", aggs.len());
            aggs.push(AggExpr {
                func: parsed_agg.func,
                alias: a.clone(),
            });
            a
        };
        return Ok(Expr::Identifier(sqlparser::ast::Ident::new(alias)));
    }
    match expr {
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_having_agg_expr(left, aggs, pre_agg_exprs)?),
            op: op.clone(),
            right: Box::new(rewrite_having_agg_expr(right, aggs, pre_agg_exprs)?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_having_agg_expr(
            inner,
            aggs,
            pre_agg_exprs,
        )?))),
        Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_having_agg_expr(inner, aggs, pre_agg_exprs)?),
        }),
        other => Ok(other.clone()),
    }
}

fn try_parse_agg_with_expr(
    expr: &Expr,
    alias: Option<String>,
    pre_agg_exprs: &mut Vec<ColumnExpr>,
) -> Result<Option<AggExpr>> {
    let Expr::Function(func) = expr else {
        return Ok(None);
    };
    if func.over.is_some() {
        return Ok(None);
    }
    let name = func.name.to_string().to_uppercase();
    let (args, is_distinct) = match &func.args {
        FunctionArguments::List(list) => (
            &list.args,
            matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct)),
        ),
        _ => return Ok(None),
    };
    let resolve_arg_col = |arg: Option<&FunctionArg>,
                           pre: &mut Vec<ColumnExpr>|
     -> Result<String> {
        if let Ok(col) = extract_fn_arg_ident(arg) {
            return Ok(col);
        }
        match arg {
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                let syn = format!("__agg_arg_{}", pre.len());
                let ce = parse_col_expr(e, Some(syn.clone()))?;
                pre.push(ce);
                Ok(syn)
            }
            other => Err(RustLakeError::Plan(format!(
                "expected column or expression in aggregate, got {other:?}"
            ))),
        }
    };
    match name.as_str() {
        "COUNT" => {
            if is_distinct {
                let col = resolve_arg_col(args.first(), pre_agg_exprs)?;
                Ok(Some(AggExpr {
                    func: AggFunc::CountDistinct(col.clone()),
                    alias: alias.unwrap_or_else(|| format!("count_distinct_{col}")),
                }))
            } else {
                Ok(Some(AggExpr {
                    func: AggFunc::Count,
                    alias: alias.unwrap_or_else(|| "count".into()),
                }))
            }
        }
        "SUM" => {
            let col = resolve_arg_col(args.first(), pre_agg_exprs)?;
            Ok(Some(AggExpr {
                func: AggFunc::Sum(col.clone()),
                alias: alias.unwrap_or_else(|| format!("sum_{col}")),
            }))
        }
        "AVG" => {
            let col = resolve_arg_col(args.first(), pre_agg_exprs)?;
            Ok(Some(AggExpr {
                func: AggFunc::Avg(col.clone()),
                alias: alias.unwrap_or_else(|| format!("avg_{col}")),
            }))
        }
        "MIN" => {
            let col = resolve_arg_col(args.first(), pre_agg_exprs)?;
            Ok(Some(AggExpr {
                func: AggFunc::Min(col.clone()),
                alias: alias.unwrap_or_else(|| format!("min_{col}")),
            }))
        }
        "MAX" => {
            let col = resolve_arg_col(args.first(), pre_agg_exprs)?;
            Ok(Some(AggExpr {
                func: AggFunc::Max(col.clone()),
                alias: alias.unwrap_or_else(|| format!("max_{col}")),
            }))
        }
        _ => Ok(None),
    }
}

/// Predicate Pushdown Through Joins (Wave 19 CBO):
/// Splits conjunctive `AND` predicates above a `HashJoin` and pushes single-table predicates
/// directly onto the owning `IcebergScan` leaf before the join executes!
async fn push_filter_through_joins(
    node: PhysicalNode,
    pred: FilterExpr,
    catalog: &dyn Catalog,
    _branch: &str,
) -> (PhysicalNode, Option<FilterExpr>, usize) {
    let mut table_cols: HashMap<String, HashSet<String>> = HashMap::new();
    let mut table_names: Vec<String> = Vec::new();
    collect_scan_table_names(&node, &mut table_names);
    for t in &table_names {
        if let Ok(meta) = catalog.get_table(t).await {
            let set: HashSet<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
            table_cols.insert(t.clone(), set);
        }
    }

    let mut conjuncts = Vec::new();
    flatten_and_conjuncts(pred, &mut conjuncts);

    let mut per_table_preds: HashMap<String, Vec<FilterExpr>> = HashMap::new();
    let mut remaining: Vec<FilterExpr> = Vec::new();
    let mut pushed_cnt = 0usize;

    for c in conjuncts {
        let mut cols = HashSet::new();
        collect_filter_cols(&c, &mut cols);
        let owners: Vec<&String> = table_cols
            .iter()
            .filter(|(_, schema_cols)| !cols.is_empty() && cols.iter().all(|col| schema_cols.contains(col)))
            .map(|(t, _)| t)
            .collect();
        if owners.len() == 1 {
            per_table_preds
                .entry(owners[0].clone())
                .or_default()
                .push(c);
            pushed_cnt += 1;
        } else {
            remaining.push(c);
        }
    }

    let rewritten = if pushed_cnt > 0 {
        attach_pushed_preds_to_scans(node, &per_table_preds)
    } else {
        node
    };
    let rem_expr = combine_and_conjuncts(remaining);
    (rewritten, rem_expr, pushed_cnt)
}

fn collect_scan_table_names(node: &PhysicalNode, out: &mut Vec<String>) {
    match node {
        PhysicalNode::IcebergScan { table_name, .. } => {
            if !out.contains(table_name) {
                out.push(table_name.clone());
            }
        }
        PhysicalNode::HashJoin { left, right, join_type, .. } => {
            // Only safely push through INNER joins
            if *join_type == JoinType::Inner {
                collect_scan_table_names(left, out);
                collect_scan_table_names(right, out);
            }
        }
        _ => {}
    }
}

fn attach_pushed_preds_to_scans(
    node: PhysicalNode,
    per_table: &HashMap<String, Vec<FilterExpr>>,
) -> PhysicalNode {
    match node {
        PhysicalNode::IcebergScan {
            table_name,
            projection,
            pushed_filter,
            splits,
        } => {
            let mut all = Vec::new();
            if let Some(existing) = pushed_filter {
                all.push(existing);
            }
            if let Some(extra) = per_table.get(&table_name) {
                all.extend(extra.iter().cloned());
            }
            PhysicalNode::IcebergScan {
                table_name,
                projection,
                pushed_filter: combine_and_conjuncts(all),
                splits,
            }
        }
        PhysicalNode::HashJoin {
            left,
            right,
            left_key,
            right_key,
            join_type,
            is_broadcast,
        } if join_type == JoinType::Inner => PhysicalNode::HashJoin {
            left: Box::new(attach_pushed_preds_to_scans(*left, per_table)),
            right: Box::new(attach_pushed_preds_to_scans(*right, per_table)),
            left_key,
            right_key,
            join_type,
            is_broadcast,
        },
        other => other,
    }
}

fn flatten_and_conjuncts(expr: FilterExpr, out: &mut Vec<FilterExpr>) {
    match expr {
        FilterExpr::And(a, b) => {
            flatten_and_conjuncts(*a, out);
            flatten_and_conjuncts(*b, out);
        }
        other => out.push(other),
    }
}

fn combine_and_conjuncts(list: Vec<FilterExpr>) -> Option<FilterExpr> {
    let mut iter = list.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| {
        FilterExpr::And(Box::new(acc), Box::new(next))
    }))
}

fn extract_fn_arg_ident(arg: Option<&FunctionArg>) -> Result<String> {
    match arg {
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => extract_ident(e),
        other => Err(RustLakeError::Plan(format!(
            "expected column argument in aggregate, got {other:?}"
        ))),
    }
}

/// Top-down Parquet Column Projection Pushdown (Wave 1):
/// Propagates the exact set of referenced columns down to every `IcebergScan` leaf
/// (across `Project`, `HashAggregate`, `Filter`, `Sort`, `Limit`, `Distinct`, and `HashJoin`)
/// so `parquet-rs` uses `ProjectionMask` to skip unread columns (especially heavy JSON blobs).
fn push_projections_to_scans(
    node: PhysicalNode,
    required: Option<HashSet<String>>,
) -> PhysicalNode {
    match node {
        PhysicalNode::IcebergScan {
            table_name,
            projection,
            pushed_filter,
            splits,
        } => {
            let final_proj = match required {
                Some(mut req) => {
                    if let Some(ref f) = pushed_filter {
                        collect_filter_cols(f, &mut req);
                    }
                    let mut v: Vec<String> = req.into_iter().collect();
                    v.sort();
                    v
                }
                None => projection,
            };
            PhysicalNode::IcebergScan {
                table_name,
                projection: final_proj,
                pushed_filter,
                splits,
            }
        }
        PhysicalNode::Project { input, exprs } => {
            let mut child_req = HashSet::new();
            for e in &exprs {
                collect_col_expr_cols(e, &mut child_req);
            }
            let req_opt = if child_req.is_empty() {
                None
            } else {
                Some(child_req)
            };
            PhysicalNode::Project {
                input: Box::new(push_projections_to_scans(*input, req_opt)),
                exprs,
            }
        }
        PhysicalNode::HashAggregate {
            input,
            group_by,
            aggs,
            is_partial,
            grouping_sets,
        } => {
            if is_partial {
                let mut child_req = HashSet::new();
                for g in &group_by {
                    collect_col_expr_cols(g, &mut child_req);
                }
                for a in &aggs {
                    match &a.func {
                        AggFunc::Count => {}
                        AggFunc::CountDistinct(c)
                        | AggFunc::Sum(c)
                        | AggFunc::Avg(c)
                        | AggFunc::Min(c)
                        | AggFunc::Max(c) => {
                            child_req.insert(c.clone());
                        }
                    }
                }
                // Only narrow scan projection if at least one column is referenced (for COUNT(*),
                // the vectorized Arrow fast path projects column 0 directly).
                let req_opt = if child_req.is_empty() {
                    None
                } else {
                    Some(child_req)
                };
                PhysicalNode::HashAggregate {
                    input: Box::new(push_projections_to_scans(*input, req_opt)),
                    group_by,
                    aggs,
                    is_partial,
                    grouping_sets,
                }
            } else {
                PhysicalNode::HashAggregate {
                    input: Box::new(push_projections_to_scans(*input, None)),
                    group_by,
                    aggs,
                    is_partial,
                    grouping_sets,
                }
            }
        }
        PhysicalNode::Filter { input, predicate } => {
            let child_req = required.map(|mut req| {
                collect_filter_cols(&predicate, &mut req);
                req
            });
            PhysicalNode::Filter {
                input: Box::new(push_projections_to_scans(*input, child_req)),
                predicate,
            }
        }
        PhysicalNode::Sort { input, keys } => {
            let child_req = required.map(|mut req| {
                for k in &keys {
                    req.insert(k.column.clone());
                }
                req
            });
            PhysicalNode::Sort {
                input: Box::new(push_projections_to_scans(*input, child_req)),
                keys,
            }
        }
        PhysicalNode::Limit {
            input,
            limit,
            offset,
        } => PhysicalNode::Limit {
            input: Box::new(push_projections_to_scans(*input, required)),
            limit,
            offset,
        },
        PhysicalNode::TopN {
            input,
            keys,
            limit,
            offset,
            is_partial,
        } => {
            let child_req = required.map(|mut req| {
                for k in &keys {
                    req.insert(k.column.clone());
                }
                req
            });
            PhysicalNode::TopN {
                input: Box::new(push_projections_to_scans(*input, child_req)),
                keys,
                limit,
                offset,
                is_partial,
            }
        }
        PhysicalNode::Distinct { input } => PhysicalNode::Distinct {
            input: Box::new(push_projections_to_scans(*input, required)),
        },
        PhysicalNode::Exchange {
            input,
            target_partitioning,
            mode,
        } => PhysicalNode::Exchange {
            input: Box::new(push_projections_to_scans(*input, required)),
            target_partitioning,
            mode,
        },
        PhysicalNode::HashJoin {
            left,
            right,
            left_key,
            right_key,
            join_type,
            is_broadcast,
        } => {
            let left_req = required.as_ref().map(|r| {
                let mut s = r.clone();
                s.insert(left_key.clone());
                s
            });
            let right_req = required.map(|mut r| {
                r.insert(right_key.clone());
                r
            });
            PhysicalNode::HashJoin {
                left: Box::new(push_projections_to_scans(*left, left_req)),
                right: Box::new(push_projections_to_scans(*right, right_req)),
                left_key,
                right_key,
                join_type,
                is_broadcast,
            }
        }
        PhysicalNode::Window { input, specs } => {
            let child_req = required.map(|mut req| {
                for s in &specs {
                    req.remove(&s.alias);
                    for p in &s.partition_by {
                        req.insert(p.clone());
                    }
                    for o in &s.order_by {
                        req.insert(o.column.clone());
                    }
                    match &s.func {
                        WindowFunc::Sum(c)
                        | WindowFunc::Avg(c)
                        | WindowFunc::Lag(c, _)
                        | WindowFunc::Lead(c, _) => {
                            req.insert(c.clone());
                        }
                        _ => {}
                    }
                }
                req
            });
            PhysicalNode::Window {
                input: Box::new(push_projections_to_scans(*input, child_req)),
                specs,
            }
        }
        PhysicalNode::UnionAll { inputs, distinct } => PhysicalNode::UnionAll {
            inputs: inputs
                .into_iter()
                .map(|inp| push_projections_to_scans(inp, required.clone()))
                .collect(),
            distinct,
        },
        PhysicalNode::SetOp {
            op,
            left,
            right,
            distinct,
        } => PhysicalNode::SetOp {
            op,
            left: Box::new(push_projections_to_scans(*left, required.clone())),
            right: Box::new(push_projections_to_scans(*right, required)),
            distinct,
        },
        other @ PhysicalNode::ShuffleRead { .. } => other,
    }
}

fn collect_col_expr_cols(expr: &ColumnExpr, out: &mut HashSet<String>) {
    match expr {
        ColumnExpr::Column(c) => {
            out.insert(c.clone());
        }
        ColumnExpr::JsonPath { column, .. }
        | ColumnExpr::DateTrunc { column, .. }
        | ColumnExpr::VectorDistance { column, .. }
        | ColumnExpr::Bm25Score { column, .. } => {
            out.insert(column.clone());
        }
        ColumnExpr::CaseWhen { branches, .. } => {
            for (pred, _) in branches {
                collect_filter_cols(pred, out);
            }
        }
        ColumnExpr::CaseExpr {
            branches,
            else_expr,
            ..
        } => {
            for (pred, res) in branches {
                collect_filter_cols(pred, out);
                collect_col_expr_cols(res, out);
            }
            collect_col_expr_cols(else_expr, out);
        }
        ColumnExpr::Extract { expr, .. } => {
            collect_col_expr_cols(expr, out);
        }
        ColumnExpr::Coalesce { columns, .. } => {
            for c in columns {
                out.insert(c.clone());
            }
        }
        ColumnExpr::BinaryArith { left, right, .. } => {
            collect_col_expr_cols(left, out);
            collect_col_expr_cols(right, out);
        }
        ColumnExpr::StringExpr { args, .. } => {
            for a in args {
                collect_col_expr_cols(a, out);
            }
        }
        ColumnExpr::MathExpr { arg, .. } | ColumnExpr::Cast { expr: arg, .. } => {
            collect_col_expr_cols(arg, out);
        }
        ColumnExpr::Literal { .. } | ColumnExpr::Now { .. } => {}
    }
}

fn collect_filter_cols(expr: &FilterExpr, out: &mut HashSet<String>) {
    match expr {
        FilterExpr::Eq { column, .. }
        | FilterExpr::NotEq { column, .. }
        | FilterExpr::Gt { column, .. }
        | FilterExpr::GtEq { column, .. }
        | FilterExpr::Lt { column, .. }
        | FilterExpr::LtEq { column, .. }
        | FilterExpr::IsNull { column }
        | FilterExpr::IsNotNull { column }
        | FilterExpr::InList { column, .. }
        | FilterExpr::Like { column, .. }
        | FilterExpr::JsonPathEq { column, .. }
        | FilterExpr::JsonContains { column, .. }
        | FilterExpr::JsonHasKey { column, .. } => {
            out.insert(column.clone());
        }
        FilterExpr::ColCmp { left, right, .. } => {
            out.insert(left.clone());
            out.insert(right.clone());
        }
        FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            collect_filter_cols(a, out);
            collect_filter_cols(b, out);
        }
        FilterExpr::Not(inner) => collect_filter_cols(inner, out),
        FilterExpr::LiteralBool(_) => {}
    }
}

fn try_parse_window(expr: &Expr, alias: Option<String>) -> Result<Option<WindowSpec>> {
    let Expr::Function(func) = expr else {
        return Ok(None);
    };
    let Some(WindowType::WindowSpec(ws)) = &func.over else {
        return Ok(None);
    };
    let name = func.name.to_string().to_uppercase();
    let args = match &func.args {
        FunctionArguments::List(list) => list.args.as_slice(),
        _ => &[],
    };
    let wf = match name.as_str() {
        "ROW_NUMBER" => WindowFunc::RowNumber,
        "RANK" => WindowFunc::Rank,
        "DENSE_RANK" => WindowFunc::DenseRank,
        "SUM" => WindowFunc::Sum(extract_fn_arg_ident(args.first())?),
        "AVG" => WindowFunc::Avg(extract_fn_arg_ident(args.first())?),
        "LAG" => {
            let col = extract_fn_arg_ident(args.first())?;
            let offset = match args.get(1) {
                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => match parse_scalar(e)? {
                    ScalarValue::Int64(n) if n > 0 => n as usize,
                    _ => 1,
                },
                _ => 1,
            };
            WindowFunc::Lag(col, offset)
        }
        "LEAD" => {
            let col = extract_fn_arg_ident(args.first())?;
            let offset = match args.get(1) {
                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => match parse_scalar(e)? {
                    ScalarValue::Int64(n) if n > 0 => n as usize,
                    _ => 1,
                },
                _ => 1,
            };
            WindowFunc::Lead(col, offset)
        }
        other => {
            return Err(RustLakeError::Plan(format!(
                "unsupported window function: {other}"
            )))
        }
    };

    let partition_by: Vec<String> = ws
        .partition_by
        .iter()
        .map(extract_ident)
        .collect::<Result<Vec<_>>>()?;
    let order_by: Vec<SortKey> = ws
        .order_by
        .iter()
        .map(|o| {
            Ok(SortKey {
                column: extract_ident(&o.expr)?,
                descending: matches!(o.asc, Some(false)),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(WindowSpec {
        func: wf,
        partition_by,
        order_by,
        alias: alias.unwrap_or_else(|| name.to_lowercase()),
    }))
}

fn parse_copy_from(sql: &str) -> Result<PlannedCommand> {
    // Syntax: COPY <table> FROM '<path>' [WITH] (FORMAT CSV|JSON|NDJSON|PARQUET [, ON_ERROR CONTINUE])
    let upper = sql.to_uppercase();
    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| RustLakeError::SqlParse("COPY requires FROM '<path>'".into()))?;
    let table_name = normalize_table_name(&sql[5..from_pos]);
    let after_from = sql[from_pos + 6..].trim();
    let first_q = after_from
        .find('\'')
        .ok_or_else(|| RustLakeError::SqlParse("COPY source path must be single-quoted".into()))?;
    let rest_q = &after_from[first_q + 1..];
    let second_q = rest_q
        .find('\'')
        .ok_or_else(|| RustLakeError::SqlParse("unterminated quote in COPY path".into()))?;
    let source_path = rest_q[..second_q].to_string();
    let opts = rest_q[second_q + 1..].to_uppercase();
    let format = if opts.contains("PARQUET") || source_path.to_lowercase().ends_with(".parquet") {
        CopyFormat::Parquet
    } else if opts.contains("JSON")
        || opts.contains("NDJSON")
        || source_path.to_lowercase().ends_with(".json")
        || source_path.to_lowercase().ends_with(".jsonl")
        || source_path.to_lowercase().ends_with(".ndjson")
    {
        CopyFormat::Json
    } else {
        CopyFormat::Csv
    };
    let on_error_continue = opts.contains("ON_ERROR CONTINUE") || opts.contains("CONTINUE");
    Ok(PlannedCommand::CopyFrom {
        table_name,
        source_path,
        format,
        on_error_continue,
    })
}

fn parse_merge_into(sql: &str) -> Result<PlannedCommand> {
    // Syntax:
    // MERGE INTO <target> [AS t] USING <source> [AS s] ON t.<tk> = s.<sk>
    //   [WHEN MATCHED THEN UPDATE SET col1 = s.col1, ... | WHEN MATCHED THEN DELETE]
    //   [WHEN NOT MATCHED THEN INSERT ...]
    let upper = sql.to_uppercase();
    let using_pos = upper
        .find(" USING ")
        .ok_or_else(|| RustLakeError::SqlParse("MERGE INTO requires USING <source>".into()))?;
    let on_pos = upper
        .find(" ON ")
        .ok_or_else(|| RustLakeError::SqlParse("MERGE INTO requires ON <cond>".into()))?;
    let when_pos = upper
        .find(" WHEN ")
        .ok_or_else(|| RustLakeError::SqlParse("MERGE INTO requires WHEN clause".into()))?;

    let target_raw = sql["MERGE INTO ".len()..using_pos]
        .split_whitespace()
        .next()
        .unwrap_or("");
    let source_raw = sql[using_pos + " USING ".len()..on_pos]
        .split_whitespace()
        .next()
        .unwrap_or("");
    let target_table = normalize_table_name(target_raw);
    let source_table = normalize_table_name(source_raw);

    let on_clause = sql[on_pos + 4..when_pos].trim();
    let eq_parts: Vec<&str> = on_clause.split('=').collect();
    if eq_parts.len() != 2 {
        return Err(RustLakeError::SqlParse(
            "MERGE ON clause must be '<target_key> = <source_key>'".into(),
        ));
    }
    let target_key = eq_parts[0]
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or("")
        .trim()
        .trim_matches('"')
        .to_string();
    let source_key = eq_parts[1]
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or("")
        .trim()
        .trim_matches('"')
        .to_string();

    let when_tail = &sql[when_pos..];
    let when_upper = when_tail.to_uppercase();
    let when_matched_delete = when_upper.contains("WHEN MATCHED THEN DELETE");
    let when_not_matched_insert = when_upper.contains("WHEN NOT MATCHED THEN INSERT");

    let mut when_matched_updates = Vec::new();
    if let Some(set_pos) = when_upper.find("WHEN MATCHED THEN UPDATE SET ") {
        let after_set = &when_tail[set_pos + "WHEN MATCHED THEN UPDATE SET ".len()..];
        let end = after_set
            .to_uppercase()
            .find(" WHEN ")
            .unwrap_or(after_set.len());
        let assigns_str = after_set[..end].trim();
        for pair in assigns_str.split(',') {
            let kv: Vec<&str> = pair.split('=').collect();
            if kv.len() == 2 {
                let lhs = kv[0]
                    .trim()
                    .rsplit('.')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .trim_matches('"')
                    .to_string();
                let rhs = kv[1]
                    .trim()
                    .rsplit('.')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .trim_matches('"')
                    .to_string();
                when_matched_updates.push((lhs, rhs));
            }
        }
    }

    Ok(PlannedCommand::MergeInto {
        target_table,
        source_table,
        target_key,
        source_key,
        when_matched_delete,
        when_matched_updates,
        when_not_matched_insert,
    })
}

fn parse_create_view(sql: &str, materialized: bool) -> Result<PlannedCommand> {
    let upper = sql.to_uppercase();
    let as_pos = upper
        .find(" AS ")
        .ok_or_else(|| RustLakeError::SqlParse("CREATE VIEW requires AS <SELECT ...>".into()))?;
    let prefix = &sql[..as_pos];
    let name_raw = prefix
        .split_whitespace()
        .last()
        .ok_or_else(|| RustLakeError::SqlParse("CREATE VIEW requires view name".into()))?;
    let query_sql = sql[as_pos + 4..].trim().to_string();
    Ok(PlannedCommand::CreateView {
        name: normalize_table_name(name_raw),
        query_sql,
        materialized,
    })
}

fn parse_grant_or_revoke(sql: &str, is_grant: bool) -> Result<PlannedCommand> {
    // GRANT <PRIV> ON <table> TO <role> | REVOKE <PRIV> ON <table> FROM <role>
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 6 {
        return Err(RustLakeError::SqlParse(
            "expected: GRANT/REVOKE <PRIV> ON <table> TO/FROM <role>".into(),
        ));
    }
    let privilege = tokens[1].to_uppercase();
    let on_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| RustLakeError::SqlParse("GRANT/REVOKE requires ON <table>".into()))?;
    let table_name = normalize_table_name(tokens.get(on_idx + 1).copied().unwrap_or(""));
    let role = tokens
        .last()
        .copied()
        .unwrap_or("")
        .trim_matches('"')
        .to_string();
    if is_grant {
        Ok(PlannedCommand::Grant {
            privilege,
            table_name,
            role,
        })
    } else {
        Ok(PlannedCommand::Revoke {
            privilege,
            table_name,
            role,
        })
    }
}

fn parse_create_rls_policy(sql: &str) -> Result<PlannedCommand> {
    // CREATE POLICY <name> ON <table> FOR <role> USING (<filter>)
    let upper = sql.to_uppercase();
    let using_pos = upper
        .find(" USING ")
        .ok_or_else(|| RustLakeError::SqlParse("CREATE POLICY requires USING (<expr>)".into()))?;
    let header_tokens: Vec<&str> = sql[..using_pos].split_whitespace().collect();
    // [CREATE, POLICY, <name>, ON, <table>, FOR, <role>]
    if header_tokens.len() < 7 {
        return Err(RustLakeError::SqlParse(
            "expected: CREATE POLICY <name> ON <table> FOR <role> USING (<expr>)".into(),
        ));
    }
    let name = header_tokens[2].trim_matches('"').to_string();
    let table_name = normalize_table_name(header_tokens[4]);
    let role = header_tokens[6].trim_matches('"').to_string();
    let filter_sql = sql[using_pos + " USING ".len()..].trim();
    let dummy_sql = format!("SELECT * FROM t WHERE {filter_sql}");
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &dummy_sql)
        .map_err(|e| RustLakeError::SqlParse(format!("invalid RLS USING expression: {e}")))?;
    let predicate = match stmts.into_iter().next() {
        Some(Statement::Query(q)) => match *q.body {
            SetExpr::Select(s) => match s.selection {
                Some(e) => parse_filter_expr(&e)?,
                None => return Err(RustLakeError::SqlParse("empty RLS USING predicate".into())),
            },
            _ => return Err(RustLakeError::SqlParse("invalid RLS USING predicate".into())),
        },
        _ => return Err(RustLakeError::SqlParse("invalid RLS USING predicate".into())),
    };
    Ok(PlannedCommand::CreateRlsPolicy {
        name,
        table_name,
        role,
        predicate,
    })
}

fn parse_create_masking_policy(sql: &str) -> Result<PlannedCommand> {
    // CREATE MASKING POLICY <name> ON <table>.<col_or_path> FOR <role> AS '<mask>'
    let upper = sql.to_uppercase();
    let as_pos = upper.find(" AS ").ok_or_else(|| {
        RustLakeError::SqlParse("CREATE MASKING POLICY requires AS '<mask>'".into())
    })?;
    let header_tokens: Vec<&str> = sql[..as_pos].split_whitespace().collect();
    // [CREATE, MASKING, POLICY, <name>, ON, <table.target>, FOR, <role>]
    if header_tokens.len() < 8 {
        return Err(RustLakeError::SqlParse(
            "expected: CREATE MASKING POLICY <name> ON <table>.<col> FOR <role> AS '<mask>'".into(),
        ));
    }
    let name = header_tokens[3].trim_matches('"').to_string();
    let full_target = header_tokens[5];
    let (tbl_raw, target) = full_target.split_once('.').ok_or_else(|| {
        RustLakeError::SqlParse("MASKING target must be <table>.<column_or_json_path>".into())
    })?;
    let role = header_tokens[7].trim_matches('"').to_string();
    let mask_value = sql[as_pos + 4..]
        .trim()
        .trim_matches('\'')
        .to_string();
    Ok(PlannedCommand::CreateMaskingPolicy {
        name,
        table_name: normalize_table_name(tbl_raw),
        target: target.to_string(),
        role,
        mask_value,
    })
}

fn parse_alter_cluster_by(sql: &str) -> Result<PlannedCommand> {
    // ALTER TABLE <table> CLUSTER BY (col1, col2)
    let upper = sql.to_uppercase();
    let cluster_pos = upper
        .find(" CLUSTER BY ")
        .ok_or_else(|| RustLakeError::SqlParse("expected CLUSTER BY (cols...)".into()))?;
    let table_name = normalize_table_name(&sql["ALTER TABLE ".len()..cluster_pos]);
    let cols_raw = sql[cluster_pos + " CLUSTER BY ".len()..]
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')');
    let columns: Vec<String> = cols_raw
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if columns.is_empty() {
        return Err(RustLakeError::SqlParse(
            "CLUSTER BY requires at least one column".into(),
        ));
    }
    Ok(PlannedCommand::AlterClusterBy {
        table_name,
        columns,
    })
}

fn parse_alter_rename_table(sql: &str) -> Result<PlannedCommand> {
    let upper = sql.to_uppercase();
    let ren_pos = upper
        .find(" RENAME TO ")
        .ok_or_else(|| RustLakeError::SqlParse("expected: ALTER TABLE <old> RENAME TO <new>".into()))?;
    let old_name = normalize_table_name(&sql["ALTER TABLE ".len()..ren_pos]);
    let new_name = normalize_table_name(&sql[ren_pos + " RENAME TO ".len()..]);
    Ok(PlannedCommand::AlterRenameTable { old_name, new_name })
}

/// Parses SQL `GROUP BY`, `GROUP BY ROLLUP(...)`, `GROUP BY CUBE(...)`, and `GROUP BY GROUPING SETS (...)` (Wave 14).
fn parse_group_by_exprs(exprs: Vec<Expr>) -> Result<(Vec<ColumnExpr>, Vec<Vec<String>>)> {
    let mut base_cols: Vec<ColumnExpr> = Vec::new();
    let mut grouping_sets: Vec<Vec<String>> = Vec::new();

    for e in exprs {
        match e {
            Expr::Rollup(groups) => {
                let cols: Vec<ColumnExpr> = groups
                    .into_iter()
                    .flatten()
                    .map(|ge| parse_col_expr(&ge, None))
                    .collect::<Result<Vec<_>>>()?;
                let names: Vec<String> = cols.iter().map(|c| c.output_name()).collect();
                for c in cols {
                    if !base_cols.contains(&c) {
                        base_cols.push(c);
                    }
                }
                // ROLLUP(a, b) -> [(a, b), (a), ()]
                for len in (0..=names.len()).rev() {
                    grouping_sets.push(names[..len].to_vec());
                }
            }
            Expr::Cube(groups) => {
                let cols: Vec<ColumnExpr> = groups
                    .into_iter()
                    .flatten()
                    .map(|ge| parse_col_expr(&ge, None))
                    .collect::<Result<Vec<_>>>()?;
                let names: Vec<String> = cols.iter().map(|c| c.output_name()).collect();
                for c in cols {
                    if !base_cols.contains(&c) {
                        base_cols.push(c);
                    }
                }
                // CUBE(a, b) -> all 2^n subsets (highest cardinality first)
                let n = names.len();
                let mut subsets: Vec<Vec<String>> = Vec::with_capacity(1 << n);
                for mask in (0..(1usize << n)).rev() {
                    let mut sub = Vec::new();
                    for (i, nm) in names.iter().enumerate() {
                        if (mask & (1 << (n - 1 - i))) != 0 {
                            sub.push(nm.clone());
                        }
                    }
                    subsets.push(sub);
                }
                grouping_sets.extend(subsets);
            }
            Expr::GroupingSets(sets) => {
                for set in sets {
                    let mut sub_names = Vec::new();
                    for ge in set {
                        let ce = parse_col_expr(&ge, None)?;
                        sub_names.push(ce.output_name());
                        if !base_cols.contains(&ce) {
                            base_cols.push(ce);
                        }
                    }
                    grouping_sets.push(sub_names);
                }
            }
            other => {
                let ce = parse_col_expr(&other, None)?;
                if !base_cols.contains(&ce) {
                    base_cols.push(ce);
                }
            }
        }
    }
    Ok((base_cols, grouping_sets))
}

fn parse_create_function(sql: &str) -> Result<PlannedCommand> {
    // Syntax: CREATE [OR REPLACE] FUNCTION <name>(<param> <type>) RETURNS <type> AS '<expr>'
    let upper = sql.to_uppercase();
    let fn_pos = upper
        .find("FUNCTION ")
        .ok_or_else(|| RustLakeError::SqlParse("expected CREATE FUNCTION".into()))?;
    let after_fn = sql[fn_pos + "FUNCTION ".len()..].trim();
    let open_p = after_fn
        .find('(')
        .ok_or_else(|| RustLakeError::SqlParse("CREATE FUNCTION requires (<param> <type>)".into()))?;
    let close_p = after_fn
        .find(')')
        .ok_or_else(|| RustLakeError::SqlParse("CREATE FUNCTION missing ')'".into()))?;
    let name = after_fn[..open_p].trim().trim_matches('"').to_lowercase();
    let param_section = after_fn[open_p + 1..close_p].trim();
    let param_name = param_section
        .split_whitespace()
        .next()
        .unwrap_or("x")
        .trim_matches('"')
        .to_string();
    let tail = &after_fn[close_p + 1..];
    let tail_upper = tail.to_uppercase();
    let as_pos = tail_upper
        .find(" AS ")
        .ok_or_else(|| RustLakeError::SqlParse("CREATE FUNCTION requires AS '<expr>'".into()))?;
    let raw_body = tail[as_pos + " AS ".len()..].trim();
    let body_expr = raw_body
        .trim_start_matches("$$")
        .trim_end_matches("$$")
        .trim_matches('\'')
        .trim()
        .to_string();
    if name.is_empty() || body_expr.is_empty() {
        return Err(RustLakeError::SqlParse(
            "CREATE FUNCTION requires non-empty name and body expression".into(),
        ));
    }
    Ok(PlannedCommand::CreateFunction {
        name,
        param_name,
        body_expr,
    })
}
