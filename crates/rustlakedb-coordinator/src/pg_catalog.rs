// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::{Coordinator, QueryExecutionResult, QueryExecutionStats};
use rustlakedb_catalog::ColumnType;
use rustlakedb_common::Result;
use rustlakedb_protocol::{RowBatch, ScalarValue};
use std::collections::BTreeMap;

/// Deterministic PostgreSQL OID base for user Iceberg tables inside `pg_class` / `pg_attribute`.
const USER_TABLE_OID_BASE: i64 = 20_000;
const PUBLIC_NS_OID: i64 = 2_200;

/// Intercepts PostgreSQL `pg_catalog.*`, `SHOW <param>`, and FROM-less driver probe queries
/// sent by `pgjdbc`, DBeaver, DataGrip, SQLx, psycopg, and `psql`.
pub async fn try_handle_pg_compat(
    coord: &Coordinator,
    raw_sql: &str,
) -> Option<Result<QueryExecutionResult>> {
    let trimmed = raw_sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();

    // 1. `SHOW <param>` queries sent by JDBC / DBeaver / psql.
    if let Some(rest) = lower.strip_prefix("show ") {
        let param = rest.trim().trim_matches('"');
        if param == "tables" || param == "queries" {
            return None; // Handled by native `SHOW TABLES` / `SHOW QUERIES`.
        }
        let val = match param {
            "transaction_isolation" | "transaction isolation level" => "read committed",
            "search_path" => "\"$user\", public",
            "standard_conforming_strings" => "on",
            "server_version" => "16.0",
            "server_version_num" => "160000",
            "client_encoding" | "server_encoding" => "UTF8",
            "timezone" | "time zone" => "UTC",
            "datestyle" => "ISO, MDY",
            "max_identifier_length" => "63",
            "is_superuser" => "on",
            _ => "",
        };
        return Some(Ok(single_row_res(param, ScalarValue::Utf8(val.into()))));
    }

    // 2. FROM-less driver probe queries (`SELECT 1`, `SELECT current_user`, `SELECT current_schemas(false)`, etc.).
    if lower.starts_with("select ") && !lower.contains(" from ") {
        if lower == "select 1" || lower == "select 1;" {
            return Some(Ok(single_row_res("?column?", ScalarValue::Int64(1))));
        }
        if lower.contains("current_user") || lower.contains("session_user") {
            return Some(Ok(single_row_res(
                "current_user",
                ScalarValue::Utf8("postgres".into()),
            )));
        }
        if lower.contains("current_schemas") {
            return Some(Ok(single_row_res(
                "current_schemas",
                ScalarValue::Utf8("{pg_catalog,public}".into()),
            )));
        }
        if lower.contains("pg_backend_pid") {
            return Some(Ok(single_row_res("pg_backend_pid", ScalarValue::Int64(1))));
        }
        if lower.contains("pg_is_in_recovery") {
            return Some(Ok(single_row_res(
                "pg_is_in_recovery",
                ScalarValue::Bool(false),
            )));
        }
        if lower.contains("txid_current") || lower.contains("pg_current_xact_id") {
            return Some(Ok(single_row_res("txid_current", ScalarValue::Int64(1))));
        }
        if lower.contains("current_setting(") {
            let setting = if lower.contains("server_version_num") {
                "160000"
            } else if lower.contains("server_version") {
                "16.0"
            } else if lower.contains("standard_conforming_strings") {
                "on"
            } else if lower.contains("search_path") {
                "\"$user\", public"
            } else {
                ""
            };
            return Some(Ok(single_row_res(
                "current_setting",
                ScalarValue::Utf8(setting.into()),
            )));
        }
    }

    // 3. `pg_catalog.*` virtual system tables (DBeaver / pgjdbc / DataGrip introspection).
    if lower.contains("pg_type") {
        return Some(Ok(pg_type_result(&lower)));
    }
    if lower.contains("pg_database") {
        return Some(Ok(pg_database_result()));
    }
    if lower.contains("pg_namespace") && !lower.contains("pg_class") && !lower.contains("pg_proc") {
        return Some(Ok(pg_namespace_result()));
    }
    if lower.contains("pg_attribute") {
        return Some(pg_attribute_result(coord, &lower).await);
    }
    if lower.contains("pg_class") {
        return Some(pg_class_result(coord).await);
    }
    if lower.contains("pg_roles") || lower.contains("pg_user") || lower.contains("pg_authid") {
        return Some(Ok(pg_roles_result()));
    }
    if lower.contains("pg_settings") {
        return Some(Ok(pg_settings_result()));
    }
    if lower.contains("pg_catalog.")
        || lower.contains(" pg_description")
        || lower.contains(" pg_constraint")
        || lower.contains(" pg_index")
        || lower.contains(" pg_inherits")
        || lower.contains(" pg_attrdef")
        || lower.contains(" pg_tablespace")
        || lower.contains(" pg_am")
        || lower.contains(" pg_proc")
        || lower.contains(" pg_enum")
        || lower.contains(" pg_extension")
        || lower.contains(" pg_views")
        || lower.contains(" pg_matviews")
        || lower.contains(" pg_sequences")
        || lower.contains(" pg_trigger")
        || lower.contains(" pg_stat_")
        || lower.contains(" pg_locks")
        || lower.contains(" pg_policy")
        || lower.contains(" pg_depend")
        || lower.contains(" pg_shdescription")
        || lower.contains(" pg_collation")
        || lower.contains(" pg_language")
        || lower.contains(" pg_opclass")
        || lower.contains(" pg_operator")
        || lower.contains(" pg_foreign_")
        || lower.contains(" pg_partitioned_table")
        || lower.contains(" pg_publication")
        || lower.contains(" pg_rules")
        || lower.contains(" pg_rewrite")
        || lower.contains(" pg_available_extensions")
    {
        return Some(Ok(empty_catalog_res()));
    }

    None
}

fn single_row_res(col: &str, val: ScalarValue) -> QueryExecutionResult {
    let row = BTreeMap::from([(col.to_string(), val)]);
    QueryExecutionResult {
        command_tag: "SELECT 1".into(),
        dag: None,
        batch: RowBatch::new(vec![col.to_string()], vec![row]),
        stats: QueryExecutionStats::default(),
    }
}

fn empty_catalog_res() -> QueryExecutionResult {
    QueryExecutionResult {
        command_tag: "SELECT 0".into(),
        dag: None,
        batch: RowBatch::new(vec!["oid".into()], vec![]),
        stats: QueryExecutionStats::default(),
    }
}

pub fn column_type_to_pg_oid(dt: &ColumnType) -> i64 {
    match dt {
        ColumnType::Bool => 16,
        ColumnType::Int64 => 20,
        ColumnType::Utf8 => 25,
        ColumnType::Float64 => 701,
        ColumnType::TimestampTz => 1184,
        ColumnType::Variant => 3802, // jsonb
    }
}

fn pg_type_result(lower_sql: &str) -> QueryExecutionResult {
    // Standard PostgreSQL built-in type OIDs inspected by `pgjdbc` and DBeaver.
    let catalog_types: &[(i64, &str, &str, i64, i64)] = &[
        (16, "bool", "B", 1, 1000),
        (17, "bytea", "U", -1, 1001),
        (20, "int8", "N", 8, 1016),
        (21, "int2", "N", 2, 1005),
        (23, "int4", "N", 4, 1007),
        (25, "text", "S", -1, 1009),
        (26, "oid", "N", 4, 1028),
        (114, "json", "U", -1, 199),
        (700, "float4", "N", 4, 1021),
        (701, "float8", "N", 8, 1022),
        (1042, "bpchar", "S", -1, 1014),
        (1043, "varchar", "S", -1, 1015),
        (1082, "date", "D", 4, 1182),
        (1083, "time", "D", 8, 1183),
        (1114, "timestamp", "D", 8, 1115),
        (1184, "timestamptz", "D", 8, 1185),
        (1700, "numeric", "N", -1, 1231),
        (2950, "uuid", "U", 16, 2951),
        (3802, "jsonb", "U", -1, 3807),
    ];

    // Honor `WHERE typname = '...'` or `typname IN (...)` if present (common in pgjdbc type lookup).
    let filter_single = extract_quoted_after(lower_sql, "typname = '")
        .or_else(|| extract_quoted_after(lower_sql, "typname='"));

    // Shape columns to match pgjdbc's common `SELECT oid, typname FROM pg_catalog.pg_type` vs DBeaver full select.
    let cols = if lower_sql.contains("select oid, typname")
        || lower_sql.contains("select t.oid, t.typname")
        || lower_sql.contains("select typinput=")
    {
        vec!["oid".into(), "typname".into()]
    } else {
        vec![
            "oid".into(),
            "typname".into(),
            "typnamespace".into(),
            "typtype".into(),
            "typcategory".into(),
            "typlen".into(),
            "typarray".into(),
            "typrelid".into(),
            "typelem".into(),
            "typbasetype".into(),
            "typnotnull".into(),
            "typdelim".into(),
            "nspname".into(),
        ]
    };

    let mut rows = Vec::new();
    for &(oid, name, cat, len, arr) in catalog_types {
        if let Some(ref want) = filter_single
            && name != want.as_str()
        {
            continue;
        }
        let mut r = BTreeMap::new();
        r.insert("oid".into(), ScalarValue::Int64(oid));
        r.insert("typname".into(), ScalarValue::Utf8(name.into()));
        if cols.len() > 2 {
            r.insert("typnamespace".into(), ScalarValue::Int64(11));
            r.insert("typtype".into(), ScalarValue::Utf8("b".into()));
            r.insert("typcategory".into(), ScalarValue::Utf8(cat.into()));
            r.insert("typlen".into(), ScalarValue::Int64(len));
            r.insert("typarray".into(), ScalarValue::Int64(arr));
            r.insert("typrelid".into(), ScalarValue::Int64(0));
            r.insert("typelem".into(), ScalarValue::Int64(0));
            r.insert("typbasetype".into(), ScalarValue::Int64(0));
            r.insert("typnotnull".into(), ScalarValue::Bool(false));
            r.insert("typdelim".into(), ScalarValue::Utf8(",".into()));
            r.insert("nspname".into(), ScalarValue::Utf8("pg_catalog".into()));
        }
        rows.push(r);
    }

    let n = rows.len();
    QueryExecutionResult {
        command_tag: format!("SELECT {n}"),
        dag: None,
        batch: RowBatch::new(cols, rows),
        stats: QueryExecutionStats::default(),
    }
}

fn pg_database_result() -> QueryExecutionResult {
    let cols = vec![
        "oid".into(),
        "datname".into(),
        "datdba".into(),
        "encoding".into(),
        "datcollate".into(),
        "datctype".into(),
        "datistemplate".into(),
        "datallowconn".into(),
        "datconnlimit".into(),
        "dattablespace".into(),
        "description".into(),
    ];
    let row = BTreeMap::from([
        ("oid".into(), ScalarValue::Int64(16384)),
        ("datname".into(), ScalarValue::Utf8("rustlakedb".into())),
        ("datdba".into(), ScalarValue::Int64(10)),
        ("encoding".into(), ScalarValue::Int64(6)),
        ("datcollate".into(), ScalarValue::Utf8("en_US.utf8".into())),
        ("datctype".into(), ScalarValue::Utf8("en_US.utf8".into())),
        ("datistemplate".into(), ScalarValue::Bool(false)),
        ("datallowconn".into(), ScalarValue::Bool(true)),
        ("datconnlimit".into(), ScalarValue::Int64(-1)),
        ("dattablespace".into(), ScalarValue::Int64(1663)),
        (
            "description".into(),
            ScalarValue::Utf8("RustLakeDB Apache Iceberg v3 Lakehouse".into()),
        ),
    ]);
    QueryExecutionResult {
        command_tag: "SELECT 1".into(),
        dag: None,
        batch: RowBatch::new(cols, vec![row]),
        stats: QueryExecutionStats::default(),
    }
}

fn pg_namespace_result() -> QueryExecutionResult {
    let cols = vec![
        "oid".into(),
        "nspname".into(),
        "nspowner".into(),
        "description".into(),
    ];
    let rows = vec![
        BTreeMap::from([
            ("oid".into(), ScalarValue::Int64(PUBLIC_NS_OID)),
            ("nspname".into(), ScalarValue::Utf8("public".into())),
            ("nspowner".into(), ScalarValue::Int64(10)),
            (
                "description".into(),
                ScalarValue::Utf8("Standard public schema".into()),
            ),
        ]),
        BTreeMap::from([
            ("oid".into(), ScalarValue::Int64(11)),
            ("nspname".into(), ScalarValue::Utf8("pg_catalog".into())),
            ("nspowner".into(), ScalarValue::Int64(10)),
            (
                "description".into(),
                ScalarValue::Utf8("System catalog schema".into()),
            ),
        ]),
        BTreeMap::from([
            ("oid".into(), ScalarValue::Int64(13000)),
            (
                "nspname".into(),
                ScalarValue::Utf8("information_schema".into()),
            ),
            ("nspowner".into(), ScalarValue::Int64(10)),
            (
                "description".into(),
                ScalarValue::Utf8("SQL information schema".into()),
            ),
        ]),
    ];
    QueryExecutionResult {
        command_tag: format!("SELECT {}", rows.len()),
        dag: None,
        batch: RowBatch::new(cols, rows),
        stats: QueryExecutionStats::default(),
    }
}

async fn pg_class_result(coord: &Coordinator) -> Result<QueryExecutionResult> {
    let tables = coord.catalog.list_tables().await?;
    let cols = vec![
        "oid".into(),
        "relname".into(),
        "relnamespace".into(),
        "relkind".into(),
        "relowner".into(),
        "reltablespace".into(),
        "relhasindex".into(),
        "relpersistence".into(),
        "reltuples".into(),
        "relpages".into(),
        "relispartition".into(),
        "relrowsecurity".into(),
        "description".into(),
    ];
    let mut rows = Vec::new();
    for (idx, tbl) in tables.into_iter().enumerate() {
        let oid = USER_TABLE_OID_BASE + idx as i64;
        let tuples = coord
            .catalog
            .get_table(&tbl)
            .await
            .map(|m| m.total_rows("main") as i64)
            .unwrap_or(0);
        rows.push(BTreeMap::from([
            ("oid".into(), ScalarValue::Int64(oid)),
            ("relname".into(), ScalarValue::Utf8(tbl)),
            ("relnamespace".into(), ScalarValue::Int64(PUBLIC_NS_OID)),
            ("relkind".into(), ScalarValue::Utf8("r".into())),
            ("relowner".into(), ScalarValue::Int64(10)),
            ("reltablespace".into(), ScalarValue::Int64(0)),
            ("relhasindex".into(), ScalarValue::Bool(true)),
            ("relpersistence".into(), ScalarValue::Utf8("p".into())),
            ("reltuples".into(), ScalarValue::Int64(tuples)),
            ("relpages".into(), ScalarValue::Int64(1)),
            ("relispartition".into(), ScalarValue::Bool(false)),
            ("relrowsecurity".into(), ScalarValue::Bool(false)),
            (
                "description".into(),
                ScalarValue::Utf8("Apache Iceberg v3 Table".into()),
            ),
        ]));
    }
    let n = rows.len();
    Ok(QueryExecutionResult {
        command_tag: format!("SELECT {n}"),
        dag: None,
        batch: RowBatch::new(cols, rows),
        stats: QueryExecutionStats::default(),
    })
}

async fn pg_attribute_result(coord: &Coordinator, lower_sql: &str) -> Result<QueryExecutionResult> {
    let tables = coord.catalog.list_tables().await?;
    let filter_oid = extract_number_after(lower_sql, "attrelid=")
        .or_else(|| extract_number_after(lower_sql, "attrelid = "));

    let cols = vec![
        "attrelid".into(),
        "attname".into(),
        "atttypid".into(),
        "attnum".into(),
        "attnotnull".into(),
        "atthasdef".into(),
        "attisdropped".into(),
        "attlen".into(),
        "atttypmod".into(),
        "attidentity".into(),
        "attgenerated".into(),
        "def_value".into(),
        "description".into(),
    ];

    let mut rows = Vec::new();
    for (idx, tbl) in tables.into_iter().enumerate() {
        let oid = USER_TABLE_OID_BASE + idx as i64;
        if let Some(want_oid) = filter_oid
            && oid != want_oid
        {
            continue;
        }
        if let Ok(meta) = coord.catalog.get_table(&tbl).await {
            for col in meta.schema {
                rows.push(BTreeMap::from([
                    ("attrelid".into(), ScalarValue::Int64(oid)),
                    ("attname".into(), ScalarValue::Utf8(col.name)),
                    (
                        "atttypid".into(),
                        ScalarValue::Int64(column_type_to_pg_oid(&col.data_type)),
                    ),
                    ("attnum".into(), ScalarValue::Int64(col.field_id as i64)),
                    ("attnotnull".into(), ScalarValue::Bool(!col.nullable)),
                    ("atthasdef".into(), ScalarValue::Bool(false)),
                    ("attisdropped".into(), ScalarValue::Bool(false)),
                    ("attlen".into(), ScalarValue::Int64(-1)),
                    ("atttypmod".into(), ScalarValue::Int64(-1)),
                    ("attidentity".into(), ScalarValue::Utf8(String::new())),
                    ("attgenerated".into(), ScalarValue::Utf8(String::new())),
                    ("def_value".into(), ScalarValue::Null),
                    ("description".into(), ScalarValue::Null),
                ]));
            }
        }
    }
    let n = rows.len();
    Ok(QueryExecutionResult {
        command_tag: format!("SELECT {n}"),
        dag: None,
        batch: RowBatch::new(cols, rows),
        stats: QueryExecutionStats::default(),
    })
}

fn pg_roles_result() -> QueryExecutionResult {
    let cols = vec![
        "oid".into(),
        "rolname".into(),
        "usename".into(),
        "rolsuper".into(),
        "rolcanlogin".into(),
    ];
    let row = BTreeMap::from([
        ("oid".into(), ScalarValue::Int64(10)),
        ("rolname".into(), ScalarValue::Utf8("postgres".into())),
        ("usename".into(), ScalarValue::Utf8("postgres".into())),
        ("rolsuper".into(), ScalarValue::Bool(true)),
        ("rolcanlogin".into(), ScalarValue::Bool(true)),
    ]);
    QueryExecutionResult {
        command_tag: "SELECT 1".into(),
        dag: None,
        batch: RowBatch::new(cols, vec![row]),
        stats: QueryExecutionStats::default(),
    }
}

fn pg_settings_result() -> QueryExecutionResult {
    let cols = vec!["name".into(), "setting".into(), "unit".into(), "short_desc".into()];
    let items = [
        ("server_version", "16.0", "PostgreSQL compatibility version"),
        ("server_version_num", "160000", "Numeric server version"),
        ("search_path", "\"$user\", public", "Schema search order"),
        ("standard_conforming_strings", "on", "Standard SQL strings"),
        ("client_encoding", "UTF8", "Client character encoding"),
        ("TimeZone", "UTC", "Server timezone"),
    ];
    let rows = items
        .into_iter()
        .map(|(k, v, d)| {
            BTreeMap::from([
                ("name".into(), ScalarValue::Utf8(k.into())),
                ("setting".into(), ScalarValue::Utf8(v.into())),
                ("unit".into(), ScalarValue::Null),
                ("short_desc".into(), ScalarValue::Utf8(d.into())),
            ])
        })
        .collect::<Vec<_>>();
    let n = rows.len();
    QueryExecutionResult {
        command_tag: format!("SELECT {n}"),
        dag: None,
        batch: RowBatch::new(cols, rows),
        stats: QueryExecutionStats::default(),
    }
}

fn extract_quoted_after(haystack: &str, needle: &str) -> Option<String> {
    let pos = haystack.find(needle)?;
    let rest = &haystack[pos + needle.len()..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn extract_number_after(haystack: &str, needle: &str) -> Option<i64> {
    let pos = haystack.find(needle)?;
    let rest = haystack[pos + needle.len()..].trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse::<i64>().ok()
}
