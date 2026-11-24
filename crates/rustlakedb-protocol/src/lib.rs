// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_common::{QueryId, TaskAttemptId, WorkerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use xxhash_rust::xxh3::xxh3_64;

/// Output partitioning property of a physical operator (RFC-001).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Partitioning {
    Single,
    RoundRobin(usize),
    Hash {
        columns: Vec<String>,
        partitions: usize,
    },
    Unknown(usize),
}

impl Partitioning {
    pub fn partition_count(&self) -> usize {
        match self {
            Partitioning::Single => 1,
            Partitioning::RoundRobin(n)
            | Partitioning::Hash { partitions: n, .. }
            | Partitioning::Unknown(n) => *n,
        }
    }
}

/// Required input distribution contract declared by a physical operator (RFC-001).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Distribution {
    Unspecified,
    SinglePartition,
    HashPartitioned(Vec<String>),
    Broadcast,
}

impl Distribution {
    pub fn satisfied_by(&self, partitioning: &Partitioning) -> bool {
        match self {
            Distribution::Unspecified => true,
            Distribution::SinglePartition => matches!(partitioning, Partitioning::Single),
            Distribution::HashPartitioned(req_cols) => match partitioning {
                Partitioning::Hash { columns, .. } => columns == req_cols,
                Partitioning::Single => true,
                _ => false,
            },
            Distribution::Broadcast => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShuffleMode {
    Pipelined,
    Staged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Json(serde_json::Value),
}

impl ScalarValue {
    pub fn hash_key(&self) -> u64 {
        let repr = match self {
            ScalarValue::Null => "N:".to_string(),
            ScalarValue::Bool(b) => format!("B:{b}"),
            ScalarValue::Int64(i) => format!("I:{i}"),
            ScalarValue::Float64(f) => format!("F:{}", f.to_bits()),
            ScalarValue::Utf8(s) => format!("S:{s}"),
            ScalarValue::Json(v) => format!("J:{v}"),
        };
        xxh3_64(repr.as_bytes())
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Int64(i) => Some(*i as f64),
            ScalarValue::Float64(f) => Some(*f),
            ScalarValue::Utf8(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ScalarValue::Utf8(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn to_pg_text(&self) -> Option<String> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Bool(b) => Some(if *b { "t".into() } else { "f".into() }),
            ScalarValue::Int64(i) => Some(i.to_string()),
            ScalarValue::Float64(f) => Some(f.to_string()),
            ScalarValue::Utf8(s) => Some(s.clone()),
            ScalarValue::Json(v) => Some(v.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RowBatch {
    pub columns: Vec<String>,
    pub rows: Vec<BTreeMap<String, ScalarValue>>,
}

impl RowBatch {
    pub fn new(columns: Vec<String>, rows: Vec<BTreeMap<String, ScalarValue>>) -> Self {
        Self { columns, rows }
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn estimated_bytes(&self) -> usize {
        self.rows.len() * std::cmp::max(1, self.columns.len()) * 32
    }
}

/// Distance metric for hybrid Vector + SQL + JSONB search (Wave 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    L2,
    Cosine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StringFunc {
    Lower,
    Upper,
    Length,
    Trim,
    Concat,
    Substring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MathFunc {
    Round,
    Abs,
}

/// Column reference or derived scalar expression (`payload->>'key'`, `CASE WHEN`, `COALESCE`, `DATE_TRUNC`, Arithmetic, String/Math, Cast, Vector, BM25).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnExpr {
    Column(String),
    JsonPath {
        column: String,
        path: String,
        alias: String,
    },
    CaseWhen {
        branches: Vec<(FilterExpr, ScalarValue)>,
        else_val: ScalarValue,
        alias: String,
    },
    Coalesce {
        columns: Vec<String>,
        default: ScalarValue,
        alias: String,
    },
    DateTrunc {
        unit: String,
        column: String,
        alias: String,
    },
    Literal {
        value: ScalarValue,
        alias: String,
    },
    VectorDistance {
        column: String,
        query_vec: Vec<f64>,
        metric: VectorMetric,
        alias: String,
    },
    BinaryArith {
        left: Box<ColumnExpr>,
        op: ArithOp,
        right: Box<ColumnExpr>,
        alias: String,
    },
    StringExpr {
        func: StringFunc,
        args: Vec<ColumnExpr>,
        alias: String,
    },
    MathExpr {
        func: MathFunc,
        arg: Box<ColumnExpr>,
        precision: Option<i32>,
        alias: String,
    },
    Cast {
        expr: Box<ColumnExpr>,
        target_type: String,
        alias: String,
    },
    Now {
        alias: String,
    },
    Bm25Score {
        column: String,
        query: String,
        alias: String,
    },
}

impl ColumnExpr {
    pub fn output_name(&self) -> String {
        match self {
            ColumnExpr::Column(c) => c.clone(),
            ColumnExpr::JsonPath { alias, .. }
            | ColumnExpr::CaseWhen { alias, .. }
            | ColumnExpr::Coalesce { alias, .. }
            | ColumnExpr::DateTrunc { alias, .. }
            | ColumnExpr::Literal { alias, .. }
            | ColumnExpr::VectorDistance { alias, .. }
            | ColumnExpr::BinaryArith { alias, .. }
            | ColumnExpr::StringExpr { alias, .. }
            | ColumnExpr::MathExpr { alias, .. }
            | ColumnExpr::Cast { alias, .. }
            | ColumnExpr::Now { alias }
            | ColumnExpr::Bm25Score { alias, .. } => alias.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Sum(String),
    Avg(String),
    Lag(String, usize),
    Lead(String, usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSpec {
    pub func: WindowFunc,
    pub partition_by: Vec<String>,
    pub order_by: Vec<SortKey>,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterExpr {
    Eq { column: String, value: ScalarValue },
    NotEq { column: String, value: ScalarValue },
    Gt { column: String, value: ScalarValue },
    GtEq { column: String, value: ScalarValue },
    Lt { column: String, value: ScalarValue },
    LtEq { column: String, value: ScalarValue },
    IsNull { column: String },
    IsNotNull { column: String },
    InList { column: String, values: Vec<ScalarValue> },
    Like { column: String, pattern: String },
    /// Postgres JSONB `->>` path equality (accelerated by Puffin Inverted/Bloom indexes, RFC-003).
    JsonPathEq {
        column: String,
        json_path: String,
        value: String,
    },
    /// Postgres JSONB `@>` containment (`payload @> '{"env": "prod"}'`), accelerated by Puffin Inverted Index.
    JsonContains {
        column: String,
        subset: serde_json::Value,
    },
    /// Postgres JSONB `?` key-exists (`payload ? 'trace_id'`), accelerated by Puffin Inverted Index.
    JsonHasKey {
        column: String,
        key: String,
    },
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    LiteralBool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggExpr {
    pub func: AggFunc,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergSplit {
    pub table_name: String,
    pub file_path: String,
    pub row_group_id: u32,
    pub estimated_bytes: u64,
}

impl IcebergSplit {
    pub fn split_key(&self) -> String {
        format!("{}#{}", self.file_path, self.row_group_id)
    }
}

/// Serializable distributed physical plan node (RFC-001).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalNode {
    IcebergScan {
        table_name: String,
        projection: Vec<String>,
        pushed_filter: Option<FilterExpr>,
        splits: Vec<IcebergSplit>,
    },
    Filter {
        input: Box<PhysicalNode>,
        predicate: FilterExpr,
    },
    Project {
        input: Box<PhysicalNode>,
        exprs: Vec<ColumnExpr>,
    },
    Distinct {
        input: Box<PhysicalNode>,
    },
    Sort {
        input: Box<PhysicalNode>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<PhysicalNode>,
        limit: usize,
        #[serde(default)]
        offset: usize,
    },
    /// Distributed Top-N operator (Wave 8): pushes local bounded heap sort down to workers!
    TopN {
        input: Box<PhysicalNode>,
        keys: Vec<SortKey>,
        limit: usize,
        #[serde(default)]
        offset: usize,
        /// When true, executes locally per split/partition without forcing `Single` distribution!
        #[serde(default)]
        is_partial: bool,
    },
    HashAggregate {
        input: Box<PhysicalNode>,
        group_by: Vec<ColumnExpr>,
        aggs: Vec<AggExpr>,
        is_partial: bool,
    },
    HashJoin {
        left: Box<PhysicalNode>,
        right: Box<PhysicalNode>,
        left_key: String,
        right_key: String,
        join_type: JoinType,
        is_broadcast: bool,
    },
    Window {
        input: Box<PhysicalNode>,
        specs: Vec<WindowSpec>,
    },
    UnionAll {
        inputs: Vec<PhysicalNode>,
        distinct: bool,
    },
    Exchange {
        input: Box<PhysicalNode>,
        target_partitioning: Partitioning,
        mode: ShuffleMode,
    },
    ShuffleRead {
        upstream_stage_id: u32,
        output_partitioning: Partitioning,
        mode: ShuffleMode,
    },
}

impl PhysicalNode {
    pub fn output_partitioning(&self) -> Partitioning {
        match self {
            PhysicalNode::IcebergScan { splits, .. } => {
                Partitioning::Unknown(std::cmp::max(1, splits.len()))
            }
            PhysicalNode::Filter { input, .. } | PhysicalNode::Project { input, .. } => {
                input.output_partitioning()
            }
            PhysicalNode::Distinct { .. }
            | PhysicalNode::Sort { .. }
            | PhysicalNode::Limit { .. }
            | PhysicalNode::Window { .. }
            | PhysicalNode::UnionAll { .. } => Partitioning::Single,
            PhysicalNode::TopN {
                input, is_partial, ..
            } => {
                if *is_partial {
                    input.output_partitioning()
                } else {
                    Partitioning::Single
                }
            }
            PhysicalNode::HashAggregate {
                input,
                group_by,
                is_partial,
                ..
            } => {
                if *is_partial {
                    input.output_partitioning()
                } else if group_by.is_empty() {
                    Partitioning::Single
                } else {
                    Partitioning::Hash {
                        columns: group_by.iter().map(|g| g.output_name()).collect(),
                        partitions: input.output_partitioning().partition_count(),
                    }
                }
            }
            PhysicalNode::HashJoin {
                left,
                left_key,
                is_broadcast,
                ..
            } => {
                if *is_broadcast {
                    left.output_partitioning()
                } else {
                    Partitioning::Hash {
                        columns: vec![left_key.clone()],
                        partitions: left.output_partitioning().partition_count(),
                    }
                }
            }
            PhysicalNode::Exchange {
                target_partitioning,
                ..
            }
            | PhysicalNode::ShuffleRead {
                output_partitioning: target_partitioning,
                ..
            } => target_partitioning.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShufflePartitionKey {
    pub query_id: QueryId,
    pub stage_id: u32,
    pub map_partition: usize,
    pub reduce_partition: usize,
    pub attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleBlockLocation {
    pub key: ShufflePartitionKey,
    pub worker_id: WorkerId,
    pub mode: ShuffleMode,
    pub uri: String,
    pub byte_size: usize,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    pub reduce_partition: usize,
    pub row_count: usize,
    pub byte_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub attempt_id: TaskAttemptId,
    pub plan: PhysicalNode,
    pub assigned_splits: Vec<IcebergSplit>,
    pub upstream_blocks: Vec<ShuffleBlockLocation>,
    pub output_partitioning: Option<Partitioning>,
    pub output_shuffle_mode: Option<ShuffleMode>,
    /// Optional explicit reduce partition IDs assigned by AQE Dynamic Partition Coalescing (RFC-001).
    #[serde(default)]
    pub assigned_reduce_partitions: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompletionReport {
    pub attempt_id: TaskAttemptId,
    pub worker_id: WorkerId,
    pub produced_blocks: Vec<ShuffleBlockLocation>,
    pub partition_metrics: Vec<PartitionMetrics>,
    pub final_output: Option<RowBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStage {
    pub stage_id: u32,
    pub upstream_stages: Vec<u32>,
    pub plan: PhysicalNode,
    pub output_partitioning: Option<Partitioning>,
    pub shuffle_mode: Option<ShuffleMode>,
    pub num_partitions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageDag {
    pub query_id: QueryId,
    pub stages: Vec<QueryStage>,
    pub collapsed_single_node: bool,
    pub cbo_notes: Vec<String>,
}
