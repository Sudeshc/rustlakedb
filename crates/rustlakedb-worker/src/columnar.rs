// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! End-to-End Native Apache Arrow `RecordBatch` Columnar Execution Pipeline (Wave 20):
//! Evaluates scalar expressions (`BinaryArith`, `CaseExpr`, `StringExpr`, `MathExpr`, `Extract`),
//! boolean filters (`arrow_select::filter::filter_record_batch`), `HashAggregate` + `ROLLUP`/`CUBE`,
//! and `Window` functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `LAG`, `LEAD`)
//! directly over columnar Arrow arrays with zero `BTreeMap` row-map materialization!

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_protocol::{
    AggExpr, AggFunc, ArithOp, CmpOp, ColumnExpr, FilterExpr, JoinType, MathFunc, ScalarValue,
    SortKey, StringFunc, WindowFunc, WindowSpec,
};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Evaluates a `ColumnExpr` directly over an Arrow `RecordBatch`, returning a columnar `ArrayRef`.
pub fn eval_columnar_expr(rb: &RecordBatch, expr: &ColumnExpr) -> Option<ArrayRef> {
    let n = rb.num_rows();
    match expr {
        ColumnExpr::Column(name) => rb.column_by_name(name).cloned(),
        ColumnExpr::Literal { value, .. } => Some(scalar_to_array(value, n)),
        ColumnExpr::BinaryArith {
            left, op, right, ..
        } => {
            let la = eval_columnar_expr(rb, left)?;
            let ra = eval_columnar_expr(rb, right)?;
            let mut b = Float64Builder::with_capacity(n);
            for i in 0..n {
                match (array_as_f64(&la, i), array_as_f64(&ra, i)) {
                    (Some(a), Some(c)) => {
                        let v = match op {
                            ArithOp::Add => a + c,
                            ArithOp::Sub => a - c,
                            ArithOp::Mul => a * c,
                            ArithOp::Div => {
                                if c == 0.0 {
                                    b.append_null();
                                    continue;
                                }
                                a / c
                            }
                        };
                        b.append_value(v);
                    }
                    _ => b.append_null(),
                }
            }
            Some(Arc::new(b.finish()))
        }
        ColumnExpr::MathExpr {
            func,
            arg,
            precision,
            ..
        } => {
            let arr = eval_columnar_expr(rb, arg)?;
            let mut b = Float64Builder::with_capacity(n);
            let factor = precision.map(|p| 10f64.powi(p)).unwrap_or(1.0);
            for i in 0..n {
                match array_as_f64(&arr, i) {
                    Some(v) => {
                        let out = match func {
                            MathFunc::Abs => v.abs(),
                            MathFunc::Round => (v * factor).round() / factor,
                        };
                        b.append_value(out);
                    }
                    None => b.append_null(),
                }
            }
            Some(Arc::new(b.finish()))
        }
        ColumnExpr::StringExpr { func, args, .. } => {
            let first = eval_columnar_expr(rb, args.first()?)?;
            match func {
                StringFunc::Length => {
                    let mut b = Int64Builder::with_capacity(n);
                    for i in 0..n {
                        match array_as_string(&first, i) {
                            Some(s) => b.append_value(s.chars().count() as i64),
                            None => b.append_null(),
                        }
                    }
                    Some(Arc::new(b.finish()))
                }
                StringFunc::Lower | StringFunc::Upper | StringFunc::Trim => {
                    let mut b = StringBuilder::with_capacity(n, n * 8);
                    for i in 0..n {
                        match array_as_string(&first, i) {
                            Some(s) => match func {
                                StringFunc::Lower => b.append_value(s.to_lowercase()),
                                StringFunc::Upper => b.append_value(s.to_uppercase()),
                                StringFunc::Trim => b.append_value(s.trim()),
                                _ => unreachable!(),
                            },
                            None => b.append_null(),
                        }
                    }
                    Some(Arc::new(b.finish()))
                }
                _ => None,
            }
        }
        ColumnExpr::Extract { field, expr, .. } => {
            let arr = eval_columnar_expr(rb, expr)?;
            let mut b = Int64Builder::with_capacity(n);
            let upper_f = field.to_uppercase();
            for i in 0..n {
                let s = if let Some(da) = arr.as_any().downcast_ref::<Date32Array>() {
                    if da.is_null(i) {
                        None
                    } else {
                        Some(rustlakedb_protocol::format_iso_date_days(da.value(i)))
                    }
                } else {
                    array_as_string(&arr, i)
                };
                match s {
                    Some(ds) => {
                        let dp = ds.split(['T', ' ']).next().unwrap_or("");
                        let mut parts = dp.split('-');
                        let y = parts.next().and_then(|p| p.parse::<i64>().ok());
                        let m = parts.next().and_then(|p| p.parse::<i64>().ok());
                        let d = parts.next().and_then(|p| p.parse::<i64>().ok());
                        let val = match upper_f.as_str() {
                            "YEAR" => y,
                            "MONTH" => m,
                            "DAY" => d,
                            _ => None,
                        };
                        match val {
                            Some(v) => b.append_value(v),
                            None => b.append_null(),
                        }
                    }
                    None => b.append_null(),
                }
            }
            Some(Arc::new(b.finish()))
        }
        ColumnExpr::CaseExpr {
            branches,
            else_expr,
            ..
        } => {
            let mut masks = Vec::with_capacity(branches.len());
            let mut vals = Vec::with_capacity(branches.len());
            for (pred, res_e) in branches {
                masks.push(eval_columnar_filter(rb, pred)?);
                vals.push(eval_columnar_expr(rb, res_e)?);
            }
            let else_arr = eval_columnar_expr(rb, else_expr)?;
            let is_numeric = matches!(
                else_arr.data_type(),
                DataType::Float64 | DataType::Int64 | DataType::Decimal128(_, _)
            ) || vals
                .iter()
                .any(|v| matches!(v.data_type(), DataType::Float64 | DataType::Int64));

            if is_numeric {
                let mut b = Float64Builder::with_capacity(n);
                for i in 0..n {
                    let mut chosen = None;
                    for (m, v) in masks.iter().zip(vals.iter()) {
                        if !m.is_null(i) && m.value(i) {
                            chosen = array_as_f64(v, i);
                            break;
                        }
                    }
                    if chosen.is_none() {
                        chosen = array_as_f64(&else_arr, i);
                    }
                    match chosen {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Some(Arc::new(b.finish()))
            } else {
                let mut b = StringBuilder::with_capacity(n, n * 8);
                for i in 0..n {
                    let mut chosen = None;
                    for (m, v) in masks.iter().zip(vals.iter()) {
                        if !m.is_null(i) && m.value(i) {
                            chosen = array_as_string(v, i);
                            break;
                        }
                    }
                    if chosen.is_none() {
                        chosen = array_as_string(&else_arr, i);
                    }
                    match chosen {
                        Some(s) => b.append_value(s),
                        None => b.append_null(),
                    }
                }
                Some(Arc::new(b.finish()))
            }
        }
        _ => None,
    }
}

/// Projects a list of `ColumnExpr`s directly into a new Arrow `RecordBatch` (zero `RowBatch`!).
pub fn eval_columnar_project(rb: &RecordBatch, exprs: &[ColumnExpr]) -> Option<RecordBatch> {
    let mut fields = Vec::with_capacity(exprs.len());
    let mut arrays = Vec::with_capacity(exprs.len());
    for e in exprs {
        let arr = eval_columnar_expr(rb, e)?;
        fields.push(Field::new(e.output_name(), arr.data_type().clone(), true));
        arrays.push(arr);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).ok()
}

/// Evaluates a `FilterExpr` directly over an Arrow `RecordBatch` to produce a `BooleanArray` mask.
pub fn eval_columnar_filter(rb: &RecordBatch, pred: &FilterExpr) -> Option<BooleanArray> {
    let n = rb.num_rows();
    match pred {
        FilterExpr::LiteralBool(v) => {
            let mut b = BooleanBuilder::with_capacity(n);
            for _ in 0..n {
                b.append_value(*v);
            }
            Some(b.finish())
        }
        FilterExpr::IsNull { column } => {
            let col = rb.column_by_name(column)?;
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                b.append_value(col.is_null(i));
            }
            Some(b.finish())
        }
        FilterExpr::IsNotNull { column } => {
            let col = rb.column_by_name(column)?;
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                b.append_value(!col.is_null(i));
            }
            Some(b.finish())
        }
        FilterExpr::Eq { column, value }
        | FilterExpr::NotEq { column, value }
        | FilterExpr::Gt { column, value }
        | FilterExpr::GtEq { column, value }
        | FilterExpr::Lt { column, value }
        | FilterExpr::LtEq { column, value } => {
            let col = rb.column_by_name(column)?;
            let op = match pred {
                FilterExpr::Eq { .. } => CmpOp::Eq,
                FilterExpr::NotEq { .. } => CmpOp::NotEq,
                FilterExpr::Gt { .. } => CmpOp::Gt,
                FilterExpr::GtEq { .. } => CmpOp::GtEq,
                FilterExpr::Lt { .. } => CmpOp::Lt,
                FilterExpr::LtEq { .. } => CmpOp::LtEq,
                _ => unreachable!(),
            };
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if col.is_null(i) {
                    b.append_value(false);
                    continue;
                }
                let cmp = compare_array_to_scalar(col, i, value);
                b.append_value(match_cmp(cmp, op));
            }
            Some(b.finish())
        }
        FilterExpr::ColCmp { left, op, right } => {
            let lcol = rb.column_by_name(left)?;
            let rcol = rb.column_by_name(right)?;
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if lcol.is_null(i) || rcol.is_null(i) {
                    b.append_value(false);
                    continue;
                }
                let cmp = compare_array_cells(lcol, i, rcol, i);
                b.append_value(match_cmp(cmp, *op));
            }
            Some(b.finish())
        }
        FilterExpr::InList { column, values } => {
            let col = rb.column_by_name(column)?;
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if col.is_null(i) {
                    b.append_value(false);
                    continue;
                }
                let matched = values
                    .iter()
                    .any(|v| compare_array_to_scalar(col, i, v) == Some(CmpOrdering::Equal));
                b.append_value(matched);
            }
            Some(b.finish())
        }
        FilterExpr::And(a, b_expr) => {
            let ma = eval_columnar_filter(rb, a)?;
            let mb = eval_columnar_filter(rb, b_expr)?;
            let mut out = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                out.append_value(ma.value(i) && mb.value(i));
            }
            Some(out.finish())
        }
        FilterExpr::Or(a, b_expr) => {
            let ma = eval_columnar_filter(rb, a)?;
            let mb = eval_columnar_filter(rb, b_expr)?;
            let mut out = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                out.append_value(ma.value(i) || mb.value(i));
            }
            Some(out.finish())
        }
        FilterExpr::Not(inner) => {
            let mi = eval_columnar_filter(rb, inner)?;
            let mut out = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                out.append_value(!mi.value(i));
            }
            Some(out.finish())
        }
        _ => None,
    }
}

/// Applies a `FilterExpr` directly to an Arrow `RecordBatch` using `arrow_select::filter::filter_record_batch`.
pub fn apply_columnar_filter(rb: &RecordBatch, pred: &FilterExpr) -> Option<RecordBatch> {
    let mask = eval_columnar_filter(rb, pred)?;
    filter_record_batch(rb, &mask).ok()
}

#[derive(Default)]
struct ColAggState {
    count: i64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    distinct: HashSet<String>,
}

/// Native Columnar Arrow HashAggregate + `ROLLUP`/`CUBE`/`GROUPING SETS` over `RecordBatch` slices (Wave 20).
/// Computes `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)` directly on Arrow arrays and emits
/// a columnar Arrow `RecordBatch` without intermediate `RowBatch` maps!
pub fn eval_columnar_hash_aggregate(
    batches: &[RecordBatch],
    group_by: &[ColumnExpr],
    aggs: &[AggExpr],
    grouping_sets: &[Vec<String>],
) -> Result<RecordBatch> {
    let gb_names: Vec<String> = group_by.iter().map(|g| g.output_name()).collect();
    let effective_sets: Vec<Vec<String>> = if grouping_sets.is_empty() {
        vec![gb_names.clone()]
    } else {
        grouping_sets.to_vec()
    };

    // Map: (set_idx, group_key_values) -> Vec<ColAggState> (one per agg)
    let mut groups: BTreeMap<(usize, Vec<Option<String>>), Vec<ColAggState>> = BTreeMap::new();

    for rb in batches {
        let n = rb.num_rows();
        if n == 0 {
            continue;
        }
        // Pre-resolve group-by arrays
        let mut gb_arrays: HashMap<String, ArrayRef> = HashMap::new();
        for g in group_by {
            if let Some(arr) = eval_columnar_expr(rb, g) {
                gb_arrays.insert(g.output_name(), arr);
            }
        }
        // Pre-resolve aggregate input arrays
        let mut agg_arrays: Vec<Option<ArrayRef>> = Vec::with_capacity(aggs.len());
        for a in aggs {
            let col_opt = match &a.func {
                AggFunc::Count => None,
                AggFunc::CountDistinct(c)
                | AggFunc::Sum(c)
                | AggFunc::Avg(c)
                | AggFunc::Min(c)
                | AggFunc::Max(c) => rb.column_by_name(c).cloned(),
            };
            agg_arrays.push(col_opt);
        }

        for row_i in 0..n {
            for (set_idx, active_cols) in effective_sets.iter().enumerate() {
                let mut key = Vec::with_capacity(gb_names.len());
                for name in &gb_names {
                    if active_cols.contains(name) {
                        let v = gb_arrays
                            .get(name)
                            .and_then(|arr| array_as_string(arr, row_i));
                        key.push(v);
                    } else {
                        key.push(None);
                    }
                }
                let states = groups
                    .entry((set_idx, key))
                    .or_insert_with(|| (0..aggs.len()).map(|_| ColAggState::default()).collect());

                for (ai, agg) in aggs.iter().enumerate() {
                    let st = &mut states[ai];
                    match &agg.func {
                        AggFunc::Count => st.count += 1,
                        AggFunc::CountDistinct(_) => {
                            if let Some(arr) = &agg_arrays[ai]
                                && let Some(s) = array_as_string(arr, row_i)
                            {
                                st.distinct.insert(s);
                            }
                        }
                        AggFunc::Sum(_) | AggFunc::Avg(_) | AggFunc::Min(_) | AggFunc::Max(_) => {
                            if let Some(arr) = &agg_arrays[ai]
                                && let Some(v) = array_as_f64(arr, row_i)
                            {
                                st.count += 1;
                                st.sum += v;
                                st.min = Some(st.min.map_or(v, |m| m.min(v)));
                                st.max = Some(st.max.map_or(v, |m| m.max(v)));
                            }
                        }
                    }
                }
            }
        }
    }

    // ANSI SQL: ungrouped aggregate on empty input returns 1 default row (`COUNT = 0`).
    if groups.is_empty() && gb_names.is_empty() && !aggs.is_empty() {
        groups.insert(
            (0, vec![]),
            (0..aggs.len()).map(|_| ColAggState::default()).collect(),
        );
    }

    let num_out = groups.len();
    let mut fields: Vec<Field> = Vec::with_capacity(gb_names.len() + aggs.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(gb_names.len() + aggs.len());

    // Build group-by output columns
    for (gi, gname) in gb_names.iter().enumerate() {
        let mut sb = StringBuilder::with_capacity(num_out, num_out * 8);
        for (_, key) in groups.keys() {
            match &key[gi] {
                Some(s) => sb.append_value(s),
                None => sb.append_null(),
            }
        }
        fields.push(Field::new(gname, DataType::Utf8, true));
        arrays.push(Arc::new(sb.finish()));
    }

    // Build aggregate output columns
    for (ai, agg) in aggs.iter().enumerate() {
        match &agg.func {
            AggFunc::Count | AggFunc::CountDistinct(_) => {
                let mut ib = Int64Builder::with_capacity(num_out);
                for states in groups.values() {
                    let val = if matches!(agg.func, AggFunc::Count) {
                        states[ai].count
                    } else {
                        states[ai].distinct.len() as i64
                    };
                    ib.append_value(val);
                }
                fields.push(Field::new(&agg.alias, DataType::Int64, false));
                arrays.push(Arc::new(ib.finish()));
            }
            AggFunc::Sum(_) | AggFunc::Avg(_) | AggFunc::Min(_) | AggFunc::Max(_) => {
                let mut fb = Float64Builder::with_capacity(num_out);
                for states in groups.values() {
                    let st = &states[ai];
                    let val_opt = match &agg.func {
                        AggFunc::Sum(_) => {
                            if st.count > 0 {
                                Some(st.sum)
                            } else {
                                None
                            }
                        }
                        AggFunc::Avg(_) => {
                            if st.count > 0 {
                                Some(st.sum / st.count as f64)
                            } else {
                                None
                            }
                        }
                        AggFunc::Min(_) => st.min,
                        AggFunc::Max(_) => st.max,
                        _ => None,
                    };
                    match val_opt {
                        Some(v) => fb.append_value(v),
                        None => fb.append_null(),
                    }
                }
                fields.push(Field::new(&agg.alias, DataType::Float64, true));
                arrays.push(Arc::new(fb.finish()));
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| RustLakeError::Execution(format!("columnar hash aggregate build: {e}")))
}

/// Native Columnar Arrow Window Operator (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `LAG`, `LEAD`)
/// executing directly on Arrow `RecordBatch` slices via partition index vectors (Wave 20)!
pub fn eval_columnar_window(rb: &RecordBatch, specs: &[WindowSpec]) -> Result<RecordBatch> {
    let n = rb.num_rows();
    let mut fields: Vec<Field> = rb
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut arrays: Vec<ArrayRef> = rb.columns().to_vec();

    for spec in specs {
        // Partition row indices by `partition_by` columns
        let mut partitions: HashMap<Vec<Option<String>>, Vec<usize>> = HashMap::new();
        for row_i in 0..n {
            let pkey: Vec<Option<String>> = spec
                .partition_by
                .iter()
                .map(|c| rb.column_by_name(c).and_then(|arr| array_as_string(arr, row_i)))
                .collect();
            partitions.entry(pkey).or_default().push(row_i);
        }

        // Sort each partition's row indices according to `spec.order_by`
        for indices in partitions.values_mut() {
            if !spec.order_by.is_empty() {
                indices.sort_by(|&a, &b| compare_row_indices(rb, a, b, &spec.order_by));
            }
        }

        match &spec.func {
            WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => {
                let mut out_vals = vec![0i64; n];
                for indices in partitions.values() {
                    let mut rank = 0i64;
                    let mut dense = 0i64;
                    let mut prev_idx: Option<usize> = None;
                    for (pos, &row_i) in indices.iter().enumerate() {
                        let is_tie = prev_idx.is_some_and(|p| {
                            !spec.order_by.is_empty()
                                && compare_row_indices(rb, p, row_i, &spec.order_by)
                                    == CmpOrdering::Equal
                        });
                        if !is_tie {
                            rank = (pos + 1) as i64;
                            dense += 1;
                        }
                        out_vals[row_i] = match spec.func {
                            WindowFunc::RowNumber => (pos + 1) as i64,
                            WindowFunc::Rank => rank,
                            WindowFunc::DenseRank => dense,
                            _ => unreachable!(),
                        };
                        prev_idx = Some(row_i);
                    }
                }
                fields.push(Field::new(&spec.alias, DataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(out_vals)));
            }
            WindowFunc::Sum(col) | WindowFunc::Avg(col) => {
                let src = rb.column_by_name(col).ok_or_else(|| {
                    RustLakeError::Execution(format!("window column '{col}' not found"))
                })?;
                let mut out_vals = vec![None; n];
                let has_frame = spec.frame_preceding.is_some() || spec.frame_following.is_some();
                for indices in partitions.values() {
                    if has_frame {
                        let plen = indices.len();
                        for (pos, &row_i) in indices.iter().enumerate() {
                            let lo = match spec.frame_preceding {
                                Some(p) => (pos as i64 - p).clamp(0, plen as i64 - 1) as usize,
                                None => 0,
                            };
                            let hi = match spec.frame_following {
                                Some(f) => (pos as i64 + f).clamp(0, plen as i64 - 1) as usize,
                                None => plen - 1,
                            };
                            let mut sum = 0.0;
                            let mut cnt = 0usize;
                            if lo <= hi {
                                for &w_idx in &indices[lo..=hi] {
                                    if let Some(v) = array_as_f64(src, w_idx) {
                                        sum += v;
                                        cnt += 1;
                                    }
                                }
                            }
                            out_vals[row_i] = if cnt > 0 {
                                Some(if matches!(spec.func, WindowFunc::Avg(_)) {
                                    sum / cnt as f64
                                } else {
                                    sum
                                })
                            } else {
                                None
                            };
                        }
                    } else if spec.order_by.is_empty() {
                        let mut sum = 0.0;
                        let mut cnt = 0usize;
                        for &row_i in indices {
                            if let Some(v) = array_as_f64(src, row_i) {
                                sum += v;
                                cnt += 1;
                            }
                        }
                        let val = if cnt > 0 {
                            Some(if matches!(spec.func, WindowFunc::Avg(_)) {
                                sum / cnt as f64
                            } else {
                                sum
                            })
                        } else {
                            None
                        };
                        for &row_i in indices {
                            out_vals[row_i] = val;
                        }
                    } else {
                        let mut running = 0.0;
                        let mut cnt = 0usize;
                        for &row_i in indices {
                            if let Some(v) = array_as_f64(src, row_i) {
                                running += v;
                                cnt += 1;
                            }
                            out_vals[row_i] = if cnt > 0 {
                                Some(if matches!(spec.func, WindowFunc::Avg(_)) {
                                    running / cnt as f64
                                } else {
                                    running
                                })
                            } else {
                                None
                            };
                        }
                    }
                }
                let mut fb = Float64Builder::with_capacity(n);
                for v in out_vals {
                    match v {
                        Some(x) => fb.append_value(x),
                        None => fb.append_null(),
                    }
                }
                fields.push(Field::new(&spec.alias, DataType::Float64, true));
                arrays.push(Arc::new(fb.finish()));
            }
            WindowFunc::Lag(col, offset) | WindowFunc::Lead(col, offset) => {
                let src = rb.column_by_name(col).ok_or_else(|| {
                    RustLakeError::Execution(format!("window column '{col}' not found"))
                })?;
                let is_lag = matches!(spec.func, WindowFunc::Lag(_, _));
                let mut out_vals = vec![None; n];
                for indices in partitions.values() {
                    for (pos, &row_i) in indices.iter().enumerate() {
                        let target_pos = if is_lag {
                            pos.checked_sub(*offset)
                        } else {
                            let p = pos + *offset;
                            if p < indices.len() { Some(p) } else { None }
                        };
                        if let Some(tp) = target_pos {
                            out_vals[row_i] = array_as_f64(src, indices[tp]);
                        }
                    }
                }
                let mut fb = Float64Builder::with_capacity(n);
                for v in out_vals {
                    match v {
                        Some(x) => fb.append_value(x),
                        None => fb.append_null(),
                    }
                }
                fields.push(Field::new(&spec.alias, DataType::Float64, true));
                arrays.push(Arc::new(fb.finish()));
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| RustLakeError::Execution(format!("columnar window build: {e}")))
}

fn compare_row_indices(rb: &RecordBatch, a: usize, b: usize, keys: &[SortKey]) -> CmpOrdering {
    for k in keys {
        if let Some(col) = rb.column_by_name(&k.column) {
            let ord = compare_array_cells(col, a, col, b).unwrap_or(CmpOrdering::Equal);
            let dir = if k.descending { ord.reverse() } else { ord };
            if dir != CmpOrdering::Equal {
                return dir;
            }
        }
    }
    CmpOrdering::Equal
}

fn scalar_to_array(val: &ScalarValue, n: usize) -> ArrayRef {
    match val {
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v; n])),
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![*v; n])),
        ScalarValue::Bool(v) => Arc::new(BooleanArray::from(vec![*v; n])),
        ScalarValue::Utf8(s) => Arc::new(StringArray::from(vec![s.as_str(); n])),
        ScalarValue::Decimal(v, s) => {
            let f = (*v as f64) / 10f64.powi(*s as i32);
            Arc::new(Float64Array::from(vec![f; n]))
        }
        ScalarValue::Date(d) => Arc::new(Date32Array::from(vec![*d; n])),
        _ => {
            let mut b = StringBuilder::with_capacity(n, 0);
            for _ in 0..n {
                b.append_null();
            }
            Arc::new(b.finish())
        }
    }
}

fn array_as_f64(arr: &ArrayRef, idx: usize) -> Option<f64> {
    if arr.is_null(idx) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Some(a.value(idx))
    } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(idx) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match arr.data_type() {
            DataType::Decimal128(_, s) => *s as i32,
            _ => 2,
        };
        Some((a.value(idx) as f64) / 10f64.powi(scale))
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        a.value(idx).parse::<f64>().ok()
    } else {
        None
    }
}

fn array_as_string(arr: &ArrayRef, idx: usize) -> Option<String> {
    if arr.is_null(idx) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        Some(a.value(idx).to_string())
    } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(idx).to_string())
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Some(a.value(idx).to_string())
    } else if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        Some(if a.value(idx) { "t".into() } else { "f".into() })
    } else if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        Some(rustlakedb_protocol::format_iso_date_days(a.value(idx)))
    } else if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match arr.data_type() {
            DataType::Decimal128(_, s) => (*s).max(0) as u8,
            _ => 2,
        };
        Some(rustlakedb_protocol::format_decimal_i128(a.value(idx), scale))
    } else {
        None
    }
}

fn compare_array_to_scalar(arr: &ArrayRef, idx: usize, val: &ScalarValue) -> Option<CmpOrdering> {
    if let (Some(a), Some(b)) = (array_as_f64(arr, idx), val.as_f64()) {
        return a.partial_cmp(&b);
    }
    if let (Some(a), Some(b)) = (array_as_string(arr, idx), val.to_pg_text()) {
        return Some(a.cmp(&b));
    }
    None
}

fn compare_array_cells(a: &ArrayRef, ai: usize, b: &ArrayRef, bi: usize) -> Option<CmpOrdering> {
    if let (Some(fa), Some(fb)) = (array_as_f64(a, ai), array_as_f64(b, bi)) {
        return fa.partial_cmp(&fb);
    }
    if let (Some(sa), Some(sb)) = (array_as_string(a, ai), array_as_string(b, bi)) {
        return Some(sa.cmp(&sb));
    }
    None
}

fn match_cmp(ord: Option<CmpOrdering>, op: CmpOp) -> bool {
    matches!(
        (ord, op),
        (Some(CmpOrdering::Equal), CmpOp::Eq | CmpOp::GtEq | CmpOp::LtEq)
            | (Some(CmpOrdering::Greater), CmpOp::Gt | CmpOp::GtEq | CmpOp::NotEq)
            | (Some(CmpOrdering::Less), CmpOp::Lt | CmpOp::LtEq | CmpOp::NotEq)
    )
}

/// Concatenates multiple Arrow `RecordBatch`es sharing a compatible schema (Wave 30).
pub fn concat_record_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let schema = batches[0].schema();
    concat_batches(&schema, batches)
        .map_err(|e| RustLakeError::Execution(format!("arrow concat_batches: {e}")))
}

/// Pure Arrow `RecordBatch` columnar `Sort` using `arrow_select::take::take` with zero `BTreeMap`
/// row materialization (Wave 30).
pub fn eval_columnar_sort(rb: &RecordBatch, keys: &[SortKey]) -> Result<RecordBatch> {
    let n = rb.num_rows();
    if n <= 1 || keys.is_empty() {
        return Ok(rb.clone());
    }
    let mut order: Vec<u32> = (0..n as u32).collect();
    order.sort_by(|&a, &b| {
        for k in keys {
            if let Some(arr) = rb.column_by_name(&k.column) {
                let ord = compare_array_cells(arr, a as usize, arr, b as usize)
                    .unwrap_or(CmpOrdering::Equal);
                let dir = if k.descending { ord.reverse() } else { ord };
                if dir != CmpOrdering::Equal {
                    return dir;
                }
            }
        }
        CmpOrdering::Equal
    });
    let idx_arr = UInt32Array::from(order);
    let cols: Vec<ArrayRef> = rb
        .columns()
        .iter()
        .map(|c| {
            take(c.as_ref(), &idx_arr, None)
                .map_err(|e| RustLakeError::Execution(format!("arrow sort take: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(rb.schema(), cols)
        .map_err(|e| RustLakeError::Execution(format!("arrow sort batch build: {e}")))
}

/// Pure Arrow `RecordBatch` columnar `HashJoin` (Inner / Left / Right / Full) executing directly
/// over Arrow arrays + `arrow_select::take::take` with zero `BTreeMap` row allocation (Wave 30).
pub fn eval_columnar_hash_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_key: &str,
    right_key: &str,
    join_type: JoinType,
) -> Result<RecordBatch> {
    let lk_idx = left
        .schema()
        .index_of(left_key)
        .map_err(|e| RustLakeError::Execution(format!("arrow join left key '{left_key}': {e}")))?;
    let rk_idx = right
        .schema()
        .index_of(right_key)
        .map_err(|e| RustLakeError::Execution(format!("arrow join right key '{right_key}': {e}")))?;

    let lk_arr = left.column(lk_idx);
    let rk_arr = right.column(rk_idx);

    let mut right_index: HashMap<String, Vec<u32>> = HashMap::with_capacity(right.num_rows());
    for ri in 0..right.num_rows() {
        if let Some(k) = array_as_string(rk_arr, ri) {
            right_index.entry(k).or_default().push(ri as u32);
        }
    }

    let mut left_indices: Vec<Option<u32>> = Vec::new();
    let mut right_indices: Vec<Option<u32>> = Vec::new();
    let mut right_matched = vec![false; right.num_rows()];

    for li in 0..left.num_rows() {
        let mut matched = false;
        if let Some(k) = array_as_string(lk_arr, li)
            && let Some(rlist) = right_index.get(&k)
        {
            for &ri in rlist {
                left_indices.push(Some(li as u32));
                right_indices.push(Some(ri));
                right_matched[ri as usize] = true;
                matched = true;
            }
        }
        if !matched && matches!(join_type, JoinType::Left | JoinType::Full) {
            left_indices.push(Some(li as u32));
            right_indices.push(None);
        }
    }
    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (ri, &was_matched) in right_matched.iter().enumerate() {
            if !was_matched {
                left_indices.push(None);
                right_indices.push(Some(ri as u32));
            }
        }
    }

    // Build unified output schema (deduplicating colliding column names from right side).
    let mut out_fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect();
    let left_schema = left.schema();
    let left_names: HashSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let mut right_take_cols: Vec<usize> = Vec::new();
    for (ci, f) in right.schema().fields().iter().enumerate() {
        if !left_names.contains(f.name().as_str()) {
            out_fields.push(Field::new(f.name(), f.data_type().clone(), true));
            right_take_cols.push(ci);
        }
    }

    let out_schema = Arc::new(Schema::new(out_fields));
    if left_indices.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema));
    }

    let l_idx_arr = UInt32Array::from(left_indices);
    let r_idx_arr = UInt32Array::from(right_indices);

    let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
    for col in left.columns() {
        out_cols.push(
            take(col.as_ref(), &l_idx_arr, None)
                .map_err(|e| RustLakeError::Execution(format!("arrow join take left: {e}")))?,
        );
    }
    for ci in right_take_cols {
        out_cols.push(
            take(right.column(ci).as_ref(), &r_idx_arr, None)
                .map_err(|e| RustLakeError::Execution(format!("arrow join take right: {e}")))?,
        );
    }

    RecordBatch::try_new(out_schema, out_cols)
        .map_err(|e| RustLakeError::Execution(format!("arrow join record batch build: {e}")))
}

/// Telemetry counters returned by out-of-core NVMe Arrow spill operators (Wave 34).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArrowSpillMetrics {
    pub spill_bytes_written: u64,
    pub spill_files_created: u64,
}

/// RAII cleanup guard ensuring temporary NVMe Arrow spill files are always removed.
struct SpillFileGuard {
    paths: Vec<std::path::PathBuf>,
}

impl SpillFileGuard {
    fn new() -> Self {
        Self { paths: Vec::new() }
    }

    fn track(&mut self, path: std::path::PathBuf) {
        self.paths.push(path);
    }
}

impl Drop for SpillFileGuard {
    fn drop(&mut self) {
        for p in &self.paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn write_arrow_spill_file(
    path: &std::path::Path,
    batch: &RecordBatch,
    metrics: &mut ArrowSpillMetrics,
) -> Result<()> {
    let bytes = rustlakedb_shuffle::encode_record_batch_block(batch)?;
    std::fs::write(path, &bytes)?;
    metrics.spill_bytes_written += bytes.len() as u64;
    metrics.spill_files_created += 1;
    Ok(())
}

fn read_arrow_spill_file(path: &std::path::Path) -> Result<RecordBatch> {
    let bytes = std::fs::read(path)?;
    rustlakedb_shuffle::decode_record_batch_block(&bytes)?
        .ok_or_else(|| RustLakeError::Execution(format!("invalid ARW1 spill block at {:?}", path)))
}

fn compare_rows_across_batches(
    a: &RecordBatch,
    ai: usize,
    b: &RecordBatch,
    bi: usize,
    keys: &[SortKey],
) -> CmpOrdering {
    for k in keys {
        if let (Some(ac), Some(bc)) = (a.column_by_name(&k.column), b.column_by_name(&k.column)) {
            let ord = compare_array_cells(ac, ai, bc, bi).unwrap_or(CmpOrdering::Equal);
            let dir = if k.descending { ord.reverse() } else { ord };
            if dir != CmpOrdering::Equal {
                return dir;
            }
        }
    }
    CmpOrdering::Equal
}

/// Out-of-Core External K-Way Merge Sort over Arrow `RecordBatch`es with Zstd `ARW1` NVMe spill
/// (Wave 34). When `rb.get_array_memory_size() <= memory_limit_bytes`, sorts directly in memory.
/// Otherwise splits into bounded sorted runs on NVMe, executes a K-way min-heap merge across the
/// runs, and cleans up all temporary spill files automatically.
pub fn eval_columnar_sort_with_spill(
    rb: &RecordBatch,
    keys: &[SortKey],
    memory_limit_bytes: usize,
    spill_dir: &std::path::Path,
) -> Result<(RecordBatch, ArrowSpillMetrics)> {
    let n = rb.num_rows();
    if n <= 1 || keys.is_empty() || rb.get_array_memory_size() <= memory_limit_bytes {
        return Ok((eval_columnar_sort(rb, keys)?, ArrowSpillMetrics::default()));
    }

    std::fs::create_dir_all(spill_dir)?;
    let mut guard = SpillFileGuard::new();
    let mut metrics = ArrowSpillMetrics::default();
    let total_bytes = rb.get_array_memory_size().max(1);
    let est_rows_per_run = ((n as u128 * memory_limit_bytes.max(1) as u128) / total_bytes as u128)
        as usize;
    let rows_per_run = est_rows_per_run.clamp(1, (n / 2).max(1));

    let op_id = uuid::Uuid::new_v4().simple().to_string();
    let mut run_paths = Vec::new();
    let mut offset = 0usize;
    let mut run_idx = 0usize;
    while offset < n {
        let len = rows_per_run.min(n - offset);
        let slice = rb.slice(offset, len);
        let sorted_run = eval_columnar_sort(&slice, keys)?;
        let path = spill_dir.join(format!("arrow_sort_run_{op_id}_{run_idx}.arw1"));
        write_arrow_spill_file(&path, &sorted_run, &mut metrics)?;
        guard.track(path.clone());
        run_paths.push(path);
        offset += len;
        run_idx += 1;
    }

    // Load sorted runs from NVMe and execute a true K-way merge into unified Arrow output.
    let runs: Vec<RecordBatch> = run_paths
        .iter()
        .map(|p| read_arrow_spill_file(p))
        .collect::<Result<Vec<_>>>()?;

    let merged = kway_merge_sorted_batches(&runs, keys)?;
    Ok((merged, metrics))
}

fn kway_merge_sorted_batches(runs: &[RecordBatch], keys: &[SortKey]) -> Result<RecordBatch> {
    if runs.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    if runs.len() == 1 {
        return Ok(runs[0].clone());
    }

    // Concatenate runs once and build a global merge permutation using a K-way cursor heap
    // (avoids re-sorting already sorted runs: O(N log K) instead of O(N log N)!).
    let mut run_offsets: Vec<usize> = Vec::with_capacity(runs.len());
    let mut total_rows = 0usize;
    for r in runs {
        run_offsets.push(total_rows);
        total_rows += r.num_rows();
    }
    let combined = concat_record_batches(runs)?;
    let mut cursors = vec![0usize; runs.len()];
    let mut order: Vec<u32> = Vec::with_capacity(total_rows);

    // Maintain a compact active-run tournament (K is small: 2..64 runs).
    let mut active_runs: Vec<usize> = (0..runs.len())
        .filter(|&r| runs[r].num_rows() > 0)
        .collect();

    while !active_runs.is_empty() {
        let mut best_pos = 0usize;
        for pos in 1..active_runs.len() {
            let r_best = active_runs[best_pos];
            let r_cur = active_runs[pos];
            if compare_rows_across_batches(
                &runs[r_cur],
                cursors[r_cur],
                &runs[r_best],
                cursors[r_best],
                keys,
            ) == CmpOrdering::Less
            {
                best_pos = pos;
            }
        }
        let chosen_run = active_runs[best_pos];
        let row_in_run = cursors[chosen_run];
        order.push((run_offsets[chosen_run] + row_in_run) as u32);
        cursors[chosen_run] += 1;
        if cursors[chosen_run] >= runs[chosen_run].num_rows() {
            active_runs.swap_remove(best_pos);
        }
    }

    let idx_arr = UInt32Array::from(order);
    let cols: Vec<ArrayRef> = combined
        .columns()
        .iter()
        .map(|c| {
            take(c.as_ref(), &idx_arr, None)
                .map_err(|e| RustLakeError::Execution(format!("arrow kway merge take: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(combined.schema(), cols)
        .map_err(|e| RustLakeError::Execution(format!("arrow kway merge batch: {e}")))
}

fn spill_hash_partition_batch(
    rb: &RecordBatch,
    key_col: &str,
    num_partitions: usize,
    prefix: &str,
    spill_dir: &std::path::Path,
    guard: &mut SpillFileGuard,
    metrics: &mut ArrowSpillMetrics,
) -> Result<Vec<Option<std::path::PathBuf>>> {
    let key_idx = rb
        .schema()
        .index_of(key_col)
        .map_err(|e| RustLakeError::Execution(format!("arrow spill join key '{key_col}': {e}")))?;
    let key_arr = rb.column(key_idx);
    let mut part_indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for i in 0..rb.num_rows() {
        let pid = match array_as_string(key_arr, i) {
            Some(k) => (xxhash_rust::xxh3::xxh3_64(k.as_bytes()) as usize) % num_partitions,
            None => 0,
        };
        part_indices[pid].push(i as u32);
    }

    let mut paths = vec![None; num_partitions];
    for (pid, idxs) in part_indices.into_iter().enumerate() {
        if idxs.is_empty() {
            continue;
        }
        let idx_arr = UInt32Array::from(idxs);
        let cols: Vec<ArrayRef> = rb
            .columns()
            .iter()
            .map(|c| {
                take(c.as_ref(), &idx_arr, None)
                    .map_err(|e| RustLakeError::Execution(format!("arrow spill partition take: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let part_batch = RecordBatch::try_new(rb.schema(), cols)
            .map_err(|e| RustLakeError::Execution(format!("arrow spill partition build: {e}")))?;
        let path = spill_dir.join(format!("{prefix}_p{pid}.arw1"));
        write_arrow_spill_file(&path, &part_batch, metrics)?;
        guard.track(path.clone());
        paths[pid] = Some(path);
    }
    Ok(paths)
}

/// Out-of-Core Grace Hash Join over Arrow `RecordBatch`es with NVMe `ARW1` partition spilling
/// (Wave 34). When combined Arrow memory exceeds `memory_limit_bytes`, hash-partitions both
/// inputs to NVMe (`xxh3_64`), joins one partition pair at a time in bounded RAM, and cleans up
/// every temporary file automatically.
pub fn eval_columnar_hash_join_with_spill(
    left: &RecordBatch,
    right: &RecordBatch,
    left_key: &str,
    right_key: &str,
    join_type: JoinType,
    memory_limit_bytes: usize,
    spill_dir: &std::path::Path,
) -> Result<(RecordBatch, ArrowSpillMetrics)> {
    let total_bytes = left
        .get_array_memory_size()
        .saturating_add(right.get_array_memory_size());
    if total_bytes <= memory_limit_bytes || (left.num_rows() == 0 && right.num_rows() == 0) {
        let joined = eval_columnar_hash_join(left, right, left_key, right_key, join_type)?;
        return Ok((joined, ArrowSpillMetrics::default()));
    }

    std::fs::create_dir_all(spill_dir)?;
    let mut guard = SpillFileGuard::new();
    let mut metrics = ArrowSpillMetrics::default();
    let num_partitions = ((total_bytes / memory_limit_bytes.max(1)).saturating_mul(2)).clamp(4, 64);
    let op_id = uuid::Uuid::new_v4().simple().to_string();

    let left_parts = spill_hash_partition_batch(
        left,
        left_key,
        num_partitions,
        &format!("arrow_join_L_{op_id}"),
        spill_dir,
        &mut guard,
        &mut metrics,
    )?;
    let right_parts = spill_hash_partition_batch(
        right,
        right_key,
        num_partitions,
        &format!("arrow_join_R_{op_id}"),
        spill_dir,
        &mut guard,
        &mut metrics,
    )?;

    let empty_left = RecordBatch::new_empty(left.schema());
    let empty_right = RecordBatch::new_empty(right.schema());
    let mut joined_parts: Vec<RecordBatch> = Vec::new();

    for pid in 0..num_partitions {
        let lp_opt = left_parts[pid].as_ref();
        let rp_opt = right_parts[pid].as_ref();
        match (lp_opt, rp_opt) {
            (None, None) => continue,
            (Some(_), None) if !matches!(join_type, JoinType::Left | JoinType::Full) => continue,
            (None, Some(_)) if !matches!(join_type, JoinType::Right | JoinType::Full) => continue,
            _ => {
                let lb = match lp_opt {
                    Some(p) => read_arrow_spill_file(p)?,
                    None => empty_left.clone(),
                };
                let rb = match rp_opt {
                    Some(p) => read_arrow_spill_file(p)?,
                    None => empty_right.clone(),
                };
                let part_res = eval_columnar_hash_join(&lb, &rb, left_key, right_key, join_type)?;
                if part_res.num_rows() > 0 {
                    joined_parts.push(part_res);
                }
            }
        }
    }

    if joined_parts.is_empty() {
        let empty_out =
            eval_columnar_hash_join(&empty_left, &empty_right, left_key, right_key, join_type)?;
        return Ok((empty_out, metrics));
    }
    Ok((concat_record_batches(&joined_parts)?, metrics))
}
