// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rustlakedb_protocol::{
    AggExpr, AggFunc, ArithOp, CmpOp, ColumnExpr, FilterExpr, ScalarValue, SortKey, StringFunc,
    WindowFunc, WindowSpec,
};
use rustlakedb_worker::columnar::{
    apply_columnar_filter, eval_columnar_hash_aggregate, eval_columnar_project,
    eval_columnar_window,
};
use std::sync::Arc;
use std::time::Instant;

#[test]
fn wave20_native_arrow_record_batch_columnar_pipeline_end_to_end() {
    // Construct a 100,000-row Arrow RecordBatch directly in columnar memory (zero RowBatch!)
    let n = 100_000usize;
    let regions_src = ["amer", "emea", "apac", "latam"];
    let ids: Vec<i64> = (1..=n as i64).collect();
    let regions: Vec<&str> = (0..n).map(|i| regions_src[i % 4]).collect();
    let prices: Vec<f64> = (0..n).map(|i| ((i % 100) + 1) as f64 * 10.0).collect();
    let discounts: Vec<f64> = (0..n).map(|i| ((i % 5) as f64) * 0.02).collect();
    let dates: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "2026-03-15" } else { "2025-11-20" })
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
        Field::new("order_date", DataType::Utf8, false),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(discounts)),
            Arc::new(StringArray::from(dates)),
        ],
    )
    .unwrap();

    // 1. Columnar Filter (`price >= 500.0 AND region IN ('amer', 'emea')`) via arrow_select::filter_record_batch
    let t0 = Instant::now();
    let pred = FilterExpr::And(
        Box::new(FilterExpr::GtEq {
            column: "price".into(),
            value: ScalarValue::Float64(500.0),
        }),
        Box::new(FilterExpr::InList {
            column: "region".into(),
            values: vec![
                ScalarValue::Utf8("amer".into()),
                ScalarValue::Utf8("emea".into()),
            ],
        }),
    );
    let filtered = apply_columnar_filter(&rb, &pred).unwrap();
    assert!(filtered.num_rows() > 20_000 && filtered.num_rows() < 30_000);

    // 2. Columnar Projection with BinaryArith (`price * (1 - discount)`), StringExpr (`UPPER`), Extract (`YEAR`), CaseExpr!
    let proj_exprs = vec![
        ColumnExpr::Column("id".into()),
        ColumnExpr::StringExpr {
            func: StringFunc::Upper,
            args: vec![ColumnExpr::Column("region".into())],
            alias: "region_up".into(),
        },
        ColumnExpr::BinaryArith {
            left: Box::new(ColumnExpr::Column("price".into())),
            op: ArithOp::Mul,
            right: Box::new(ColumnExpr::BinaryArith {
                left: Box::new(ColumnExpr::Literal {
                    value: ScalarValue::Float64(1.0),
                    alias: "one".into(),
                }),
                op: ArithOp::Sub,
                right: Box::new(ColumnExpr::Column("discount".into())),
                alias: "factor".into(),
            }),
            alias: "net_price".into(),
        },
        ColumnExpr::Extract {
            field: "YEAR".into(),
            expr: Box::new(ColumnExpr::Column("order_date".into())),
            alias: "yr".into(),
        },
        ColumnExpr::CaseExpr {
            branches: vec![(
                FilterExpr::ColCmp {
                    left: "price".into(),
                    op: CmpOp::Gt,
                    right: "discount".into(),
                },
                Box::new(ColumnExpr::Literal {
                    value: ScalarValue::Float64(1.0),
                    alias: "hi".into(),
                }),
            )],
            else_expr: Box::new(ColumnExpr::Literal {
                value: ScalarValue::Float64(0.0),
                alias: "lo".into(),
            }),
            alias: "valid_flag".into(),
        },
    ];
    let projected = eval_columnar_project(&filtered, &proj_exprs).unwrap();
    assert_eq!(projected.num_rows(), filtered.num_rows());
    assert_eq!(projected.num_columns(), 5);

    // 3. Columnar HashAggregate + ROLLUP directly over Arrow RecordBatch!
    let agg_rb = eval_columnar_hash_aggregate(
        std::slice::from_ref(&projected),
        &[ColumnExpr::Column("region_up".into())],
        &[
            AggExpr {
                func: AggFunc::Count,
                alias: "cnt".into(),
            },
            AggExpr {
                func: AggFunc::Sum("net_price".into()),
                alias: "total_net".into(),
            },
            AggExpr {
                func: AggFunc::Avg("net_price".into()),
                alias: "avg_net".into(),
            },
            AggExpr {
                func: AggFunc::CountDistinct("yr".into()),
                alias: "distinct_yrs".into(),
            },
        ],
        &[vec!["region_up".into()], vec![]],
    )
    .unwrap();
    // 2 regions (AMER, EMEA) + 1 ROLLUP grand total row = 3 rows!
    assert_eq!(agg_rb.num_rows(), 3);
    let cnt_col = agg_rb
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let reg_col = agg_rb
        .column_by_name("region_up")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut found_grand_total = false;
    for i in 0..agg_rb.num_rows() {
        if reg_col.is_null(i) {
            assert_eq!(cnt_col.value(i) as usize, projected.num_rows());
            found_grand_total = true;
        }
    }
    assert!(found_grand_total, "expected ROLLUP grand total row");

    // 4. Native Columnar Window (`ROW_NUMBER()`, `SUM(net_price) OVER (PARTITION BY region_up ORDER BY id)`)
    let win_rb = eval_columnar_window(
        &projected.slice(0, 1000),
        &[
            WindowSpec {
                func: WindowFunc::RowNumber,
                partition_by: vec!["region_up".into()],
                order_by: vec![SortKey {
                    column: "id".into(),
                    descending: false,
                }],
                alias: "rn".into(),
            },
            WindowSpec {
                func: WindowFunc::Sum("net_price".into()),
                partition_by: vec!["region_up".into()],
                order_by: vec![SortKey {
                    column: "id".into(),
                    descending: false,
                }],
                alias: "running_net".into(),
            },
        ],
    )
    .unwrap();
    assert_eq!(win_rb.num_rows(), 1000);
    let rn_col = win_rb
        .column_by_name("rn")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(rn_col.value(0), 1);
    let running_col = win_rb
        .column_by_name("running_net")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(running_col.value(999) > running_col.value(0));

    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "Wave 20 Native Arrow Columnar Pipeline (100K Filter + Project + ROLLUP HashAgg + Window): {:.2} ms",
        ms
    );
}
