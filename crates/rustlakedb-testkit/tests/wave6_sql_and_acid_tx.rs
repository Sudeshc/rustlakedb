// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_testkit::TestCluster;

#[tokio::test]
async fn test_wave6_ctas_insert_select_arith_string_math_and_cast() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE items (id BIGINT, name TEXT, price DOUBLE, qty BIGINT)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO items VALUES (1, '  Widget  ', 19.95, 3), (2, 'Gadget', -5.4, 2)")
        .await
        .unwrap();

    // 1. Arithmetic, String functions (LOWER/UPPER/TRIM/LENGTH/CONCAT/SUBSTRING), Math (ROUND/ABS), CAST, NOW()
    let res = cluster
        .sql(
            "SELECT id, UPPER(TRIM(name)) AS clean_name, LENGTH(TRIM(name)) AS name_len, \
             CONCAT(LOWER(TRIM(name)), '_sku') AS sku, SUBSTRING(TRIM(name), 1, 3) AS prefix, \
             ROUND(price * qty, 1) AS line_total, ABS(price) AS abs_price, \
             CAST(price AS BIGINT) AS price_int, NOW() AS ts \
             FROM items ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(res.batch.num_rows(), 2);
    assert_eq!(res.batch.rows[0].get("clean_name"), Some(&ScalarValue::Utf8("WIDGET".into())));
    assert_eq!(res.batch.rows[0].get("name_len"), Some(&ScalarValue::Int64(6)));
    assert_eq!(res.batch.rows[0].get("sku"), Some(&ScalarValue::Utf8("widget_sku".into())));
    assert_eq!(res.batch.rows[0].get("prefix"), Some(&ScalarValue::Utf8("Wid".into())));
    assert_eq!(res.batch.rows[0].get("line_total"), Some(&ScalarValue::Float64(59.9)));
    assert_eq!(res.batch.rows[0].get("price_int"), Some(&ScalarValue::Int64(19)));
    assert_eq!(res.batch.rows[1].get("abs_price"), Some(&ScalarValue::Float64(5.4)));

    // 1b. Regression: computed projection + ORDER BY unprojected raw column DESC + LIMIT!
    let top_item = cluster
        .sql("SELECT id, UPPER(TRIM(name)) AS cname, ROUND(price * 2, 1) AS doubled FROM items ORDER BY price DESC LIMIT 1")
        .await
        .unwrap();
    assert_eq!(top_item.batch.num_rows(), 1);
    assert_eq!(top_item.batch.rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(top_item.batch.rows[0].get("doubled"), Some(&ScalarValue::Float64(39.9)));

    // 2. CTAS (CREATE TABLE AS SELECT)
    cluster
        .sql("CREATE TABLE items_summary AS SELECT id, UPPER(TRIM(name)) AS cname, price * qty AS total FROM items")
        .await
        .unwrap();
    let ctas_res = cluster
        .sql("SELECT id, cname, total FROM items_summary ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ctas_res.batch.num_rows(), 2);
    assert_eq!(ctas_res.batch.rows[0].get("cname"), Some(&ScalarValue::Utf8("WIDGET".into())));

    // 3. INSERT INTO ... SELECT ...
    cluster
        .sql("INSERT INTO items_summary SELECT id, LOWER(TRIM(name)), price FROM items WHERE id = 1")
        .await
        .unwrap();
    let cnt = cluster
        .sql("SELECT COUNT(*) AS c FROM items_summary")
        .await
        .unwrap();
    assert_eq!(cnt.batch.rows[0].get("c"), Some(&ScalarValue::Int64(3)));
}

#[tokio::test]
async fn test_wave6_right_full_cross_joins_and_subqueries() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster
        .sql("CREATE TABLE left_t (id BIGINT, lval TEXT)")
        .await
        .unwrap();
    cluster
        .sql("CREATE TABLE right_t (id BIGINT, rval TEXT)")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO left_t VALUES (1, 'L1'), (2, 'L2')")
        .await
        .unwrap();
    cluster
        .sql("INSERT INTO right_t VALUES (2, 'R2'), (3, 'R3')")
        .await
        .unwrap();

    // RIGHT JOIN -> keeps id=2 (matched) and id=3 (unmatched right)
    let rj = cluster
        .sql("SELECT id, lval, rval FROM left_t RIGHT JOIN right_t ON left_t.id = right_t.id ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rj.batch.num_rows(), 2);
    assert_eq!(rj.batch.rows[1].get("id"), Some(&ScalarValue::Int64(3)));
    assert_eq!(rj.batch.rows[1].get("lval"), Some(&ScalarValue::Null));
    assert_eq!(rj.batch.rows[1].get("rval"), Some(&ScalarValue::Utf8("R3".into())));

    // FULL OUTER JOIN -> keeps 1, 2, 3!
    let fj = cluster
        .sql("SELECT id, lval, rval FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id ORDER BY id")
        .await
        .unwrap();
    assert_eq!(fj.batch.num_rows(), 3);

    // CROSS JOIN -> 2 x 2 = 4 rows!
    let cj = cluster
        .sql("SELECT lval, rval FROM left_t CROSS JOIN right_t")
        .await
        .unwrap();
    assert_eq!(cj.batch.num_rows(), 4);

    // IN (SELECT ...) subquery
    let in_sub = cluster
        .sql("SELECT id, lval FROM left_t WHERE id IN (SELECT id FROM right_t)")
        .await
        .unwrap();
    assert_eq!(in_sub.batch.num_rows(), 1);
    assert_eq!(in_sub.batch.rows[0].get("id"), Some(&ScalarValue::Int64(2)));

    // EXISTS (SELECT ...) subquery
    let ex_sub = cluster
        .sql("SELECT id FROM left_t WHERE EXISTS (SELECT id FROM right_t WHERE id = 3) ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ex_sub.batch.num_rows(), 2);
}

#[tokio::test]
async fn test_wave6_multistatement_acid_begin_commit_and_rollback() {
    let cluster = TestCluster::start(2, ShuffleMode::Staged).unwrap();
    cluster.sql("CREATE TABLE tx_a (id BIGINT)").await.unwrap();
    cluster.sql("CREATE TABLE tx_b (id BIGINT)").await.unwrap();

    // 1. BEGIN -> INSERT into both tables -> ROLLBACK -> neither table has rows!
    cluster.sql("BEGIN").await.unwrap();
    cluster.sql("INSERT INTO tx_a VALUES (10)").await.unwrap();
    cluster.sql("INSERT INTO tx_b VALUES (20)").await.unwrap();
    // Uncommitted writes are invisible before COMMIT (ANSI SQL COUNT(*) on empty table == 0)
    let pre_a = cluster.sql("SELECT COUNT(*) AS c FROM tx_a").await.unwrap();
    assert_eq!(pre_a.batch.rows[0].get("c"), Some(&ScalarValue::Int64(0)));
    cluster.sql("ROLLBACK").await.unwrap();
    let rb_b = cluster.sql("SELECT COUNT(*) AS c FROM tx_b").await.unwrap();
    assert_eq!(rb_b.batch.rows[0].get("c"), Some(&ScalarValue::Int64(0)));

    // 2. BEGIN -> INSERT into tx_a & tx_b -> COMMIT -> atomic multi-table visibility!
    cluster.sql("BEGIN").await.unwrap();
    cluster.sql("INSERT INTO tx_a VALUES (100)").await.unwrap();
    cluster.sql("INSERT INTO tx_b VALUES (200)").await.unwrap();
    cluster.sql("COMMIT").await.unwrap();

    let post_a = cluster.sql("SELECT id FROM tx_a").await.unwrap();
    let post_b = cluster.sql("SELECT id FROM tx_b").await.unwrap();
    assert_eq!(post_a.batch.rows[0].get("id"), Some(&ScalarValue::Int64(100)));
    assert_eq!(post_b.batch.rows[0].get("id"), Some(&ScalarValue::Int64(200)));
}
