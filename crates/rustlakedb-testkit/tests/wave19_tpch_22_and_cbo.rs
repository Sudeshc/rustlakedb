// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_protocol::{ScalarValue, ShuffleMode};
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn wave19_full_22_analytical_decision_support_queries_and_cbo_pushdown() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());
    let w1 = Arc::new(WorkerNode::new("w1", catalog.clone(), shuffle.clone(), store.clone()));
    let w2 = Arc::new(WorkerNode::new("w2", catalog.clone(), shuffle.clone(), store.clone()));
    let coord = Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![w1, w2],
        PlannerConfig {
            default_shuffle_partitions: 2,
            shuffle_mode: ShuffleMode::Staged,
            ..Default::default()
        },
    );

    // Build all 8 normalized analytical lakehouse tables
    for ddl in [
        "CREATE TABLE region (r_regionkey BIGINT, r_name TEXT)",
        "CREATE TABLE nation (n_nationkey BIGINT, n_name TEXT, n_regionkey BIGINT)",
        "CREATE TABLE supplier (s_suppkey BIGINT, s_name TEXT, s_nationkey BIGINT, s_acctbal DOUBLE, s_comment TEXT)",
        "CREATE TABLE part (p_partkey BIGINT, p_name TEXT, p_brand TEXT, p_type TEXT, p_size BIGINT, p_container TEXT, p_retailprice DOUBLE)",
        "CREATE TABLE partsupp (ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty BIGINT, ps_supplycost DOUBLE)",
        "CREATE TABLE customer (c_custkey BIGINT, c_name TEXT, c_nationkey BIGINT, c_phone TEXT, c_acctbal DOUBLE, c_mktsegment TEXT)",
        "CREATE TABLE orders (o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus TEXT, o_totalprice DOUBLE, o_orderdate TEXT, o_orderpriority TEXT, o_comment TEXT)",
        "CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT)",
    ] {
        coord.execute_sql(ddl, "main").await.unwrap();
    }

    coord.execute_sql("INSERT INTO region VALUES (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST')", "main").await.unwrap();
    coord.execute_sql("INSERT INTO nation VALUES (1,'ARGENTINA',1),(2,'BRAZIL',1),(3,'CANADA',1),(6,'FRANCE',3),(7,'GERMANY',3),(12,'JAPAN',2),(24,'UNITED STATES',1)", "main").await.unwrap();
    coord.execute_sql(
        "INSERT INTO supplier VALUES \
         (1,'Supplier#001',7,4500.0,'reliable partner'), \
         (2,'Supplier#002',6,3200.0,'express logistics'), \
         (3,'Supplier#003',24,6100.0,'customer complaints resolved'), \
         (4,'Supplier#004',12,2100.0,'standard vendor')",
        "main",
    ).await.unwrap();
    coord.execute_sql(
        "INSERT INTO part VALUES \
         (1,'green steel bolt','Brand#12','PROMO BRASS',15,'SM CASE',950.0), \
         (2,'forest copper pipe','Brand#23','ECONOMY STEEL',25,'MED BAG',1120.0), \
         (3,'green titanium rod','Brand#34','PROMO COPPER',15,'LG BOX',1400.0), \
         (4,'silver alloy plate','Brand#45','MEDIUM POLISHED',10,'SM BOX',890.0)",
        "main",
    ).await.unwrap();
    coord.execute_sql(
        "INSERT INTO partsupp VALUES \
         (1,1,120,200.0),(1,2,80,240.0),(2,2,150,310.0),(2,3,60,295.0), \
         (3,1,90,410.0),(3,4,110,390.0),(4,3,200,150.0),(4,4,70,180.0)",
        "main",
    ).await.unwrap();
    coord.execute_sql(
        "INSERT INTO customer VALUES \
         (1,'Cust#001',7,'13-555-0101',1200.0,'BUILDING'), \
         (2,'Cust#002',6,'31-555-0102',850.0,'AUTOMOBILE'), \
         (3,'Cust#003',24,'23-555-0103',2300.0,'BUILDING'), \
         (4,'Cust#004',12,'29-555-0104',-100.0,'MACHINERY'), \
         (5,'Cust#005',2,'13-555-0105',1500.0,'HOUSEHOLD')",
        "main",
    ).await.unwrap();
    coord.execute_sql(
        "INSERT INTO orders VALUES \
         (101,1,'F',18000.0,'1995-02-10','1-URGENT','regular order'), \
         (102,2,'O',24000.0,'1995-07-15','2-HIGH','special requests noted'), \
         (103,3,'F',31000.0,'1994-11-20','3-MEDIUM','fast delivery'), \
         (104,1,'O',15000.0,'1996-03-05','1-URGENT','normal order'), \
         (105,4,'F',9500.0,'1995-05-18','4-NOT SPECIFIED','ok')",
        "main",
    ).await.unwrap();
    coord.execute_sql(
        "INSERT INTO lineitem VALUES \
         (101,1,1,12.0,11400.0,0.05,0.02,'R','F','1995-03-01','1995-03-10','1995-03-15','DELIVER IN PERSON','MAIL'), \
         (101,2,2,8.0,8960.0,0.06,0.03,'A','F','1994-06-10','1994-06-20','1994-06-25','DELIVER IN PERSON','AIR'), \
         (102,3,1,35.0,49000.0,0.07,0.02,'N','O','1995-08-01','1995-08-05','1995-08-12','NONE','SHIP'), \
         (103,1,2,18.0,17100.0,0.05,0.01,'R','F','1994-12-01','1994-12-10','1994-12-05','DELIVER IN PERSON','MAIL'), \
         (104,4,3,45.0,40050.0,0.04,0.04,'N','O','1996-04-01','1996-04-05','1996-04-10','DELIVER IN PERSON','AIR'), \
         (105,3,4,6.0,8400.0,0.06,0.02,'A','F','1995-06-01','1995-06-15','1995-06-20','DELIVER IN PERSON','SHIP')",
        "main",
    ).await.unwrap();

    // All 22 analytical decision-support query patterns (Q1..Q22)!
    let queries: Vec<(&str, &str)> = vec![
        // Q1: Pricing summary with DATE - INTERVAL + derived expressions inside SUM/AVG!
        (
            "Q1",
            "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
             SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
             SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
             COUNT(*) AS count_order \
             FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY \
             GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
        ),
        // Q2: Minimum cost supplier in region (4-way join + CBO predicate pushdown)
        (
            "Q2",
            "SELECT supplier.s_name, supplier.s_acctbal, nation.n_name, part.p_partkey \
             FROM part \
             INNER JOIN partsupp ON part.p_partkey = partsupp.ps_partkey \
             INNER JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey \
             INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             INNER JOIN region ON nation.n_regionkey = region.r_regionkey \
             WHERE part.p_size = 15 AND region.r_name = 'EUROPE' \
             ORDER BY supplier.s_acctbal DESC LIMIT 10",
        ),
        // Q3: Shipping priority (3-way join + derived revenue agg + Top-10)
        (
            "Q3",
            "SELECT orders.o_orderkey, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue \
             FROM customer \
             INNER JOIN orders ON customer.c_custkey = orders.o_custkey \
             INNER JOIN lineitem ON orders.o_orderkey = lineitem.l_orderkey \
             WHERE customer.c_mktsegment = 'BUILDING' AND orders.o_orderdate < DATE '1995-03-15' AND lineitem.l_shipdate > DATE '1995-01-01' \
             GROUP BY orders.o_orderkey ORDER BY revenue DESC LIMIT 10",
        ),
        // Q4: Order priority checking with EXISTS + col-vs-col comparison (l_commitdate < l_receiptdate)
        (
            "Q4",
            "SELECT o_orderpriority, COUNT(*) AS order_count \
             FROM orders \
             WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '3' YEAR \
               AND o_orderkey IN (SELECT l_orderkey FROM lineitem WHERE l_commitdate < l_receiptdate) \
             GROUP BY o_orderpriority ORDER BY o_orderpriority",
        ),
        // Q5: Local supplier volume (5-way join + INTERVAL + derived revenue)
        (
            "Q5",
            "SELECT nation.n_name, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue \
             FROM customer \
             INNER JOIN orders ON customer.c_custkey = orders.o_custkey \
             INNER JOIN lineitem ON orders.o_orderkey = lineitem.l_orderkey \
             INNER JOIN nation ON customer.c_nationkey = nation.n_nationkey \
             INNER JOIN region ON nation.n_regionkey = region.r_regionkey \
             WHERE region.r_name = 'EUROPE' AND orders.o_orderdate >= DATE '1995-01-01' AND orders.o_orderdate < DATE '1995-01-01' + INTERVAL '2' YEAR \
             GROUP BY nation.n_name ORDER BY revenue DESC",
        ),
        // Q6: Forecasting revenue change (BETWEEN + constant folding 0.06 - 0.01)
        (
            "Q6",
            "SELECT SUM(l_extendedprice * l_discount) AS revenue \
             FROM lineitem \
             WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '2' YEAR \
               AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND l_quantity < 24.0",
        ),
        // Q7: Volume shipping with EXTRACT(YEAR FROM ...) + BETWEEN
        (
            "Q7",
            "SELECT nation.n_name, EXTRACT(YEAR FROM lineitem.l_shipdate) AS l_year, lineitem.l_extendedprice * (1 - lineitem.l_discount) AS volume \
             FROM supplier \
             INNER JOIN lineitem ON supplier.s_suppkey = lineitem.l_suppkey \
             INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             WHERE lineitem.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
             ORDER BY nation.n_name, l_year",
        ),
        // Q8: National market share using CASE WHEN inside SUM(...)
        (
            "Q8",
            "SELECT nation.n_name, \
             SUM(CASE WHEN nation.n_name = 'GERMANY' THEN lineitem.l_extendedprice * (1 - lineitem.l_discount) ELSE 0.0 END) AS target_vol, \
             SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS total_vol \
             FROM lineitem \
             INNER JOIN supplier ON lineitem.l_suppkey = supplier.s_suppkey \
             INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             GROUP BY nation.n_name ORDER BY nation.n_name",
        ),
        // Q9: Product type profit measure (LIKE + 4-way join + derived profit expression)
        (
            "Q9",
            "SELECT nation.n_name, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount) - partsupp.ps_supplycost * lineitem.l_quantity) AS sum_profit \
             FROM part \
             INNER JOIN lineitem ON part.p_partkey = lineitem.l_partkey \
             INNER JOIN supplier ON lineitem.l_suppkey = supplier.s_suppkey \
             INNER JOIN partsupp ON part.p_partkey = partsupp.ps_partkey \
             INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             WHERE part.p_name LIKE '%green%' \
             GROUP BY nation.n_name ORDER BY nation.n_name",
        ),
        // Q10: Returned item reporting (4-way join + Top-20)
        (
            "Q10",
            "SELECT customer.c_custkey, customer.c_name, nation.n_name, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue \
             FROM customer \
             INNER JOIN orders ON customer.c_custkey = orders.o_custkey \
             INNER JOIN lineitem ON orders.o_orderkey = lineitem.l_orderkey \
             INNER JOIN nation ON customer.c_nationkey = nation.n_nationkey \
             WHERE lineitem.l_returnflag = 'R' \
             GROUP BY customer.c_custkey, customer.c_name, nation.n_name ORDER BY revenue DESC LIMIT 20",
        ),
        // Q11: Important stock identification (HAVING threshold + derived inventory value)
        (
            "Q11",
            "SELECT partsupp.ps_partkey, SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS value \
             FROM partsupp \
             INNER JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey \
             INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             WHERE nation.n_name = 'GERMANY' \
             GROUP BY partsupp.ps_partkey HAVING value > 10000.0 ORDER BY value DESC",
        ),
        // Q12: Shipping modes & order priority (col-vs-col date checks + CASE inside SUM!)
        (
            "Q12",
            "SELECT lineitem.l_shipmode, \
             SUM(CASE WHEN orders.o_orderpriority = '1-URGENT' OR orders.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, \
             SUM(CASE WHEN orders.o_orderpriority <> '1-URGENT' AND orders.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count \
             FROM orders \
             INNER JOIN lineitem ON orders.o_orderkey = lineitem.l_orderkey \
             WHERE lineitem.l_shipmode IN ('MAIL', 'SHIP') AND lineitem.l_commitdate < lineitem.l_receiptdate AND lineitem.l_shipdate < lineitem.l_commitdate \
             GROUP BY lineitem.l_shipmode ORDER BY lineitem.l_shipmode",
        ),
        // Q13: Customer distribution (LEFT OUTER JOIN + NOT LIKE)
        (
            "Q13",
            "SELECT customer.c_custkey, COUNT(*) AS c_count \
             FROM customer LEFT JOIN orders ON customer.c_custkey = orders.o_custkey \
             WHERE orders.o_comment NOT LIKE '%special%requests%' OR orders.o_comment IS NULL \
             GROUP BY customer.c_custkey ORDER BY c_count DESC",
        ),
        // Q14: Promotion effect (CASE WHEN LIKE inside SUM)
        (
            "Q14",
            "SELECT SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN lineitem.l_extendedprice * (1 - lineitem.l_discount) ELSE 0.0 END) AS promo_rev, \
             SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS total_rev \
             FROM lineitem INNER JOIN part ON lineitem.l_partkey = part.p_partkey \
             WHERE lineitem.l_shipdate >= DATE '1995-01-01' AND lineitem.l_shipdate < DATE '1995-01-01' + INTERVAL '1' YEAR",
        ),
        // Q15: Top supplier (CTE revenue view + ORDER BY DESC LIMIT 1)
        (
            "Q15",
            "WITH supplier_rev AS ( \
               SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue \
               FROM lineitem WHERE l_shipdate >= DATE '1995-01-01' GROUP BY l_suppkey \
             ) \
             SELECT supplier.s_suppkey, supplier.s_name, supplier_rev.total_revenue \
             FROM supplier INNER JOIN supplier_rev ON supplier.s_suppkey = supplier_rev.l_suppkey \
             ORDER BY supplier_rev.total_revenue DESC LIMIT 1",
        ),
        // Q16: Parts/supplier relationship (NOT IN subquery + COUNT(DISTINCT))
        (
            "Q16",
            "SELECT part.p_brand, part.p_type, part.p_size, COUNT(DISTINCT partsupp.ps_suppkey) AS supplier_cnt \
             FROM partsupp INNER JOIN part ON partsupp.ps_partkey = part.p_partkey \
             WHERE part.p_brand <> 'Brand#45' AND part.p_size IN (15, 25) \
               AND partsupp.ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%complaints%') \
             GROUP BY part.p_brand, part.p_type, part.p_size ORDER BY supplier_cnt DESC",
        ),
        // Q17: Small-quantity order revenue (scalar subquery threshold)
        (
            "Q17",
            "SELECT SUM(lineitem.l_extendedprice) AS small_qty_rev \
             FROM lineitem INNER JOIN part ON lineitem.l_partkey = part.p_partkey \
             WHERE part.p_brand = 'Brand#12' AND lineitem.l_quantity < (SELECT AVG(l_quantity) FROM lineitem)",
        ),
        // Q18: Large volume customer (IN subquery with GROUP BY + HAVING)
        (
            "Q18",
            "SELECT customer.c_name, orders.o_orderkey, orders.o_totalprice \
             FROM customer INNER JOIN orders ON customer.c_custkey = orders.o_custkey \
             WHERE orders.o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 30.0) \
             ORDER BY orders.o_totalprice DESC",
        ),
        // Q19: Discounted revenue (complex OR + BETWEEN + IN)
        (
            "Q19",
            "SELECT SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue \
             FROM lineitem INNER JOIN part ON lineitem.l_partkey = part.p_partkey \
             WHERE (part.p_brand = 'Brand#12' AND lineitem.l_quantity BETWEEN 1.0 AND 20.0 AND lineitem.l_shipmode IN ('AIR', 'MAIL')) \
                OR (part.p_brand = 'Brand#45' AND lineitem.l_quantity BETWEEN 30.0 AND 50.0 AND lineitem.l_shipinstruct = 'DELIVER IN PERSON')",
        ),
        // Q20: Potential part promotion (IN subquery chain)
        (
            "Q20",
            "SELECT supplier.s_name \
             FROM supplier INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             WHERE nation.n_name = 'GERMANY' \
               AND supplier.s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_availqty > 50) \
             ORDER BY supplier.s_name",
        ),
        // Q21: Suppliers who kept orders waiting (col-vs-col l_receiptdate > l_commitdate)
        (
            "Q21",
            "SELECT supplier.s_name, COUNT(*) AS numwait \
             FROM supplier \
             INNER JOIN lineitem ON supplier.s_suppkey = lineitem.l_suppkey \
             INNER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey \
             INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey \
             WHERE orders.o_orderstatus = 'F' AND lineitem.l_receiptdate > lineitem.l_commitdate \
             GROUP BY supplier.s_name ORDER BY numwait DESC",
        ),
        // Q22: Global sales opportunity (SUBSTRING + NOT IN subquery + scalar AVG subquery!)
        (
            "Q22",
            "SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal \
             FROM customer \
             WHERE c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.0) \
               AND c_custkey NOT IN (SELECT o_custkey FROM orders) \
             ORDER BY cntrycode",
        ),
    ];

    assert_eq!(queries.len(), 22);
    for (qid, sql) in &queries {
        let res = coord
            .execute_sql(sql, "main")
            .await
            .unwrap_or_else(|e| panic!("{qid} failed: {e}\nSQL: {sql}"));
        assert!(
            res.batch.num_rows() >= 1,
            "{qid} returned 0 rows unexpectedly!"
        );
        if *qid == "Q2" {
            let dag = res.dag.as_ref().unwrap();
            assert!(
                dag.cbo_notes
                    .iter()
                    .any(|n| n.contains("pushed") && n.contains("JOIN")),
                "expected CBO predicate pushdown through JOIN note in Q2: {:?}",
                dag.cbo_notes
            );
        }
        println!("{qid}: {} rows OK", res.batch.num_rows());
    }

    // Spot-check exact Q22 value: only Cust#005 (not in orders, acctbal 1500 > avg) with cntrycode '13'
    let q22 = coord.execute_sql(queries[21].1, "main").await.unwrap();
    assert_eq!(q22.batch.num_rows(), 1);
    assert_eq!(
        q22.batch.rows[0].get("cntrycode"),
        Some(&ScalarValue::Utf8("13".into()))
    );
}
