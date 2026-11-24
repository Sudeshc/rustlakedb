-- RustLakeDB v1.0.0 Live 500,000-Row Distributed Multi-Container Benchmark
\set ON_ERROR_STOP on
\timing on

-- 1. EXPLAIN distributed DAG over 20 Parquet splits (500K rows) x 4 splits (20K rows)
EXPLAIN SELECT c.tier, COUNT(*) AS orders, SUM(o.amount) AS revenue
FROM public.big_orders o JOIN public.big_customers c ON o.customer_id = c.customer_id
GROUP BY c.tier ORDER BY c.tier;

-- 2. Ultra-Selective JSONB `@>` Needle-in-a-Haystack across 500,000 rows (19/20 Parquet files pruned via Puffin!)
SELECT order_id, region, amount FROM public.big_orders WHERE payload @> '{"tag": "needle_whale_999"}';

-- 3. Full 500,000-Row Distributed GROUP BY + SUM/AVG/MIN/MAX across 3 Worker Containers
SELECT region, COUNT(*) AS orders, SUM(amount) AS total_rev, AVG(amount) AS avg_amt, MIN(amount) AS min_amt, MAX(amount) AS max_amt
FROM public.big_orders
GROUP BY region ORDER BY region;

-- 4. Full 500,000-Row JSON Path GROUP BY (`payload->>'channel'`)
SELECT payload->>'channel' AS channel, COUNT(*) AS cnt, SUM(amount) AS rev
FROM public.big_orders
GROUP BY payload->>'channel' ORDER BY channel;

-- 5. Massive 500,000 x 20,000 Distributed JOIN + GROUP BY across 3 Containers (cross-container HTTP Zstd shuffle!)
SELECT c.tier, COUNT(*) AS orders, SUM(o.amount) AS revenue
FROM public.big_orders o JOIN public.big_customers c ON o.customer_id = c.customer_id
GROUP BY c.tier ORDER BY c.tier;

-- 6. Filtered Distributed JOIN + HAVING + ORDER BY + LIMIT/OFFSET
SELECT c.segment, COUNT(*) AS orders, SUM(o.amount) AS revenue
FROM public.big_orders o JOIN public.big_customers c ON o.customer_id = c.customer_id
WHERE o.region IN ('us-east', 'eu-central') AND o.amount >= 250.0
GROUP BY c.segment HAVING orders > 10000 ORDER BY revenue DESC LIMIT 3 OFFSET 0;
