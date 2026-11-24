-- RustLakeDB v1.0.0 Live Multi-Container PostgreSQL 16 (`psql`) Test Bench Workload
\set ON_ERROR_STOP on

SELECT version();
SELECT current_database();
SELECT current_schema();

-- 1. Create Iceberg v3 tables with JSONB/VARIANT + Puffin Inverted Index
CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, region TEXT, amount DOUBLE, payload JSONB);
CREATE TABLE customers (customer_id BIGINT, tier TEXT);
CREATE INDEX idx_orders_payload ON orders USING INVERTED (payload);

-- 2. Ingest multiple Parquet + Puffin splits
INSERT INTO orders VALUES
  (1, 10, 'us', 100.0, '{"event_type": "purchase", "channel": "web", "trace_id": "tr-1"}'),
  (2, 20, 'eu', 200.0, '{"event_type": "refund", "channel": "app"}'),
  (3, 10, 'us', 999.0, '{"event_type": "purchase", "channel": "fraud"}');

INSERT INTO orders VALUES
  (4, 10, 'us', 150.0, '{"event_type": "purchase", "channel": "app"}'),
  (5, 30, 'apac', 300.0, '{"event_type": "purchase", "channel": "web"}');

INSERT INTO orders VALUES
  (6, 20, 'eu', 50.0, '{"event_type": "view", "channel": "web"}');

INSERT INTO customers VALUES
  (10, 'enterprise'),
  (20, 'pro'),
  (30, 'enterprise'),
  (40, 'starter');

-- 3. EXPLAIN distributed Stage DAG + CBO decisions
EXPLAIN SELECT tier, SUM(amount) AS revenue FROM orders JOIN customers ON orders.customer_id = customers.customer_id GROUP BY tier ORDER BY tier;

-- 4. Multi-container distributed INNER JOIN + GROUP BY + ORDER BY (cross-container HTTP Zstd shuffle!)
SELECT c.* FROM public.customers AS c ORDER BY c.customer_id LIMIT 2 OFFSET 1;
SELECT tier, COUNT(*) AS cnt, SUM(amount) AS revenue
FROM public.orders o JOIN public.customers c ON o.customer_id = c.customer_id
GROUP BY tier ORDER BY tier;

-- 5. Distributed LEFT JOIN + JSONB `@>`, `?`, `->>` Puffin-indexed queries
SELECT order_id, region FROM orders WHERE payload @> '{"event_type": "purchase", "channel": "web"}' ORDER BY order_id;
SELECT order_id FROM orders WHERE payload ? 'trace_id';
SELECT payload->>'channel' AS channel, COUNT(*) AS cnt FROM orders GROUP BY payload->>'channel' ORDER BY channel;

-- 6. SQL CTE (`WITH ... AS (...)`)
WITH high_val AS (SELECT order_id, customer_id, amount FROM orders WHERE amount >= 150.0)
SELECT customer_id, SUM(amount) AS big_spend FROM high_val GROUP BY customer_id ORDER BY customer_id;

-- 7. Iceberg v3 UPDATE + DELETE (Roaring Bitmap Deletion Vectors) + Schema Evolution
DELETE FROM orders WHERE order_id = 3;
UPDATE orders SET amount = 125.0 WHERE order_id = 1;
ALTER TABLE orders ADD COLUMN currency TEXT;
INSERT INTO orders VALUES (7, 40, 'latam', 75.0, '{"event_type": "purchase", "channel": "web"}', 'USD');

-- 8. Verify post-DML & post-evolution distributed query
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders WHERE payload->>'event_type' = 'purchase'
GROUP BY region ORDER BY region;

-- 9. Git-style Branching + Compaction + VACUUM + Metadata Introspection
CREATE BRANCH qa_branch FROM main ON orders;
MERGE BRANCH qa_branch INTO main ON orders;
COMPACT TABLE orders;
VACUUM TABLE orders RETAIN 1;
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;
