-- Aggregated regional revenue fact table in Apache Iceberg v3
{{ config(materialized='table') }}

SELECT
    region,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM {{ ref('stg_orders') }}
GROUP BY region
ORDER BY total_revenue DESC
