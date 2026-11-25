-- Staging view over RustLakeDB Iceberg v3 table
{{ config(materialized='view') }}

SELECT
    order_id,
    region,
    amount
FROM public.big_orders
WHERE amount > 0
