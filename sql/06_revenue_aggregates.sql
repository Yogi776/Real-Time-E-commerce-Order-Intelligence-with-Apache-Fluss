-- =============================================================================
-- 06_revenue_aggregates.sql
-- Real-time windowed aggregation pipelines.
-- Produces 5-minute tumbling window revenue metrics by category and city.
--
-- IMPORTANT: These are long-running streaming jobs. Requires:
--   - 03_seed_data.sql executed (dimension tables populated)
--   - 04_generate_orders.sql running (orders streaming in)
--
-- NOTE: Aggregates directly from orders_raw (which has watermarks) with
-- inline dimension lookups. Includes window_day partition key for pruning.
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

SET 'execution.runtime-mode' = 'streaming';

-- ---------------------------------------------------------------------------
-- Revenue by category every 5 minutes
-- Includes window_day for partition-pruned dashboard reads.
-- ---------------------------------------------------------------------------
INSERT INTO revenue_5min
SELECT
    DATE_FORMAT(window_start, 'yyyyMMdd')                      AS window_day,
    window_start,
    window_end,
    p.category,
    COUNT(*)                                                   AS total_orders,
    SUM(o.order_amount)                                        AS total_revenue,
    SUM(CASE WHEN o.payment_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_payments,
    CAST(SUM(o.order_amount) / COUNT(*) AS DECIMAL(20, 2))     AS avg_order_value
FROM TABLE(
    TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
) AS o
LEFT JOIN product_catalog FOR SYSTEM_TIME AS OF o.ptime AS p
    ON o.product_id = p.product_id
GROUP BY DATE_FORMAT(window_start, 'yyyyMMdd'), window_start, window_end, p.category;

-- ---------------------------------------------------------------------------
-- Revenue by city every 5 minutes
-- ---------------------------------------------------------------------------
INSERT INTO city_revenue_5min
SELECT
    DATE_FORMAT(window_start, 'yyyyMMdd') AS window_day,
    window_start,
    window_end,
    city,
    COUNT(*)          AS total_orders,
    SUM(order_amount) AS total_revenue
FROM TABLE(
    TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY DATE_FORMAT(window_start, 'yyyyMMdd'), window_start, window_end, city;
