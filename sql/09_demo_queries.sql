-- =============================================================================
-- 09_demo_queries.sql
-- Interactive analytics queries for the e-commerce intelligence platform.
-- Run these in the Flink SQL client after the streaming jobs are active.
--
-- Switch to batch mode for point-in-time analytics:
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.dml-sync' = 'true';

-- =============================================================================
-- A. Latest Orders (most recent enriched orders)
-- =============================================================================
SELECT
    order_id,
    customer_name,
    product_name,
    category,
    order_amount,
    payment_status,
    city,
    event_time
FROM orders_enriched
LIMIT 20;

-- =============================================================================
-- B. Failed Payments (all failed payment orders)
-- =============================================================================
SELECT
    order_id,
    customer_name,
    order_amount,
    payment_method,
    device_type,
    city,
    event_time
FROM orders_enriched
WHERE payment_status = 'FAILED'
LIMIT 20;

-- =============================================================================
-- C. High-Value Orders
-- =============================================================================
SELECT
    order_id,
    customer_name,
    loyalty_tier,
    order_amount,
    category,
    city,
    event_time
FROM high_value_orders
LIMIT 20;

-- =============================================================================
-- D. Suspicious Orders (fraud-like patterns)
-- =============================================================================
SELECT
    order_id,
    customer_id,
    order_amount,
    payment_status,
    payment_method,
    device_type,
    reason,
    event_time
FROM suspicious_orders
LIMIT 20;

-- =============================================================================
-- E. Revenue by Category (latest 5-minute windows)
-- =============================================================================
SELECT
    window_start,
    window_end,
    category,
    total_orders,
    total_revenue,
    failed_payments,
    avg_order_value
FROM revenue_5min
ORDER BY window_end DESC, total_revenue DESC
LIMIT 30;

-- =============================================================================
-- F. Revenue by City (latest 5-minute windows)
-- =============================================================================
SELECT
    window_start,
    window_end,
    city,
    total_orders,
    total_revenue
FROM city_revenue_5min
ORDER BY window_end DESC, total_revenue DESC
LIMIT 30;

-- =============================================================================
-- G. Revenue by Loyalty Tier
-- =============================================================================
SELECT
    loyalty_tier,
    COUNT(*)          AS total_orders,
    SUM(order_amount) AS total_revenue,
    CAST(SUM(order_amount) / COUNT(*) AS DECIMAL(10, 2)) AS avg_order_value
FROM orders_enriched
GROUP BY loyalty_tier
ORDER BY total_revenue DESC;

-- =============================================================================
-- H. Top Products by Revenue
-- =============================================================================
SELECT
    product_name,
    brand,
    category,
    COUNT(*)          AS units_sold,
    SUM(order_amount) AS total_revenue
FROM orders_enriched
GROUP BY product_name, brand, category
ORDER BY total_revenue DESC
LIMIT 15;

-- =============================================================================
-- I. Failed Payment Rate by Payment Method
-- =============================================================================
SELECT
    payment_method,
    COUNT(*)                                                       AS total_orders,
    SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END)    AS failed_count,
    CAST(
        SUM(CASE WHEN payment_status = 'FAILED' THEN 1.0 ELSE 0.0 END)
        / COUNT(*) * 100
        AS DECIMAL(5, 2)
    )                                                              AS failure_rate_pct
FROM orders_enriched
GROUP BY payment_method
ORDER BY failure_rate_pct DESC;

-- =============================================================================
-- J. Device-wise Revenue
-- =============================================================================
SELECT
    device_type,
    COUNT(*)          AS total_orders,
    SUM(order_amount) AS total_revenue,
    CAST(SUM(order_amount) / COUNT(*) AS DECIMAL(10, 2)) AS avg_order_value
FROM orders_enriched
GROUP BY device_type
ORDER BY total_revenue DESC;

-- =============================================================================
-- K. Inventory Risk Products (low stock items)
-- =============================================================================
SELECT
    product_id,
    product_name,
    category,
    brand,
    inventory_count
FROM product_catalog
WHERE inventory_count < 100
ORDER BY inventory_count ASC;

-- =============================================================================
-- L. Customer Order History (orders per customer)
-- =============================================================================
SELECT
    customer_id,
    customer_name,
    loyalty_tier,
    city,
    COUNT(*)          AS total_orders,
    SUM(order_amount) AS total_spent,
    CAST(SUM(order_amount) / COUNT(*) AS DECIMAL(10, 2)) AS avg_order_value
FROM orders_enriched
GROUP BY customer_id, customer_name, loyalty_tier, city
ORDER BY total_spent DESC
LIMIT 20;
