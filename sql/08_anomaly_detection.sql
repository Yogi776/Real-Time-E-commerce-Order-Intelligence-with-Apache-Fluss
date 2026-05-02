-- =============================================================================
-- 08_anomaly_detection.sql
-- Rule-based anomaly detection pipelines for high-value and suspicious orders.
-- Thresholds are calibrated to the realistic order_amount distribution.
--
-- IMPORTANT: These are long-running streaming jobs. Requires:
--   - 05_enrich_orders.sql running (enriched orders streaming)
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

SET 'execution.runtime-mode' = 'streaming';

-- ---------------------------------------------------------------------------
-- HIGH-VALUE ORDER DETECTION
-- Threshold: >= 12999 INR (~15% of generated orders)
-- Catches: electronics, premium fashion, multi-item high-value carts
-- ---------------------------------------------------------------------------
INSERT INTO high_value_orders
SELECT
    order_id,
    customer_id,
    customer_name,
    loyalty_tier,
    order_amount,
    city,
    category,
    event_time
FROM orders_enriched
WHERE order_amount >= 12999;

-- ---------------------------------------------------------------------------
-- SUSPICIOUS ORDER DETECTION
-- Multi-rule fraud signal detection with reason classification.
-- Each rule targets a real fraud pattern in Indian e-commerce.
-- ---------------------------------------------------------------------------
INSERT INTO suspicious_orders
SELECT
    order_id,
    customer_id,
    order_amount,
    payment_status,
    payment_method,
    device_type,
    city,
    CASE
        WHEN order_amount >= 25000
            THEN 'Very high order amount exceeds 25K INR'
        WHEN payment_status = 'FAILED' AND order_amount >= 5000
            THEN 'Failed payment on high-value order'
        WHEN payment_method = 'COD' AND order_amount >= 8999
            THEN 'High-value COD order risk'
        WHEN payment_status = 'FAILED' AND device_type = 'WEB' AND order_amount >= 2999
            THEN 'Suspicious failed web payment'
    END AS reason,
    event_time
FROM orders_enriched
WHERE order_amount >= 25000
   OR (payment_status = 'FAILED' AND order_amount >= 5000)
   OR (payment_method = 'COD' AND order_amount >= 8999)
   OR (payment_status = 'FAILED' AND device_type = 'WEB' AND order_amount >= 2999);
