-- =============================================================================
-- 05_enrich_orders.sql
-- Enrichment pipeline: joins raw orders with customer and product dimensions.
-- Uses Fluss temporal lookup joins for high-QPS dimension lookups.
--
-- IMPORTANT: This starts a long-running streaming job. Requires:
--   - 03_seed_data.sql executed (dimension tables populated)
--   - 04_generate_orders.sql running (orders streaming in)
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

SET 'execution.runtime-mode' = 'streaming';

-- ---------------------------------------------------------------------------
-- Temporal lookup join:
--   orders_raw.ptime (PROCTIME) drives the lookup against PK tables.
--   customer_profile provides: customer_name, loyalty_tier
--   product_catalog provides: product_name, brand, category
--
-- This is how real systems work: the order event carries only foreign keys
-- (customer_id, product_id). Denormalized attributes are resolved at
-- enrichment time, not at event emission.
-- ---------------------------------------------------------------------------
INSERT INTO orders_enriched
SELECT
    o.event_day,
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.loyalty_tier,
    o.product_id,
    p.product_name,
    p.brand,
    p.category,
    o.city,
    o.order_amount,
    o.quantity,
    o.payment_status,
    o.order_status,
    o.payment_method,
    o.device_type,
    o.event_time
FROM orders_raw AS o
LEFT JOIN customer_profile FOR SYSTEM_TIME AS OF o.ptime AS c
    ON o.customer_id = c.customer_id
LEFT JOIN product_catalog FOR SYSTEM_TIME AS OF o.ptime AS p
    ON o.product_id = p.product_id;
