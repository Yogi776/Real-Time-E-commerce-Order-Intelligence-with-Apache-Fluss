-- =============================================================================
-- 02_create_tables.sql
-- Creates all Fluss tables for the e-commerce intelligence platform.
--
-- OPTIMIZED for query performance and large-volume handling:
--   - Time-based partitioning (event_day) on high-volume tables
--   - Auto-partition with 7-day retention for hot data lifecycle
--   - Increased bucket counts for read/write parallelism
--   - Pre-materialized dashboard_kpis table for sub-second dashboard reads
--
-- Prerequisite: Run 01_create_catalog.sql first.
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

-- -----------------------------------------------------------------------------
-- A. orders_raw - Log Table (append-only, PARTITIONED by day)
--
-- Raw immutable order events. Partitioned by event_day for:
--   1. Partition pruning (dashboard queries only today's data)
--   2. Auto-retention (drop partitions older than 7 days)
--   3. Bounded scan size per partition
-- -----------------------------------------------------------------------------
CREATE TABLE orders_raw (
    event_day       STRING,
    order_id        STRING,
    customer_id     STRING,
    product_id      STRING,
    order_amount    DECIMAL(10, 2),
    quantity        INT,
    payment_status  STRING,
    order_status    STRING,
    payment_method  STRING,
    device_type     STRING,
    city            STRING,
    event_time      TIMESTAMP(3),
    ptime AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) PARTITIONED BY (event_day) WITH (
    'bucket.num' = '8',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '7'
);

-- -----------------------------------------------------------------------------
-- B. customer_profile - PK Table (updatable dimension)
-- Small table (~20-1000 rows), no partitioning needed.
-- -----------------------------------------------------------------------------
CREATE TABLE customer_profile (
    customer_id     STRING,
    customer_name   STRING,
    city            STRING,
    loyalty_tier    STRING,
    signup_date     DATE,
    updated_at      TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4'
);

-- -----------------------------------------------------------------------------
-- C. product_catalog - PK Table (updatable dimension)
-- Small table (~30-500 rows), no partitioning needed.
-- -----------------------------------------------------------------------------
CREATE TABLE product_catalog (
    product_id      STRING,
    product_name    STRING,
    category        STRING,
    brand           STRING,
    unit_price      DECIMAL(10, 2),
    inventory_count INT,
    updated_at      TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4'
);

-- -----------------------------------------------------------------------------
-- D. orders_enriched - PK Table (updatable fact, PARTITIONED by day)
--
-- Partition key must be part of PK for Fluss PK tables.
-- PK = (event_day, order_id) enables partition-pruned lookups.
-- 16 buckets per partition for high read parallelism on dashboard queries.
-- -----------------------------------------------------------------------------
CREATE TABLE orders_enriched (
    event_day       STRING,
    order_id        STRING,
    customer_id     STRING,
    customer_name   STRING,
    loyalty_tier    STRING,
    product_id      STRING,
    product_name    STRING,
    brand           STRING,
    category        STRING,
    city            STRING,
    order_amount    DECIMAL(10, 2),
    quantity        INT,
    payment_status  STRING,
    order_status    STRING,
    payment_method  STRING,
    device_type     STRING,
    event_time      TIMESTAMP(3),
    PRIMARY KEY (event_day, order_id) NOT ENFORCED
) PARTITIONED BY (event_day) WITH (
    'bucket.num' = '16',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '7'
);

-- -----------------------------------------------------------------------------
-- E. revenue_5min - PK Table (windowed aggregate, PARTITIONED by day)
--
-- Pre-aggregated revenue by category for fast dashboard reads.
-- Partitioned by window_day to prune old windows efficiently.
-- -----------------------------------------------------------------------------
CREATE TABLE revenue_5min (
    window_day      STRING,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    category        STRING,
    total_orders    BIGINT,
    total_revenue   DECIMAL(20, 2),
    failed_payments BIGINT,
    avg_order_value DECIMAL(20, 2),
    PRIMARY KEY (window_day, window_start, window_end, category) NOT ENFORCED
) PARTITIONED BY (window_day) WITH (
    'bucket.num' = '8',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '7'
);

-- -----------------------------------------------------------------------------
-- F. city_revenue_5min - PK Table (windowed aggregate, PARTITIONED by day)
-- -----------------------------------------------------------------------------
CREATE TABLE city_revenue_5min (
    window_day      STRING,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    city            STRING,
    total_orders    BIGINT,
    total_revenue   DECIMAL(20, 2),
    PRIMARY KEY (window_day, window_start, window_end, city) NOT ENFORCED
) PARTITIONED BY (window_day) WITH (
    'bucket.num' = '8',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '7'
);

-- -----------------------------------------------------------------------------
-- G. high_value_orders - PK Table (alerts, keyed by order_id)
-- Relatively small table, no partitioning needed.
-- -----------------------------------------------------------------------------
CREATE TABLE high_value_orders (
    order_id        STRING,
    customer_id     STRING,
    customer_name   STRING,
    loyalty_tier    STRING,
    order_amount    DECIMAL(10, 2),
    city            STRING,
    category        STRING,
    event_time      TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4'
);

-- -----------------------------------------------------------------------------
-- H. suspicious_orders - PK Table (alerts, keyed by order_id)
-- -----------------------------------------------------------------------------
CREATE TABLE suspicious_orders (
    order_id        STRING,
    customer_id     STRING,
    order_amount    DECIMAL(10, 2),
    payment_status  STRING,
    payment_method  STRING,
    device_type     STRING,
    city            STRING,
    reason          STRING,
    event_time      TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4'
);

-- -----------------------------------------------------------------------------
-- I. dashboard_kpis - PK Table (pre-materialized metrics for instant reads)
--
-- Tiny table (~30 rows) continuously updated by a streaming Flink job.
-- Dashboard reads this instead of scanning large fact tables.
-- Sub-second query latency guaranteed.
-- -----------------------------------------------------------------------------
CREATE TABLE dashboard_kpis (
    metric_key      STRING,
    metric_value    DOUBLE,
    updated_at      TIMESTAMP(3),
    PRIMARY KEY (metric_key) NOT ENFORCED
) WITH (
    'bucket.num' = '4'
);
