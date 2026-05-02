-- =============================================================================
-- 04_generate_orders.sql
-- Continuous synthetic order event generation using flink-faker.
-- Prerequisite: Run 01-03 first to set up catalog, tables, and seed data.
--
-- IMPORTANT: This starts a long-running streaming job. It will run
-- continuously until cancelled. Monitor via Flink Web UI at localhost:8083.
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

-- Switch to streaming mode for continuous generation
SET 'execution.runtime-mode' = 'streaming';

-- ---------------------------------------------------------------------------
-- Faker source table with realistic weighted distributions.
-- order_amount_str is STRING because Options.option returns STRING;
-- it gets CAST to DECIMAL in the INSERT SELECT below.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY TABLE fake_orders (
    order_id          STRING,
    customer_id       STRING,
    product_id        STRING,
    order_amount_str  STRING,
    quantity          INT,
    payment_status    STRING,
    order_status      STRING,
    payment_method    STRING,
    device_type       STRING,
    city              STRING,
    event_time        TIMESTAMP(3)
) WITH (
    'connector' = 'faker',

    -- Default: 50 rows/sec (~3K orders/min, ~180K orders/hour)
    -- For load testing: change to 500, 1000, or 5000
    'rows-per-second' = '50',

    -- UUID v4 order IDs (globally unique, industry standard)
    'fields.order_id.expression' = '#{Internet.uuid}',

    -- All 20 customer IDs matching seed data exactly
    'fields.customer_id.expression' = '#{Options.option ''CUS-bc3f5734'',''CUS-244579ca'',''CUS-ae9a6504'',''CUS-7f150bce'',''CUS-8e8e0349'',''CUS-6ece6507'',''CUS-96aac47f'',''CUS-b3d3f099'',''CUS-27fe0d2e'',''CUS-6bf9b9ec'',''CUS-2ea26c14'',''CUS-168bc0ab'',''CUS-3fc6fc46'',''CUS-b17e412c'',''CUS-2af65034'',''CUS-4412e697'',''CUS-37aadd9e'',''CUS-1cc815bd'',''CUS-db0f99df'',''CUS-9af32bee''}',

    -- All 30 product SKU IDs matching seed data exactly
    'fields.product_id.expression' = '#{Options.option ''SKU-ELEC-1001'',''SKU-ELEC-1002'',''SKU-ELEC-1003'',''SKU-ELEC-1004'',''SKU-ELEC-1005'',''SKU-ELEC-1006'',''SKU-FASH-2001'',''SKU-FASH-2002'',''SKU-FASH-2003'',''SKU-FASH-2004'',''SKU-FASH-2005'',''SKU-FASH-2006'',''SKU-GROC-3001'',''SKU-GROC-3002'',''SKU-GROC-3003'',''SKU-GROC-3004'',''SKU-GROC-3005'',''SKU-BOOK-4001'',''SKU-BOOK-4002'',''SKU-BOOK-4003'',''SKU-BOOK-4004'',''SKU-BOOK-4005'',''SKU-BEAU-5001'',''SKU-BEAU-5002'',''SKU-BEAU-5003'',''SKU-BEAU-5004'',''SKU-SPRT-6001'',''SKU-SPRT-6002'',''SKU-SPRT-6003'',''SKU-SPRT-6004''}',

    -- Weighted multi-tier pricing (INR) for realistic distribution:
    --   25% grocery/books range (275-549)
    --   25% mid-range fashion/beauty (899-2499)
    --   25% premium fashion/sports (2999-7999)
    --   25% electronics/high-value (8999-79999)
    'fields.order_amount_str.expression' = '#{Options.option ''275'',''299'',''349'',''399'',''549'',''899'',''1175'',''1299'',''1499'',''2999'',''3499'',''4499'',''5999'',''8999'',''8999'',''24999'',''54999'',''69999'',''79999'',''114999''}',

    -- 1-3 items per order (realistic e-commerce)
    'fields.quantity.expression' = '#{number.numberBetween ''1'',''3''}',

    -- ~75% SUCCESS, ~17% FAILED, ~8% PENDING
    'fields.payment_status.expression' = '#{Options.option ''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''FAILED'',''FAILED'',''PENDING''}',

    -- ~50% PLACED, ~20% SHIPPED, ~20% DELIVERED, ~10% CANCELLED
    'fields.order_status.expression' = '#{Options.option ''PLACED'',''PLACED'',''PLACED'',''PLACED'',''PLACED'',''SHIPPED'',''SHIPPED'',''DELIVERED'',''DELIVERED'',''CANCELLED''}',

    -- Indian market: ~40% UPI, ~20% CARD, ~15% WALLET, ~15% COD, ~10% NETBANKING
    'fields.payment_method.expression' = '#{Options.option ''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''CARD'',''CARD'',''CARD'',''CARD'',''WALLET'',''WALLET'',''WALLET'',''COD'',''COD'',''COD'',''NETBANKING'',''NETBANKING''}',

    -- India: ~60% ANDROID, ~20% WEB, ~20% IOS
    'fields.device_type.expression' = '#{Options.option ''ANDROID'',''ANDROID'',''ANDROID'',''ANDROID'',''ANDROID'',''ANDROID'',''WEB'',''WEB'',''IOS'',''IOS''}',

    -- City distribution weighted by e-commerce penetration
    'fields.city.expression' = '#{Options.option ''Mumbai'',''Mumbai'',''Mumbai'',''Mumbai'',''Delhi'',''Delhi'',''Delhi'',''Delhi'',''Bengaluru'',''Bengaluru'',''Bengaluru'',''Pune'',''Pune'',''Hyderabad'',''Hyderabad'',''Chennai'',''Chennai'',''Ahmedabad'',''Ahmedabad'',''Kolkata''}',

    -- Events within last 15 seconds for fresh watermarks
    'fields.event_time.expression' = '#{date.past ''15'',''SECONDS''}'
);

-- ---------------------------------------------------------------------------
-- Stream fake orders into Fluss orders_raw table.
-- CAST order_amount_str from STRING to DECIMAL.
-- This job runs continuously until cancelled.
-- ---------------------------------------------------------------------------
INSERT INTO orders_raw
SELECT
    DATE_FORMAT(event_time, 'yyyyMMdd') AS event_day,
    order_id,
    customer_id,
    product_id,
    CAST(order_amount_str AS DECIMAL(10, 2)),
    quantity,
    payment_status,
    order_status,
    payment_method,
    device_type,
    city,
    event_time
FROM fake_orders;
