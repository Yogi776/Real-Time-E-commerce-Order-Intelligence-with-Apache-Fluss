-- =============================================================================
-- 11_production_load_test.sql
-- Production-scale synthetic order generator: 5,000 events/sec
--
-- This simulates ~432M orders/day production throughput.
-- REQUIREMENTS:
--   - 3+ TaskManagers (30 slots)
--   - 16GB+ Docker memory allocation
--   - Use docker-compose.prod-test.yml override
--
-- Monitor: Flink Web UI at localhost:8084
--   - Backpressure should stay < 80%
--   - Checkpoint duration should stay < 30s
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60s';
SET 'pipeline.object-reuse' = 'true';

-- ---------------------------------------------------------------------------
-- Production-rate faker source: 5,000 events/sec
-- Same realistic weighted distributions as the demo generator.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY TABLE fake_orders_production (
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
    'rows-per-second' = '5000',

    'fields.order_id.expression' = '#{Internet.uuid}',

    'fields.customer_id.expression' = '#{Options.option ''CUS-bc3f5734'',''CUS-244579ca'',''CUS-ae9a6504'',''CUS-7f150bce'',''CUS-8e8e0349'',''CUS-6ece6507'',''CUS-96aac47f'',''CUS-b3d3f099'',''CUS-27fe0d2e'',''CUS-6bf9b9ec'',''CUS-2ea26c14'',''CUS-168bc0ab'',''CUS-3fc6fc46'',''CUS-b17e412c'',''CUS-2af65034'',''CUS-4412e697'',''CUS-37aadd9e'',''CUS-1cc815bd'',''CUS-db0f99df'',''CUS-9af32bee''}',

    'fields.product_id.expression' = '#{Options.option ''SKU-ELEC-1001'',''SKU-ELEC-1002'',''SKU-ELEC-1003'',''SKU-ELEC-1004'',''SKU-ELEC-1005'',''SKU-ELEC-1006'',''SKU-FASH-2001'',''SKU-FASH-2002'',''SKU-FASH-2003'',''SKU-FASH-2004'',''SKU-FASH-2005'',''SKU-FASH-2006'',''SKU-GROC-3001'',''SKU-GROC-3002'',''SKU-GROC-3003'',''SKU-GROC-3004'',''SKU-GROC-3005'',''SKU-BOOK-4001'',''SKU-BOOK-4002'',''SKU-BOOK-4003'',''SKU-BOOK-4004'',''SKU-BOOK-4005'',''SKU-BEAU-5001'',''SKU-BEAU-5002'',''SKU-BEAU-5003'',''SKU-BEAU-5004'',''SKU-SPRT-6001'',''SKU-SPRT-6002'',''SKU-SPRT-6003'',''SKU-SPRT-6004''}',

    'fields.order_amount_str.expression' = '#{Options.option ''275'',''299'',''349'',''399'',''549'',''899'',''1175'',''1299'',''1499'',''2999'',''3499'',''4499'',''5999'',''8999'',''8999'',''24999'',''54999'',''69999'',''79999'',''114999''}',

    'fields.quantity.expression' = '#{number.numberBetween ''1'',''3''}',

    'fields.payment_status.expression' = '#{Options.option ''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''SUCCESS'',''FAILED'',''FAILED'',''PENDING''}',

    'fields.order_status.expression' = '#{Options.option ''PLACED'',''PLACED'',''PLACED'',''PLACED'',''PLACED'',''SHIPPED'',''SHIPPED'',''DELIVERED'',''DELIVERED'',''CANCELLED''}',

    'fields.payment_method.expression' = '#{Options.option ''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''UPI'',''CARD'',''CARD'',''CARD'',''CARD'',''WALLET'',''WALLET'',''WALLET'',''COD'',''COD'',''COD'',''NETBANKING'',''NETBANKING''}',

    'fields.device_type.expression' = '#{Options.option ''ANDROID'',''ANDROID'',''ANDROID'',''ANDROID'',''ANDROID'',''ANDROID'',''WEB'',''WEB'',''IOS'',''IOS''}',

    'fields.city.expression' = '#{Options.option ''Mumbai'',''Mumbai'',''Mumbai'',''Mumbai'',''Delhi'',''Delhi'',''Delhi'',''Delhi'',''Bengaluru'',''Bengaluru'',''Bengaluru'',''Pune'',''Pune'',''Hyderabad'',''Hyderabad'',''Chennai'',''Chennai'',''Ahmedabad'',''Ahmedabad'',''Kolkata''}',

    'fields.event_time.expression' = '#{date.past ''15'',''SECONDS''}'
);

-- ---------------------------------------------------------------------------
-- Stream production-rate orders into Fluss.
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
FROM fake_orders_production;
