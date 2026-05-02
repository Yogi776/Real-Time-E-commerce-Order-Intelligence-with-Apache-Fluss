-- =============================================================================
-- 10_dashboard_kpis.sql
-- Pre-materialized KPI metrics for sub-second dashboard reads.
--
-- This streaming job continuously updates ~30 metric rows in dashboard_kpis.
-- The dashboard reads this tiny PK table instead of scanning large fact tables.
--
-- IMPORTANT: This is a long-running streaming job. Requires:
--   - 04_generate_orders.sql running (orders streaming in)
--   - 05_enrich_orders.sql running (enriched orders streaming)
--
-- Metrics are computed via 1-minute tumbling windows on orders_raw,
-- giving near-real-time KPI freshness with bounded state.
-- =============================================================================

USE CATALOG fluss_catalog;
USE ecommerce;

SET 'execution.runtime-mode' = 'streaming';

-- ---------------------------------------------------------------------------
-- KPI: Overall order volume and revenue metrics (updated every 1 minute)
--
-- Computes rolling metrics from the latest 1-minute window and upserts
-- into dashboard_kpis. Each metric_key is a stable string identifier.
-- ---------------------------------------------------------------------------
INSERT INTO dashboard_kpis
SELECT
    metric_key,
    metric_value,
    window_end AS updated_at
FROM (
    SELECT
        window_end,
        'total_orders_1min' AS metric_key,
        CAST(COUNT(*) AS DOUBLE) AS metric_value
    FROM TABLE(
        TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
    )
    GROUP BY window_start, window_end

    UNION ALL

    SELECT
        window_end,
        'total_revenue_1min' AS metric_key,
        CAST(SUM(order_amount) AS DOUBLE) AS metric_value
    FROM TABLE(
        TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
    )
    GROUP BY window_start, window_end

    UNION ALL

    SELECT
        window_end,
        'failed_payments_1min' AS metric_key,
        CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS DOUBLE) AS metric_value
    FROM TABLE(
        TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
    )
    GROUP BY window_start, window_end

    UNION ALL

    SELECT
        window_end,
        'avg_order_value_1min' AS metric_key,
        CAST(SUM(order_amount) / COUNT(*) AS DOUBLE) AS metric_value
    FROM TABLE(
        TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
    )
    GROUP BY window_start, window_end

    UNION ALL

    SELECT
        window_end,
        'events_per_second' AS metric_key,
        CAST(COUNT(*) / 60.0 AS DOUBLE) AS metric_value
    FROM TABLE(
        TUMBLE(TABLE orders_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
    )
    GROUP BY window_start, window_end
);
