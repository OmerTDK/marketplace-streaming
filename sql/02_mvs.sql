-- marketplace-streaming: RisingWave materialized view definitions
-- Phase 0: DDL skeleton — reviewed for correctness, not executed in CI yet.
--
-- Sources must be created first (01_sources.sql).
-- All views use the sources defined there; watermark semantics are
-- inherited from the source declarations.

-- =============================================================================
-- MV 1: mv_fulfillment_sla_5min
-- 5-minute tumbling window, windowed on the ORDER's event_time.
-- Tracks per-seller, per-category SLA compliance: was the order delivered
-- (final delivery_update with status='delivered') before sla_deadline_at?
--
-- Join strategy: LEFT JOIN delivery_update_source on order_id, keeping only
-- final delivery events (is_final=TRUE, status='delivered'). Orders with no
-- matching delivery row are counted as not-yet-delivered (breach pending).
--
-- null_field faults on the order source are excluded (freight/payment nulls
-- don't affect SLA compliance; including them would skew counts).
--
-- SLA compliance definition:
--   delivered_at (delivery scanned_at) <= sla_deadline_at (order-level deadline)
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fulfillment_sla_5min AS
WITH order_events AS (
    SELECT
        order_id,
        seller_id,
        product_category,
        state_code,
        sla_deadline_at,
        event_time
    FROM order_placed_source
    WHERE is_injected_fault = FALSE OR fault_type IS DISTINCT FROM 'null_field'
),
delivery_finals AS (
    SELECT
        order_id,
        scanned_at AS delivered_at
    FROM delivery_update_source
    WHERE is_final = TRUE
      AND status = 'delivered'
)
SELECT
    window_start,
    window_end,
    o.seller_id,
    o.product_category,
    o.state_code,
    COUNT(*)                                                            AS orders_placed_count,
    COUNT(d.delivered_at)                                               AS delivered_count,
    COUNT(*) FILTER (WHERE d.delivered_at <= o.sla_deadline_at)        AS within_sla_count,
    COUNT(*) FILTER (WHERE d.delivered_at > o.sla_deadline_at)         AS breached_sla_count,
    ROUND(
        COUNT(*) FILTER (WHERE d.delivered_at <= o.sla_deadline_at)::DECIMAL
        / NULLIF(COUNT(d.delivered_at), 0) * 100, 2
    )                                                                   AS sla_compliance_pct
FROM TUMBLE(order_events, event_time, INTERVAL '5 minutes') AS o
LEFT JOIN delivery_finals AS d ON o.order_id = d.order_id
GROUP BY window_start, window_end, o.seller_id, o.product_category, o.state_code;

-- =============================================================================
-- MV 2: mv_seller_health_1hour
-- 1-hour tumbling window on seller_activity_source.
-- Tracks activity counts, average review score, and response rate per seller.
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_seller_health_1hour AS
SELECT
    window_start,
    window_end,
    seller_id,
    state_code,
    COUNT(*)                                                        AS activity_event_count,
    COUNT(*) FILTER (WHERE activity_type = 'listing_created')       AS listings_created_count,
    COUNT(*) FILTER (WHERE activity_type = 'response_sent')         AS responses_sent_count,
    COUNT(*) FILTER (WHERE activity_type = 'review_replied')        AS reviews_replied_count,
    AVG(review_score) FILTER (WHERE review_score IS NOT NULL)       AS avg_review_score,
    COUNT(*) FILTER (WHERE activity_type = 'response_sent')::DECIMAL
        / NULLIF(COUNT(*) FILTER (
            WHERE activity_type IN ('listing_created', 'listing_updated')
          ), 0)                                                      AS response_rate
FROM TUMBLE(seller_activity_source, event_time, INTERVAL '1 hour')
GROUP BY window_start, window_end, seller_id, state_code;

-- =============================================================================
-- MV 2b: mv_seller_health_alert_candidates
-- Consecutive-window degradation detection.
-- Emits one row per seller per degradation event.
-- Uses a self-join on the prior window rather than LAG() — LAG() over MVs
-- is not supported in RisingWave; the self-join achieves equivalent semantics.
--
-- Alert fires only when BOTH the current AND prior window are below threshold
-- (avg_review_score < 3.5). This is the "exactly-right-once alert" pattern:
-- the alert does not fire on every MV update, only when the condition
-- persists across two consecutive windows.
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_seller_health_alert_candidates AS
SELECT
    curr.window_end          AS alert_window_end,
    curr.seller_id,
    curr.state_code,
    curr.avg_review_score    AS current_score,
    prev.avg_review_score    AS previous_score
FROM mv_seller_health_1hour curr
JOIN mv_seller_health_1hour prev
    ON curr.seller_id = prev.seller_id
    AND prev.window_start = curr.window_start - INTERVAL '1 hour'
WHERE curr.avg_review_score < 3.5
  AND prev.avg_review_score < 3.5;

-- =============================================================================
-- MV 3: mv_late_shipment_alert
-- 15-minute slide over 1-hour HOP window.
-- Joins order_placed with shipment_created to detect orders without shipments
-- and orders with late pickups (>48 hours from order to pickup).
-- Only non-on-time rows are materialized (WHERE alert_status <> 'on_time').
-- =============================================================================
-- RisingWave requires the first argument of a window table function (HOP/TUMBLE)
-- to be a named relation — a source, CTE, or view — not an inline subquery.
-- The order projection is therefore lifted into the order_windows CTE and HOP
-- is applied to that name (mirrors the TUMBLE-over-CTE pattern in MV 1).
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_late_shipment_alert AS
WITH order_windows AS (
    SELECT
        order_id,
        seller_id,
        state_code,
        product_category,
        event_time
    FROM order_placed_source
)
SELECT
    orders.window_start,
    orders.window_end,
    orders.order_id,
    orders.seller_id,
    orders.state_code,
    orders.product_category,
    orders.event_time                                AS order_placed_at,
    shipments.actual_pickup_at                       AS pickup_at,
    shipments.shipment_id,
    CASE
        WHEN shipments.shipment_id IS NULL
            THEN 'no_shipment_created'
        WHEN shipments.actual_pickup_at > orders.event_time + INTERVAL '48 hours'
            THEN 'late_pickup'
        ELSE 'on_time'
    END                                              AS alert_status
FROM HOP(order_windows, event_time, INTERVAL '15 minutes', INTERVAL '1 hour') AS orders
LEFT JOIN shipment_created_source AS shipments
    ON orders.order_id = shipments.order_id
WHERE CASE
    WHEN shipments.shipment_id IS NULL THEN TRUE
    WHEN shipments.actual_pickup_at > orders.event_time + INTERVAL '48 hours' THEN TRUE
    ELSE FALSE
END;

-- =============================================================================
-- MV 4: mv_delivery_zone_status
-- 5-minute tumbling window on delivery_update_source, final events only.
-- Aggregates delivery outcomes by delivery zone (first 3 digits of CEP).
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_delivery_zone_status AS
WITH final_updates AS (
    SELECT
        order_id,
        shipment_id,
        delivery_zone,
        status,
        scanned_at AS event_time
    FROM delivery_update_source
    WHERE is_final = TRUE
)
SELECT
    window_start,
    window_end,
    delivery_zone,
    COUNT(*)                                                  AS deliveries_finalized_count,
    COUNT(*) FILTER (WHERE status = 'delivered')              AS delivered_count,
    COUNT(*) FILTER (WHERE status = 'failed_attempt')         AS failed_attempt_count,
    COUNT(*) FILTER (WHERE status = 'returned')               AS returned_count,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'delivered')::DECIMAL
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                         AS delivery_success_pct
FROM TUMBLE(final_updates, event_time, INTERVAL '5 minutes')
GROUP BY window_start, window_end, delivery_zone;
