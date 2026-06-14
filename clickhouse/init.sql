-- marketplace-streaming: ClickHouse sink table definitions
-- Phase 0: DDL skeleton — reviewed for correctness, not executed in CI yet.
--
-- IMPORTANT: All queries against ReplacingMergeTree tables in this schema
-- MUST use FINAL. ReplacingMergeTree deduplication is lazy — it runs at
-- background merge time, not at write time. Without FINAL, a query may
-- return duplicate rows for the same (window_start, seller_id, ...) key.
--
-- Example:
--   SELECT * FROM fulfillment_sla FINAL WHERE window_start > now() - INTERVAL 1 HOUR
--
-- A CI linter enforces this: any SELECT against these tables without FINAL
-- in tests/ or scripts/ will fail the lint check.
--
-- Deduplication version column: window_end (Unix milliseconds as Int64).
-- Higher window_end wins on merge (in practice all re-inserts have the same
-- window_end, so this is a no-op dedup key — idempotent writes).

CREATE TABLE IF NOT EXISTS fulfillment_sla
(
    window_start          DateTime,
    window_end            DateTime,
    seller_id             String,
    product_category      String,
    state_code            String,
    orders_placed_count   UInt64,
    within_sla_count      UInt64,
    breached_sla_count    UInt64,
    sla_compliance_pct    Float64
)
ENGINE = ReplacingMergeTree(window_end)
ORDER BY (window_start, seller_id, product_category, state_code);

CREATE TABLE IF NOT EXISTS seller_health
(
    window_start            DateTime,
    window_end              DateTime,
    seller_id               String,
    state_code              String,
    activity_event_count    UInt64,
    listings_created_count  UInt64,
    responses_sent_count    UInt64,
    reviews_replied_count   UInt64,
    avg_review_score        Nullable(Float64),
    response_rate           Nullable(Float64)
)
ENGINE = ReplacingMergeTree(window_end)
ORDER BY (window_start, seller_id, state_code);

CREATE TABLE IF NOT EXISTS late_shipment_alert
(
    window_start      DateTime,
    window_end        DateTime,
    order_id          String,
    seller_id         String,
    state_code        String,
    product_category  String,
    order_placed_at   DateTime,
    pickup_at         Nullable(DateTime),
    shipment_id       Nullable(String),
    alert_status      String
)
ENGINE = ReplacingMergeTree(window_end)
ORDER BY (window_start, order_id);

CREATE TABLE IF NOT EXISTS delivery_zone_status
(
    window_start                DateTime,
    window_end                  DateTime,
    delivery_zone               String,
    deliveries_finalized_count  UInt64,
    delivered_count             UInt64,
    failed_attempt_count        UInt64,
    returned_count              UInt64,
    delivery_success_pct        Float64
)
ENGINE = ReplacingMergeTree(window_end)
ORDER BY (window_start, delivery_zone);

-- Batch recompute table for stream-vs-batch reconciliation.
-- Identical schema to fulfillment_sla — populated by Dagster batch_recompute_asset.
CREATE TABLE IF NOT EXISTS batch_recompute_fulfillment_sla
(
    window_start          DateTime,
    window_end            DateTime,
    seller_id             String,
    product_category      String,
    state_code            String,
    orders_placed_count   UInt64,
    within_sla_count      UInt64,
    breached_sla_count    UInt64,
    sla_compliance_pct    Float64
)
ENGINE = ReplacingMergeTree(window_end)
ORDER BY (window_start, seller_id, product_category, state_code);

-- Reconciliation audit trail.
-- Populated by Dagster reconciliation_sensor.
-- status values: 'diverged' | 'converged' | 'within_tolerance'
CREATE TABLE IF NOT EXISTS reconciliation_audit
(
    checked_at       DateTime,
    window_start     DateTime,
    window_end       DateTime,
    seller_id        String,
    streaming_value  UInt64,
    batch_value      UInt64,
    abs_delta        UInt64,
    late_event_ids   Array(String),
    status           String
)
ENGINE = MergeTree()
ORDER BY (checked_at, window_start, seller_id);
