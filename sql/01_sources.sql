-- marketplace-streaming: RisingWave source definitions
-- Phase 0: DDL skeleton — reviewed for correctness, not executed in CI yet.
--
-- Watermark constants (change here, not in individual CREATE SOURCE statements):
--   STANDARD_WATERMARK_LAG    = INTERVAL '5 minutes'  (clean-stream default)
--   LATE_EVENT_WATERMARK_LAG  = INTERVAL '6 hours'    (fault-injection mode)
--
-- The watermark decision is documented in docs/adr/0002-architecture.md
-- (section "Watermark Decision"). Summary:
--   - We watermark on event_time (business timestamp), not produced_at
--     (Redpanda ingest time). Under late_arrival fault injection,
--     event_time can be 2–6 hours behind produced_at.
--   - A 5-minute lag is correct for clean-stream operation.
--   - A 6-hour lag is required to absorb the maximum late-arrival fault.
--   - The demo script (make fault-demo) switches LATE_EVENT_WATERMARK_MINUTES
--     to demonstrate the latency-vs-correctness trade-off explicitly.
--
-- Primary keys on sources: ensure idempotent state updates on duplicate-event
-- fault injection. RisingWave uses the PK for internal state dedup.

CREATE SOURCE IF NOT EXISTS order_placed_source (
    event_id             VARCHAR,
    event_type           VARCHAR,
    event_version        VARCHAR,
    produced_at          TIMESTAMPTZ,
    event_time           TIMESTAMPTZ,
    is_injected_fault    BOOLEAN,
    fault_type           VARCHAR,
    order_id             VARCHAR,
    customer_id          VARCHAR,
    seller_id            VARCHAR,
    product_category     VARCHAR,
    payment_type         VARCHAR,
    order_item_count     INT,
    freight_value_brl    DECIMAL,
    payment_value_brl    DECIMAL,
    sla_deadline_at      TIMESTAMPTZ,
    state_code           VARCHAR,
    city                 VARCHAR,
    WATERMARK FOR event_time AS event_time - INTERVAL '5 minutes'
)
WITH (
    connector = 'kafka',
    topic = 'order_placed',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;

CREATE SOURCE IF NOT EXISTS shipment_created_source (
    event_id               VARCHAR,
    event_type             VARCHAR,
    event_version          VARCHAR,
    produced_at            TIMESTAMPTZ,
    event_time             TIMESTAMPTZ,
    is_injected_fault      BOOLEAN,
    fault_type             VARCHAR,
    shipment_id            VARCHAR,
    order_id               VARCHAR,
    seller_id              VARCHAR,
    carrier_code           VARCHAR,
    estimated_delivery_at  TIMESTAMPTZ,
    actual_pickup_at       TIMESTAMPTZ,
    days_to_pickup         INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5 minutes'
)
WITH (
    connector = 'kafka',
    topic = 'shipment_created',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;

CREATE SOURCE IF NOT EXISTS delivery_update_source (
    event_id           VARCHAR,
    event_type         VARCHAR,
    event_version      VARCHAR,
    produced_at        TIMESTAMPTZ,
    -- Watermark field: scanned_at (carrier event-time), NOT produced_at.
    -- Under late_arrival fault, scanned_at can be 2–6 hours behind produced_at.
    -- See ADR-0002 "Watermark Decision" for the full trade-off explanation.
    event_time         TIMESTAMPTZ,
    scanned_at         TIMESTAMPTZ,
    is_injected_fault  BOOLEAN,
    fault_type         VARCHAR,
    update_id          VARCHAR,
    shipment_id        VARCHAR,
    order_id           VARCHAR,
    status             VARCHAR,
    location_state     VARCHAR,
    delivery_zone      VARCHAR,
    sequence_number    INT,
    is_final           BOOLEAN,
    WATERMARK FOR scanned_at AS scanned_at - INTERVAL '5 minutes'
)
WITH (
    connector = 'kafka',
    topic = 'delivery_update',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;

CREATE SOURCE IF NOT EXISTS seller_activity_source (
    event_id           VARCHAR,
    event_type         VARCHAR,
    event_version      VARCHAR,
    produced_at        TIMESTAMPTZ,
    event_time         TIMESTAMPTZ,
    is_injected_fault  BOOLEAN,
    fault_type         VARCHAR,
    activity_id        VARCHAR,
    seller_id          VARCHAR,
    activity_type      VARCHAR,
    review_score       FLOAT,
    product_category   VARCHAR,
    state_code         VARCHAR,
    -- 5-minute lag: lower-volume topic, slightly more batching expected.
    WATERMARK FOR event_time AS event_time - INTERVAL '5 minutes'
)
WITH (
    connector = 'kafka',
    topic = 'seller_activity',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;
