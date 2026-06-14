# ADR-0002: System Architecture — docker-compose Topology, Event Domain Model, Generator Design

**Date:** 2026-06-14
**Status:** Accepted

## Context

This document records the concrete architecture decisions for the full stack:
service topology, event schema, generator behavior, and fault injection design.
These decisions are made in Phase 0 before any service is stood up, so that
Phase 1 implementation has a stable target.

## Decision

### docker-compose Topology (6 services)

Six services, designed in Phase 0 and implemented in Phase 1.

**Startup order:**
`redpanda (healthy)` → `redpanda-init (exits 0)` → `risingwave + generator (parallel)` → `dagster`

**Port map for local access:**

| Port | Service | Protocol |
|------|---------|---------|
| `9092` | Redpanda | Kafka API |
| `9644` | Redpanda | Admin (rpk) |
| `4566` | RisingWave | SQL (psql-compatible) |
| `8123` | ClickHouse | HTTP |
| `3000` | Dagster | UI |

**Memory budget (~2.5 GB total).** Docker Desktop must be configured to at least
4 GB. A `docker-compose.low-mem.yml` override reduces Redpanda to `--memory 256M`
and RisingWave to 512 MB for constrained environments.

**One-command demo:** `docker compose up --build`

#### Services

**redpanda** — Single-node Redpanda (`redpandadata/redpanda:v23.3.18`), Kafka-compatible
message broker. `--smp 1 --memory 512M`. Health check via `rpk cluster health`.
Volumes: `redpanda_data`.

**redpanda-init** — One-shot init container that creates four topics via `rpk topic create`:
`order_placed`, `shipment_created`, `delivery_update`, `seller_activity`.
Each topic: 4 partitions, 1 replica. Idempotent on restart (rpk skips existing topics).

**risingwave** — Single-node RisingWave (`risingwavelabs/risingwave:v1.8.2`) in
standalone mode. Sources and materialized views initialized via SQL files mounted
at `/docker-entrypoint-initdb.d/`. 1 GB memory cap via `RW_TOTAL_MEMORY_BYTES`.
Health check via `pg_isready`. Volumes: `risingwave_data`, `./risingwave.toml`.

**clickhouse** — ClickHouse 24.3 LTS Alpine image. Four `ReplacingMergeTree` sink
tables plus reconciliation audit table initialized via `./clickhouse/init.sql`.
Port 8123 (HTTP) exposed for Dagster writes. Volumes: `clickhouse_data`.

**generator** — Python-based event generator (`./generator`). Produces synthetic
marketplace events to Redpanda at configurable `EVENTS_PER_SECOND` (default 50).
Hot-reloads `FAULT_CONTROL_FILE` every 5 seconds to enable live fault injection
without container restarts. See Generator Design section below.

**dagster** — Dagster orchestrator (`./dagster`). Runs sensor + asset pairs for
RisingWave-to-ClickHouse sync and batch reconciliation. UI at `localhost:3000`.

---

### Event Domain Model

All topics share a common envelope:

```
event_id          UUID v4, generator-assigned
event_type        topic name (redundant, aids dead-letter routing)
event_version     "1.0"
produced_at       ISO 8601 UTC, generator wall-clock
event_time        ISO 8601 UTC, business timestamp — what windowing uses
is_injected_fault boolean
fault_type        null | late_arrival | duplicate | null_field | requeue
```

#### Topics

**order_placed** (Kafka key: `order_id`)

| Field | Type | Notes |
|-------|------|-------|
| `order_id` | STRING | UUID |
| `customer_id` | STRING | |
| `seller_id` | STRING | |
| `product_category` | STRING | Top-5: bed_bath_table, health_beauty, sports_leisure, computers_accessories, furniture_decor |
| `payment_type` | STRING | credit_card, boleto, voucher, debit_card |
| `order_item_count` | INT | |
| `freight_value_brl` | DECIMAL | |
| `payment_value_brl` | DECIMAL | lognormal(mu=4.8, sigma=0.9) BRL cents shape |
| `sla_deadline_at` | STRING | event_time + category SLA constant |
| `state_code` | STRING | 2-char BR state |
| `city` | STRING | |

**shipment_created** (Kafka key: `shipment_id`)

| Field | Type | Notes |
|-------|------|-------|
| `shipment_id` | STRING | UUID |
| `order_id` | STRING | FK → order_placed |
| `seller_id` | STRING | |
| `carrier_code` | STRING | CORREIOS, JADLOG, TOTAL, AZUL_CARGO |
| `estimated_delivery_at` | STRING | lognormal(mu=1.8, sigma=0.5) days from dispatch |
| `actual_pickup_at` | STRING | ISO 8601 |
| `days_to_pickup` | INT | |

**delivery_update** (Kafka key: `shipment_id + "_" + sequence_number`)

| Field | Type | Notes |
|-------|------|-------|
| `update_id` | STRING | idempotency key |
| `shipment_id` | STRING | FK → shipment_created |
| `order_id` | STRING | denormalized (avoids join in windowed MVs) |
| `status` | STRING | in_transit, out_for_delivery, delivered, failed_attempt, returned |
| `location_state` | STRING | current carrier scan state |
| `delivery_zone` | STRING | first 3 digits of customer CEP |
| `scanned_at` | STRING | carrier event-time — drives watermarking |
| `sequence_number` | INT | |
| `is_final` | BOOL | true when status in (delivered, returned) |

**seller_activity** (Kafka key: `seller_id`)

| Field | Type | Notes |
|-------|------|-------|
| `activity_id` | STRING | UUID |
| `seller_id` | STRING | |
| `activity_type` | STRING | listing_created, listing_updated, response_sent, review_replied |
| `review_score` | FLOAT | null unless review_replied; 1.0–5.0 (mean ~4.07) |
| `product_category` | STRING | |
| `state_code` | STRING | |

---

### Statistical Calibration

The generator uses these as distribution parameters, not raw data rows.
No third-party CSV data is committed to this repo (CC BY-NC-SA 4.0 compliance).

| Signal | Distribution |
|--------|-------------|
| Order volume | Poisson with daily seasonality, peak Friday afternoon |
| Seller concentration | Pareto-shaped — top 10% sellers ~35% of orders |
| Payment value | lognormal(mu=4.8, sigma=0.9) BRL cents |
| Delivery latency | lognormal(mu=1.8, sigma=0.5) days from dispatch |
| Late delivery rate | ~7% (threshold: `estimated_delivery_at` exceeded) |

---

### Generator Design (`generator/main.py`)

- Python, single synchronous module, no async.
- `numpy.random.default_rng(SEED)` for all sampling. `Faker(locale='pt_BR').seed_instance(SEED)` for names/cities. Bit-for-bit reproducible from same SEED.
- `event_time` = simulated clock starting at `SIM_START`, advanced by `TIME_ACCELERATION_FACTOR` (default 3600 = 1 real-second per sim-hour). Enables a 10-minute demo covering weeks of order lifecycle.
- `produced_at` = real wall-clock UTC. RisingWave watermarking uses `event_time` only.
- The generator uses only the documented distribution parameters (lognormal mu/sigma, Pareto shape, category weights). No Olist CSV data is loaded or required.

#### Fault Injection

Controlled by hot-reloaded `FAULT_CONTROL_FILE` (`shared/fault_injection.json`),
re-read every 5 seconds. No container restarts required.

```json
{
  "active": false,
  "late_arrival_rate": 0.03,
  "late_arrival_max_delay_seconds": 300,
  "duplicate_rate": 0.01,
  "null_field_rate": 0.02,
  "null_field_targets": ["freight_value_brl", "days_to_pickup"],
  "requeue_rate": 0.005,
  "zone_blackout_prefix": null,
  "zone_blackout_duration_event_seconds": 7200
}
```

Fault durations are parameterized in **event-time seconds**, not wall-clock seconds,
so they interact correctly with `TIME_ACCELERATION_FACTOR` across all speed settings.
A zone blackout of 7200 event-seconds at 3600x acceleration lasts 2 real-seconds —
observable in a fast demo.

The demo control script (`make fault-demo`) sets `active: true` for one fault type
at a time, waits for windowed MVs to reflect the fault, then sets `active: false`
and waits for convergence. No container restarts required.

---

### Watermark Decision (the hardest design call in Phase 0)

For `delivery_update_source`, the watermark is declared on `scanned_at`
(carrier event-time), not `produced_at` (Redpanda ingest time).

Under `late_arrival` fault injection, `event_time` is rewound by up to
`late_arrival_max_delay_seconds` (default 300 s). `scanned_at` is computed from
`event_time` before fault injection and is therefore unchanged by `late_arrival`.
The `zone_blackout` fault is the primary demo path for showing watermark
divergence on the delivery stream.  If a larger `scanned_at` lag is needed for
the demo, increase `late_arrival_max_delay_seconds` and set the watermark lag to
match.

**The trade-off:** a 6-hour watermark means `mv_late_shipment_alert` has up to
6 hours of latency in the late-arrival scenario. This is intentional — the fault
demo shows this trade-off explicitly.

The watermark is declared as a literal `INTERVAL '5 minutes'` in
`sql/01_sources.sql` (standard mode). For fault-injection mode, the constant
must be changed to `INTERVAL '6 hours'` by editing the SQL directly and
re-running the CREATE SOURCE statements. A migration script or `make fault-demo`
step to automate this switch is deferred to Phase 2.
Without fault mode, the 5-minute literal is the correct value.

This is not solvable by adding infra. It is a fundamental streaming semantics
judgment: watermark lag = tolerated lateness = maximum late-event wait.
There is no configuration that gives you both zero latency and zero dropped events
when events arrive arbitrarily late.

---

### Windowed Materialized Views

Four primary views in `sql/02_mvs.sql`:

| View | Window | Source |
|------|--------|--------|
| `mv_fulfillment_sla_5min` | 5-min TUMBLE | `order_placed_source` |
| `mv_seller_health_1hour` | 1-hour TUMBLE | `seller_activity_source` |
| `mv_seller_health_alert_candidates` | consecutive-window join | `mv_seller_health_1hour` |
| `mv_late_shipment_alert` | 15-min slide / 1-hour HOP | `order_placed_source` LEFT JOIN `shipment_created_source` |
| `mv_delivery_zone_status` | 5-min TUMBLE | `delivery_update_source` (final events only) |

---

### ClickHouse Sink Tables

Four `ReplacingMergeTree` tables, one per primary MV. Deduplication version
column: `window_end` cast to Int64 (Unix ms).

**Critical:** Queries against these tables must use `FINAL`. `ReplacingMergeTree`
dedup is lazy (runs at merge time, not on write). This is documented at the top
of `clickhouse/init.sql` and enforced by a CI linter that fails on any SELECT
against these tables missing `FINAL`.

Additional tables:
- `batch_recompute_fulfillment_sla` — batch recompute results for reconciliation
- `reconciliation_audit` — Dagster reconciliation trail (diverged / converged / within_tolerance)

---

### Dagster Reconciliation Design

Two sensor + asset pairs per primary MV = 8 Dagster objects total.

**Sensor pattern (`risingwave_mv_sensor`):**
- Polls RisingWave every 30 seconds via `psycopg2`.
- Emits `RunRequest` when `window_end` advances past stored cursor.
- Cursor persisted in Dagster instance storage. Cold-start reads `MIN(window_end)`.

**Asset pattern (`clickhouse_sync_asset`):**
- Reads new windows from RisingWave (60s grace period).
- Writes to ClickHouse via HTTP API. Idempotent via `ReplacingMergeTree`.
- Runs asset check: monitors unshipped orders (no shipment created after 2 hours).

**Batch-vs-stream reconciliation:**
- `batch_recompute_asset`: recomputes fulfillment_sla metrics for closed windows
  using pandas/DuckDB, writes to `batch_recompute_fulfillment_sla`.
- `reconciliation_sensor`: runs every 5 minutes, compares streaming vs batch
  results, logs divergences to `reconciliation_audit`.

Three demonstrable scenarios (controlled by `fault_injection.json`):
1. `active: false` — reconciliation passes immediately (clean stream).
2. `active: true, late_arrival_rate: 0.03` + watermark not yet advanced — sensor reports divergence.
3. Same + wait for watermark to advance — discrepancy resolves, sensor records `converged`.

All three are reproducible from `SEED=42`.

## Consequences

- Phase 1 implementation has a fully-specified target: exact service names,
  image versions, port map, SQL DDL structure, Python generator contract.
- Reviewer can read this ADR and understand the full system before running a
  single command.
- The watermark trade-off is documented explicitly — no magic numbers in SQL.
- The Olist license restriction is handled at the distribution parameter level;
  no CSV data is committed to the repo.
