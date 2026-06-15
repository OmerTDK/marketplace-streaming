# ADR-0005: Batch-vs-Stream Reconciliation and In-Process Dagster Testing

**Date:** 2026-06-15
**Status:** Accepted

## Context

The streaming MV `mv_fulfillment_sla_5min` (RisingWave) computes per-window
SLA-compliance counts incrementally. Incremental streaming aggregation is the
right tool for sub-minute freshness, but it has a failure mode that batch does
not: under late arrivals, watermark misconfiguration, or a join bug, the stream
can silently emit a *wrong* number and nobody notices, because there is nothing
to compare it against.

The portfolio differentiator for this project is a **reconciliation layer**:
an independent batch recompute of the same metric from the same events, a diff
against the streaming result, and a guard that **fails loudly** when the two
disagree beyond tolerance. This is the thing most "streaming demo" projects
skip, and it is exactly the thing a production streaming platform needs.

Three forces shaped the design:

1. **The two compute paths must be genuinely independent.** If the batch path
   reads the streaming MV's output, it proves nothing. The batch path must
   recompute from the raw event log (the Kafka-backed RisingWave source), using
   a different engine (pandas), so a bug in one path does not hide in the other.

2. **The reconciliation must mirror the MV's semantics exactly**, including the
   non-obvious ones — or it produces false divergences that train operators to
   ignore the guard.

3. **Testing must not require booting the Dagster daemon.** ADR-0004 established
   that the daemon cold-starts 90s+ and its health is orthogonal to correctness.
   Phase 3 keeps that discipline.

## Decision

### Three assets plus one asset-check

| Object | Type | Responsibility |
|--------|------|----------------|
| `clickhouse_sync_asset` | `@asset` | Read `mv_fulfillment_sla_5min` windows from RisingWave, write the `fulfillment_sla` ReplacingMergeTree sink (idempotent). |
| `batch_recompute_asset` | `@asset` | Read raw order/delivery events from the RisingWave **source**, recompute the SLA metric via pandas, write `batch_recompute_fulfillment_sla`. |
| `reconciliation_audit_asset` | `@asset` | Diff streaming vs batch per window key, append the verdict to `reconciliation_audit`. |
| `reconciliation_check` | `@asset_check` | Re-read both tables, re-run the diff, and **fail** (`passed=False`, `ERROR` severity) when any window diverges beyond tolerance. |

### Logic lives in plain functions; assets are thin wrappers

All reconciliation logic is in two import-light modules:

- `reconciliation/logic.py` — **pure** (pandas only, no network, no Dagster):
  `batch_recompute_fulfillment_sla`, `reconcile`, `is_clean`, `max_abs_delta`.
- `reconciliation/io.py` — plain IO functions over a passed-in connection:
  RisingWave reads, ClickHouse sync/writes.

The Dagster `@asset` bodies in `reconciliation/assets.py` call those functions.
This is the ADR-0004 lesson generalised: extract the logic from the asset so it
is testable without the orchestrator. The fast CI lane unit-tests the recompute
math and the diff with **zero containers** (`tests/test_reconciliation_logic.py`,
26 tests). `dagster` is an integration-only dependency; the fast lane never
imports it.

### Mirroring the MV semantics: the LEFT JOIN fan-out

The MV is `TUMBLE(order_events) LEFT JOIN delivery_finals ON order_id`, then
`COUNT(*)` / `COUNT(*) FILTER(...)`. The non-obvious consequence: an order with
**two** delivered-final delivery events fans out into two joined rows and is
counted **twice** in `orders_placed_count` and (if both are within SLA) twice in
`within_sla_count`.

At `SEED=42` this is not hypothetical — the integration run surfaced exactly one
order with two delivered finals (8 delivered finals across 7 distinct orders),
producing a streaming `within_sla_count` of 2 against a naive batch value of 1.
The batch recompute therefore replicates the fan-out: it emits one joined row
per matching delivered final (and one NULL-delivered row when an order has none),
rather than collapsing to one row per order. This is captured by a fast-lane
unit test (`test_multiple_delivered_finals_fan_out_like_the_mv_join`).

### Naive-UTC as the canonical reconciliation timestamp

ClickHouse `DateTime` columns are timezone-naive UTC; rows read back from
ClickHouse are naive. The batch path floors `event_time` with `floor_to_window`,
which yields tz-**aware** UTC. A tz-aware and a tz-naive datetime with the same
wall-clock value are not equal in Python, so without normalisation **every**
window key would mismatch and the reconciliation would report total divergence.
`reconcile` and the batch writer normalise `window_start` to naive-UTC (the form
both ClickHouse SLA tables store) before keying. This too was caught by the
integration run, not by speculation.

### The three reproducible scenarios

Driven from `SEED=42`, classified by `reconcile`:

1. **clean** — streaming and batch agree on every window (`within_tolerance`);
   the asset-check passes. (`test_asset_check_passes_on_clean_run`,
   `test_batch_recompute_matches_streaming_on_clean_run`.)
2. **diverged-under-fault** — a window where the stream and batch disagree
   beyond tolerance is classified `diverged`; the asset-check fails. The
   integration kill-test injects a divergence into the streaming sink and
   verifies `passed=False`. (`test_asset_check_fails_on_injected_divergence`.)
3. **converged-after-watermark** — a window that previously diverged and now
   agrees is classified `converged` (the caller passes the prior diverged keys).
   Unit-tested end-to-end in `test_three_scenario_lifecycle`.

`reconcile` returns the diverged keys, so scenario 3 is a re-check fed scenario
2's output — the lifecycle round-trips cleanly.

**Scope note on the `converged` status in the running asset.**
`reconciliation_audit_asset` currently calls `reconcile(...)` **without**
`previously_diverged_keys`, so the `converged` status is produced by the
unit-tested logic path (above) but **not** by the live asset run — the audit
table the asset writes only ever holds `within_tolerance` and `diverged` rows.
Wiring the asset to feed prior diverged keys back into the next run requires the
prior keys to round-trip through the `reconciliation_audit` table, which today
stores only `(window_start, seller_id)` and **not** `product_category` /
`state_code`. Persisting the truncated 2-tuple and re-keying on it would
re-introduce the multi-category false-`converged` bug that the 4-tuple
`diverged_keys` fix closes. Closing this gap correctly therefore means widening
the `reconciliation_audit` schema to carry the full window key first; it is
deliberately deferred and tracked as a follow-up rather than shipped half-done.
The `reconciliation_check` kill-switch is unaffected — it inspects `abs_delta`
via `is_clean`, never `status`.

### In-process Dagster testing (no daemon)

The integration test materialises the assets with `dagster.materialize(...)`
against resources pointed at the live compose endpoints — the in-process
executor, no daemon, no scheduler. The asset-check is verified two ways:

- **Directly invoked** (`reconciliation_check(clickhouse=...)`) returns an
  `AssetCheckResult` whose `.passed` / `.severity` / `.metadata` are asserted —
  deterministic, no IO-manager indirection.
- **Via `materialize`**, with the check included in the assets list (verified
  empirically: `materialize([asset])` alone does *not* run the asset's checks;
  `materialize([asset, check])` does), asserting on
  `result.get_asset_check_evaluations()`.

Both the clean pass and the injected-divergence fail are kill-verified.

## Alternatives considered

- **Batch path reads the streaming MV output instead of raw events.** Rejected:
  it would make the two paths the same compute, so the diff could never catch a
  streaming bug — the entire point.
- **Reconcile by trusting a stored audit verdict in the check.** Rejected: the
  check re-runs the diff against current table state so it reflects reality at
  check time, not a possibly-stale prior write.
- **Naive per-order delivered-at dedup in the batch.** Rejected after the
  integration run proved it undercounts the MV fan-out (the SEED=42 case above).
- **DuckDB for the batch recompute.** Considered (the spec allowed "pandas/
  DuckDB"). pandas is sufficient for this aggregation, is already a natural fit
  for the dict-shaped event rows, and avoids a second heavy dependency. DuckDB
  remains a drop-in for the same logic if the event volume outgrows pandas.
- **Containerise the Dagster daemon for the integration test.** Rejected per
  ADR-0004: 90s+ cold start, orthogonal to correctness, and the in-process
  executor exercises the same asset/check code paths.

## Consequences

- The reconciliation guard is a real kill-switch: a streaming-vs-batch
  divergence cannot pass silently — it fails an `ERROR`-severity asset-check.
- The fast lane stays container-free and dagster-free (`reconciliation.logic`
  imports only pandas; `tests/integration/test_reconciliation.py` guards its
  dagster import with `pytest.importorskip`).
- The batch path is coupled to the MV's exact semantics (fan-out, null-field
  filter, delivered-final definition). If the MV changes, the batch and its
  unit tests must change in lockstep — this coupling is intentional and
  documented here and in `reconciliation/logic.py`.
- The full integration suite (Phase 2 + Phase 3) runs in ~3 minutes and is
  green: 14 tests, including the reconciliation kill-verify, against the real
  compose topology.
- Dagster's introspection requires real (non-stringized) `context` annotations,
  so `reconciliation/assets.py` deliberately omits `from __future__ import
  annotations` — noted in the module header to prevent a re-introduction.
