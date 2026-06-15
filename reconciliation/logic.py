"""Pure reconciliation logic — no containers, no Dagster, no network.

This module is the heart of the batch-vs-stream reconciliation. It is split
out from all IO (RisingWave reads, ClickHouse writes) and from the Dagster
asset wrappers so the windowing math and the divergence diff are unit-testable
in the fast CI lane with zero containers.

Two responsibilities:

1. ``batch_recompute_fulfillment_sla`` — recompute the same fulfillment-SLA
   metric the streaming MV (``mv_fulfillment_sla_5min``) produces, from the
   same raw order/delivery events, using pandas. This is the independent
   second compute path: same inputs, different engine. If the two paths
   disagree on a closed window, one of them is wrong (or the stream has not
   yet caught up — see the watermark scenario in ADR-0005).

2. ``reconcile`` — diff the streaming rows against the batch rows per window
   key and classify each window as ``within_tolerance`` (delta == 0 within
   tolerance), ``diverged`` (delta exceeds tolerance), or ``converged`` (a
   window that previously diverged now agrees). The caller supplies the set of
   previously-diverged keys so a re-check after the watermark advances can mark
   the resolution explicitly.

The streaming MV definition this mirrors (sql/02_mvs.sql, mv_fulfillment_sla_5min):

    window     = 5-minute TUMBLE on the order event_time
    grouping   = (window_start, seller_id, product_category, state_code)
    orders     = COUNT(*) of order_placed rows, EXCLUDING null_field faults
    within_sla = COUNT(*) WHERE delivered_at <= sla_deadline_at
    breached   = COUNT(*) WHERE delivered_at >  sla_deadline_at
    delivered_at = scanned_at of the matching final delivered delivery_update
                   (LEFT JOIN on order_id; orders with no delivered final
                   contribute to neither within nor breached)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

# The streaming MV uses a 5-minute tumbling window. The batch path must floor
# event_time to the same boundary or the two paths would never align.
TUMBLE_WINDOW = timedelta(minutes=5)

# Default divergence tolerance: streaming and batch must agree exactly (delta 0)
# on within_sla_count for a closed window. A small non-zero tolerance can be
# passed to absorb known boundary effects; 0 is the strict default.
DEFAULT_TOLERANCE = 0

# The metric reconciled across the two compute paths. within_sla_count is the
# headline SLA-compliance numerator and the most divergence-sensitive field
# (it depends on the order/delivery JOIN landing within the window).
RECONCILED_METRIC = "within_sla_count"

# Reconciliation status values written to reconciliation_audit (ADR-0002 /
# clickhouse/init.sql). within_tolerance: agree. diverged: disagree beyond
# tolerance. converged: a previously-diverged window now agrees.
STATUS_WITHIN_TOLERANCE = "within_tolerance"
STATUS_DIVERGED = "diverged"
STATUS_CONVERGED = "converged"

# The grouping key for a fulfillment-SLA window. Matches the MV GROUP BY.
WINDOW_KEY_FIELDS = ("window_start", "seller_id", "product_category", "state_code")

NULL_FIELD_FAULT = "null_field"
DELIVERED_STATUS = "delivered"


@dataclass(frozen=True)
class ReconciliationRow:
    """One reconciled window: streaming value vs batch value and the verdict.

    Mirrors the reconciliation_audit table columns (clickhouse/init.sql) the
    Dagster sensor writes. ``late_event_ids`` is reserved for the watermark
    scenario where the diverging events are the late deliveries not yet in the
    stream; the diff logic populates it when the batch sees deliveries the
    stream window does not.
    """

    window_start: datetime
    window_end: datetime
    seller_id: str
    product_category: str
    state_code: str
    streaming_value: int
    batch_value: int
    abs_delta: int
    status: str
    late_event_ids: tuple[str, ...] = ()


def floor_to_window(event_time: datetime, window: timedelta = TUMBLE_WINDOW) -> datetime:
    """Floor a timestamp to the start of its tumbling window.

    Mirrors RisingWave's TUMBLE window assignment: a 5-minute tumble places an
    event with event_time T into the window [floor(T, 5min), floor(T, 5min)+5min).

    Args:
        event_time: The event business timestamp (tz-aware UTC).
        window: Window width. Defaults to the 5-minute MV tumble.

    Returns:
        The window_start boundary for event_time.
    """
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    elapsed = (event_time - epoch).total_seconds()
    window_seconds = window.total_seconds()
    floored = (elapsed // window_seconds) * window_seconds
    return epoch + timedelta(seconds=floored)


def _parse_ts(value: object) -> datetime:
    """Parse an ISO-8601 string or pass through a datetime to tz-aware UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    raise TypeError(f"Cannot parse timestamp from {type(value).__name__}: {value!r}")


def batch_recompute_fulfillment_sla(
    orders: list[dict],
    deliveries: list[dict],
) -> list[dict]:
    """Recompute fulfillment-SLA metrics from raw events via pandas.

    The independent batch compute path. Reads the same order_placed and
    delivery_update events the streaming MV consumes and recomputes, per
    5-minute window and (seller, category, state), the orders_placed_count,
    within_sla_count and breached_sla_count.

    Matching the MV exactly (sql/02_mvs.sql), which is a LEFT JOIN of the
    tumbled orders onto delivery_finals on order_id, then COUNT(*)/FILTER:
      - order rows are excluded ONLY when both is_injected_fault is true AND
        fault_type == 'null_field', mirroring the MV's exact WHERE clause
        ``is_injected_fault = FALSE OR fault_type IS DISTINCT FROM 'null_field'``
        (sql/02_mvs.sql). A row with fault_type == 'null_field' but
        is_injected_fault false is KEPT — the same row the MV keeps. The
        generator always sets the two atomically (generator/fault_injection.py),
        but an external producer or manual replay could land in the gap, so the
        batch mirrors the full two-axis condition rather than the simplified
        fault_type-only paraphrase to avoid a false divergence.
      - the join FANS OUT: an order with N matching delivered finals produces
        N rows, so orders_placed_count (the post-join COUNT(*)) counts that
        order N times. The batch replicates the fan-out — counting one
        delivered_at per matching final delivery, NOT one per order. (An order
        appearing twice in the source as two delivered finals is a real,
        observed case at SEED=42; collapsing it to one undercounts the stream.)
      - an order with NO delivered final produces exactly ONE row with a NULL
        delivered_at (the LEFT side is preserved), counted in
        orders_placed_count only.
      - within_sla: delivered_at <= sla_deadline_at
      - breached_sla: delivered_at > sla_deadline_at

    Args:
        orders: order_placed event dicts. Required keys: order_id, seller_id,
            product_category, state_code, event_time, sla_deadline_at,
            fault_type (may be None).
        deliveries: delivery_update event dicts. Required keys: order_id,
            status, is_final, scanned_at.

    Returns:
        One dict per window key with the recomputed counts, schema-matching the
        batch_recompute_fulfillment_sla ClickHouse table. Empty list if no
        orders survive the null-field filter.
    """
    # Mirror the MV WHERE clause exactly (sql/02_mvs.sql):
    #   is_injected_fault = FALSE OR fault_type IS DISTINCT FROM 'null_field'
    # i.e. exclude a row ONLY when it is an INJECTED null_field fault. A row with
    # fault_type == 'null_field' but is_injected_fault false is kept, matching
    # the MV. (De Morgan of the OR: NOT(is_injected_fault AND fault==null_field).)
    eligible_orders = [
        order
        for order in orders
        if not (order.get("is_injected_fault") and order.get("fault_type") == NULL_FIELD_FAULT)
    ]
    if not eligible_orders:
        return []

    delivered_ats_by_order = _delivered_ats_by_order(deliveries)
    joined_rows = _left_join_orders_to_deliveries(eligible_orders, delivered_ats_by_order)

    orders_frame = pd.DataFrame(joined_rows)
    orders_frame["is_within_sla"] = orders_frame.apply(_row_is_within_sla, axis=1)
    orders_frame["is_breached_sla"] = orders_frame.apply(_row_is_breached_sla, axis=1)

    grouped = orders_frame.groupby(
        ["window_start", "seller_id", "product_category", "state_code"],
        as_index=False,
    ).agg(
        orders_placed_count=("order_id", "size"),
        within_sla_count=("is_within_sla", "sum"),
        breached_sla_count=("is_breached_sla", "sum"),
    )

    return [_to_batch_row(record) for record in grouped.to_dict("records")]


def _left_join_orders_to_deliveries(
    eligible_orders: list[dict],
    delivered_ats_by_order: dict[str, list[datetime]],
) -> list[dict]:
    """Expand each order into one row per matching delivered final (LEFT JOIN).

    Mirrors ``TUMBLE(order_events) LEFT JOIN delivery_finals ON order_id``:
    an order with N delivered finals yields N rows; an order with none yields
    one row with delivered_at = None.
    """
    rows: list[dict] = []
    for order in eligible_orders:
        base = {
            "order_id": order["order_id"],
            "seller_id": order["seller_id"],
            "product_category": order["product_category"],
            "state_code": order["state_code"],
            "window_start": floor_to_window(_parse_ts(order["event_time"])),
            "sla_deadline_at": _parse_ts(order["sla_deadline_at"]),
        }
        delivered_ats = delivered_ats_by_order.get(order["order_id"], [])
        if not delivered_ats:
            rows.append({**base, "delivered_at": None})
            continue
        for delivered_at in delivered_ats:
            rows.append({**base, "delivered_at": delivered_at})
    return rows


def _delivered_ats_by_order(deliveries: list[dict]) -> dict[str, list[datetime]]:
    """Map order_id -> [delivered scanned_at, ...] for final delivered events.

    A list (not a single value) because one order can have multiple delivered
    finals in the source; the MV join counts each, so the batch must keep all.
    """
    delivered: dict[str, list[datetime]] = {}
    for delivery in deliveries:
        if not delivery.get("is_final"):
            continue
        if delivery.get("status") != DELIVERED_STATUS:
            continue
        delivered.setdefault(delivery["order_id"], []).append(_parse_ts(delivery["scanned_at"]))
    return delivered


def _row_is_within_sla(row: pd.Series) -> bool:
    delivered_at = row["delivered_at"]
    if delivered_at is None or pd.isna(delivered_at):
        return False
    return delivered_at <= row["sla_deadline_at"]


def _row_is_breached_sla(row: pd.Series) -> bool:
    delivered_at = row["delivered_at"]
    if delivered_at is None or pd.isna(delivered_at):
        return False
    return delivered_at > row["sla_deadline_at"]


def _naive_utc(value: datetime) -> datetime:
    """Drop tzinfo, returning a naive-UTC datetime.

    ClickHouse ``DateTime`` columns are timezone-naive UTC, so streaming rows
    read back from ClickHouse are naive. The batch path floors event_time with
    ``floor_to_window`` which yields tz-AWARE UTC. Reconciling the two requires
    a single canonical representation or the window keys never match (a tz-aware
    and a naive datetime with the same wall-clock value are NOT equal). Naive-UTC
    is the canonical form because that is what both ClickHouse tables store.
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _to_batch_row(record: dict) -> dict:
    """Convert a grouped pandas record into a batch_recompute_fulfillment_sla row."""
    window_start = _naive_utc(record["window_start"].to_pydatetime())
    window_end = window_start + TUMBLE_WINDOW
    orders_placed = int(record["orders_placed_count"])
    within = int(record["within_sla_count"])
    breached = int(record["breached_sla_count"])
    compliance_pct = round(within / (within + breached) * 100, 2) if (within + breached) else 0.0
    return {
        "window_start": window_start,
        "window_end": window_end,
        "seller_id": record["seller_id"],
        "product_category": record["product_category"],
        "state_code": record["state_code"],
        "orders_placed_count": orders_placed,
        "within_sla_count": within,
        "breached_sla_count": breached,
        "sla_compliance_pct": compliance_pct,
    }


def _window_key(row: dict) -> tuple:
    """Build the comparison key from a streaming or batch row.

    window_start is normalized to naive-UTC so a streaming row (naive, from
    ClickHouse) and a batch row (tz-aware, from floor_to_window) for the same
    wall-clock window produce the SAME key. Without this, every window would
    appear to diverge.
    """
    window_start = _naive_utc(row["window_start"])
    return (window_start, *(row[field] for field in WINDOW_KEY_FIELDS[1:]))


def reconcile(
    streaming_rows: list[dict],
    batch_rows: list[dict],
    tolerance: int = DEFAULT_TOLERANCE,
    previously_diverged_keys: frozenset[tuple] | None = None,
    checked_at: datetime | None = None,
) -> list[ReconciliationRow]:
    """Diff streaming vs batch per window key and classify each window.

    A window present in only one side is treated as a divergence with the
    missing side's value at 0 — a window the stream has not yet produced but
    the batch has (the watermark-lag case) shows up here as a positive delta.

    Classification per window:
      - abs_delta <= tolerance                          -> within_tolerance
      - abs_delta >  tolerance, key was diverged before -> still diverged
      - abs_delta <= tolerance, key was diverged before -> converged
      - abs_delta >  tolerance, key is newly diverging  -> diverged

    Args:
        streaming_rows: rows from the streaming sink (fulfillment_sla), each
            with the WINDOW_KEY_FIELDS plus RECONCILED_METRIC.
        batch_rows: rows from batch_recompute_fulfillment_sla, same shape.
        tolerance: maximum allowed abs delta on RECONCILED_METRIC. Default 0.
        previously_diverged_keys: full window keys
            (window_start, seller_id, product_category, state_code) that diverged
            on a prior check — the same shape ``diverged_keys`` emits. A key in
            this set that now agrees is marked ``converged``.
        checked_at: timestamp stamped on every audit row. Defaults to now(UTC).

    Returns:
        One ReconciliationRow per window key seen on either side.
    """
    if checked_at is None:
        checked_at = datetime.now(tz=UTC)
    prior = previously_diverged_keys or frozenset()

    streaming_by_key = {_window_key(row): row for row in streaming_rows}
    batch_by_key = {_window_key(row): row for row in batch_rows}
    all_keys = sorted(set(streaming_by_key) | set(batch_by_key))

    audit: list[ReconciliationRow] = []
    for key in all_keys:
        streaming_row = streaming_by_key.get(key)
        batch_row = batch_by_key.get(key)
        streaming_value = int(streaming_row[RECONCILED_METRIC]) if streaming_row else 0
        batch_value = int(batch_row[RECONCILED_METRIC]) if batch_row else 0
        abs_delta = abs(streaming_value - batch_value)

        window_start = key[0]
        seller_id = key[1]
        product_category = key[2]
        state_code = key[3]
        # previously_diverged_keys is keyed on the FULL window key
        # (window_start, seller_id, product_category, state_code) — the same shape
        # diverged_keys() emits — so a prior-check verdict round-trips into the
        # next reconcile call without conflating two windows that share a
        # (window_start, seller_id) but differ on category/state. Truncating to
        # the 2-tuple here would mislabel a never-diverged category/state as
        # `converged` whenever any OTHER category/state of the same seller+window
        # had diverged before.
        was_diverged = key in prior
        status = _classify(abs_delta, tolerance, was_diverged)
        audit.append(
            ReconciliationRow(
                window_start=window_start,
                window_end=window_start + TUMBLE_WINDOW,
                seller_id=seller_id,
                product_category=product_category,
                state_code=state_code,
                streaming_value=streaming_value,
                batch_value=batch_value,
                abs_delta=abs_delta,
                status=status,
            )
        )
    return audit


def _classify(abs_delta: int, tolerance: int, was_diverged: bool) -> str:
    """Classify one window's delta into a reconciliation status."""
    within_tolerance = abs_delta <= tolerance
    if within_tolerance:
        return STATUS_CONVERGED if was_diverged else STATUS_WITHIN_TOLERANCE
    return STATUS_DIVERGED


def diverged_keys(audit: list[ReconciliationRow]) -> frozenset[tuple]:
    """Return the set of FULL window keys currently classified as diverged.

    Emits the same 4-tuple shape reconcile keys on —
    (window_start, seller_id, product_category, state_code) — so a prior-check
    verdict round-trips into the next reconcile call without conflating two
    windows that share a (window_start, seller_id) but differ on category/state.
    """
    return frozenset(
        (row.window_start, row.seller_id, row.product_category, row.state_code)
        for row in audit
        if row.status == STATUS_DIVERGED
    )


def is_clean(audit: list[ReconciliationRow], tolerance: int = DEFAULT_TOLERANCE) -> bool:
    """True if no window in the audit exceeds tolerance (nothing diverged).

    This is the predicate the Dagster asset-check uses: a clean reconciliation
    passes, any diverged window fails the check.
    """
    return all(row.abs_delta <= tolerance for row in audit)


def max_abs_delta(audit: list[ReconciliationRow]) -> int:
    """Largest abs_delta across all reconciled windows (0 if audit is empty)."""
    return max((row.abs_delta for row in audit), default=0)
