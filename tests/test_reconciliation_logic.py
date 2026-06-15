"""Fast-lane unit tests for the pure reconciliation logic — NO containers.

Covers the two pure-logic responsibilities that the Dagster assets wrap:
  - batch_recompute_fulfillment_sla: the pandas recompute math
  - reconcile / is_clean / max_abs_delta: the divergence diff and verdict

These run in the default CI lane (pytest -m "not integration") with only pandas
installed. The real RisingWave/ClickHouse path is exercised separately in
tests/integration/test_reconciliation.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from reconciliation.logic import (
    STATUS_CONVERGED,
    STATUS_DIVERGED,
    STATUS_WITHIN_TOLERANCE,
    batch_recompute_fulfillment_sla,
    diverged_keys,
    floor_to_window,
    is_clean,
    max_abs_delta,
    reconcile,
)

SELLER = "seller-1"
CATEGORY = "computers_accessories"
STATE = "SP"

# A window base time aligned to a 5-minute boundary. W0 is the tz-aware INPUT
# value (event_time / sla_deadline_at). W0_NAIVE is the canonical naive-UTC form
# the reconcile/batch OUTPUTS use for window_start — see logic._naive_utc: both
# ClickHouse SLA tables store naive-UTC DateTime, so reconciled keys are naive.
W0 = datetime(2024, 1, 8, 9, 0, 0, tzinfo=UTC)
W0_NAIVE = datetime(2024, 1, 8, 9, 0, 0)


def _order(
    order_id: str,
    event_time: datetime,
    sla_deadline_at: datetime,
    fault_type: str | None = None,
    seller_id: str = SELLER,
    category: str = CATEGORY,
    state: str = STATE,
) -> dict:
    return {
        "order_id": order_id,
        "seller_id": seller_id,
        "product_category": category,
        "state_code": state,
        "event_time": event_time,
        "sla_deadline_at": sla_deadline_at,
        "fault_type": fault_type,
    }


def _delivery(
    order_id: str,
    scanned_at: datetime,
    status: str = "delivered",
    is_final: bool = True,
) -> dict:
    return {
        "order_id": order_id,
        "status": status,
        "is_final": is_final,
        "scanned_at": scanned_at,
    }


# ---------------------------------------------------------------------------
# floor_to_window
# ---------------------------------------------------------------------------


class TestFloorToWindow:
    def test_floors_to_five_minute_boundary(self) -> None:
        event_time = datetime(2024, 1, 8, 9, 3, 47, tzinfo=UTC)
        assert floor_to_window(event_time) == datetime(2024, 1, 8, 9, 0, 0, tzinfo=UTC)

    def test_boundary_value_stays_in_its_own_window(self) -> None:
        event_time = datetime(2024, 1, 8, 9, 5, 0, tzinfo=UTC)
        assert floor_to_window(event_time) == event_time

    def test_naive_datetime_is_treated_as_utc(self) -> None:
        naive = datetime(2024, 1, 8, 9, 7, 0)
        assert floor_to_window(naive) == datetime(2024, 1, 8, 9, 5, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# batch_recompute_fulfillment_sla
# ---------------------------------------------------------------------------


class TestBatchRecompute:
    def test_within_sla_when_delivered_before_deadline(self) -> None:
        deadline = W0 + timedelta(hours=96)
        orders = [_order("o1", W0 + timedelta(seconds=30), deadline)]
        deliveries = [_delivery("o1", scanned_at=W0 + timedelta(hours=48))]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        assert len(rows) == 1
        row = rows[0]
        assert row["window_start"] == W0_NAIVE
        assert row["window_end"] == W0_NAIVE + timedelta(minutes=5)
        assert row["orders_placed_count"] == 1
        assert row["within_sla_count"] == 1
        assert row["breached_sla_count"] == 0
        assert row["sla_compliance_pct"] == 100.0

    def test_breached_when_delivered_after_deadline(self) -> None:
        deadline = W0 + timedelta(hours=96)
        orders = [_order("o1", W0 + timedelta(seconds=30), deadline)]
        deliveries = [_delivery("o1", scanned_at=deadline + timedelta(hours=1))]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        assert rows[0]["within_sla_count"] == 0
        assert rows[0]["breached_sla_count"] == 1
        assert rows[0]["sla_compliance_pct"] == 0.0

    def test_order_with_no_delivery_counts_only_as_placed(self) -> None:
        orders = [_order("o1", W0 + timedelta(seconds=30), W0 + timedelta(hours=96))]

        rows = batch_recompute_fulfillment_sla(orders, deliveries=[])

        assert rows[0]["orders_placed_count"] == 1
        assert rows[0]["within_sla_count"] == 0
        assert rows[0]["breached_sla_count"] == 0
        # No final delivered events at all -> compliance defaults to 0.0.
        assert rows[0]["sla_compliance_pct"] == 0.0

    def test_null_field_fault_orders_are_excluded(self) -> None:
        orders = [
            _order("o1", W0 + timedelta(seconds=10), W0 + timedelta(hours=96)),
            _order(
                "o2",
                W0 + timedelta(seconds=20),
                W0 + timedelta(hours=96),
                fault_type="null_field",
            ),
        ]
        deliveries = [
            _delivery("o1", scanned_at=W0 + timedelta(hours=1)),
            _delivery("o2", scanned_at=W0 + timedelta(hours=1)),
        ]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        assert len(rows) == 1
        assert rows[0]["orders_placed_count"] == 1
        assert rows[0]["within_sla_count"] == 1

    def test_multiple_delivered_finals_fan_out_like_the_mv_join(self) -> None:
        """An order with 2 delivered finals counts TWICE (LEFT JOIN fan-out).

        The MV (sql/02_mvs.sql) LEFT JOINs orders to delivery_finals on order_id
        and COUNT(*)s the result, so an order with two delivered finals is
        counted twice. Collapsing to one (the naive per-order dedup) undercounts
        the stream — this is the exact SEED=42 divergence the integration test
        surfaced. Both delivered finals are within SLA here.
        """
        deadline = W0 + timedelta(hours=96)
        orders = [_order("o1", W0 + timedelta(seconds=30), deadline)]
        deliveries = [
            _delivery("o1", scanned_at=W0 + timedelta(hours=24)),
            _delivery("o1", scanned_at=W0 + timedelta(hours=36)),
        ]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        assert rows[0]["orders_placed_count"] == 2, "fan-out: post-join COUNT(*) is 2"
        assert rows[0]["within_sla_count"] == 2, "both delivered finals are within SLA"
        assert rows[0]["breached_sla_count"] == 0

    def test_order_with_no_delivery_produces_single_row(self) -> None:
        """An order with no delivered final is preserved once (LEFT JOIN keeps it)."""
        orders = [_order("o1", W0 + timedelta(seconds=30), W0 + timedelta(hours=96))]

        rows = batch_recompute_fulfillment_sla(orders, deliveries=[])

        assert rows[0]["orders_placed_count"] == 1

    def test_non_delivered_final_does_not_count_as_within_sla(self) -> None:
        orders = [_order("o1", W0 + timedelta(seconds=30), W0 + timedelta(hours=96))]
        # 'returned' is a final status but not 'delivered' — must not count.
        deliveries = [_delivery("o1", scanned_at=W0 + timedelta(hours=1), status="returned")]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        assert rows[0]["within_sla_count"] == 0
        assert rows[0]["breached_sla_count"] == 0

    def test_groups_separate_windows_independently(self) -> None:
        orders = [
            _order("o1", W0 + timedelta(seconds=30), W0 + timedelta(hours=96)),
            _order("o2", W0 + timedelta(minutes=6), W0 + timedelta(hours=96)),
        ]
        deliveries = [
            _delivery("o1", scanned_at=W0 + timedelta(hours=1)),
            _delivery("o2", scanned_at=W0 + timedelta(hours=1)),
        ]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        windows = sorted(row["window_start"] for row in rows)
        assert windows == [W0_NAIVE, W0_NAIVE + timedelta(minutes=5)]

    def test_iso_string_timestamps_are_parsed(self) -> None:
        orders = [
            {
                "order_id": "o1",
                "seller_id": SELLER,
                "product_category": CATEGORY,
                "state_code": STATE,
                "event_time": "2024-01-08T09:00:30Z",
                "sla_deadline_at": "2024-01-12T09:00:00Z",
                "fault_type": None,
            }
        ]
        deliveries = [
            {
                "order_id": "o1",
                "status": "delivered",
                "is_final": True,
                "scanned_at": "2024-01-08T10:00:00Z",
            }
        ]

        rows = batch_recompute_fulfillment_sla(orders, deliveries)

        assert rows[0]["window_start"] == W0_NAIVE
        assert rows[0]["within_sla_count"] == 1

    def test_empty_orders_returns_empty(self) -> None:
        assert batch_recompute_fulfillment_sla([], []) == []

    def test_all_null_field_returns_empty(self) -> None:
        orders = [_order("o1", W0, W0 + timedelta(hours=96), fault_type="null_field")]
        assert batch_recompute_fulfillment_sla(orders, []) == []


# ---------------------------------------------------------------------------
# reconcile / is_clean / max_abs_delta
# ---------------------------------------------------------------------------


def _sla_row(window_start: datetime, within: int, seller: str = SELLER) -> dict:
    return {
        "window_start": window_start,
        "seller_id": seller,
        "product_category": CATEGORY,
        "state_code": STATE,
        "within_sla_count": within,
    }


class TestReconcile:
    def test_matching_rows_are_within_tolerance(self) -> None:
        streaming = [_sla_row(W0, within=5)]
        batch = [_sla_row(W0, within=5)]

        audit = reconcile(streaming, batch)

        assert len(audit) == 1
        assert audit[0].status == STATUS_WITHIN_TOLERANCE
        assert audit[0].abs_delta == 0
        assert is_clean(audit)
        assert max_abs_delta(audit) == 0

    def test_value_mismatch_beyond_tolerance_is_diverged(self) -> None:
        streaming = [_sla_row(W0, within=5)]
        batch = [_sla_row(W0, within=8)]

        audit = reconcile(streaming, batch, tolerance=0)

        assert audit[0].status == STATUS_DIVERGED
        assert audit[0].abs_delta == 3
        assert audit[0].streaming_value == 5
        assert audit[0].batch_value == 8
        assert not is_clean(audit)
        assert max_abs_delta(audit) == 3

    def test_window_only_in_batch_diverges_with_streaming_zero(self) -> None:
        # The watermark-lag case: the batch has produced a window the stream has not.
        streaming: list[dict] = []
        batch = [_sla_row(W0, within=4)]

        audit = reconcile(streaming, batch, tolerance=0)

        assert audit[0].status == STATUS_DIVERGED
        assert audit[0].streaming_value == 0
        assert audit[0].batch_value == 4
        assert audit[0].abs_delta == 4

    def test_delta_within_nonzero_tolerance_passes(self) -> None:
        streaming = [_sla_row(W0, within=5)]
        batch = [_sla_row(W0, within=6)]

        audit = reconcile(streaming, batch, tolerance=1)

        assert audit[0].status == STATUS_WITHIN_TOLERANCE
        assert is_clean(audit, tolerance=1)

    def test_previously_diverged_key_now_agreeing_is_converged(self) -> None:
        streaming = [_sla_row(W0, within=5)]
        batch = [_sla_row(W0, within=5)]
        # previously_diverged_keys is naive-UTC — the same form diverged_keys emits.
        prior = frozenset({(W0_NAIVE, SELLER)})

        audit = reconcile(streaming, batch, previously_diverged_keys=prior)

        assert audit[0].status == STATUS_CONVERGED
        assert is_clean(audit)

    def test_diverged_keys_extracts_diverging_windows(self) -> None:
        streaming = [_sla_row(W0, within=5), _sla_row(W0 + timedelta(minutes=5), within=2)]
        batch = [_sla_row(W0, within=5), _sla_row(W0 + timedelta(minutes=5), within=9)]

        audit = reconcile(streaming, batch)

        keys = diverged_keys(audit)
        assert keys == frozenset({(W0_NAIVE + timedelta(minutes=5), SELLER)})

    def test_three_scenario_lifecycle(self) -> None:
        """clean -> diverged -> converged, the headline reproducible sequence."""
        # 1. clean: both sides agree.
        clean_audit = reconcile([_sla_row(W0, 5)], [_sla_row(W0, 5)])
        assert is_clean(clean_audit)

        # 2. diverged-under-fault: late events landed in batch but not yet in stream.
        diverged_audit = reconcile([_sla_row(W0, 5)], [_sla_row(W0, 7)])
        assert not is_clean(diverged_audit)
        prior = diverged_keys(diverged_audit)
        assert prior == frozenset({(W0_NAIVE, SELLER)})

        # 3. converged-after-watermark: stream caught up; the prior divergence resolves.
        converged_audit = reconcile(
            [_sla_row(W0, 7)], [_sla_row(W0, 7)], previously_diverged_keys=prior
        )
        assert is_clean(converged_audit)
        assert converged_audit[0].status == STATUS_CONVERGED


def test_max_abs_delta_empty_audit_is_zero() -> None:
    assert max_abs_delta([]) == 0


def test_is_clean_empty_audit_is_true() -> None:
    assert is_clean([])


def test_batch_recompute_rejects_unparseable_timestamp() -> None:
    orders = [_order("o1", event_time=12345, sla_deadline_at=W0)]  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        batch_recompute_fulfillment_sla(orders, [])
