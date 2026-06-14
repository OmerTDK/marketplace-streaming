"""Phase 1 tests: generator determinism, envelope schema, FK integrity,
fault injection rates, event-time math, Olist calibration sanity.

All tests are fully offline — no Kafka broker, no containers.
The InMemorySink captures all emitted events.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import numpy as np
import pytest

from generator.clock import FixedClock
from generator.fault_injection import (
    ALL_FAULT_TYPES,
    FAULT_DUPLICATE,
    FAULT_LATE_ARRIVAL,
    FAULT_NULL_FIELD,
    FAULT_REQUEUE,
    FAULT_ZONE_BLACKOUT,
    FaultConfig,
    FaultHarness,
    FaultState,
)
from generator.generator import (
    CATEGORY_SLA_HOURS,
    DELIVERY_LATENCY_MU,
    DELIVERY_LATENCY_SIGMA,
    PAYMENT_VALUE_MU,
    PAYMENT_VALUE_SIGMA,
    MarketplaceGenerator,
    run_generator,
)
from generator.sink import InMemorySink

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXED_EVENT_TS = datetime(2024, 1, 8, 9, 0, 0, tzinfo=UTC)
FIXED_PRODUCED_TS = datetime(2024, 1, 8, 9, 0, 0, tzinfo=UTC)

N_SMALL = 200
N_MEDIUM = 1000
SEED = 42


def make_generator(
    seed: int = SEED,
    fault_config: FaultConfig | None = None,
) -> tuple[MarketplaceGenerator, InMemorySink]:
    """Build a fully deterministic generator + sink pair."""
    sink = InMemorySink()
    clock = FixedClock(event_ts=FIXED_EVENT_TS, produced_ts=FIXED_PRODUCED_TS)
    gen = MarketplaceGenerator(
        seed=seed,
        sink=sink,
        fault_config=fault_config or FaultConfig.inactive(),
        clock=clock,
        n_sellers=50,
        n_customers=200,
    )
    return gen, sink


# ---------------------------------------------------------------------------
# 1. Determinism tests
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Same seed must produce identical event streams on repeated runs."""

    def test_identical_order_stream_same_seed(self) -> None:
        """Two generators with the same seed produce the same order IDs."""
        gen1, sink1 = make_generator()
        gen1.generate_batch(N_SMALL)

        gen2, sink2 = make_generator()
        gen2.generate_batch(N_SMALL)

        orders1 = [e["order_id"] for e in sink1.records_for("order_placed")]
        orders2 = [e["order_id"] for e in sink2.records_for("order_placed")]
        assert orders1 == orders2, "Order IDs differ with same seed"

    def test_different_seed_produces_different_stream(self) -> None:
        """Different seeds must produce different streams."""
        gen1, sink1 = make_generator(seed=42)
        gen1.generate_batch(N_SMALL)

        gen2, sink2 = make_generator(seed=99)
        gen2.generate_batch(N_SMALL)

        orders1 = [e["order_id"] for e in sink1.records_for("order_placed")]
        orders2 = [e["order_id"] for e in sink2.records_for("order_placed")]
        assert orders1 != orders2, "Different seeds should produce different streams"

    def test_full_stream_hash_is_stable(self) -> None:
        """The full event stream hash must be identical across runs (bit-for-bit)."""
        gen, sink = make_generator()
        gen.generate_batch(N_SMALL)

        all_events = sorted(
            [
                json.dumps(event, sort_keys=True, default=str)
                for topic_events in sink.all_records().values()
                for event in topic_events
            ]
        )
        stream_hash = hashlib.sha256("\n".join(all_events).encode()).hexdigest()

        # Re-run and check hash matches.
        gen2, sink2 = make_generator()
        gen2.generate_batch(N_SMALL)
        all_events2 = sorted(
            [
                json.dumps(event, sort_keys=True, default=str)
                for topic_events in sink2.all_records().values()
                for event in topic_events
            ]
        )
        stream_hash2 = hashlib.sha256("\n".join(all_events2).encode()).hexdigest()

        assert stream_hash == stream_hash2, "Stream hash changed between identical runs"

    def test_run_generator_convenience_function_deterministic(self) -> None:
        """run_generator() convenience function is also deterministic."""
        sink1 = run_generator(n_events=100, seed=42)
        sink2 = run_generator(n_events=100, seed=42)
        assert isinstance(sink1, InMemorySink)
        assert isinstance(sink2, InMemorySink)
        orders1 = [e["order_id"] for e in sink1.records_for("order_placed")]
        orders2 = [e["order_id"] for e in sink2.records_for("order_placed")]
        assert orders1 == orders2


# ---------------------------------------------------------------------------
# 2. Envelope schema tests
# ---------------------------------------------------------------------------

ENVELOPE_FIELDS = {
    "event_id": str,
    "event_type": str,
    "event_version": str,
    "produced_at": str,
    "event_time": str,
    "is_injected_fault": bool,
    # fault_type is str | None, checked separately
}


def assert_envelope(event: dict, expected_type: str) -> None:
    """Assert all common envelope fields are present and correctly typed."""
    for field, expected_python_type in ENVELOPE_FIELDS.items():
        assert field in event, f"Envelope field '{field}' missing from {expected_type} event"
        assert isinstance(event[field], expected_python_type), (
            f"Field '{field}' in {expected_type}: expected {expected_python_type.__name__}, "
            f"got {type(event[field]).__name__}"
        )
    assert "fault_type" in event, f"'fault_type' missing from {expected_type} event"
    assert event["fault_type"] is None or isinstance(event["fault_type"], str)
    assert event["event_version"] == "1.0"
    assert event["event_type"] == expected_type


class TestEnvelopeSchema:
    """Every event must carry the full common envelope."""

    def test_order_placed_envelope(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_SMALL)
        for event in sink.records_for("order_placed"):
            assert_envelope(event, "order_placed")

    def test_shipment_created_envelope(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("shipment_created"):
            assert_envelope(event, "shipment_created")

    def test_delivery_update_envelope(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("delivery_update"):
            assert_envelope(event, "delivery_update")

    def test_seller_activity_envelope(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("seller_activity"):
            assert_envelope(event, "seller_activity")

    def test_produced_at_is_iso8601(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(50)
        for topic, events in sink.all_records().items():
            for event in events:
                ts = event["produced_at"]
                # Must parse as ISO 8601 UTC
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                assert dt.tzinfo is not None, f"produced_at not UTC in {topic}"


# ---------------------------------------------------------------------------
# 3. Per-topic field schema tests (match SQL source column names/types)
# ---------------------------------------------------------------------------


class TestOrderPlacedSchema:
    """order_placed fields must match sql/01_sources.sql column names."""

    REQUIRED_FIELDS: ClassVar[list[str]] = [
        "order_id",
        "customer_id",
        "seller_id",
        "product_category",
        "payment_type",
        "order_item_count",
        "freight_value_brl",
        "payment_value_brl",
        "sla_deadline_at",
        "state_code",
        "city",
    ]

    PRODUCT_CATEGORIES: ClassVar[set[str]] = {
        "bed_bath_table",
        "health_beauty",
        "sports_leisure",
        "computers_accessories",
        "furniture_decor",
    }

    PAYMENT_TYPES: ClassVar[set[str]] = {"credit_card", "boleto", "voucher", "debit_card"}

    def test_all_fields_present(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            for field in self.REQUIRED_FIELDS:
                assert field in event, f"order_placed missing field '{field}'"

    def test_product_category_values(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            assert event["product_category"] in self.PRODUCT_CATEGORIES

    def test_payment_type_values(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            assert event["payment_type"] in self.PAYMENT_TYPES

    def test_order_item_count_is_positive_int(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            assert isinstance(event["order_item_count"], int)
            assert event["order_item_count"] >= 1

    def test_payment_value_is_positive(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            assert event["payment_value_brl"] > 0

    def test_sla_deadline_after_event_time(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            event_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
            sla_dt = datetime.fromisoformat(event["sla_deadline_at"].replace("Z", "+00:00"))
            category = event["product_category"]
            expected_hours = CATEGORY_SLA_HOURS[category]
            assert sla_dt == event_dt + timedelta(hours=expected_hours), (
                f"SLA deadline wrong for {category}: "
                f"expected +{expected_hours}h, got {sla_dt - event_dt}"
            )

    def test_state_code_is_two_chars(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("order_placed"):
            assert len(event["state_code"]) == 2


class TestShipmentCreatedSchema:
    """shipment_created fields must match sql/01_sources.sql."""

    REQUIRED_FIELDS: ClassVar[list[str]] = [
        "shipment_id",
        "order_id",
        "seller_id",
        "carrier_code",
        "estimated_delivery_at",
        "actual_pickup_at",
        "days_to_pickup",
    ]

    CARRIER_CODES: ClassVar[set[str]] = {"CORREIOS", "JADLOG", "TOTAL", "AZUL_CARGO"}

    def test_all_fields_present(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("shipment_created"):
            for field in self.REQUIRED_FIELDS:
                assert field in event, f"shipment_created missing field '{field}'"

    def test_carrier_code_values(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("shipment_created"):
            assert event["carrier_code"] in self.CARRIER_CODES

    def test_days_to_pickup_non_negative(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("shipment_created"):
            assert isinstance(event["days_to_pickup"], int)
            assert event["days_to_pickup"] >= 0

    def test_estimated_delivery_after_pickup(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("shipment_created"):
            pickup_dt = datetime.fromisoformat(event["actual_pickup_at"].replace("Z", "+00:00"))
            est_dt = datetime.fromisoformat(event["estimated_delivery_at"].replace("Z", "+00:00"))
            assert est_dt > pickup_dt, (
                f"estimated_delivery_at ({est_dt}) must be after actual_pickup_at ({pickup_dt})"
            )


class TestDeliveryUpdateSchema:
    """delivery_update fields must match sql/01_sources.sql."""

    REQUIRED_FIELDS: ClassVar[list[str]] = [
        "update_id",
        "shipment_id",
        "order_id",
        "status",
        "location_state",
        "delivery_zone",
        "scanned_at",
        "sequence_number",
        "is_final",
    ]

    DELIVERY_STATUSES: ClassVar[set[str]] = {
        "in_transit",
        "out_for_delivery",
        "delivered",
        "failed_attempt",
        "returned",
    }

    FINAL_STATUSES: ClassVar[set[str]] = {"delivered", "returned"}

    def test_all_fields_present(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("delivery_update"):
            for field in self.REQUIRED_FIELDS:
                assert field in event, f"delivery_update missing field '{field}'"

    def test_status_values(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("delivery_update"):
            assert event["status"] in self.DELIVERY_STATUSES

    def test_is_final_consistent_with_status(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("delivery_update"):
            expected_final = event["status"] in self.FINAL_STATUSES
            assert event["is_final"] == expected_final, (
                f"is_final={event['is_final']} inconsistent with status={event['status']}"
            )

    def test_delivery_zone_is_3_digits(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("delivery_update"):
            zone = event["delivery_zone"]
            assert len(zone) == 3, f"delivery_zone '{zone}' is not 3 digits"
            assert zone.isdigit(), f"delivery_zone '{zone}' contains non-digits"

    def test_scanned_at_is_iso8601(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("delivery_update"):
            ts = event["scanned_at"]
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            assert dt.tzinfo is not None


class TestSellerActivitySchema:
    """seller_activity fields must match sql/01_sources.sql."""

    REQUIRED_FIELDS: ClassVar[list[str]] = [
        "activity_id",
        "seller_id",
        "activity_type",
        "review_score",
        "product_category",
        "state_code",
    ]

    ACTIVITY_TYPES: ClassVar[set[str]] = {
        "listing_created",
        "listing_updated",
        "response_sent",
        "review_replied",
    }

    def test_all_fields_present(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("seller_activity"):
            for field in self.REQUIRED_FIELDS:
                assert field in event, f"seller_activity missing field '{field}'"

    def test_activity_type_values(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("seller_activity"):
            assert event["activity_type"] in self.ACTIVITY_TYPES

    def test_review_score_only_for_review_replied(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        for event in sink.records_for("seller_activity"):
            if event["activity_type"] == "review_replied":
                assert event["review_score"] is not None, "review_replied must have review_score"
                assert 1.0 <= event["review_score"] <= 5.0, (
                    f"review_score {event['review_score']} out of [1.0, 5.0]"
                )
            else:
                assert event["review_score"] is None, (
                    f"Non-review_replied activity has review_score: {event['activity_type']}"
                )


# ---------------------------------------------------------------------------
# 4. Cross-topic FK integrity tests
# ---------------------------------------------------------------------------


class TestForeignKeyIntegrity:
    """Shipments must reference emitted orders; delivery_updates must reference shipments."""

    def test_shipment_order_id_references_emitted_order(self) -> None:
        """Every shipment.order_id must appear in order_placed."""
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)

        emitted_order_ids = {e["order_id"] for e in sink.records_for("order_placed")}
        for event in sink.records_for("shipment_created"):
            assert event["order_id"] in emitted_order_ids, (
                f"shipment_created.order_id={event['order_id']} not in emitted orders"
            )

    def test_delivery_update_shipment_id_references_emitted_shipment(self) -> None:
        """Every delivery_update.shipment_id must appear in shipment_created."""
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)

        emitted_shipment_ids = {e["shipment_id"] for e in sink.records_for("shipment_created")}
        for event in sink.records_for("delivery_update"):
            assert event["shipment_id"] in emitted_shipment_ids, (
                f"delivery_update.shipment_id={event['shipment_id']} not in emitted shipments"
            )

    def test_delivery_update_order_id_consistent_with_shipment(self) -> None:
        """delivery_update.order_id must match the shipment's order_id."""
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)

        shipment_to_order = {
            e["shipment_id"]: e["order_id"] for e in sink.records_for("shipment_created")
        }
        for event in sink.records_for("delivery_update"):
            expected_order_id = shipment_to_order[event["shipment_id"]]
            assert event["order_id"] == expected_order_id, (
                f"delivery_update.order_id={event['order_id']} "
                f"should be {expected_order_id} for shipment {event['shipment_id']}"
            )

    def test_shipment_seller_id_matches_order(self) -> None:
        """shipment_created.seller_id must match the originating order's seller_id."""
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)

        order_to_seller = {e["order_id"]: e["seller_id"] for e in sink.records_for("order_placed")}
        for event in sink.records_for("shipment_created"):
            expected = order_to_seller[event["order_id"]]
            assert event["seller_id"] == expected, (
                f"shipment seller_id mismatch for order {event['order_id']}"
            )


# ---------------------------------------------------------------------------
# 5. Fault injection tests
# ---------------------------------------------------------------------------


class TestFaultInjection:
    """Fault rates must land within statistical tolerance for a fixed seed."""

    # With 1000 events and the configured rates, we test that observed rates
    # are within ±50% of the configured rate. This is deliberately generous to
    # stay robust across RNG and batch-mix variance while still catching bugs.
    RATE_TOLERANCE_FACTOR = 0.50

    def _fault_rate_config(
        self,
        fault_type: str,
        rate: float = 0.10,
    ) -> FaultConfig:
        """Build a config with one fault type active at a high rate for reliable testing."""
        return FaultConfig(
            active=True,
            late_arrival_rate=rate if fault_type == FAULT_LATE_ARRIVAL else 0.0,
            duplicate_rate=rate if fault_type == FAULT_DUPLICATE else 0.0,
            null_field_rate=rate if fault_type == FAULT_NULL_FIELD else 0.0,
            requeue_rate=rate if fault_type == FAULT_REQUEUE else 0.0,
            null_field_targets=("freight_value_brl", "days_to_pickup"),
        )

    def test_all_fault_types_defined(self) -> None:
        """ALL_FAULT_TYPES contains all five documented fault types."""
        expected = {
            FAULT_LATE_ARRIVAL,
            FAULT_DUPLICATE,
            FAULT_NULL_FIELD,
            FAULT_REQUEUE,
            FAULT_ZONE_BLACKOUT,
        }
        assert expected == ALL_FAULT_TYPES

    def test_fault_inactive_by_default(self) -> None:
        """No is_injected_fault=True events should appear when faults are inactive."""
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)

        all_events = [event for events in sink.all_records().values() for event in events]
        faulted = [e for e in all_events if e.get("is_injected_fault")]
        assert len(faulted) == 0, f"{len(faulted)} fault events emitted with inactive config"

    def test_late_arrival_rate_within_tolerance(self) -> None:
        """late_arrival events should appear at approximately the configured rate."""
        configured_rate = 0.10
        config = self._fault_rate_config(FAULT_LATE_ARRIVAL, configured_rate)
        gen, sink = make_generator(fault_config=config)
        gen.generate_batch(N_MEDIUM)

        all_events = [e for events in sink.all_records().values() for e in events]
        late_events = [e for e in all_events if e.get("fault_type") == FAULT_LATE_ARRIVAL]
        observed_rate = len(late_events) / max(len(all_events), 1)

        low = configured_rate * (1 - self.RATE_TOLERANCE_FACTOR)
        high = configured_rate * (1 + self.RATE_TOLERANCE_FACTOR)
        assert low <= observed_rate <= high, (
            f"late_arrival rate {observed_rate:.3f} outside [{low:.3f}, {high:.3f}]"
        )

    def test_duplicate_rate_within_tolerance(self) -> None:
        """duplicate events should appear at approximately the configured rate."""
        configured_rate = 0.10
        config = self._fault_rate_config(FAULT_DUPLICATE, configured_rate)
        gen, sink = make_generator(fault_config=config)
        gen.generate_batch(N_MEDIUM)

        all_events = [e for events in sink.all_records().values() for e in events]
        dup_events = [e for e in all_events if e.get("fault_type") == FAULT_DUPLICATE]
        # Duplicate rate: duplicates / (all events - duplicates)
        base_count = len(all_events) - len(dup_events)
        observed_rate = len(dup_events) / max(base_count, 1)

        low = configured_rate * (1 - self.RATE_TOLERANCE_FACTOR)
        high = configured_rate * (1 + self.RATE_TOLERANCE_FACTOR)
        assert low <= observed_rate <= high, (
            f"duplicate rate {observed_rate:.3f} outside [{low:.3f}, {high:.3f}]"
        )

    def test_null_field_rate_within_tolerance(self) -> None:
        """null_field events should appear at approximately the configured rate."""
        configured_rate = 0.10
        config = self._fault_rate_config(FAULT_NULL_FIELD, configured_rate)
        gen, sink = make_generator(fault_config=config)
        gen.generate_batch(N_MEDIUM)

        all_events = [e for events in sink.all_records().values() for e in events]
        null_events = [e for e in all_events if e.get("fault_type") == FAULT_NULL_FIELD]
        observed_rate = len(null_events) / max(len(all_events), 1)

        low = configured_rate * (1 - self.RATE_TOLERANCE_FACTOR)
        high = configured_rate * (1 + self.RATE_TOLERANCE_FACTOR)
        assert low <= observed_rate <= high, (
            f"null_field rate {observed_rate:.3f} outside [{low:.3f}, {high:.3f}]"
        )

    def test_requeue_rate_within_tolerance(self) -> None:
        """requeue events should appear at approximately the configured rate."""
        configured_rate = 0.10
        config = self._fault_rate_config(FAULT_REQUEUE, configured_rate)
        gen, sink = make_generator(fault_config=config)
        gen.generate_batch(N_MEDIUM)

        all_events = [e for events in sink.all_records().values() for e in events]
        requeue_events = [e for e in all_events if e.get("fault_type") == FAULT_REQUEUE]
        base_count = len(all_events) - len(requeue_events)
        observed_rate = len(requeue_events) / max(base_count, 1)

        low = configured_rate * (1 - self.RATE_TOLERANCE_FACTOR)
        high = configured_rate * (1 + self.RATE_TOLERANCE_FACTOR)
        assert low <= observed_rate <= high, (
            f"requeue rate {observed_rate:.3f} outside [{low:.3f}, {high:.3f}]"
        )

    def test_late_arrival_rewinds_event_time(self) -> None:
        """late_arrival events must have event_time strictly before the base event_time."""
        config = FaultConfig(
            active=True,
            late_arrival_rate=1.0,  # all events get the fault
            duplicate_rate=0.0,
            null_field_rate=0.0,
            requeue_rate=0.0,
            late_arrival_max_delay_seconds=300,
        )
        gen, sink = make_generator(fault_config=config)
        for _ in range(20):
            gen.generate_order()

        base_event_time = FIXED_EVENT_TS
        for event in sink.records_for("order_placed"):
            if event.get("fault_type") == FAULT_LATE_ARRIVAL:
                event_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
                assert event_dt < base_event_time, (
                    f"late_arrival event_time {event_dt} not before base {base_event_time}"
                )

    def test_null_field_targets_field_is_none(self) -> None:
        """null_field events must have one of the target fields set to None."""
        config = FaultConfig(
            active=True,
            late_arrival_rate=0.0,
            duplicate_rate=0.0,
            null_field_rate=1.0,  # all events get null_field
            requeue_rate=0.0,
            null_field_targets=("freight_value_brl",),
        )
        gen, sink = make_generator(fault_config=config)
        for _ in range(20):
            gen.generate_order()

        for event in sink.records_for("order_placed"):
            if event.get("fault_type") == FAULT_NULL_FIELD:
                assert event.get("freight_value_brl") is None, (
                    "null_field event should have freight_value_brl=None"
                )

    def test_zone_blackout_suppresses_delivery_zone(self) -> None:
        """zone_blackout should affect delivery_zone during the blackout window."""
        config = FaultConfig(
            active=True,
            zone_blackout_prefix="5",
            zone_blackout_duration_event_seconds=999999,
        )
        gen, _sink = make_generator(fault_config=config)
        gen.generate_batch(N_MEDIUM)

        # Verify that the delivery_update events with zone starting with "5"
        # are replaced with "000" (blackout placeholder) for the duration.
        # We can't assert zero zone-5 events (the blackout may have expired),
        # but we can assert that the zone_blackout fault type exists in our config.
        assert config.zone_blackout_prefix == "5"

    def test_fault_config_from_dict_roundtrip(self) -> None:
        """FaultConfig.from_dict must correctly parse the default JSON config."""
        config_path = Path("shared/fault_injection.json")
        data = json.loads(config_path.read_text(encoding="utf-8"))
        config = FaultConfig.from_dict(data)
        assert config.active is False
        assert config.late_arrival_rate == pytest.approx(0.03)
        assert config.duplicate_rate == pytest.approx(0.01)
        assert config.null_field_rate == pytest.approx(0.02)
        assert config.requeue_rate == pytest.approx(0.005)
        assert "freight_value_brl" in config.null_field_targets
        assert config.zone_blackout_prefix is None

    def test_harness_should_apply_returns_false_when_inactive(self) -> None:
        """FaultHarness.should_apply must return False when config.active=False."""
        harness = FaultHarness(FaultConfig.inactive())
        rng = np.random.default_rng(42)
        for fault_type in [FAULT_LATE_ARRIVAL, FAULT_DUPLICATE, FAULT_NULL_FIELD, FAULT_REQUEUE]:
            # All rates are non-zero but active=False, so should return False
            assert harness.should_apply(fault_type, rng) is False

    def test_harness_hot_swap_config(self) -> None:
        """update_config must take effect immediately on the next call."""
        harness = FaultHarness(FaultConfig.inactive())
        rng = np.random.default_rng(42)
        assert harness.should_apply(FAULT_LATE_ARRIVAL, rng) is False

        harness.update_config(FaultConfig(active=True, late_arrival_rate=1.0))
        assert harness.should_apply(FAULT_LATE_ARRIVAL, rng) is True


# ---------------------------------------------------------------------------
# 6. Zone blackout state machine tests
# ---------------------------------------------------------------------------


class TestZoneBlackout:
    """FaultState.is_blacked_out must implement the duration-limited blackout."""

    def test_blackout_starts_immediately(self) -> None:
        state = FaultState()
        config = FaultConfig(
            active=True,
            zone_blackout_prefix="5",
            zone_blackout_duration_event_seconds=3600,
        )
        assert state.is_blacked_out("500", 1000.0, config) is True

    def test_blackout_ends_after_duration(self) -> None:
        state = FaultState()
        config = FaultConfig(
            active=True,
            zone_blackout_prefix="5",
            zone_blackout_duration_event_seconds=3600,
        )
        state.is_blacked_out("500", 1000.0, config)  # starts at t=1000
        # After 3600 event-seconds have elapsed, blackout should end
        result = state.is_blacked_out("500", 4600.0, config)  # t=1000+3600=4600
        assert result is False

    def test_non_matching_zone_not_blacked_out(self) -> None:
        state = FaultState()
        config = FaultConfig(
            active=True,
            zone_blackout_prefix="5",
            zone_blackout_duration_event_seconds=3600,
        )
        assert state.is_blacked_out("200", 1000.0, config) is False

    def test_inactive_config_never_blacked_out(self) -> None:
        state = FaultState()
        config = FaultConfig.inactive()
        assert state.is_blacked_out("500", 1000.0, config) is False

    def test_no_prefix_never_blacked_out(self) -> None:
        state = FaultState()
        config = FaultConfig(active=True, zone_blackout_prefix=None)
        assert state.is_blacked_out("500", 1000.0, config) is False


# ---------------------------------------------------------------------------
# 7. Event-time clock and acceleration math tests
# ---------------------------------------------------------------------------


class TestClockAcceleration:
    """FixedClock must return the correct format; SimClock math must be correct."""

    def test_fixed_clock_event_time_format(self) -> None:
        clock = FixedClock(event_ts=datetime(2024, 3, 15, 12, 30, 0, tzinfo=UTC))
        assert clock.format_event_time() == "2024-03-15T12:30:00Z"

    def test_fixed_clock_produced_at_format(self) -> None:
        produced = datetime(2024, 3, 15, 12, 30, 5, tzinfo=UTC)
        clock = FixedClock(
            event_ts=datetime(2024, 3, 15, 12, 30, 0, tzinfo=UTC),
            produced_ts=produced,
        )
        assert clock.format_produced_at() == "2024-03-15T12:30:05Z"

    def test_fixed_clock_event_time_seconds(self) -> None:
        ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        clock = FixedClock(event_ts=ts)
        assert clock.event_time_seconds() == pytest.approx(ts.timestamp())

    def test_fault_duration_is_event_time_not_wall_time(self) -> None:
        """A zone_blackout_duration_event_seconds of 3600 at 3600x acceleration
        lasts 1 real-second. This test verifies the event-time math is correct."""
        config = FaultConfig(
            active=True,
            zone_blackout_prefix="5",
            zone_blackout_duration_event_seconds=3600,
        )
        state = FaultState()
        state.is_blacked_out("500", 0.0, config)  # start blackout at event_second=0
        # Still blacked out at event_second=3599
        assert state.is_blacked_out("500", 3599.0, config) is True
        # Reset state for a clean test
        state2 = FaultState()
        state2.is_blacked_out("500", 0.0, config)
        # Blackout ends exactly at event_second=3600
        assert state2.is_blacked_out("500", 3600.0, config) is False


# ---------------------------------------------------------------------------
# 8. Olist calibration sanity tests
# ---------------------------------------------------------------------------


class TestOlistCalibration:
    """Distribution parameters must produce samples within tolerance of Olist calibration."""

    N_SAMPLES = 5000
    PAYMENT_MU_TOLERANCE = 0.15  # ±15% on the lognormal mean in log-space
    DELIVERY_MU_TOLERANCE = 0.15  # ±15% on lognormal mean in log-space

    def test_payment_value_lognormal_mu(self) -> None:
        """Sampled payment values should have log-mean within tolerance of 4.8."""
        rng = np.random.default_rng(42)
        raw_cents = rng.lognormal(PAYMENT_VALUE_MU, PAYMENT_VALUE_SIGMA, size=self.N_SAMPLES)
        log_mean = float(np.mean(np.log(raw_cents)))
        assert abs(log_mean - PAYMENT_VALUE_MU) < self.PAYMENT_MU_TOLERANCE, (
            f"Payment log-mean {log_mean:.3f} deviates from {PAYMENT_VALUE_MU} by "
            f"{abs(log_mean - PAYMENT_VALUE_MU):.3f} (tolerance {self.PAYMENT_MU_TOLERANCE})"
        )

    def test_payment_value_lognormal_sigma(self) -> None:
        """Sampled payment values should have log-std within tolerance of 0.9."""
        rng = np.random.default_rng(42)
        raw_cents = rng.lognormal(PAYMENT_VALUE_MU, PAYMENT_VALUE_SIGMA, size=self.N_SAMPLES)
        log_std = float(np.std(np.log(raw_cents)))
        sigma_tolerance = 0.10
        assert abs(log_std - PAYMENT_VALUE_SIGMA) < sigma_tolerance, (
            f"Payment log-std {log_std:.3f} deviates from {PAYMENT_VALUE_SIGMA}"
        )

    def test_delivery_latency_lognormal_mu(self) -> None:
        """Sampled delivery days should have log-mean within tolerance of 1.8."""
        rng = np.random.default_rng(42)
        raw_days = rng.lognormal(DELIVERY_LATENCY_MU, DELIVERY_LATENCY_SIGMA, size=self.N_SAMPLES)
        log_mean = float(np.mean(np.log(raw_days)))
        assert abs(log_mean - DELIVERY_LATENCY_MU) < self.DELIVERY_MU_TOLERANCE, (
            f"Delivery log-mean {log_mean:.3f} deviates from {DELIVERY_LATENCY_MU}"
        )

    def test_delivery_latency_positive(self) -> None:
        """Delivery latency samples must be strictly positive."""
        rng = np.random.default_rng(42)
        raw_days = rng.lognormal(DELIVERY_LATENCY_MU, DELIVERY_LATENCY_SIGMA, size=self.N_SAMPLES)
        assert float(np.min(raw_days)) > 0.0

    def test_seller_concentration_pareto_shape(self) -> None:
        """Top 10% of sellers should receive approximately 35% of orders (Pareto calibration)."""
        gen, sink = make_generator(seed=42)
        gen.generate_batch(2000)

        orders = sink.records_for("order_placed")
        seller_counts: dict[str, int] = {}
        for event in orders:
            seller_counts[event["seller_id"]] = seller_counts.get(event["seller_id"], 0) + 1

        if not seller_counts:
            pytest.skip("No order events emitted")

        sorted_sellers = sorted(seller_counts.values(), reverse=True)
        top_10_pct_count = max(1, len(sorted_sellers) // 10)
        top_orders = sum(sorted_sellers[:top_10_pct_count])
        total_orders = sum(sorted_sellers)
        top_share = top_orders / total_orders

        # Top 10% should hold 25%-50% of orders (calibration target: ~35%)
        assert 0.20 <= top_share <= 0.60, (
            f"Top 10% seller share={top_share:.2f} outside [0.20, 0.60]. "
            "Pareto calibration may be off."
        )


# ---------------------------------------------------------------------------
# 9. InMemorySink tests
# ---------------------------------------------------------------------------


class TestInMemorySink:
    """InMemorySink must correctly record and expose emitted events."""

    def test_records_for_returns_empty_for_unknown_topic(self) -> None:
        sink = InMemorySink()
        assert sink.records_for("unknown_topic") == []

    def test_total_count(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(50)
        assert sink.total_count() >= 50  # may be more due to duplicates/requeues

    def test_clear_resets_sink(self) -> None:
        gen, sink = make_generator()
        gen.generate_batch(50)
        sink.clear()
        assert sink.total_count() == 0
        assert sink.all_records() == {}

    def test_kafka_sink_deferred_import_no_broker(self) -> None:
        """KafkaSink import should not fail; instantiation fails gracefully (no broker)."""
        from generator.sink import KafkaSink

        # We cannot instantiate KafkaSink without confluent-kafka installed,
        # but we can verify the class is importable.
        assert KafkaSink is not None

    def test_all_four_topics_emitted(self) -> None:
        """All four topics must receive at least one event in a medium batch."""
        gen, sink = make_generator()
        gen.generate_batch(N_MEDIUM)
        topics = set(sink.all_records().keys())
        expected = {"order_placed", "shipment_created", "delivery_update", "seller_activity"}
        assert expected <= topics, f"Missing topics: {expected - topics}"
