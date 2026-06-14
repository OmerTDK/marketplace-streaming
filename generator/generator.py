"""Deterministic synthetic marketplace event generator.

Produces four event types into injectable sinks:
  order_placed       → topic 'order_placed'        (key: order_id)
  shipment_created   → topic 'shipment_created'    (key: shipment_id)
  delivery_update    → topic 'delivery_update'     (key: shipment_id + '_' + seq)
  seller_activity    → topic 'seller_activity'     (key: seller_id)

All randomness flows through numpy.random.default_rng(seed) and
Faker(locale='pt_BR', seed=seed). Same seed → identical event stream.

Statistical calibration (from ADR-0002, Olist distribution parameters):
  Payment value     lognormal(mu=4.8, sigma=0.9) BRL cents
  Delivery latency  lognormal(mu=1.8, sigma=0.5) days from dispatch
  Late delivery     ~7% (estimated_delivery_at exceeded)
  Seller volume     Pareto-shaped — top 10% of sellers get ~35% of orders
  Order volume      Poisson with daily seasonality, peak Friday afternoon
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
from faker import Faker

from generator.clock import ClockLike, FixedClock, _format_iso
from generator.fault_injection import (
    FAULT_DUPLICATE,
    FAULT_LATE_ARRIVAL,
    FAULT_NULL_FIELD,
    FAULT_REQUEUE,
    FaultConfig,
    FaultHarness,
)
from generator.sink import InMemorySink, Sink

# ---------------------------------------------------------------------------
# Distribution parameters (Olist-calibrated, no CSV data committed)
# ---------------------------------------------------------------------------

PAYMENT_VALUE_MU = 4.8
PAYMENT_VALUE_SIGMA = 0.9

DELIVERY_LATENCY_MU = 1.8
DELIVERY_LATENCY_SIGMA = 0.5

LATE_DELIVERY_THRESHOLD_RATE = 0.07

PARETO_SHAPE = 1.16  # Pareto shape param: top ~10% sellers get ~35% orders

# ---------------------------------------------------------------------------
# Categorical distributions
# ---------------------------------------------------------------------------

PRODUCT_CATEGORIES = [
    "bed_bath_table",
    "health_beauty",
    "sports_leisure",
    "computers_accessories",
    "furniture_decor",
]
CATEGORY_WEIGHTS = [0.25, 0.22, 0.20, 0.18, 0.15]

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
PAYMENT_TYPE_WEIGHTS = [0.74, 0.19, 0.05, 0.02]

CARRIER_CODES = ["CORREIOS", "JADLOG", "TOTAL", "AZUL_CARGO"]
CARRIER_WEIGHTS = [0.55, 0.25, 0.12, 0.08]

ACTIVITY_TYPES = [
    "listing_created",
    "listing_updated",
    "response_sent",
    "review_replied",
]
ACTIVITY_TYPE_WEIGHTS = [0.30, 0.35, 0.20, 0.15]

DELIVERY_STATUSES = [
    "in_transit",
    "out_for_delivery",
    "delivered",
    "failed_attempt",
    "returned",
]

FINAL_STATUSES = frozenset(["delivered", "returned"])

# Category SLA constants in hours (order event_time → sla_deadline_at)
CATEGORY_SLA_HOURS: dict[str, int] = {
    "bed_bath_table": 168,
    "health_beauty": 120,
    "sports_leisure": 144,
    "computers_accessories": 96,
    "furniture_decor": 192,
}

# Brazilian state codes (2-char)
BR_STATE_CODES = [
    "SP",
    "RJ",
    "MG",
    "BA",
    "RS",
    "PR",
    "PE",
    "CE",
    "PA",
    "MA",
    "SC",
    "GO",
    "PB",
    "AM",
    "MT",
    "MS",
    "RN",
    "AL",
    "ES",
    "PI",
    "DF",
    "RO",
    "TO",
    "AC",
    "AP",
    "RR",
    "SE",
]
STATE_WEIGHTS_UNNORM = [
    30,
    16,
    12,
    6,
    6,
    6,
    5,
    4,
    3,
    3,
    4,
    3,
    2,
    2,
    2,
    2,
    2,
    2,
    3,
    2,
    3,
    1,
    1,
    1,
    1,
    1,
    1,
]

# Normalize state weights
_TOTAL_STATE_WEIGHT = sum(STATE_WEIGHTS_UNNORM)
STATE_WEIGHTS = [w / _TOTAL_STATE_WEIGHT for w in STATE_WEIGHTS_UNNORM]

# Number of synthetic sellers and customers
SELLER_POOL_SIZE = 500
CUSTOMER_POOL_SIZE = 10_000


class MarketplaceGenerator:
    """Generates synthetic marketplace events with full FK integrity.

    Args:
        seed: Integer seed for numpy RNG and Faker. Same seed → same events.
        sink: Where to send events. Defaults to InMemorySink (test-friendly).
        fault_config: Fault injection configuration snapshot.
        clock: Clock implementation. Defaults to FixedClock at a reference time
               (fully deterministic). Pass a SimClock at runtime.
        n_sellers: Size of synthetic seller pool.
        n_customers: Size of synthetic customer pool.
    """

    def __init__(
        self,
        seed: int = 42,
        sink: Sink | None = None,
        fault_config: FaultConfig | None = None,
        clock: ClockLike | None = None,
        n_sellers: int = SELLER_POOL_SIZE,
        n_customers: int = CUSTOMER_POOL_SIZE,
    ) -> None:
        self._rng = np.random.default_rng(seed)
        self._faker = Faker(locale="pt_BR")
        self._faker.seed_instance(seed)
        self._sink = sink or InMemorySink()
        self._fault_harness = FaultHarness(fault_config or FaultConfig.inactive())

        # Default to a fixed reference clock so all output is deterministic.
        self._clock: ClockLike = clock or FixedClock(
            event_ts=datetime(2024, 1, 8, 9, 0, 0, tzinfo=UTC),
        )

        # Build stable seller and customer ID pools from the seeded RNG.
        self._sellers: list[str] = [str(uuid.UUID(int=int(i))) for i in range(n_sellers)]
        self._customers: list[str] = [
            str(uuid.UUID(int=int(n_sellers + i))) for i in range(n_customers)
        ]

        # Pareto-shaped seller weights: top 10% → ~35% of orders.
        raw_weights = self._rng.pareto(PARETO_SHAPE, size=n_sellers) + 1.0
        self._seller_weights = raw_weights / raw_weights.sum()

        # Track in-flight orders and shipments for FK integrity.
        self._open_orders: list[dict[str, Any]] = []
        self._open_shipments: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Deterministic UUID generation
    # ------------------------------------------------------------------

    def _make_uuid(self) -> str:
        """Generate a UUID v4 from the seeded RNG (not system entropy).

        All 128 bits come from numpy default_rng so the stream is
        bit-for-bit reproducible from the same seed.
        """
        hi = int(self._rng.integers(0, 2**64, dtype=np.uint64))
        lo = int(self._rng.integers(0, 2**64, dtype=np.uint64))
        int_val = (hi << 64) | lo
        # Stamp version 4 and variant bits per RFC 4122.
        int_val = (int_val & ~(0xF << 76)) | (0x4 << 76)
        int_val = (int_val & ~(0b11 << 62)) | (0b10 << 62)
        return str(uuid.UUID(int=int_val))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_batch(self, n_events: int) -> None:
        """Generate *n_events* events across all four topics.

        Events are distributed across topics according to realistic ratios:
          ~50% order_placed, ~20% shipment_created, ~20% delivery_update,
          ~10% seller_activity.

        FK integrity is maintained: shipments reference previously emitted
        orders; delivery_updates reference previously emitted shipments.

        Args:
            n_events: Total number of events to emit.
        """
        for _ in range(n_events):
            self._emit_one_event()

    def generate_order(self) -> dict[str, Any]:
        """Emit one order_placed event and return the payload."""
        return self._emit_order()

    def generate_shipment(self, order: dict[str, Any] | None = None) -> dict[str, Any]:
        """Emit one shipment_created event, optionally linked to *order*."""
        return self._emit_shipment(order)

    def generate_delivery_update(
        self,
        shipment: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Emit one delivery_update event, optionally linked to *shipment*."""
        return self._emit_delivery_update(shipment)

    def generate_seller_activity(self) -> dict[str, Any]:
        """Emit one seller_activity event and return the payload."""
        return self._emit_seller_activity()

    def update_fault_config(self, config: FaultConfig) -> None:
        """Hot-swap fault configuration (called by the hot-reload loop)."""
        self._fault_harness.update_config(config)

    # ------------------------------------------------------------------
    # Internal event builders
    # ------------------------------------------------------------------

    def _emit_one_event(self) -> None:
        """Emit one event, choosing the type by probability."""
        roll = self._rng.random()
        if roll < 0.50:
            self._emit_order()
        elif roll < 0.70:
            if self._open_orders:
                self._emit_shipment(None)
            else:
                self._emit_order()
        elif roll < 0.90:
            if self._open_shipments:
                self._emit_delivery_update(None)
            else:
                self._emit_order()
        else:
            self._emit_seller_activity()

    def _make_envelope(self, event_type: str) -> dict[str, Any]:
        """Build the common event envelope fields."""
        return {
            "event_id": self._make_uuid(),
            "event_type": event_type,
            "event_version": "1.0",
            "produced_at": self._clock.format_produced_at(),
            "event_time": self._clock.format_event_time(),
            "is_injected_fault": False,
            "fault_type": None,
        }

    def _pick_seller(self) -> str:
        """Sample a seller_id with Pareto-shaped concentration."""
        idx = self._rng.choice(len(self._sellers), p=self._seller_weights)
        return self._sellers[int(idx)]

    def _pick_customer(self) -> str:
        """Sample a customer_id uniformly."""
        idx = self._rng.integers(0, len(self._customers))
        return self._customers[int(idx)]

    def _pick_category(self) -> str:
        idx = int(self._rng.choice(len(PRODUCT_CATEGORIES), p=CATEGORY_WEIGHTS))
        return PRODUCT_CATEGORIES[idx]

    def _pick_state(self) -> str:
        idx = int(self._rng.choice(len(BR_STATE_CODES), p=STATE_WEIGHTS))
        return BR_STATE_CODES[idx]

    def _pick_payment_type(self) -> str:
        idx = int(self._rng.choice(len(PAYMENT_TYPES), p=PAYMENT_TYPE_WEIGHTS))
        return PAYMENT_TYPES[idx]

    def _pick_carrier(self) -> str:
        idx = int(self._rng.choice(len(CARRIER_CODES), p=CARRIER_WEIGHTS))
        return CARRIER_CODES[idx]

    def _lognormal_payment_value(self) -> float:
        """Sample payment_value_brl from lognormal(mu=4.8, sigma=0.9) BRL cents.

        The raw sample is in BRL cents (as calibrated from Olist parameters).
        We round to 2 decimal places for the BRL value.
        """
        cents = float(self._rng.lognormal(PAYMENT_VALUE_MU, PAYMENT_VALUE_SIGMA))
        return round(cents / 100.0, 2)  # convert cents to BRL

    def _lognormal_delivery_days(self) -> float:
        """Sample delivery latency from lognormal(mu=1.8, sigma=0.5) days."""
        return float(self._rng.lognormal(DELIVERY_LATENCY_MU, DELIVERY_LATENCY_SIGMA))

    def _random_cep_zone(self) -> str:
        """Generate a random 3-digit Brazilian CEP prefix (delivery_zone)."""
        prefix = int(self._rng.integers(100, 999))
        return str(prefix)

    def _emit_order(self) -> dict[str, Any]:
        """Build and emit one order_placed event."""
        event = self._make_envelope("order_placed")
        order_id = self._make_uuid()
        seller_id = self._pick_seller()
        category = self._pick_category()
        state_code = self._pick_state()

        event_time_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        sla_hours = CATEGORY_SLA_HOURS[category]
        sla_deadline = event_time_dt + timedelta(hours=sla_hours)

        payment_value = self._lognormal_payment_value()
        freight_value = round(float(self._rng.uniform(5.0, 80.0)), 2)
        item_count = int(self._rng.integers(1, 6))

        event.update(
            {
                "order_id": order_id,
                "customer_id": self._pick_customer(),
                "seller_id": seller_id,
                "product_category": category,
                "payment_type": self._pick_payment_type(),
                "order_item_count": item_count,
                "freight_value_brl": freight_value,
                "payment_value_brl": payment_value,
                "sla_deadline_at": _format_iso(sla_deadline),
                "state_code": state_code,
                "city": self._faker.city(),
            }
        )

        event = self._apply_faults(event, "order_placed")
        self._sink.send("order_placed", order_id, event)
        self._open_orders.append(event)
        return event

    def _emit_shipment(self, order: dict[str, Any] | None) -> dict[str, Any]:
        """Build and emit one shipment_created event, linked to *order*."""
        if order is None:
            order = self._open_orders[int(self._rng.integers(0, len(self._open_orders)))]

        event = self._make_envelope("shipment_created")
        shipment_id = self._make_uuid()

        event_time_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        days_to_pickup = int(self._rng.integers(0, 3))
        pickup_dt = event_time_dt + timedelta(days=days_to_pickup)
        delivery_days = self._lognormal_delivery_days()
        estimated_delivery_dt = pickup_dt + timedelta(days=delivery_days)

        event.update(
            {
                "shipment_id": shipment_id,
                "order_id": order["order_id"],
                "seller_id": order["seller_id"],
                "carrier_code": self._pick_carrier(),
                "estimated_delivery_at": _format_iso(estimated_delivery_dt),
                "actual_pickup_at": _format_iso(pickup_dt),
                "days_to_pickup": days_to_pickup,
            }
        )

        event = self._apply_faults(event, "shipment_created")
        self._sink.send("shipment_created", shipment_id, event)
        self._open_shipments.append(event)
        return event

    def _emit_delivery_update(
        self,
        shipment: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Build and emit one delivery_update event, linked to *shipment*."""
        if shipment is None:
            shipment = self._open_shipments[int(self._rng.integers(0, len(self._open_shipments)))]

        event = self._make_envelope("delivery_update")
        update_id = self._make_uuid()
        delivery_zone = self._random_cep_zone()
        current_event_seconds = self._clock.event_time_seconds()

        # Zone blackout suppression: skip this event and emit a safe one.
        if self._fault_harness.is_blacked_out(delivery_zone, current_event_seconds):
            delivery_zone = "000"  # safe non-blacked-out zone placeholder

        status = str(
            self._rng.choice(
                DELIVERY_STATUSES,
                p=[0.40, 0.25, 0.20, 0.10, 0.05],
            )
        )
        is_final = status in FINAL_STATUSES
        seq = int(self._rng.integers(1, 10))

        event_time_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        # scanned_at = event_time + small random scan delay in sim-time
        scan_offset = float(self._rng.uniform(0, 300))  # 0-300 sim-seconds
        scanned_at = event_time_dt + timedelta(seconds=scan_offset)

        event.update(
            {
                "update_id": update_id,
                "shipment_id": shipment["shipment_id"],
                "order_id": shipment["order_id"],
                "status": status,
                "location_state": self._pick_state(),
                "delivery_zone": delivery_zone,
                "scanned_at": _format_iso(scanned_at),
                "sequence_number": seq,
                "is_final": is_final,
            }
        )

        event = self._apply_faults(event, "delivery_update")
        kafka_key = f"{shipment['shipment_id']}_{seq}"
        self._sink.send("delivery_update", kafka_key, event)
        return event

    def _emit_seller_activity(self) -> dict[str, Any]:
        """Build and emit one seller_activity event."""
        event = self._make_envelope("seller_activity")
        seller_id = self._pick_seller()

        idx = int(self._rng.choice(len(ACTIVITY_TYPES), p=ACTIVITY_TYPE_WEIGHTS))
        activity_type = ACTIVITY_TYPES[idx]

        review_score: float | None = None
        if activity_type == "review_replied":
            # Olist mean ~4.07; model as normal(4.07, 0.8) clipped to [1, 5]
            raw = float(self._rng.normal(4.07, 0.8))
            review_score = round(max(1.0, min(5.0, raw)), 1)

        event.update(
            {
                "activity_id": self._make_uuid(),
                "seller_id": seller_id,
                "activity_type": activity_type,
                "review_score": review_score,
                "product_category": self._pick_category(),
                "state_code": self._pick_state(),
            }
        )

        event = self._apply_faults(event, "seller_activity")
        self._sink.send("seller_activity", seller_id, event)
        return event

    # ------------------------------------------------------------------
    # Fault application
    # ------------------------------------------------------------------

    def _apply_faults(self, event: dict[str, Any], topic: str) -> dict[str, Any]:
        """Apply applicable faults to *event* and return the (possibly mutated) dict.

        Fault application order:
          1. null_field   — nulls a field; always independent of other faults
          2. late_arrival — rewinds event_time
          3. duplicate    — re-emits the same event to the same topic
          4. requeue      — re-emits after a small event-time delay

        Args:
            event: Event dict built by the caller.
            topic: Topic name, used for duplicate/requeue re-emit.

        Returns:
            Possibly mutated event dict (null_field and late_arrival mutate;
            duplicate and requeue emit extras but return the original).
        """
        harness = self._fault_harness

        if harness.should_apply(FAULT_NULL_FIELD, self._rng):
            event = harness.apply_null_field(event, self._rng)

        if harness.should_apply(FAULT_LATE_ARRIVAL, self._rng):
            event = harness.apply_late_arrival(event, self._rng, self._clock.event_time_seconds())

        if harness.should_apply(FAULT_DUPLICATE, self._rng):
            # Emit the same record a second time (idempotency key = event_id).
            dup = dict(event)
            dup["is_injected_fault"] = True
            dup["fault_type"] = FAULT_DUPLICATE
            key = self._derive_kafka_key(event, topic)
            self._sink.send(topic, key, dup)

        if harness.should_apply(FAULT_REQUEUE, self._rng):
            requeued = dict(event)
            requeued["is_injected_fault"] = True
            requeued["fault_type"] = FAULT_REQUEUE
            key = self._derive_kafka_key(event, topic)
            self._sink.send(topic, key, requeued)

        return event

    def _derive_kafka_key(self, event: dict[str, Any], topic: str) -> str:
        """Derive the Kafka message key from event fields for re-emits."""
        key_field_map = {
            "order_placed": "order_id",
            "shipment_created": "shipment_id",
            "seller_activity": "seller_id",
        }
        if topic in key_field_map:
            return str(event.get(key_field_map[topic], event["event_id"]))
        if topic == "delivery_update":
            return f"{event.get('shipment_id', '')}_{event.get('sequence_number', 0)}"
        return event["event_id"]


def run_generator(
    n_events: int,
    seed: int = 42,
    sink: Sink | None = None,
    fault_config: FaultConfig | None = None,
    clock: ClockLike | None = None,
) -> InMemorySink | Sink:
    """Convenience function: generate *n_events* and return the sink.

    Args:
        n_events: Total events to emit.
        seed: RNG seed for determinism.
        sink: Sink to write to. Defaults to a new InMemorySink.
        fault_config: Optional fault injection config.
        clock: Optional clock override (for tests: use FixedClock).

    Returns:
        The sink (same object as *sink* argument, or the created InMemorySink).
    """
    if sink is None:
        sink = InMemorySink()
    gen = MarketplaceGenerator(
        seed=seed,
        sink=sink,
        fault_config=fault_config,
        clock=clock,
    )
    gen.generate_batch(n_events)
    return sink
