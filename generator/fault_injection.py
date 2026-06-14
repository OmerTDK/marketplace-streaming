"""Fault injection harness for the event generator.

Fault types (from ADR-0002):
  late_arrival  — rewind event_time by a random delay in EVENT-TIME seconds.
  duplicate     — emit the same event record a second time.
  null_field    — set one of the target fields to None.
  requeue       — re-emit the event after a short event-time delay.
  zone_blackout — suppress delivery_update events for a CEP prefix for
                  zone_blackout_duration_event_seconds event-time seconds.

All durations are in EVENT-TIME seconds so they compose correctly with
TIME_ACCELERATION_FACTOR (real-wall duration = event_duration / factor).

The harness is driven by a FaultConfig dataclass that can be built from
a dict (loaded from shared/fault_injection.json). Hot-reload of the file
is a runtime concern (main.py); the unit-tested core only takes a config object.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC
from typing import Any

FAULT_LATE_ARRIVAL = "late_arrival"
FAULT_DUPLICATE = "duplicate"
FAULT_NULL_FIELD = "null_field"
FAULT_REQUEUE = "requeue"
FAULT_ZONE_BLACKOUT = "zone_blackout"

ALL_FAULT_TYPES = frozenset(
    [FAULT_LATE_ARRIVAL, FAULT_DUPLICATE, FAULT_NULL_FIELD, FAULT_REQUEUE, FAULT_ZONE_BLACKOUT]
)


@dataclasses.dataclass(frozen=True)
class FaultConfig:
    """Immutable snapshot of the fault injection configuration.

    Durations are in event-time seconds. They interact with the generator's
    TIME_ACCELERATION_FACTOR: a zone_blackout_duration_event_seconds of 7200
    at 3600x acceleration lasts 2 real-seconds — observable in a fast demo.
    """

    active: bool = False
    late_arrival_rate: float = 0.03
    late_arrival_max_delay_seconds: int = 300
    duplicate_rate: float = 0.01
    null_field_rate: float = 0.02
    null_field_targets: tuple[str, ...] = ("freight_value_brl", "days_to_pickup")
    requeue_rate: float = 0.005
    zone_blackout_prefix: str | None = None
    zone_blackout_duration_event_seconds: int = 7200

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FaultConfig:
        """Build a FaultConfig from a parsed JSON dict.

        Unknown keys are silently ignored so the schema can be extended
        without breaking existing configs.
        """
        targets = data.get("null_field_targets", ("freight_value_brl", "days_to_pickup"))
        return cls(
            active=bool(data.get("active", False)),
            late_arrival_rate=float(data.get("late_arrival_rate", 0.03)),
            late_arrival_max_delay_seconds=int(data.get("late_arrival_max_delay_seconds", 300)),
            duplicate_rate=float(data.get("duplicate_rate", 0.01)),
            null_field_rate=float(data.get("null_field_rate", 0.02)),
            null_field_targets=tuple(targets),
            requeue_rate=float(data.get("requeue_rate", 0.005)),
            zone_blackout_prefix=data.get("zone_blackout_prefix"),
            zone_blackout_duration_event_seconds=int(
                data.get("zone_blackout_duration_event_seconds", 7200)
            ),
        )

    @classmethod
    def inactive(cls) -> FaultConfig:
        """Return a clean config with all faults disabled."""
        return cls(active=False)


@dataclasses.dataclass
class FaultState:
    """Mutable runtime state for the zone_blackout fault.

    The blackout starts when zone_blackout_prefix is set and active=True.
    It ends after zone_blackout_duration_event_seconds of event time has
    elapsed from the moment the blackout began.

    All other faults are stateless (per-event coin-flip); only zone_blackout
    requires tracking across events.
    """

    blackout_started_at_event_seconds: float | None = None

    def is_blacked_out(
        self,
        delivery_zone: str,
        current_event_seconds: float,
        config: FaultConfig,
    ) -> bool:
        """Return True if *delivery_zone* is currently blacked out.

        Args:
            delivery_zone: First 3 digits of the customer CEP.
            current_event_seconds: Current simulated event-time clock (seconds).
            config: Current fault configuration snapshot.
        """
        if not config.active or config.zone_blackout_prefix is None:
            self.blackout_started_at_event_seconds = None
            return False

        if not delivery_zone.startswith(config.zone_blackout_prefix):
            return False

        if self.blackout_started_at_event_seconds is None:
            self.blackout_started_at_event_seconds = current_event_seconds
            return True

        elapsed = current_event_seconds - self.blackout_started_at_event_seconds
        if elapsed >= config.zone_blackout_duration_event_seconds:
            self.blackout_started_at_event_seconds = None
            return False

        return True


class FaultHarness:
    """Applies fault injection rules to a candidate event dict.

    The harness is stateless except for zone_blackout tracking (FaultState).
    It uses the caller-supplied RNG for all coin-flips so determinism is
    preserved end-to-end.

    Args:
        config: Fault injection configuration.
        state: Mutable zone-blackout state (shared across calls).
    """

    def __init__(self, config: FaultConfig, state: FaultState | None = None) -> None:
        self._config = config
        self._state = state or FaultState()

    @property
    def config(self) -> FaultConfig:
        return self._config

    def update_config(self, config: FaultConfig) -> None:
        """Hot-swap configuration (called by the hot-reload loop in main.py)."""
        self._config = config

    def apply_late_arrival(
        self,
        event: dict[str, Any],
        rng: Any,
        event_time_seconds: float,
    ) -> dict[str, Any]:
        """Rewind event_time by a uniform random delay in event-time seconds.

        Args:
            event: Event dict to mutate (copy returned; original unchanged).
            rng: numpy default_rng instance.
            event_time_seconds: Current sim-clock value (seconds from epoch).

        Returns:
            Mutated copy with is_injected_fault=True, fault_type='late_arrival',
            and event_time rewound by up to late_arrival_max_delay_seconds.
        """
        from datetime import datetime

        delay = rng.integers(1, self._config.late_arrival_max_delay_seconds + 1)
        mutated = dict(event)
        original_seconds = event_time_seconds - delay
        mutated["event_time"] = datetime.fromtimestamp(original_seconds, tz=UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        mutated["is_injected_fault"] = True
        mutated["fault_type"] = FAULT_LATE_ARRIVAL
        return mutated

    def apply_null_field(
        self,
        event: dict[str, Any],
        rng: Any,
    ) -> dict[str, Any]:
        """Set one randomly chosen target field to None.

        Args:
            event: Event dict to mutate.
            rng: numpy default_rng instance.

        Returns:
            Mutated copy with one field nulled and fault markers set.
        """
        targets = [t for t in self._config.null_field_targets if t in event]
        if not targets:
            return event
        chosen = targets[rng.integers(0, len(targets))]
        mutated = dict(event)
        mutated[chosen] = None
        mutated["is_injected_fault"] = True
        mutated["fault_type"] = FAULT_NULL_FIELD
        return mutated

    def should_apply(self, fault_type: str, rng: Any) -> bool:
        """Return True with the configured probability for *fault_type*.

        Args:
            fault_type: One of the FAULT_* constants.
            rng: numpy default_rng instance.

        Returns:
            True if the fault should fire for this event.
        """
        if not self._config.active:
            return False
        rate_map = {
            FAULT_LATE_ARRIVAL: self._config.late_arrival_rate,
            FAULT_DUPLICATE: self._config.duplicate_rate,
            FAULT_NULL_FIELD: self._config.null_field_rate,
            FAULT_REQUEUE: self._config.requeue_rate,
        }
        rate = rate_map.get(fault_type, 0.0)
        return bool(rng.random() < rate)

    def is_blacked_out(
        self,
        delivery_zone: str,
        current_event_seconds: float,
    ) -> bool:
        """Delegate zone-blackout check to the FaultState."""
        return self._state.is_blacked_out(delivery_zone, current_event_seconds, self._config)
