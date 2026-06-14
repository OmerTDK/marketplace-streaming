"""Simulated event-time clock for the generator.

event_time = SIM_START + (real_elapsed * TIME_ACCELERATION_FACTOR)

At 3600x acceleration, 1 real-second = 1 sim-hour. A 10-minute demo
covers ~600 sim-hours (~25 days) of order lifecycle.

produced_at uses a separate injectable wall-clock callable (default: real UTC
time). Tests inject a fixed callable so produced_at is also deterministic.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any


def utc_now() -> datetime:
    """Real UTC wall clock. Replaced in tests with a fixed callable."""
    return datetime.now(tz=UTC)


class SimClock:
    """Deterministic simulation clock.

    Advances event_time in proportion to real elapsed time multiplied by
    the acceleration factor. produced_at uses the injected wall-clock
    callable, defaulting to real UTC time.

    Args:
        sim_start: Simulated start time (ISO 8601 string or datetime).
        acceleration_factor: How many sim-seconds per real-second.
        wall_clock: Callable returning a datetime for produced_at.
                    Inject a fixed callable in tests for full determinism.
        real_start: Real wall-clock start time. Defaults to now.
    """

    def __init__(
        self,
        sim_start: str | datetime,
        acceleration_factor: float = 3600.0,
        wall_clock: Callable[[], datetime] | None = None,
        real_start: float | None = None,
    ) -> None:
        if isinstance(sim_start, str):
            sim_start = datetime.fromisoformat(sim_start.replace("Z", "+00:00"))
        self._sim_start: datetime = sim_start
        self._acceleration_factor = acceleration_factor
        self._wall_clock: Callable[[], datetime] = wall_clock or utc_now
        self._real_start: float = real_start if real_start is not None else time.monotonic()

    @property
    def acceleration_factor(self) -> float:
        return self._acceleration_factor

    def event_time(self) -> datetime:
        """Current simulated event-time (business timestamp for windowing)."""
        elapsed_real = time.monotonic() - self._real_start
        elapsed_sim = elapsed_real * self._acceleration_factor
        from datetime import timedelta

        return self._sim_start + timedelta(seconds=elapsed_sim)

    def event_time_seconds(self) -> float:
        """Current simulated event-time as POSIX timestamp (seconds)."""
        return self.event_time().timestamp()

    def produced_at(self) -> datetime:
        """Wall-clock time of production (used for produced_at field)."""
        return self._wall_clock()

    def format_event_time(self) -> str:
        """Current event_time as ISO 8601 UTC string."""
        return _format_iso(self.event_time())

    def format_produced_at(self) -> str:
        """Current produced_at as ISO 8601 UTC string."""
        return _format_iso(self.produced_at())


class FixedClock:
    """Deterministic clock for tests: event_time and produced_at are fixed.

    Use this when you need bit-for-bit reproducible produced_at values
    in addition to event_time.

    Args:
        event_ts: Fixed event timestamp (datetime or ISO string).
        produced_ts: Fixed produced_at timestamp. Defaults to event_ts.
        acceleration_factor: Stored but not used (event_time is fixed).
    """

    def __init__(
        self,
        event_ts: datetime | str,
        produced_ts: datetime | str | None = None,
        acceleration_factor: float = 3600.0,
    ) -> None:
        if isinstance(event_ts, str):
            event_ts = _parse_iso(event_ts)
        self._event_ts: datetime = event_ts
        if produced_ts is None:
            self._produced_ts: datetime = event_ts
        elif isinstance(produced_ts, str):
            self._produced_ts = _parse_iso(produced_ts)
        else:
            self._produced_ts = produced_ts
        self._acceleration_factor = acceleration_factor

    @property
    def acceleration_factor(self) -> float:
        return self._acceleration_factor

    def event_time(self) -> datetime:
        return self._event_ts

    def event_time_seconds(self) -> float:
        return self._event_ts.timestamp()

    def produced_at(self) -> datetime:
        return self._produced_ts

    def format_event_time(self) -> str:
        return _format_iso(self._event_ts)

    def format_produced_at(self) -> str:
        return _format_iso(self._produced_ts)


def _format_iso(dt: datetime) -> str:
    """Format a datetime as ISO 8601 UTC with Z suffix."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(s: str) -> datetime:
    """Parse ISO 8601 string to UTC datetime."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# Type alias used in generator.py to accept either clock implementation.
ClockLike = Any  # SimClock | FixedClock
