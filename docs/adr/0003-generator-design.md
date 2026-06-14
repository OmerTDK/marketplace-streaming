# ADR-0003: Generator Design — Determinism, Injectable Sink, Event-Time Fault Parameterization

**Date:** 2026-06-14
**Status:** Accepted

## Context

Phase 1 requires a synthetic event generator that is:

1. Bit-for-bit reproducible from a seed (for the reconciliation demo in Phase 5
   and for stable CI tests).
2. Fully unit-testable without running Redpanda, RisingWave, or any container.
3. Compatible with configurable `TIME_ACCELERATION_FACTOR` so a 10-minute demo
   covers weeks of order lifecycle.
4. Capable of fault injection with observable statistical properties (needed by
   the Phase 5 divergence/convergence demo).

The naive design — a generator that calls `confluent_kafka.Producer.produce()`
directly — fails requirement 2: every test would need a live broker, making CI
slow and fragile.

## Decision

### 1. Deterministic RNG

All randomness flows through `numpy.random.default_rng(seed)`. This includes:

- Event routing (which of the four topics receives the next event)
- All field sampling (payment value, delivery days, carrier selection, etc.)
- UUID generation: **UUIDs are derived from the seeded RNG, not from
  `uuid.uuid4()` (system entropy)**. The technique: draw two `uint64` values,
  combine into a 128-bit integer, stamp RFC 4122 version and variant bits.
  This is the key move that makes the stream bit-for-bit reproducible.

`Faker(locale='pt_BR').seed_instance(seed)` provides deterministic city names.
Note: the `seed` kwarg does not exist on the `Faker` constructor — seeding must
be done via `.seed_instance(seed)` after construction. The locale is `pt_BR` to
match the Olist calibration (Brazilian marketplace data).

**Exception:** seller and customer pool IDs use `uuid.UUID(int=i)` (sequential
integers 0, 1, 2, … cast to UUID), not the seeded RNG. This is intentional —
the pool membership is fixed and deterministic by position; only the
Pareto-shaped selection weights that govern which seller is chosen for each order
event are drawn from the seeded RNG.

**Consequence:** same `SEED` → identical event stream within a given Python
environment and dependency lockfile. The SHA-256 hash test
(`TestDeterminism::test_full_stream_hash_is_stable`) pins the expected value so
any cross-session regression (Faker version bump, numpy RNG change, new field)
is caught immediately.

### 2. Injectable Sink (testability / CI-speed vs runtime fidelity trade-off)

The generator writes through a `Sink` abstract base class with two
implementations:

| Implementation | Used in | Broker required |
|---------------|---------|----------------|
| `InMemorySink` | unit tests | No — stores events in a `dict[topic, list]` |
| `KafkaSink` | runtime (Docker container) | Yes — confluent-kafka producer |

`KafkaSink` defers the `confluent_kafka` import to `__init__` so that
`from generator.sink import InMemorySink` never triggers the import.
Tests that import `InMemorySink` or `FaultHarness` can run in a vanilla
`uv sync` environment with no Kafka client installed.

**Trade-off accepted:** `InMemorySink` does not exercise Kafka serialization,
partitioning, or delivery semantics. A separate integration test (Phase 2)
will validate end-to-end against a running Redpanda instance. The unit test
suite (Phase 1) validates business logic only.

**Why this matters for CI:** `make ci` runs ruff + sqlfluff + pytest in under
10 seconds on a laptop and under 60 seconds on GitHub Actions (ubuntu-latest,
no Docker). No containers in CI is a hard constraint.

### 3. Event-Time Fault Parameterization

Fault durations (`late_arrival_max_delay_seconds`,
`zone_blackout_duration_event_seconds`) are specified in **event-time seconds**,
not wall-clock seconds.

**Why:** the generator advances event-time by `TIME_ACCELERATION_FACTOR`
real-seconds per sim-second (default 3600). A zone blackout of 7200
event-seconds at 3600x lasts 2 real-seconds — observable in a 10-minute demo.
If durations were wall-clock seconds, the same config would behave differently
at different acceleration factors, breaking the reconciliation demo
reproducibility guarantee.

**Implementation:** `FaultState.is_blacked_out()` takes
`current_event_seconds` (the sim-clock POSIX timestamp), not `time.monotonic()`.
The `SimClock` passes `event_time_seconds()` to fault application; the `FixedClock`
returns a constant for tests. Neither calls `time.monotonic()` in the fault
path.

### 4. FixedClock for Tests

`FixedClock` provides a deterministic `produced_at` in addition to a fixed
`event_time`. This eliminates the last source of non-determinism (wall-clock
`produced_at`) in the unit-tested path.

At runtime, `SimClock` is used with real `time.monotonic()` for event-time
advancement and `datetime.now(UTC)` for `produced_at`. The two implementations
share the same interface (`ClockLike`), so the generator is unaware of which
it is using.

## Consequences

- **CI is fast and container-free.** `make ci` (ruff + sqlfluff + pytest)
  completes in under 60 seconds on GitHub Actions without any Docker services.
- **Tests are stable.** Bit-for-bit determinism means flaky-test failures
  from RNG variance are impossible. The SHA-256 hash test catches any
  accidental non-determinism regression.
- **Runtime fidelity is deferred.** Kafka serialization bugs, partition
  assignment issues, and back-pressure behavior are not tested in Phase 1.
  Phase 2 will add integration tests against a live Redpanda container.
- **Fault demo is acceleration-factor-agnostic.** The same
  `fault_injection.json` works at 1x, 100x, or 3600x acceleration because
  all durations are in event-time seconds.
- **No Olist CSV data in the repo.** The generator uses only the documented
  distribution parameters (lognormal mu/sigma, Pareto shape, category
  weights). CC BY-NC-SA compliance is maintained.
