# ADR-0004: CI Strategy — Fast Lane vs Gated Integration

**Date:** 2026-06-15
**Status:** Accepted

## Context

Phase 2 adds real infrastructure: a Redpanda broker, a RisingWave streaming SQL
engine, and a ClickHouse analytical sink. The integration tests that validate the
end-to-end path require Docker containers that start in 30–90 seconds per service
and consume 512 MB–1 GB RAM each.

The Phase 0/1 CI job runs in ~5 seconds with zero containers. Forcing every PR
push to boot three heavyweight containers would:

1. Extend the default feedback loop from 5s to 5–10 minutes.
2. Make the CI pipeline the bottleneck for trivial changes (typo fixes, doc
   updates, ruff auto-fixes).
3. Risk flaky failures on GitHub-hosted runners where container startup latency
   is variable.

Two CI jobs are needed with different triggers.

## Decision

### Lane 1 — Fast CI (every PR, every push)

The existing `lint-test` job in `.github/workflows/ci.yml` is preserved with
one change: `pytest -v` becomes `pytest -v -m "not integration"`. All tests
in `tests/integration/` are auto-marked with `@pytest.mark.integration` via
`pytest_collection_modifyitems` in `tests/integration/conftest.py`.

This keeps the fast lane free of containers and under 30 seconds total (ruff +
sqlfluff + 77+ unit tests).

### Lane 2 — Gated integration job

A second `integration` job is added to the same `ci.yml` file (not a separate
file). It runs only when:
- `github.event_name == 'workflow_dispatch'` (manual trigger), OR
- The PR has the label `run-integration`

This gives developers control over when to pay the container startup cost. A PR
that only changes a docstring does not need to boot RisingWave.

The integration job uses `uv sync --frozen --group integration` to install the
integration dependency group (`testcontainers[redpanda]`, `psycopg2-binary`,
`clickhouse-driver`, `confluent-kafka`). These are never installed by `uv sync`
alone, keeping the fast lane's dependency footprint minimal.

## Why testcontainers, not docker compose

Three alternatives were evaluated:

### Option A: `docker compose up` in the integration job

Rejected. `docker compose up` starts containers and then CI must `sleep N` to
wait for health checks, or poll with an external tool. This introduces:
- Magic sleep values tuned to CI runner performance (fragile).
- Port mapping managed by compose (fixed ports — risk of collision on shared runners).
- No programmatic access to container lifecycle from Python.

testcontainers solves all three: dynamic port mapping, programmatic readiness
polling, and `with`-statement lifecycle guarantees.

### Option B: testcontainers-python (chosen)

testcontainers-python provides first-class `RedpandaContainer` and
`ClickHouseContainer` modules. RisingWave uses `GenericContainer` with a bind-
mounted `risingwave.toml` (same file used by docker-compose.yml — single source
of truth for configuration).

Fixture scope is `class` to amortize the 30–90s container startup across all
test methods in a class. Each test module gets independent containers, so test
modules are isolated from each other.

### Option C: Full testcontainers + docker compose low-mem override

The `docker-compose.low-mem.yml` file exists for local demo use on constrained
machines. For the integration CI job, testcontainers applies equivalent memory
limits via `.with_env("RW_TOTAL_MEMORY_BYTES", "536870912")` on the RisingWave
container and the `--memory 256M` flag inside the `RedpandaContainer` image's
default command. No compose file is needed in CI.

## Why Dagster is not containerised in Phase 2

The Dagster daemon cold-starts 90+ seconds on GitHub-hosted runners (daemon
health check, asset materialisation catalog, scheduler init). The daemon health
is orthogonal to streaming correctness.

The RisingWave-to-ClickHouse sync logic is extracted as a plain Python function
(`sync_fulfillment_sla_to_clickhouse` in `tests/integration/test_clickhouse_sink.py`)
and called directly in the test. This proves the sync logic is correct without
the Dagster overhead.

A dedicated Dagster integration job is deferred to Phase 4/5. The `make
fault-demo` script provides a manual path for observing the full Dagster
reconciliation flow locally.

## Watermark mode switch: two named SQL files

ADR-0002 noted that switching the watermark between standard (5 minutes) and
fault-injection (6 hours) mode required editing `01_sources.sql` and
re-running the DDL. Phase 2 formalises this as two named files:

- `sql/01_sources.sql` — standard mode (5-minute watermark)
- `sql/01_sources_fault_mode.sql` — fault-injection mode (6-hour watermark)

The `scripts/switch_watermark.py` script drops the existing sources (CASCADE),
recreates them from the chosen file (with broker address substitution), and
recreates the MVs.

Rejected alternative: `sed -i` replacing the INTERVAL literal in `01_sources.sql`.

Why rejected:
- sed-in-place mutates a tracked file, creating noise in `git status`.
- The two modes are now independently reviewable in a diff — a reviewer can see
  exactly what changes between modes.
- `grep -l "6 hours" sql/` unambiguously identifies fault-mode SQL.
- If someone accidentally commerts the fault-mode file as the default, CI catches
  it via the IP hygiene test (which can be extended to check watermark values).

## Sentinel delivery zones and the kill-test isolation pattern

The watermark kill-test (`tests/integration/test_watermark_kill.py`) uses three
sentinel delivery zones:
- `KILL_TEST_ZONE` — receives the 5.5h-late event (should land in MV)
- `ADVANCE_ZONE` — receives 20 watermark-advance events (4 distinct shipment IDs,
  one per Redpanda partition) to ensure all partitions advance past T0+6h
- `BEYOND_TOLERANCE_ZONE` — receives a 7h-late event (should be dropped)

All zone names are alphabetic strings. The generator's CEP prefix format is a
3-digit numeric string (e.g. `"450"`). This structural distinction eliminates
any coordination race: if a live generator is running in parallel, its CEP zones
will never collide with the sentinel zones.

The kill-test starts RisingWave with the fault-mode sources file (6-hour
watermark) so that the 5.5h-late sentinel event falls within the tolerated
window. Standard-mode (5-minute watermark) would correctly drop this event,
but the test would not be able to observe the positive landing assertion.
The negative assertion (BEYOND_TOLERANCE_ZONE) uses a 7h-late event, which
is beyond the 6-hour tolerance and must be dropped in both modes.

## Polling discipline

All test waiting uses `poll_until` from `tests/integration/conftest.py`. No
bare `time.sleep()` calls outside `poll_until`. This makes the timeout
behaviour explicit and the polling interval configurable per call-site.

The one exception: `test_beyond_tolerance_event_is_dropped` uses a 30s sleep
after emitting the beyond-tolerance event. This is a negative assertion (prove
something does NOT appear) — the only correct implementation for negative
assertions in a streaming system without deterministic completion signals.
30 seconds at 6-hour watermark mode is sufficient for RisingWave to have
processed the advance signal and committed the window.

## Consequences

**Fast lane:**
- All PRs get ruff + sqlfluff + 77+ unit tests in ~5–30s.
- No containers, no flakiness from container startup latency.
- Integration tests are skipped (marked, not omitted).

**Integration lane:**
- Boots Redpanda + RisingWave + ClickHouse via testcontainers.
- Proves byte-parity, windowed MV correctness, CH sync, and watermark kill-test.
- Runs on-demand (workflow_dispatch or PR label).
- 15-minute timeout with 120s per-test pytest-timeout.

**Deferred to Phase 4/5:**
- Dagster daemon integration test.
- Reconciliation sensor divergence/convergence test.
- `make fault-demo` manual path covers these scenarios locally.
