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

## Test substrate: the repo's docker-compose.yml (via testcontainers DockerCompose)

The integration tests use the repo's own `docker-compose.yml` as their substrate,
driven from Python by testcontainers' `DockerCompose` helper
(`from testcontainers.compose import DockerCompose`). `wait=True` runs
`docker compose up --wait`, blocking on the compose healthchecks before the test
proceeds, and the `with`/context-managed lifecycle tears the topology down.

### Why the compose topology, not hand-wired containers

An earlier iteration hand-wired three separate testcontainers
(`RedpandaContainer`, a raw RisingWave `DockerContainer`, `ClickHouseContainer`)
and rewrote the source DDL to point RisingWave at the Redpanda *host* port. That
approach is broken: RisingWave runs inside its own container and cannot reach a
broker advertised as `localhost:<host-port>`, so `CREATE SOURCE` fails with
`failed to fetch metadata from kafka`. Each hand-wired container also sits on its
own network, so there is no name `redpanda:9092` for RisingWave to resolve.

The compose topology fixes this at the root: all services share one compose
network, so `redpanda:9092` resolves for RisingWave and the SQL artifacts in
`sql/01_sources.sql` / `sql/02_mvs.sql` are applied **unchanged** — no
broker-address substitution. The test exercises the *same* artifact users run.

### Dual advertised listeners (the host-producer requirement)

The test produces its own events from the host (for determinism and the kill
test), but RisingWave reads from inside the network. A single advertised Kafka
listener cannot serve both, because a Kafka client always reconnects to the
*advertised* address returned in metadata. Redpanda therefore advertises two
listeners (see `docker-compose.yml`):

- `internal://redpanda:9092` — RisingWave's `CREATE SOURCE` bootstrap (in-network).
- `external://localhost:19092` — the host test producer/consumer and local `rpk`.

The host `KafkaSink` produces to `localhost:19092`; RisingWave's `CREATE SOURCE`
uses `redpanda:9092` unchanged.

### Module isolation and fixed ports

Each test module brings up the topology under a distinct `COMPOSE_PROJECT_NAME`
so the standard-watermark suite and the 6-hour-watermark kill test never share a
RisingWave instance. Modules run sequentially and the module-scoped fixture tears
each topology down before the next starts, so the fixed published host ports
(`4566` / `19092` / `9000`) never clash. The `generator` and `dagster` services
stay down — the test produces its own events; topics are created from the host.

The `docker-compose.low-mem.yml` override remains for local demo use on
constrained machines; it is not used by the integration job.

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
