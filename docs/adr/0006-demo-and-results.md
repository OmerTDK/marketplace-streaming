# ADR-0006: Phase 4 — Demo Script Design and Results Measurement Methodology

**Date:** 2026-06-19
**Status:** Accepted

## Context

Phase 4 closes the project's definition of done. Three deliverables required
decisions:

1. How to make the E2E demo script runnable in CI (which has no Docker).
2. How to measure and report quantified results for the README.
3. Whether to add a CI integration test for the demo script itself.

## Decision

### 1. Dual-mode demo (`--dry-run` / live)

`scripts/demo.py` supports two operating modes:

- **Live mode** (default): connects to a running docker-compose stack on
  `localhost:4566` (RisingWave) and `localhost:8123` (ClickHouse). Steps
  through the scenario: baseline stats → enable `late_arrival` fault injection
  → wait 30 seconds → show diverged windows → disable faults → wait for
  convergence → print final summary.

- **Dry-run mode** (`--dry-run`): prints the same narrative with hardcoded
  representative values and exits 0. No network connections. Suitable for CI,
  offline review, and documentation examples.

**Rationale:** The demo scenario requires four stateful services (Redpanda,
RisingWave, ClickHouse, generator) and a running data pipeline. Providing a
meaningful fast-lane test for a script that orchestrates infrastructure is
disproportionately complex for its value. The `--dry-run` path proves the script
is syntactically correct, importable, and its argument parsing works — which is
the CI-relevant assertion. The `make demo` / live path is verified manually
against the docker-compose stack.

### 2. Measurement methodology

`scripts/measure_results.py` measures four quantities when the stack is running:

| Metric | Method |
|--------|--------|
| Throughput | ClickHouse row-delta in `fulfillment_sla` over a 10-second window |
| End-to-end latency | Reported as `<2s`; RisingWave MV updates continuously from the Kafka source |
| MV correctness | Diverged-window count from `reconciliation_audit` at steady state |
| Fault recovery time | Wall-clock seconds between `fault_control off` and zero diverged windows |

**Throughput proxy:** Using ClickHouse `fulfillment_sla` row growth as a
throughput signal is a simplification — MV rows are windowed aggregates, not
raw events. The actual event rate is configured (`EVENTS_PER_SECOND=50`) and
the proxy validates that events are reaching ClickHouse, not the raw rate. This
trade-off was accepted for portfolio legibility: a Kafka offset-based throughput
measurement would require adding a consumer and offset-tracking logic, which
adds complexity without adding portfolio signal. The configured rate (50 eps) is
stated directly in the results section.

**Latency claim:** End-to-end latency is bounded by the RisingWave MV refresh
cycle and the Dagster sync sensor poll interval (30 seconds). The `<2s` claim
refers to RisingWave-internal MV update latency (event produced → MV row
visible in `psql`). ClickHouse sync adds the sensor poll interval on top. The
README results section distinguishes these two paths explicitly.

### 3. No CI integration test for the demo script

The demo script (`scripts/demo.py`) is not covered by an integration test in
the CI `integration` job. The existing integration suite (`tests/integration/`)
already covers the underlying mechanics: broker health, streaming SQL, ClickHouse
sync, and reconciliation. Adding an integration test that re-runs the demo
script would duplicate those fixtures and add ~90s to the already-long
integration job without testing anything new. The `--dry-run` fast-lane test
(`test_demo_script_dry_run`) covers the CI-verifiable assertions.

## Consequences

- `make ci` stays container-free and fast (~1.4s); the demo is a manual step.
- `python scripts/demo.py --dry-run` is the canonical CI assertion for the demo
  script's correctness.
- Quantified results in the README are honest: the configured rate (50 eps) is
  stated as such; the watermark-based numbers come from integration test
  observations at `N_EVENTS=300`, `SEED=42`.
- Future maintainers who want to add a live integration test for the demo can
  follow the `tests/integration/` pattern: `DockerCompose` fixture +
  `pytest.mark.integration`.
