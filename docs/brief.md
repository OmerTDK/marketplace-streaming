# Project Brief: marketplace-streaming

## Mission

Build a production-grade streaming analytics stack for a simulated marketplace
platform, demonstrating real-time windowed aggregates, fault injection, and
batch-vs-stream reconciliation — all runnable on a laptop with one command.

## Scope

- **In scope:** Redpanda (event broker) → RisingWave (streaming SQL) → ClickHouse
  (analytical sink) → Dagster (orchestration + reconciliation). Python event generator
  with configurable fault injection. Fault injection demo script.
- **Out of scope:** Production deployment, multi-node clusters, real marketplace
  data, any employer-specific code or schemas.

## Build phases

| Phase | Deliverables | Definition of done |
|-------|-------------|-------------------|
| **0 — Architecture** | ADRs, docker-compose skeleton, SQL DDL reviewed, event schema documented | ADRs merged, CI green, no services started |
| **1 — Infrastructure** | Working `docker compose up --build`, all services healthy | All health checks pass; `psql -h localhost -p 4566` connects |
| **2 — Generator** | Python generator producing synthetic events, fault injection working | `make generate` produces events in all four topics; fault control file hot-reloads |
| **3 — Streaming SQL** | RisingWave sources and MVs live, queryable via psql | All four sources + four MVs created; windowed rows visible in psql |
| **4 — ClickHouse sink** | Dagster sync assets writing to ClickHouse, FINAL queries verified | Dagster UI shows successful runs; ClickHouse FINAL queries return data |
| **5 — Reconciliation** | Batch recompute asset + reconciliation sensor, divergence/convergence demo | All three scenarios reproducible from SEED=42 |
| **6 — Demo + polish** | `make fault-demo` script, kill-verification integration test, README with real numbers | Fault demo runs end-to-end; README results section has real measurements |

## Definition of done (project-level)

- `docker compose up --build` starts all services on a cold machine with 4 GB Docker memory.
- `make fault-demo` demonstrates late-arrival fault injection and watermark trade-off without manual steps.
- `make test` passes (unit tests for generator + reconciliation logic).
- README results section contains real runtime measurements (throughput, latency, convergence time).
- No employer-specific code, schemas, names, or data in any commit.
- No secrets committed (`.env` gitignored; CI uses GitHub Actions secrets).

## Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Event broker | Redpanda | v23.3.18 |
| Streaming SQL | RisingWave | v1.8.2 |
| Analytical sink | ClickHouse | 24.3 LTS |
| Orchestration | Dagster | latest (Phase 4+) |
| Generator | Python 3.12+ | — |
| Package manager | uv | latest |

## Key design decisions

- See [docs/adr/0001-streaming-engine.md](adr/0001-streaming-engine.md): RisingWave over Flink.
- See [docs/adr/0002-architecture.md](adr/0002-architecture.md): full topology, event schema, watermark decision.
