# ADR-0001: Streaming SQL Engine — RisingWave v1.8.2

**Date:** 2026-06-14
**Status:** Accepted

## Context

The project requires a streaming SQL engine to process marketplace event topics
(order placement, shipment creation, delivery updates, seller activity) and
materialize windowed aggregates for real-time analytics.

Two credible options were evaluated: **Apache Flink** and **RisingWave**.

The primary constraint is a solo-developer, portfolio-demo context:

- A reviewer must be able to read and understand the streaming logic without
  prior knowledge of the chosen system's internals.
- The full stack must start with `docker compose up --build` — no external
  cluster, no JVM toolchain, no connector JARs.
- The interesting engineering signal is windowing correctness and watermark
  semantics, not operational topology management.

## Decision

**Use RisingWave v1.8.2.**

RisingWave wins on three axes that matter at solo-demo scale:

1. **Single binary, PostgreSQL wire protocol.** One container is the entire
   compute tier. A reviewer can `psql -h localhost -p 4566 -U root` and
   `SELECT * FROM mv_fulfillment_sla_5min` as if it were Postgres. Zero JVM,
   zero JobManager/TaskManager split, zero Flink SQL gateway service.

2. **`CREATE MATERIALIZED VIEW` is the programming model.** The artifact a
   reviewer reads is ANSI SQL with `TUMBLE`/`HOP` window clauses — not a Flink
   fat JAR, not a PyFlink DAG. The signal (windowing correctness, watermark
   semantics) is legible without knowing Flink internals.

3. **Native Kafka-compatible source connector.** `CREATE SOURCE ... WITH
   (connector = 'kafka', ...)` connects directly to Redpanda. No Kafka Connect
   cluster, no connector JAR management.

## Alternatives considered

### Apache Flink

Flink is correct at scale: exactly-once semantics across heterogeneous sinks,
mature RocksDB state backend, fine-grained `ProcessFunction` watermark control,
10+ years of production hardening. For a production team at 100k+ events/sec
with multiple engineers, Flink is the safer bet.

Flink loses for this project on:

- **Operational complexity.** Flink requires a JobManager, at least one
  TaskManager, and a Flink SQL gateway to speak SQL. That is three JVM
  processes before writing a single line of logic. Container memory budget
  exceeds the 4 GB prerequisite threshold on entry-level machines.
- **Legibility cost.** A non-Flink reviewer must first understand job
  submission, checkpoint directories, and state backend configuration before
  reaching the windowing SQL. That overhead is noise for a portfolio signal.
- **No native Kafka source.** Flink requires Kafka Connector JARs managed
  separately, adding a build step and version-pinning burden.

### Kafka Streams / ksqlDB

Dismissed. ksqlDB bundles its own query engine on top of Kafka and requires a
separate ksqlDB server process. The SQL dialect diverges from ANSI standard
enough to obscure the windowing semantics. Kafka Streams (Java API) removes
the SQL readability advantage entirely.

### Spark Structured Streaming

Dismissed. Spark adds a heavyweight compute cluster (driver + executor JVM
processes), and its micro-batch semantics are not true streaming — watermark
behavior differs meaningfully from Flink/RisingWave and would require
additional explanation, not less.

## Consequences

**What gets easier:**

- Reviewer can inspect live materialized view state with any Postgres client.
- All streaming logic is auditable as plain SQL files in `sql/`.
- `docker compose up` is the complete setup path.
- CI can lint SQL syntax without standing up containers.

**What gets harder / trade-offs accepted:**

- **Exactly-once delivery to multiple heterogeneous sinks** is not guaranteed
  at the same level as Flink's two-phase commit. RisingWave provides
  at-least-once to the ClickHouse sink; duplicates are absorbed by
  `ReplacingMergeTree` deduplication.
- **CEP patterns** (complex event processing, e.g. Flink `PatternStream`) are
  not available. The alert pattern in `mv_seller_health_alert_candidates` uses
  a self-join on adjacent windows instead.
- **Throughput ceiling** is approximately 100k events/sec on a single
  RisingWave node. Beyond that, horizontal scaling requires a distributed
  RisingWave deployment (not single-binary). At demo scale (~50 events/sec),
  this ceiling is irrelevant.

**Upgrade path documented:**

If the project later requires exactly-once delivery to multiple heterogeneous
sinks, CEP patterns, or throughput beyond ~100k events/sec on a single node,
Flink is the documented migration target. The SQL DDL in `sql/01_sources.sql`
and `sql/02_mvs.sql` is close-enough to Flink SQL (both implement the
SQL-2016 streaming extensions) that the windowing logic ports with minimal
changes. The event schema and Redpanda topology are engine-agnostic.
