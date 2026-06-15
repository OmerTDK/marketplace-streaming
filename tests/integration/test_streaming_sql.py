"""Integration test: generator → Redpanda → RisingWave MV.

Boots Redpanda + RisingWave via testcontainers, initialises sources and MVs
via psycopg2 (one statement at a time — RisingWave wire protocol requirement),
produces 200 events with TIME_ACCELERATION_FACTOR=3600 and a SimClock, then
polls the mv_fulfillment_sla_5min MV until at least one windowed row appears.

Asserts:
  (a) COUNT(*) >= 1 in mv_fulfillment_sla_5min
  (b) within_sla_count + breached_sla_count <= orders_placed_count (structural)
  (c) sla_compliance_pct BETWEEN 0 AND 100
"""

from __future__ import annotations

import pytest

from generator.clock import SimClock
from generator.generator import MarketplaceGenerator
from generator.sink import KafkaSink
from tests.integration.conftest import (
    KAFKA_TOPICS,
    REDPANDA_IMAGE,
    RISINGWAVE_IMAGE,
    SQL_DIR,
    create_topics,
    init_risingwave,
    poll_until,
)

N_EVENTS = 200
SEED = 42
SIM_START = "2024-01-08T00:00:00Z"
# 3600x: 1 real-second = 1 sim-hour. 200 events over a few seconds covers
# multiple 5-minute tumbling windows in sim-time.
TIME_ACCELERATION = 3600.0

RISINGWAVE_PORT = 4566
RISINGWAVE_USER = "root"
RISINGWAVE_DB = "dev"


@pytest.fixture(scope="class")
def redpanda():
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer(image=REDPANDA_IMAGE) as container:
        bootstrap = container.get_bootstrap_server()
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)
        yield bootstrap


@pytest.fixture(scope="class")
def risingwave(redpanda: str):
    """Start RisingWave, init sources+MVs pointing at the test Redpanda, yield psycopg2 conn."""
    import psycopg2
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(RISINGWAVE_IMAGE)
        # RisingWave v1.8 all-in-one mode is `single-node`; bare `standalone` panics
        # ("No service is specified to start") without explicit per-service opts.
        .with_command("single-node")
        .with_exposed_ports(RISINGWAVE_PORT)
    )
    container.start()

    # Wait for RisingWave to be ready (pg_isready-equivalent: accept connections).
    mapped_port = int(container.get_exposed_port(RISINGWAVE_PORT))
    container_host = container.get_container_host_ip()

    def _rw_ready() -> bool:
        try:
            conn = psycopg2.connect(
                host=container_host,
                port=mapped_port,
                user=RISINGWAVE_USER,
                dbname=RISINGWAVE_DB,
                connect_timeout=2,
            )
            conn.close()
            return True
        except Exception:
            return False

    poll_until(_rw_ready, timeout_s=90, interval_s=2)

    # Init sources and MVs. Substitute the broker address so sources point
    # at the testcontainers Redpanda instead of 'redpanda:9092'.
    sources_sql = (SQL_DIR / "01_sources.sql").read_text(encoding="utf-8")
    sources_sql = sources_sql.replace("redpanda:9092", redpanda)

    # Verify substitution was complete.
    assert "redpanda:9092" not in sources_sql, (
        "Broker substitution incomplete — 'redpanda:9092' still present in sources SQL"
    )

    mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")

    conn = psycopg2.connect(
        host=container_host,
        port=mapped_port,
        user=RISINGWAVE_USER,
        dbname=RISINGWAVE_DB,
    )
    conn.autocommit = True
    init_risingwave(conn, sources_sql, mvs_sql)

    yield conn, container_host, mapped_port

    conn.close()
    container.stop()


@pytest.mark.integration
class TestStreamingSql:
    """Verify events flow through Redpanda into RisingWave windowed MVs."""

    def test_fulfillment_mv_rows_appear(self, redpanda: str, risingwave) -> None:
        """After 200 events, mv_fulfillment_sla_5min has at least one row."""
        conn, _rw_host, _rw_port = risingwave
        bootstrap = redpanda

        clock = SimClock(
            sim_start=SIM_START,
            acceleration_factor=TIME_ACCELERATION,
        )
        kafka_sink = KafkaSink(bootstrap_servers=bootstrap)
        gen = MarketplaceGenerator(seed=SEED, sink=kafka_sink, clock=clock)
        gen.generate_batch(N_EVENTS)
        kafka_sink.flush()

        cur = conn.cursor()

        def _mv_has_rows() -> bool:
            cur.execute("SELECT COUNT(*) FROM mv_fulfillment_sla_5min")
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        poll_until(_mv_has_rows, timeout_s=90, interval_s=2)

    def test_sla_counts_are_structurally_valid(self, redpanda: str, risingwave) -> None:
        """within_sla_count + breached_sla_count <= orders_placed_count for all rows."""
        conn, _, _ = risingwave
        cur = conn.cursor()
        cur.execute(
            "SELECT orders_placed_count, within_sla_count, breached_sla_count, sla_compliance_pct "
            "FROM mv_fulfillment_sla_5min"
        )
        rows = cur.fetchall()
        assert rows, (
            "mv_fulfillment_sla_5min is empty — test_fulfillment_mv_rows_appear must pass first"
        )

        for orders, within, breached, pct in rows:
            assert within + breached <= orders, (
                f"SLA counts invalid: within={within} + breached={breached} > orders={orders}"
            )
            if pct is not None:
                assert 0.0 <= float(pct) <= 100.0, f"sla_compliance_pct out of range: {pct}"

    def test_delivery_zone_mv_rows_appear(self, redpanda: str, risingwave) -> None:
        """mv_delivery_zone_status has at least one row after event production."""
        conn, _, _ = risingwave
        cur = conn.cursor()

        def _dz_has_rows() -> bool:
            cur.execute("SELECT COUNT(*) FROM mv_delivery_zone_status")
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        # Events already produced by test_fulfillment_mv_rows_appear — just poll.
        try:
            poll_until(_dz_has_rows, timeout_s=60, interval_s=2)
        except TimeoutError:
            # Delivery zone MV requires is_final=TRUE events. At N=200 with
            # 20% delivery_update ratio and 25% final status rate, expect ~10 finals.
            # If the window hasn't closed yet, report a diagnostic.
            cur.execute("SELECT COUNT(*) FROM delivery_update_source WHERE is_final = TRUE")
            final_count = cur.fetchone()
            pytest.fail(
                f"mv_delivery_zone_status still empty after 60s. "
                f"Final delivery_update events in source: {final_count}"
            )
