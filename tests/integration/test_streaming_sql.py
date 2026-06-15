"""Integration test: generator → Redpanda → RisingWave MV (compose substrate).

Brings up the repo's docker-compose topology, applies sql/01_sources.sql +
sql/02_mvs.sql to RisingWave UNCHANGED (sources point at redpanda:9092, which
resolves on the compose network — no broker substitution), produces 200 events
with TIME_ACCELERATION_FACTOR=3600 and a SimClock to the host-reachable external
listener, then polls mv_fulfillment_sla_5min until at least one windowed row
appears.

Asserts:
  (a) COUNT(*) >= 1 in mv_fulfillment_sla_5min
  (b) within_sla_count + breached_sla_count <= orders_placed_count (structural)
  (c) sla_compliance_pct BETWEEN 0 AND 100
  (d) mv_delivery_zone_status has at least one row
"""

from __future__ import annotations

import pytest

from generator.clock import SimClock
from generator.generator import MarketplaceGenerator
from generator.sink import KafkaSink
from tests.integration.conftest import (
    KAFKA_TOPICS,
    SQL_DIR,
    compose_topology,
    connect_risingwave,
    create_topics,
    init_risingwave,
    kafka_bootstrap,
    poll_until,
    risingwave_endpoint,
)

N_EVENTS = 200
SEED = 42
SIM_START = "2024-01-08T00:00:00Z"
# 3600x: 1 real-second = 1 sim-hour. 200 events over a few seconds covers
# multiple 5-minute tumbling windows in sim-time.
TIME_ACCELERATION = 3600.0


@pytest.fixture(scope="class")
def streaming_env():
    """Compose topology + RisingWave initialised with standard (5-min) sources.

    Yields (rw_conn, bootstrap). The SQL is applied unchanged — sources connect
    to redpanda:9092 over the compose network.
    """
    with compose_topology("mktstream_sql") as compose:
        bootstrap = kafka_bootstrap(compose)
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)

        rw_host, rw_port = risingwave_endpoint(compose)
        conn = connect_risingwave(rw_host, rw_port)

        sources_sql = (SQL_DIR / "01_sources.sql").read_text(encoding="utf-8")
        mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")
        init_risingwave(conn, sources_sql, mvs_sql)

        try:
            yield conn, bootstrap
        finally:
            conn.close()


@pytest.mark.integration
class TestStreamingSql:
    """Verify events flow through Redpanda into RisingWave windowed MVs."""

    def test_fulfillment_mv_rows_appear(self, streaming_env) -> None:
        """After 200 events, mv_fulfillment_sla_5min has at least one row."""
        conn, bootstrap = streaming_env

        clock = SimClock(sim_start=SIM_START, acceleration_factor=TIME_ACCELERATION)
        kafka_sink = KafkaSink(bootstrap_servers=bootstrap)
        gen = MarketplaceGenerator(seed=SEED, sink=kafka_sink, clock=clock)
        gen.generate_batch(N_EVENTS)
        kafka_sink.flush()

        cur = conn.cursor()

        def _mv_has_rows() -> bool:
            cur.execute("SELECT COUNT(*) FROM mv_fulfillment_sla_5min")
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        poll_until(_mv_has_rows, timeout_s=120, interval_s=2)

    def test_sla_counts_are_structurally_valid(self, streaming_env) -> None:
        """within_sla_count + breached_sla_count <= orders_placed_count for all rows."""
        conn, _ = streaming_env
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

    def test_delivery_zone_mv_rows_appear(self, streaming_env) -> None:
        """mv_delivery_zone_status has at least one row after event production."""
        conn, _ = streaming_env
        cur = conn.cursor()

        def _dz_has_rows() -> bool:
            cur.execute("SELECT COUNT(*) FROM mv_delivery_zone_status")
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        # Events already produced by test_fulfillment_mv_rows_appear — just poll.
        try:
            poll_until(_dz_has_rows, timeout_s=90, interval_s=2)
        except TimeoutError:
            # Delivery zone MV requires is_final=TRUE events. At N=200 with
            # 20% delivery_update ratio and ~25% final status rate, expect ~10 finals.
            cur.execute("SELECT COUNT(*) FROM delivery_update_source WHERE is_final = TRUE")
            final_count = cur.fetchone()
            pytest.fail(
                f"mv_delivery_zone_status still empty after 90s. "
                f"Final delivery_update events in source: {final_count}"
            )
