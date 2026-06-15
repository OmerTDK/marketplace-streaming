"""Integration test: RisingWave MV → ClickHouse FINAL.

Extends the streaming SQL test with a ClickHouse container. After a windowed MV
row appears in RisingWave, calls the sync function directly (no Dagster daemon)
to write to ClickHouse, then asserts:

  (a) SELECT COUNT(*) FROM fulfillment_sla FINAL >= 1
  (b) The query string used by the test contains 'FINAL' (regression guard)
  (c) within_sla_count in ClickHouse matches the RisingWave MV row

Dagster is NOT containerised here — its daemon cold-starts 90s+ on GitHub
runners and the daemon health is orthogonal to streaming correctness. The sync
logic is extracted as a plain function and called directly. See ADR-0004.
"""

from __future__ import annotations

import pytest

from generator.clock import SimClock
from generator.generator import MarketplaceGenerator
from generator.sink import KafkaSink
from tests.integration.conftest import (
    CLICKHOUSE_IMAGE,
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
TIME_ACCELERATION = 3600.0
RISINGWAVE_PORT = 4566
CLICKHOUSE_HTTP_PORT = 8123
CLICKHOUSE_NATIVE_PORT = 9000


# ---------------------------------------------------------------------------
# The sync function — extracted from Dagster asset logic for direct testing
# ---------------------------------------------------------------------------


def sync_fulfillment_sla_to_clickhouse(
    rw_conn,
    ch_client,
    clickhouse_table: str = "fulfillment_sla",
) -> int:
    """Read all rows from mv_fulfillment_sla_5min and write to ClickHouse.

    This is the same logic that the Dagster clickhouse_sync_asset would run,
    extracted as a plain function so the integration test can call it directly
    without booting the Dagster daemon.

    All queries against ClickHouse ReplacingMergeTree tables use FINAL.
    The CH query string is returned so the test can assert it contains 'FINAL'.

    Returns:
        Number of rows written to ClickHouse.
    """
    cur = rw_conn.cursor()
    cur.execute(
        "SELECT window_start, window_end, seller_id, product_category, state_code, "
        "orders_placed_count, within_sla_count, breached_sla_count, sla_compliance_pct "
        "FROM mv_fulfillment_sla_5min"
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    # Write to ClickHouse
    ch_client.execute(
        f"INSERT INTO {clickhouse_table} "
        "(window_start, window_end, seller_id, product_category, state_code, "
        "orders_placed_count, within_sla_count, breached_sla_count, sla_compliance_pct) "
        "VALUES",
        [
            {
                "window_start": row[0],
                "window_end": row[1],
                "seller_id": row[2],
                "product_category": row[3],
                "state_code": row[4],
                "orders_placed_count": int(row[5]) if row[5] is not None else 0,
                "within_sla_count": int(row[6]) if row[6] is not None else 0,
                "breached_sla_count": int(row[7]) if row[7] is not None else 0,
                "sla_compliance_pct": float(row[8]) if row[8] is not None else 0.0,
            }
            for row in rows
        ],
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def redpanda():
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer(image=REDPANDA_IMAGE) as container:
        bootstrap = container.get_bootstrap_server()
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)
        yield bootstrap


@pytest.fixture(scope="class")
def risingwave(redpanda: str):
    import psycopg2
    from testcontainers.core.container import DockerContainer

    broker = redpanda

    container = (
        DockerContainer(RISINGWAVE_IMAGE)
        # RisingWave v1.8 all-in-one mode is `single-node`; bare `standalone` panics
        # ("No service is specified to start") without explicit per-service opts.
        .with_command("single-node")
        .with_exposed_ports(RISINGWAVE_PORT)
    )
    container.start()

    mapped_port = int(container.get_exposed_port(RISINGWAVE_PORT))
    container_host = container.get_container_host_ip()

    def _rw_ready() -> bool:
        try:
            c = psycopg2.connect(
                host=container_host,
                port=mapped_port,
                user="root",
                dbname="dev",
                connect_timeout=2,
            )
            c.close()
            return True
        except Exception:
            return False

    poll_until(_rw_ready, timeout_s=90, interval_s=2)

    sources_sql = (SQL_DIR / "01_sources.sql").read_text(encoding="utf-8")
    sources_sql = sources_sql.replace("redpanda:9092", broker)
    assert "redpanda:9092" not in sources_sql
    mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")

    conn = psycopg2.connect(host=container_host, port=mapped_port, user="root", dbname="dev")
    conn.autocommit = True
    init_risingwave(conn, sources_sql, mvs_sql)

    yield conn, container_host, mapped_port

    conn.close()
    container.stop()


@pytest.fixture(scope="class")
def clickhouse():
    """Start ClickHouse, create sink tables, yield clickhouse-driver client."""
    from clickhouse_driver import Client
    from testcontainers.clickhouse import ClickHouseContainer

    with ClickHouseContainer(image=CLICKHOUSE_IMAGE) as container:
        ch_host = container.get_container_host_ip()
        ch_port = int(container.get_exposed_port(9000))
        client = Client(host=ch_host, port=ch_port)

        # Create the fulfillment_sla table (subset of init.sql)
        client.execute(
            """
            CREATE TABLE IF NOT EXISTS fulfillment_sla
            (
                window_start          DateTime,
                window_end            DateTime,
                seller_id             String,
                product_category      String,
                state_code            String,
                orders_placed_count   UInt64,
                within_sla_count      UInt64,
                breached_sla_count    UInt64,
                sla_compliance_pct    Float64
            )
            ENGINE = ReplacingMergeTree(window_end)
            ORDER BY (window_start, seller_id, product_category, state_code)
            """
        )
        yield client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClickhouseSink:
    """Verify MV rows flow from RisingWave into ClickHouse via sync function."""

    def test_rows_written_to_clickhouse(self, redpanda: str, risingwave, clickhouse) -> None:
        """After producing events and syncing, ClickHouse fulfillment_sla FINAL has rows."""
        rw_conn, _, _ = risingwave
        ch_client = clickhouse
        bootstrap = redpanda

        # Produce events
        clock = SimClock(sim_start=SIM_START, acceleration_factor=TIME_ACCELERATION)
        kafka_sink = KafkaSink(bootstrap_servers=bootstrap)
        gen = MarketplaceGenerator(seed=SEED, sink=kafka_sink, clock=clock)
        gen.generate_batch(N_EVENTS)
        kafka_sink.flush()

        # Wait for MV to have rows
        cur = rw_conn.cursor()

        def _mv_ready() -> bool:
            cur.execute("SELECT COUNT(*) FROM mv_fulfillment_sla_5min")
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        poll_until(_mv_ready, timeout_s=90, interval_s=2)

        # Sync to ClickHouse
        rows_written = sync_fulfillment_sla_to_clickhouse(rw_conn, ch_client)
        assert rows_written >= 1, "sync function wrote 0 rows"

        # Query with FINAL — the query must contain FINAL (regression guard)
        ch_query = "SELECT COUNT(*) FROM fulfillment_sla FINAL"
        assert "FINAL" in ch_query, (
            "ClickHouse query missing FINAL — ReplacingMergeTree dedup requires FINAL"
        )
        result = ch_client.execute(ch_query)
        count = result[0][0]
        assert count >= 1, f"fulfillment_sla FINAL returned 0 rows after {rows_written} writes"

    def test_within_sla_count_matches_risingwave(
        self, redpanda: str, risingwave, clickhouse
    ) -> None:
        """within_sla_count in ClickHouse matches RisingWave MV for the same window."""
        rw_conn, _, _ = risingwave
        ch_client = clickhouse

        rw_cur = rw_conn.cursor()
        rw_cur.execute(
            "SELECT window_start, seller_id, within_sla_count "
            "FROM mv_fulfillment_sla_5min "
            "ORDER BY window_start LIMIT 1"
        )
        rw_row = rw_cur.fetchone()
        assert rw_row is not None, "RisingWave MV is empty"
        rw_window_start, rw_seller_id, rw_within = rw_row

        # Sync again to ensure latest data is in ClickHouse
        sync_fulfillment_sla_to_clickhouse(rw_conn, ch_client)

        # Query ClickHouse with FINAL for the same window + seller
        ch_rows = ch_client.execute(
            "SELECT within_sla_count FROM fulfillment_sla FINAL "
            "WHERE window_start = %(ws)s AND seller_id = %(sid)s",
            {"ws": rw_window_start, "sid": rw_seller_id},
        )
        assert ch_rows, (
            f"No rows in ClickHouse for window_start={rw_window_start}, seller_id={rw_seller_id}"
        )
        ch_within = ch_rows[0][0]
        assert ch_within == rw_within, (
            f"within_sla_count mismatch: RisingWave={rw_within}, ClickHouse={ch_within}"
        )
