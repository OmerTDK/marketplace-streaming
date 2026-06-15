"""Integration test: RisingWave MV → ClickHouse FINAL (compose substrate).

Brings up the repo's docker-compose topology (Redpanda + RisingWave +
ClickHouse), applies the standard sources + MVs to RisingWave unchanged,
produces events, waits for a windowed MV row, then calls the sync function
directly (no Dagster daemon) to write to ClickHouse and asserts:

  (a) SELECT COUNT(*) FROM fulfillment_sla FINAL >= 1
  (b) The shared ch_read_query builder injects FINAL by construction; the test
      asserts on the builder output, so the guard fails if FINAL is ever removed
  (c) within_sla_count in ClickHouse matches the RisingWave MV row

Dagster is NOT containerised here — its daemon cold-starts 90s+ and daemon
health is orthogonal to streaming correctness. The sync logic is extracted as a
plain function and called directly. See ADR-0004.
"""

from __future__ import annotations

import pytest

from generator.clock import SimClock
from generator.generator import MarketplaceGenerator
from generator.sink import KafkaSink
from tests.integration.conftest import (
    KAFKA_TOPICS,
    SQL_DIR,
    clickhouse_endpoint,
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
TIME_ACCELERATION = 3600.0

CH_SLA_TABLE = "fulfillment_sla"


def ch_read_query(columns: str, where: str | None = None, table: str = CH_SLA_TABLE) -> str:
    """Build a read query against a ClickHouse ReplacingMergeTree table.

    FINAL is injected by CONSTRUCTION — every query this builder produces forces
    merge-time deduplication at read. ADR-0002 requires FINAL on every read of
    these tables (ReplacingMergeTree dedup is lazy). The integration tests assert
    'FINAL' in the OUTPUT of this builder, not in a hand-written literal, so the
    guard has teeth: if someone removes FINAL here, the assertion fails. Every
    ClickHouse read in this module goes through this function for that reason.
    """
    query = f"SELECT {columns} FROM {table} FINAL"
    if where:
        query += f" WHERE {where}"
    return query


# ---------------------------------------------------------------------------
# The sync function — extracted from Dagster asset logic for direct testing
# ---------------------------------------------------------------------------


def sync_fulfillment_sla_to_clickhouse(
    rw_conn,
    ch_client,
    clickhouse_table: str = CH_SLA_TABLE,
) -> int:
    """Read all rows from mv_fulfillment_sla_5min and write to ClickHouse.

    This is the same logic the Dagster clickhouse_sync_asset would run, extracted
    as a plain function so the integration test can call it directly without
    booting the Dagster daemon.

    This function only WRITES (INSERT) into ClickHouse; the FINAL-on-read
    requirement is exercised by the test's read-back queries, which go through
    ``ch_read_query`` (FINAL injected by construction).

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
def sink_env():
    """Compose topology + RisingWave (standard sources) + ClickHouse client.

    Yields (rw_conn, ch_client, bootstrap).
    """
    from clickhouse_driver import Client

    with compose_topology("mktstream_clickhouse") as compose:
        bootstrap = kafka_bootstrap(compose)
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)

        rw_host, rw_port = risingwave_endpoint(compose)
        rw_conn = connect_risingwave(rw_host, rw_port)

        sources_sql = (SQL_DIR / "01_sources.sql").read_text(encoding="utf-8")
        mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")
        init_risingwave(rw_conn, sources_sql, mvs_sql)

        ch_host, ch_port = clickhouse_endpoint(compose)
        ch_client = Client(host=ch_host, port=ch_port)

        # The compose ClickHouse runs clickhouse/init.sql at startup, but create
        # the table here too so the test is independent of that mount.
        ch_client.execute(
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

        try:
            yield rw_conn, ch_client, bootstrap
        finally:
            rw_conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClickhouseSink:
    """Verify MV rows flow from RisingWave into ClickHouse via sync function."""

    def test_rows_written_to_clickhouse(self, sink_env) -> None:
        """After producing events and syncing, ClickHouse fulfillment_sla FINAL has rows."""
        rw_conn, ch_client, bootstrap = sink_env

        clock = SimClock(sim_start=SIM_START, acceleration_factor=TIME_ACCELERATION)
        kafka_sink = KafkaSink(bootstrap_servers=bootstrap)
        gen = MarketplaceGenerator(seed=SEED, sink=kafka_sink, clock=clock)
        gen.generate_batch(N_EVENTS)
        kafka_sink.flush()

        cur = rw_conn.cursor()

        def _mv_ready() -> bool:
            cur.execute("SELECT COUNT(*) FROM mv_fulfillment_sla_5min")
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        poll_until(_mv_ready, timeout_s=120, interval_s=2)

        rows_written = sync_fulfillment_sla_to_clickhouse(rw_conn, ch_client)
        assert rows_written >= 1, "sync function wrote 0 rows"

        # Regression guard: the read query is BUILT by ch_read_query, which injects
        # FINAL by construction. Asserting on the builder OUTPUT (not a hand-written
        # literal) means the guard actually fails if FINAL is ever removed from the
        # builder — ReplacingMergeTree dedup requires FINAL on read (ADR-0002).
        ch_query = ch_read_query("COUNT(*)")
        assert "FINAL" in ch_query, (
            "ch_read_query dropped FINAL — ReplacingMergeTree dedup requires FINAL on read"
        )
        result = ch_client.execute(ch_query)
        count = result[0][0]
        assert count >= 1, f"fulfillment_sla FINAL returned 0 rows after {rows_written} writes"

    def test_within_sla_count_matches_risingwave(self, sink_env) -> None:
        """within_sla_count in ClickHouse matches RisingWave MV for the same window."""
        rw_conn, ch_client, _ = sink_env

        rw_cur = rw_conn.cursor()
        rw_cur.execute(
            "SELECT window_start, seller_id, within_sla_count "
            "FROM mv_fulfillment_sla_5min "
            "ORDER BY window_start LIMIT 1"
        )
        rw_row = rw_cur.fetchone()
        assert rw_row is not None, "RisingWave MV is empty"
        rw_window_start, rw_seller_id, rw_within = rw_row

        # Sync again to ensure latest data is in ClickHouse.
        sync_fulfillment_sla_to_clickhouse(rw_conn, ch_client)

        ch_rows = ch_client.execute(
            ch_read_query(
                "within_sla_count",
                where="window_start = %(ws)s AND seller_id = %(sid)s",
            ),
            {"ws": rw_window_start, "sid": rw_seller_id},
        )
        assert ch_rows, (
            f"No rows in ClickHouse for window_start={rw_window_start}, seller_id={rw_seller_id}"
        )
        ch_within = ch_rows[0][0]
        assert ch_within == rw_within, (
            f"within_sla_count mismatch: RisingWave={rw_within}, ClickHouse={ch_within}"
        )
