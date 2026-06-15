"""ADR-0002 watermark kill-test.

Proves that a delivery_update event with scanned_at ~5.5h before produced_at
lands in the correct 5-minute tumbling window once the watermark advances
past that window's end time. Also proves that events beyond the 6-hour
tolerance are correctly dropped.

This test is self-contained: it produces all events via KafkaSink directly,
with no dependency on the generator process being alive.

The kill-test fixture temporarily switches delivery_update_source to a 6-hour
watermark (the fault-injection mode) so that the 5.5h-late event falls within
the tolerated late-arrival window. The fixture restores the 5-minute watermark
in its finally block regardless of test outcome.

Sentinel delivery zones:
  KILL_TEST_ZONE         — receives the 5.5h-late event (should land)
  ADVANCE_ZONE           — receives 20 watermark-advance events (4 shipment IDs)
  BEYOND_TOLERANCE_ZONE  — receives a 7h-late event (should be dropped)

All zone names are alphabetic — structurally distinct from the generator's
3-digit numeric CEP prefix format, so there is no coordination race with
any live generator instance.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from generator.sink import KafkaSink
from tests.integration.conftest import (
    ADVANCE_ZONE,
    BEYOND_TOLERANCE_ZONE,
    KAFKA_TOPICS,
    KILL_TEST_ZONE,
    SQL_DIR,
    compose_topology,
    connect_risingwave,
    create_topics,
    init_risingwave,
    kafka_bootstrap,
    poll_until,
    risingwave_endpoint,
)

# T0: the scanned_at (carrier scan time) for the kill-test delivery event.
# This is the timestamp the delivery_update watermark and mv_delivery_zone_status
# window are both keyed on. event_time is set 30 min behind scanned_at in
# _make_delivery_update (the two are never equal — see that function).
T0 = datetime(2024, 1, 8, 10, 0, 0, tzinfo=UTC)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_delivery_update(
    shipment_id: str,
    delivery_zone: str,
    scanned_at: datetime,
    produced_at: datetime,
    is_final: bool = True,
    status: str = "delivered",
    seq: int = 1,
) -> dict:
    # event_time is deliberately OFFSET from scanned_at (30 min earlier) so the
    # two timestamps are never equal. scanned_at is the carrier scan time that
    # drives the delivery_update watermark (ADR-0002); event_time is the upstream
    # business event time. If the watermark were ever switched from
    # `WATERMARK FOR scanned_at` to `WATERMARK FOR event_time`, the advance
    # events would carry an event_time 30 min behind their scanned_at, the
    # watermark would lag further, and the sentinel window would NOT close —
    # making this kill-test fail. That is the regression property we want.
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "delivery_update",
        "event_version": "1.0",
        "produced_at": _fmt(produced_at),
        "event_time": _fmt(scanned_at - timedelta(minutes=30)),
        "scanned_at": _fmt(scanned_at),
        "is_injected_fault": False,
        "fault_type": None,
        "update_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "order_id": str(uuid.uuid4()),
        "status": status,
        "location_state": "SP",
        "delivery_zone": delivery_zone,
        "sequence_number": seq,
        "is_final": is_final,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def risingwave_kill():
    """Compose topology + RisingWave initialised with FAULT-MODE (6-hour) sources.

    The fault-mode sources file (sql/01_sources_fault_mode.sql) widens every
    watermark to 6 hours, so a 5.5-hour-late delivery_update lands in-window
    while a 7-hour-late one is dropped. The SQL is applied UNCHANGED — sources
    point at redpanda:9092, which resolves on the compose network.

    Yields (rw_conn, bootstrap).
    """
    with compose_topology("mktstream_killtest") as compose:
        bootstrap = kafka_bootstrap(compose)
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)

        rw_host, rw_port = risingwave_endpoint(compose)
        conn = connect_risingwave(rw_host, rw_port)

        fault_sources_sql = (SQL_DIR / "01_sources_fault_mode.sql").read_text(encoding="utf-8")
        mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")
        init_risingwave(conn, fault_sources_sql, mvs_sql)

        try:
            yield conn, bootstrap
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Kill-test
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestWatermarkKill:
    """ADR-0002 kill-test: late events land correctly; beyond-tolerance events drop."""

    def test_late_event_lands_in_correct_window(self, risingwave_kill) -> None:
        """A delivery_update with scanned_at=T0 and produced_at=T0+5.5h lands in the window."""
        conn, bootstrap = risingwave_kill

        # 1. Emit the sentinel event: scanned_at=T0, produced_at=T0+5.5h (beyond normal tolerance)
        sentinel_shipment = str(uuid.uuid4())
        sentinel_event = _make_delivery_update(
            shipment_id=sentinel_shipment,
            delivery_zone=KILL_TEST_ZONE,
            scanned_at=T0,
            produced_at=T0 + timedelta(hours=5, minutes=30),
            is_final=True,
            status="delivered",
        )

        sink = KafkaSink(bootstrap_servers=bootstrap)
        sink.send(
            "delivery_update",
            f"{sentinel_shipment}_1",
            sentinel_event,
        )

        # 2. Emit 20 watermark-advance events: scanned_at=T0+6h+1s, 4 distinct shipment IDs
        #    to hit all 4 Redpanda partitions (one shipment per partition via key routing).
        advance_time = T0 + timedelta(hours=6, seconds=1)
        advance_shipment_ids = [str(uuid.uuid4()) for _ in range(4)]
        for _i, ship_id in enumerate(advance_shipment_ids):
            for seq in range(1, 6):  # 5 events per shipment = 20 total
                adv_event = _make_delivery_update(
                    shipment_id=ship_id,
                    delivery_zone=ADVANCE_ZONE,
                    scanned_at=advance_time,
                    produced_at=advance_time,
                    is_final=False,
                    status="in_transit",
                    seq=seq,
                )
                sink.send("delivery_update", f"{ship_id}_{seq}", adv_event)

        sink.flush()

        # 3. Poll mv_delivery_zone_status for the sentinel zone.
        cur = conn.cursor()

        def _sentinel_landed() -> bool:
            cur.execute(
                "SELECT delivered_count, window_start, window_end "
                "FROM mv_delivery_zone_status "
                "WHERE delivery_zone = %s",
                (KILL_TEST_ZONE,),
            )
            row = cur.fetchone()
            return row is not None and row[0] >= 1

        try:
            poll_until(_sentinel_landed, timeout_s=90, interval_s=2)
        except TimeoutError:
            # Diagnostic: show watermark state
            cur.execute("SELECT MAX(scanned_at) FROM delivery_update_source")
            max_scanned = cur.fetchone()
            pytest.fail(
                f"Sentinel event for zone '{KILL_TEST_ZONE}' never appeared in MV "
                f"after 90s. MAX(scanned_at) in source: {max_scanned}"
            )

        # 4. Positive assertions on the landed row
        cur.execute(
            "SELECT delivered_count, window_start, window_end "
            "FROM mv_delivery_zone_status "
            "WHERE delivery_zone = %s",
            (KILL_TEST_ZONE,),
        )
        row = cur.fetchone()
        assert row is not None
        delivered_count, window_start, window_end = row

        assert delivered_count == 1, (
            f"Expected delivered_count=1 for {KILL_TEST_ZONE}, got {delivered_count}"
        )

        # Window is a 5-minute tumble — duration should be exactly 5 minutes
        window_duration = window_end - window_start
        assert window_duration == timedelta(minutes=5), (
            f"Expected 5-minute window, got {window_duration}"
        )

        # T0 must fall within [window_start, window_end)
        # psycopg2 returns naive datetime for TIMESTAMPTZ in some configs; normalise.
        ws = window_start.replace(tzinfo=UTC) if window_start.tzinfo is None else window_start
        we = window_end.replace(tzinfo=UTC) if window_end.tzinfo is None else window_end
        t0_aware = T0

        assert ws <= t0_aware < we, f"T0={t0_aware} not within window [{ws}, {we})"

    def test_beyond_tolerance_event_is_dropped(self, risingwave_kill) -> None:
        """A delivery_update with scanned_at=T0-7h is correctly dropped (beyond 6h tolerance)."""
        conn, bootstrap = risingwave_kill

        # Emit an event with scanned_at=T0-7h — beyond the 6-hour watermark tolerance.
        drop_shipment = str(uuid.uuid4())
        drop_event = _make_delivery_update(
            shipment_id=drop_shipment,
            delivery_zone=BEYOND_TOLERANCE_ZONE,
            scanned_at=T0 - timedelta(hours=7),
            produced_at=T0 + timedelta(hours=5, minutes=30),
            is_final=True,
            status="delivered",
        )

        sink = KafkaSink(bootstrap_servers=bootstrap)
        sink.send("delivery_update", f"{drop_shipment}_1", drop_event)

        # Re-advance the watermark to ensure all partitions have processed past this event.
        advance_time = T0 + timedelta(hours=6, seconds=2)
        advance_ids = [str(uuid.uuid4()) for _ in range(4)]
        for ship_id in advance_ids:
            for seq in range(1, 4):
                adv = _make_delivery_update(
                    shipment_id=ship_id,
                    delivery_zone=ADVANCE_ZONE,
                    scanned_at=advance_time,
                    produced_at=advance_time,
                    is_final=False,
                    status="in_transit",
                    seq=seq,
                )
                sink.send("delivery_update", f"{ship_id}_{seq}", adv)
        sink.flush()

        # Wait 30s then assert the beyond-tolerance zone never appeared in the MV.
        time.sleep(30)

        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM mv_delivery_zone_status WHERE delivery_zone = %s",
            (BEYOND_TOLERANCE_ZONE,),
        )
        row = cur.fetchone()
        count = row[0] if row else 0

        assert count == 0, (
            f"Beyond-tolerance event for zone '{BEYOND_TOLERANCE_ZONE}' appeared in MV "
            f"(count={count}). The watermark kill-test FAILED — late events beyond "
            f"the 6-hour tolerance are not being dropped."
        )
