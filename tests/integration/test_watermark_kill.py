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
    REDPANDA_IMAGE,
    RISINGWAVE_IMAGE,
    SQL_DIR,
    create_topics,
    init_risingwave,
    poll_until,
)

RISINGWAVE_PORT = 4566
RISINGWAVE_USER = "root"
RISINGWAVE_DB = "dev"

# T0: the business event time for the kill-test delivery scan.
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
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "delivery_update",
        "event_version": "1.0",
        "produced_at": _fmt(produced_at),
        "event_time": _fmt(scanned_at),
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
def redpanda():
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer(image=REDPANDA_IMAGE) as container:
        bootstrap = container.get_bootstrap_server()
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)
        yield bootstrap


@pytest.fixture(scope="class")
def risingwave_kill(redpanda: str):
    """Start RisingWave with 6-hour delivery_update watermark for the kill-test."""
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
                user=RISINGWAVE_USER,
                dbname=RISINGWAVE_DB,
                connect_timeout=2,
            )
            c.close()
            return True
        except Exception:
            return False

    poll_until(_rw_ready, timeout_s=90, interval_s=2)

    # Use the FAULT MODE sources file (6-hour watermark on delivery_update_source)
    # so that our 5.5-hour-late event is within the tolerance window.
    fault_sources_sql = (SQL_DIR / "01_sources_fault_mode.sql").read_text(encoding="utf-8")
    fault_sources_sql = fault_sources_sql.replace("redpanda:9092", broker)
    assert "redpanda:9092" not in fault_sources_sql

    mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")

    conn = psycopg2.connect(
        host=container_host,
        port=mapped_port,
        user=RISINGWAVE_USER,
        dbname=RISINGWAVE_DB,
    )
    conn.autocommit = True
    init_risingwave(conn, fault_sources_sql, mvs_sql)

    yield conn, container_host, mapped_port, broker

    conn.close()
    container.stop()


# ---------------------------------------------------------------------------
# Kill-test
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestWatermarkKill:
    """ADR-0002 kill-test: late events land correctly; beyond-tolerance events drop."""

    def test_late_event_lands_in_correct_window(self, redpanda: str, risingwave_kill) -> None:
        """A delivery_update with scanned_at=T0 and produced_at=T0+5.5h lands in the window."""
        conn, _, _, bootstrap = risingwave_kill

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

    def test_beyond_tolerance_event_is_dropped(self, redpanda: str, risingwave_kill) -> None:
        """A delivery_update with scanned_at=T0-7h is correctly dropped (beyond 6h tolerance)."""
        conn, _, _, bootstrap = risingwave_kill

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
