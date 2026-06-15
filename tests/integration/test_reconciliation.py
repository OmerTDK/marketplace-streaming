"""Integration test: batch-vs-stream reconciliation on the compose substrate.

The Phase-3 headline. Brings up the repo's docker-compose topology (Redpanda +
RisingWave + ClickHouse), produces events, and proves the full reconciliation
flow against REAL infrastructure — without booting the Dagster daemon (ADR-0005):

  1. clickhouse_sync writes streaming SLA rows into the ClickHouse sink.
  2. batch_recompute reads the SAME raw events from the RisingWave source and
     recomputes within_sla_count via pandas — and MATCHES the streaming MV on
     every window the stream has emitted (the clean run).
  3. The reconciliation asset-check is materialized IN-PROCESS and PASSES on the
     clean run, then FAILS on an injected divergence — kill-verified both ways.

The assets are materialized via Dagster's ``materialize`` against an
``InMemoryIOManager`` using resources pointed at the live compose endpoints. No
Dagster daemon, no scheduler — just the in-process executor, which is the
supported testing path (ADR-0005, Dagster testing docs).
"""

from __future__ import annotations

import pytest

# dagster is an integration-only dependency. Skip the whole module at COLLECTION
# time when it is absent so the fast CI lane (which does not install dagster)
# never fails importing it. The reconciliation.logic/io imports below are
# pandas-only and safe in either lane; dagster, reconciliation.assets and
# reconciliation.resources must come AFTER this guard.
pytest.importorskip("dagster")

from dagster import (
    AssetCheckSeverity,
    materialize,
)

from generator.clock import SimClock
from generator.generator import MarketplaceGenerator
from generator.sink import KafkaSink
from reconciliation.assets import (
    batch_recompute_asset,
    clickhouse_sync_asset,
    reconciliation_audit_asset,
    reconciliation_check,
)
from reconciliation.io import (
    BATCH_SLA_TABLE,
    STREAMING_SLA_TABLE,
    read_deliveries_from_risingwave,
    read_orders_from_risingwave,
    read_streaming_sla_from_clickhouse,
    sync_streaming_sla_to_clickhouse,
    write_batch_recompute_to_clickhouse,
)
from reconciliation.logic import (
    RECONCILED_METRIC,
    batch_recompute_fulfillment_sla,
    is_clean,
    reconcile,
)
from reconciliation.resources import ClickHouseResource, RisingWaveResource
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

N_EVENTS = 300
SEED = 42
SIM_START = "2024-01-08T00:00:00Z"
TIME_ACCELERATION = 3600.0

# The two SLA sink tables and the audit table the reconciliation flow writes.
SLA_TABLES_DDL = {
    STREAMING_SLA_TABLE: """
        CREATE TABLE IF NOT EXISTS {table}
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
    """,
    BATCH_SLA_TABLE: """
        CREATE TABLE IF NOT EXISTS {table}
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
    """,
}

RECONCILIATION_AUDIT_DDL = """
    CREATE TABLE IF NOT EXISTS reconciliation_audit
    (
        checked_at       DateTime,
        window_start     DateTime,
        window_end       DateTime,
        seller_id        String,
        streaming_value  UInt64,
        batch_value      UInt64,
        abs_delta        UInt64,
        late_event_ids   Array(String),
        status           String
    )
    ENGINE = MergeTree()
    ORDER BY (checked_at, window_start, seller_id)
"""


@pytest.fixture(scope="class")
def reconciliation_env():
    """Compose topology + RisingWave (standard sources) + populated streaming MV.

    Produces N_EVENTS, waits for mv_fulfillment_sla_5min to populate, creates
    the three reconciliation ClickHouse tables, and yields the live endpoints
    plus a clickhouse_driver client.

    Yields a dict with: rw_conn, ch_client, rw_host, rw_port, ch_host, ch_port.
    """
    from clickhouse_driver import Client

    with compose_topology("mktstream_reconciliation") as compose:
        bootstrap = kafka_bootstrap(compose)
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)

        rw_host, rw_port = risingwave_endpoint(compose)
        rw_conn = connect_risingwave(rw_host, rw_port)

        sources_sql = (SQL_DIR / "01_sources.sql").read_text(encoding="utf-8")
        mvs_sql = (SQL_DIR / "02_mvs.sql").read_text(encoding="utf-8")
        init_risingwave(rw_conn, sources_sql, mvs_sql)

        ch_host, ch_port = clickhouse_endpoint(compose)
        ch_client = Client(host=ch_host, port=ch_port)
        for table, ddl in SLA_TABLES_DDL.items():
            ch_client.execute(ddl.format(table=table))
        ch_client.execute(RECONCILIATION_AUDIT_DDL)

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

        try:
            yield {
                "rw_conn": rw_conn,
                "ch_client": ch_client,
                "rw_host": rw_host,
                "rw_port": rw_port,
                "ch_host": ch_host,
                "ch_port": ch_port,
            }
        finally:
            ch_client.disconnect()
            rw_conn.close()


def _resources(env: dict) -> dict:
    """Build Dagster resources pointed at the live compose endpoints."""
    return {
        "risingwave": RisingWaveResource(host=env["rw_host"], port=env["rw_port"]),
        "clickhouse": ClickHouseResource(host=env["ch_host"], port=env["ch_port"]),
    }


def _streaming_by_key(rows: list[dict]) -> dict[tuple, int]:
    return {
        (r["window_start"], r["seller_id"], r["product_category"], r["state_code"]): r[
            RECONCILED_METRIC
        ]
        for r in rows
    }


@pytest.mark.integration
class TestReconciliation:
    """Batch-vs-stream reconciliation against real RisingWave + ClickHouse."""

    def test_sync_writes_streaming_rows(self, reconciliation_env) -> None:
        """clickhouse_sync_asset logic writes >=1 row into the streaming sink."""
        rw_conn = reconciliation_env["rw_conn"]
        ch_client = reconciliation_env["ch_client"]

        rows_written = sync_streaming_sla_to_clickhouse(rw_conn, ch_client)
        assert rows_written >= 1, "sync wrote 0 streaming rows"

        count = ch_client.execute(f"SELECT COUNT(*) FROM {STREAMING_SLA_TABLE} FINAL")[0][0]
        assert count >= 1

    def test_batch_recompute_matches_streaming_on_clean_run(self, reconciliation_env) -> None:
        """Batch within_sla_count matches the streaming MV on every emitted window.

        Independent compute paths over the SAME events: the streaming MV (RisingWave)
        and the pandas recompute. On a clean (fault-free) stream they must agree
        exactly on within_sla_count for every window the stream has emitted.
        """
        rw_conn = reconciliation_env["rw_conn"]
        ch_client = reconciliation_env["ch_client"]

        # Ensure the streaming sink is populated.
        sync_streaming_sla_to_clickhouse(rw_conn, ch_client)
        streaming_rows = read_streaming_sla_from_clickhouse(ch_client)
        assert streaming_rows, "streaming sink is empty"

        # Batch recompute from the raw source events.
        orders = read_orders_from_risingwave(rw_conn)
        deliveries = read_deliveries_from_risingwave(rw_conn)
        batch_rows = batch_recompute_fulfillment_sla(orders, deliveries)
        assert batch_rows, "batch recompute produced no rows"
        write_batch_recompute_to_clickhouse(ch_client, batch_rows)

        # For every window the STREAM emitted, the batch must agree exactly.
        # (The batch may additionally hold not-yet-closed windows; the stream is
        # the closed-window reference here.)
        streaming_values = _streaming_by_key(streaming_rows)
        batch_values = _streaming_by_key(batch_rows)
        for key, streaming_value in streaming_values.items():
            assert key in batch_values, f"window {key} in stream but missing from batch"
            assert batch_values[key] == streaming_value, (
                f"clean-run divergence at {key}: "
                f"streaming={streaming_value}, batch={batch_values[key]}"
            )

        # The reconcile diff over shared windows is clean.
        shared_keys = set(streaming_values) & set(batch_values)
        streaming_subset = [
            r
            for r in streaming_rows
            if (r["window_start"], r["seller_id"], r["product_category"], r["state_code"])
            in shared_keys
        ]
        batch_subset = [
            r
            for r in batch_rows
            if (r["window_start"], r["seller_id"], r["product_category"], r["state_code"])
            in shared_keys
        ]
        audit = reconcile(streaming_subset, batch_subset, tolerance=0)
        assert is_clean(audit), (
            "reconcile reported divergence on a clean run: "
            f"max_delta={max(r.abs_delta for r in audit)}"
        )

    def test_dagster_assets_materialize_in_process(self, reconciliation_env) -> None:
        """The 3 assets materialize in-process against real RisingWave + ClickHouse."""
        result = materialize(
            [clickhouse_sync_asset, batch_recompute_asset, reconciliation_audit_asset],
            resources=_resources(reconciliation_env),
        )
        assert result.success, "in-process materialize of reconciliation assets failed"

        ch_client = reconciliation_env["ch_client"]
        streaming_count = ch_client.execute(f"SELECT COUNT(*) FROM {STREAMING_SLA_TABLE} FINAL")[0][
            0
        ]
        batch_count = ch_client.execute(f"SELECT COUNT(*) FROM {BATCH_SLA_TABLE} FINAL")[0][0]
        audit_count = ch_client.execute("SELECT COUNT(*) FROM reconciliation_audit")[0][0]
        assert streaming_count >= 1
        assert batch_count >= 1
        assert audit_count >= 1

    def test_asset_check_passes_on_clean_run(self, reconciliation_env) -> None:
        """The reconciliation asset-check PASSES when streaming and batch agree.

        Two paths, both asserted: (a) the full in-process materialize of the
        assets + their checks succeeds, and (b) the check function invoked
        directly returns passed=True against the live, aligned tables.
        """
        resources = _resources(reconciliation_env)
        env = reconciliation_env

        # Align both tables from the same source events (full asset+check run).
        # The check must be in the assets list for materialize to RUN it —
        # materialize([asset]) alone does not execute the asset's checks.
        result = materialize(
            [
                clickhouse_sync_asset,
                batch_recompute_asset,
                reconciliation_audit_asset,
                reconciliation_check,
            ],
            resources=resources,
        )
        assert result.success
        evaluations = result.get_asset_check_evaluations()
        assert len(evaluations) == 1, f"expected 1 check evaluation, got {len(evaluations)}"
        assert evaluations[0].passed, (
            f"reconciliation_check FAILED on a clean run — metadata={evaluations[0].metadata}"
        )

        # Direct invocation of the check (deterministic) — also passes.
        check_result = reconciliation_check(
            clickhouse=ClickHouseResource(host=env["ch_host"], port=env["ch_port"])
        )
        assert check_result.passed, (
            f"direct reconciliation_check FAILED on clean run — {check_result.metadata}"
        )

    def test_asset_check_fails_on_injected_divergence(self, reconciliation_env) -> None:
        """KILL-VERIFY: inject a divergence and prove the asset-check FAILS.

        Writes one streaming-sink row whose within_sla_count is deliberately
        inflated above the batch value for an existing window. The asset-check
        re-reads both tables, the reconcile diff flags the window as diverged,
        and the check must return passed=False with ERROR severity.

        If this assertion ever sees passed=True, the reconciliation guard is
        broken — a divergence would pass silently, which is the exact failure
        the headline differentiator exists to prevent.
        """
        rw_conn = reconciliation_env["rw_conn"]
        ch_client = reconciliation_env["ch_client"]
        resources = _resources(reconciliation_env)
        env = reconciliation_env

        # Establish a clean baseline first.
        materialize(
            [clickhouse_sync_asset, batch_recompute_asset, reconciliation_audit_asset],
            resources=resources,
        )

        # Pick an existing streaming window and inflate within_sla_count well
        # beyond the batch value, using a NEW window_end so ReplacingMergeTree
        # keeps this (diverging) version on FINAL read.
        streaming_rows = read_streaming_sla_from_clickhouse(ch_client)
        assert streaming_rows, "streaming sink unexpectedly empty"
        target = streaming_rows[0]
        inflated_within = int(target["within_sla_count"]) + 999
        bumped_window_end = target["window_end"].replace(year=target["window_end"].year + 1)
        ch_client.execute(
            f"INSERT INTO {STREAMING_SLA_TABLE} "
            "(window_start, window_end, seller_id, product_category, state_code, "
            "orders_placed_count, within_sla_count, breached_sla_count, sla_compliance_pct) "
            "VALUES",
            [
                {
                    "window_start": target["window_start"],
                    "window_end": bumped_window_end,
                    "seller_id": target["seller_id"],
                    "product_category": target["product_category"],
                    "state_code": target["state_code"],
                    "orders_placed_count": int(target["orders_placed_count"]),
                    "within_sla_count": inflated_within,
                    "breached_sla_count": int(target["breached_sla_count"]),
                    "sla_compliance_pct": float(target["sla_compliance_pct"]),
                }
            ],
        )

        # Run the asset-check against the now-diverged streaming sink — direct
        # invocation, so the AssetCheckResult is asserted on without any
        # materialize/IO-manager indirection.
        check_result = reconciliation_check(
            clickhouse=ClickHouseResource(host=env["ch_host"], port=env["ch_port"])
        )
        assert not check_result.passed, (
            "reconciliation_check PASSED on an injected divergence — "
            "the reconciliation guard is broken (a divergence passed silently)"
        )
        assert check_result.severity == AssetCheckSeverity.ERROR
        assert int(check_result.metadata["diverged_windows"].value) >= 1

        # Same divergence also fails the full in-process materialize + check.
        # The check is included in the assets list so materialize RUNS it, and
        # the audit asset re-reads the now-diverged streaming sink at run time.
        # raise_on_error=False because a failing ERROR-severity check surfaces in
        # the result without aborting the run.
        result = materialize(
            [reconciliation_audit_asset, reconciliation_check],
            resources=resources,
            raise_on_error=False,
        )
        evaluations = result.get_asset_check_evaluations()
        assert len(evaluations) == 1
        assert not evaluations[0].passed, (
            "in-process reconciliation_check PASSED on an injected divergence"
        )

        # Sanity: the same divergence is visible through the pure reconcile path.
        orders = read_orders_from_risingwave(rw_conn)
        deliveries = read_deliveries_from_risingwave(rw_conn)
        batch_rows = batch_recompute_fulfillment_sla(orders, deliveries)
        post_audit = reconcile(
            read_streaming_sla_from_clickhouse(ch_client), batch_rows, tolerance=0
        )
        assert not is_clean(post_audit)
