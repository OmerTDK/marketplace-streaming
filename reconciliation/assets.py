"""Dagster assets and the reconciliation asset-check — the headline differentiator.

Three materializable assets plus one asset-check:

  clickhouse_sync_asset       reads mv_fulfillment_sla_5min from RisingWave and
                              writes the streaming sink fulfillment_sla
                              (ReplacingMergeTree, idempotent).

  batch_recompute_asset       recomputes the same SLA metric from the raw
                              order/delivery events via pandas and writes
                              batch_recompute_fulfillment_sla. The independent
                              second compute path.

  reconciliation_audit_asset  diffs the streaming sink against the batch table
                              per window key and appends to reconciliation_audit.

  reconciliation_check        an asset-check on reconciliation_audit_asset that
                              FAILS when any window diverges beyond tolerance
                              and PASSES on a clean run. This is the kill-switch:
                              a divergence cannot pass silently.

Every asset body is a thin wrapper over the plain functions in
``reconciliation.io`` and ``reconciliation.logic`` so the same logic is unit-
and integration-testable without booting the Dagster daemon (ADR-0005).
"""

# NOTE: no `from __future__ import annotations` here. Dagster introspects the
# REAL `context: AssetExecutionContext` annotation at decoration time; stringized
# annotations (PEP 563) break that resolution with DagsterInvalidDefinitionError.

from datetime import UTC, datetime

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    asset,
    asset_check,
)
from reconciliation.io import (
    read_deliveries_from_risingwave,
    read_orders_from_risingwave,
    read_streaming_sla_from_clickhouse,
    sync_streaming_sla_to_clickhouse,
    write_batch_recompute_to_clickhouse,
    write_reconciliation_audit,
)
from reconciliation.logic import (
    DEFAULT_TOLERANCE,
    STATUS_DIVERGED,
    batch_recompute_fulfillment_sla,
    is_clean,
    max_abs_delta,
    reconcile,
)
from reconciliation.resources import ClickHouseResource, RisingWaveResource

# Tolerance the asset-check enforces. 0 = streaming and batch must agree exactly
# on within_sla_count for every closed window. Kept as a module constant so the
# asset and the check share one source of truth (no magic numbers).
RECONCILIATION_TOLERANCE = DEFAULT_TOLERANCE


@asset(
    description="Sync mv_fulfillment_sla_5min windows from RisingWave into the "
    "ClickHouse fulfillment_sla ReplacingMergeTree (idempotent).",
)
def clickhouse_sync_asset(
    context: AssetExecutionContext,
    risingwave: RisingWaveResource,
    clickhouse: ClickHouseResource,
) -> MaterializeResult:
    """Read new streaming MV windows and write them to the ClickHouse sink."""
    with risingwave.connection() as rw_conn, clickhouse.client() as ch_client:
        rows_written = sync_streaming_sla_to_clickhouse(rw_conn, ch_client)
    context.log.info("clickhouse_sync_asset wrote %d streaming SLA rows", rows_written)
    return MaterializeResult(metadata={"rows_written": rows_written})


@asset(
    description="Recompute closed-window fulfillment_sla metrics from the raw "
    "order/delivery events via pandas; write batch_recompute_fulfillment_sla.",
)
def batch_recompute_asset(
    context: AssetExecutionContext,
    risingwave: RisingWaveResource,
    clickhouse: ClickHouseResource,
) -> MaterializeResult:
    """Independent batch compute path: same events, recomputed with pandas."""
    with risingwave.connection() as rw_conn:
        orders = read_orders_from_risingwave(rw_conn)
        deliveries = read_deliveries_from_risingwave(rw_conn)
    batch_rows = batch_recompute_fulfillment_sla(orders, deliveries)
    with clickhouse.client() as ch_client:
        rows_written = write_batch_recompute_to_clickhouse(ch_client, batch_rows)
    context.log.info("batch_recompute_asset wrote %d batch rows", rows_written)
    return MaterializeResult(metadata={"rows_written": rows_written})


@asset(
    deps=[clickhouse_sync_asset, batch_recompute_asset],
    description="Diff the streaming sink against the batch recompute per window "
    "key and append the verdict to reconciliation_audit.",
)
def reconciliation_audit_asset(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> MaterializeResult:
    """Reconcile streaming vs batch and write the audit trail."""
    checked_at = datetime.now(tz=UTC)
    with clickhouse.client() as ch_client:
        streaming_rows = read_streaming_sla_from_clickhouse(ch_client)
        batch_rows = read_streaming_sla_from_clickhouse(
            ch_client, table="batch_recompute_fulfillment_sla"
        )
        audit = reconcile(
            streaming_rows,
            batch_rows,
            tolerance=RECONCILIATION_TOLERANCE,
            checked_at=checked_at,
        )
        rows_written = write_reconciliation_audit(ch_client, audit, checked_at)
    diverged = sum(1 for row in audit if row.status == STATUS_DIVERGED)
    context.log.info(
        "reconciliation_audit_asset wrote %d audit rows (%d diverged)",
        rows_written,
        diverged,
    )
    return MaterializeResult(
        metadata={
            "audit_rows": rows_written,
            "diverged_windows": diverged,
            "max_abs_delta": max_abs_delta(audit),
        }
    )


@asset_check(
    asset=reconciliation_audit_asset,
    description="Fail when streaming and batch disagree on within_sla_count "
    "beyond tolerance for any closed window.",
)
def reconciliation_check(clickhouse: ClickHouseResource) -> AssetCheckResult:
    """The kill-switch: a divergence beyond tolerance fails this check.

    Re-reads the streaming sink and batch table and re-runs the diff (rather
    than trusting a stored verdict) so the check reflects the current state of
    both tables at check time.
    """
    with clickhouse.client() as ch_client:
        streaming_rows = read_streaming_sla_from_clickhouse(ch_client)
        batch_rows = read_streaming_sla_from_clickhouse(
            ch_client, table="batch_recompute_fulfillment_sla"
        )
    audit = reconcile(streaming_rows, batch_rows, tolerance=RECONCILIATION_TOLERANCE)
    passed = is_clean(audit, tolerance=RECONCILIATION_TOLERANCE)
    diverged = [row for row in audit if row.status == STATUS_DIVERGED]
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "windows_checked": len(audit),
            "diverged_windows": len(diverged),
            "max_abs_delta": max_abs_delta(audit),
            "tolerance": RECONCILIATION_TOLERANCE,
        },
    )


defs = Definitions(
    assets=[clickhouse_sync_asset, batch_recompute_asset, reconciliation_audit_asset],
    asset_checks=[reconciliation_check],
    resources={
        "risingwave": RisingWaveResource.from_env(),
        "clickhouse": ClickHouseResource.from_env(),
    },
)
