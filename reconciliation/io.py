"""IO adapters for the reconciliation flow — RisingWave reads, ClickHouse writes.

Plain functions, no Dagster. The Dagster assets in ``reconciliation.assets``
wrap these; the integration test calls them directly against the real compose
substrate (the Phase-2 lesson: extract the logic from the asset so it is
testable without booting the Dagster daemon — see ADR-0004 / ADR-0005).

Connection objects are passed in, never created here, so the caller owns the
lifecycle and the same functions work against a live RisingWave/ClickHouse in
the integration test and against a Dagster resource at runtime.
"""

from __future__ import annotations

from datetime import datetime

from reconciliation.logic import ReconciliationRow

# Streaming sink table (the clickhouse_sync_asset target) and its column order.
STREAMING_SLA_TABLE = "fulfillment_sla"
BATCH_SLA_TABLE = "batch_recompute_fulfillment_sla"
RECONCILIATION_AUDIT_TABLE = "reconciliation_audit"

# Column order for the fulfillment_sla / batch_recompute_fulfillment_sla tables.
SLA_COLUMNS = (
    "window_start",
    "window_end",
    "seller_id",
    "product_category",
    "state_code",
    "orders_placed_count",
    "within_sla_count",
    "breached_sla_count",
    "sla_compliance_pct",
)

# The streaming MV name in RisingWave.
STREAMING_MV = "mv_fulfillment_sla_5min"


def read_orders_from_risingwave(rw_conn) -> list[dict]:
    """Read order_placed events from the RisingWave source for batch recompute.

    Reads the durable event log (the Kafka-backed source), NOT the streaming MV.
    This is the independent input the batch path recomputes from. Returns every
    order including null_field faults; the batch logic filters them, mirroring
    the MV's WHERE clause, so the filter lives in exactly one place.

    Args:
        rw_conn: psycopg2 connection to RisingWave (autocommit).

    Returns:
        order_placed event dicts with the keys batch_recompute_fulfillment_sla
        requires.
    """
    cur = rw_conn.cursor()
    cur.execute(
        "SELECT order_id, seller_id, product_category, state_code, "
        "event_time, sla_deadline_at, fault_type "
        "FROM order_placed_source"
    )
    return [
        {
            "order_id": row[0],
            "seller_id": row[1],
            "product_category": row[2],
            "state_code": row[3],
            "event_time": row[4],
            "sla_deadline_at": row[5],
            "fault_type": row[6],
        }
        for row in cur.fetchall()
    ]


def read_deliveries_from_risingwave(rw_conn) -> list[dict]:
    """Read final delivered delivery_update events from the RisingWave source.

    Only is_final delivered rows matter for SLA recompute (the MV's
    delivery_finals CTE), but the filter stays in the batch logic; this read
    returns all final rows so the same data feeds both within/breached logic.

    Args:
        rw_conn: psycopg2 connection to RisingWave (autocommit).

    Returns:
        delivery_update event dicts keyed by order_id with scanned_at.
    """
    cur = rw_conn.cursor()
    cur.execute(
        "SELECT order_id, status, is_final, scanned_at "
        "FROM delivery_update_source "
        "WHERE is_final = TRUE"
    )
    return [
        {
            "order_id": row[0],
            "status": row[1],
            "is_final": row[2],
            "scanned_at": row[3],
        }
        for row in cur.fetchall()
    ]


def sync_streaming_sla_to_clickhouse(
    rw_conn,
    ch_client,
    table: str = STREAMING_SLA_TABLE,
) -> int:
    """Read mv_fulfillment_sla_5min from RisingWave and write to ClickHouse.

    The clickhouse_sync_asset body. Reads every window the streaming MV has
    produced and inserts into the ReplacingMergeTree sink. Idempotent: re-runs
    re-insert the same (window_start, seller, category, state) keys with the
    same window_end version, so ReplacingMergeTree collapses them on merge — a
    re-sync never double-counts.

    Args:
        rw_conn: psycopg2 connection to RisingWave (autocommit).
        ch_client: clickhouse_driver Client.
        table: target sink table. Defaults to fulfillment_sla.

    Returns:
        Number of rows written.
    """
    cur = rw_conn.cursor()
    cur.execute(
        "SELECT window_start, window_end, seller_id, product_category, state_code, "
        "orders_placed_count, within_sla_count, breached_sla_count, sla_compliance_pct "
        f"FROM {STREAMING_MV}"
    )
    rows = cur.fetchall()
    if not rows:
        return 0
    columns = ", ".join(SLA_COLUMNS)
    ch_client.execute(
        f"INSERT INTO {table} ({columns}) VALUES",
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


def read_streaming_sla_from_clickhouse(ch_client, table: str = STREAMING_SLA_TABLE) -> list[dict]:
    """Read the streaming SLA rows from ClickHouse with FINAL.

    FINAL is mandatory on every read of a ReplacingMergeTree table (ADR-0002):
    dedup is lazy, so without FINAL a window may appear twice. This is the
    streaming side of the reconciliation diff.

    Args:
        ch_client: clickhouse_driver Client.
        table: source table name. Defaults to the streaming sink fulfillment_sla.

    Returns:
        One dict per window with the SLA_COLUMNS fields.
    """
    columns = ", ".join(SLA_COLUMNS)
    rows = ch_client.execute(f"SELECT {columns} FROM {table} FINAL")
    return [dict(zip(SLA_COLUMNS, row, strict=True)) for row in rows]


def write_batch_recompute_to_clickhouse(
    ch_client,
    batch_rows: list[dict],
    table: str = BATCH_SLA_TABLE,
) -> int:
    """Write batch recompute rows to ClickHouse (idempotent via ReplacingMergeTree).

    Args:
        ch_client: clickhouse_driver Client.
        batch_rows: rows from logic.batch_recompute_fulfillment_sla.
        table: target table. Defaults to batch_recompute_fulfillment_sla.

    Returns:
        Number of rows written.
    """
    if not batch_rows:
        return 0
    columns = ", ".join(SLA_COLUMNS)
    ch_client.execute(
        f"INSERT INTO {table} ({columns}) VALUES",
        [{column: row[column] for column in SLA_COLUMNS} for row in batch_rows],
    )
    return len(batch_rows)


def write_reconciliation_audit(
    ch_client,
    audit_rows: list[ReconciliationRow],
    checked_at: datetime,
    table: str = RECONCILIATION_AUDIT_TABLE,
) -> int:
    """Append reconciliation audit rows to the audit trail (MergeTree, append-only).

    Args:
        ch_client: clickhouse_driver Client.
        audit_rows: ReconciliationRow objects from logic.reconcile.
        checked_at: the check timestamp stamped on every row.
        table: target table. Defaults to reconciliation_audit.

    Returns:
        Number of audit rows written.
    """
    if not audit_rows:
        return 0
    ch_client.execute(
        f"INSERT INTO {table} "
        "(checked_at, window_start, window_end, seller_id, streaming_value, "
        "batch_value, abs_delta, late_event_ids, status) VALUES",
        [
            {
                "checked_at": checked_at,
                "window_start": row.window_start,
                "window_end": row.window_end,
                "seller_id": row.seller_id,
                "streaming_value": row.streaming_value,
                "batch_value": row.batch_value,
                "abs_delta": row.abs_delta,
                "late_event_ids": list(row.late_event_ids),
                "status": row.status,
            }
            for row in audit_rows
        ],
    )
    return len(audit_rows)
