"""Switch RisingWave watermark mode between standard (5 minutes) and fault (6 hours).

Usage:
    python scripts/switch_watermark.py --mode standard
    python scripts/switch_watermark.py --mode fault
    python scripts/switch_watermark.py --mode standard --host localhost --port 4566

What it does:
    1. Drops delivery_update_source (and dependent MVs) via CASCADE.
    2. Recreates all four sources from the appropriate SQL file.
    3. Recreates all materialized views from sql/02_mvs.sql.

The two SQL files (01_sources.sql and 01_sources_fault_mode.sql) are the
authoritative, independently reviewable record of each mode. No sed-in-place.
See docs/adr/0004-ci-strategy.md § "Watermark mode switch" for rationale.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = REPO_ROOT / "sql"

SQL_FILES: dict[str, Path] = {
    "standard": SQL_DIR / "01_sources.sql",
    "fault": SQL_DIR / "01_sources_fault_mode.sql",
}
MV_FILE = SQL_DIR / "02_mvs.sql"

# Sources must be dropped in reverse-dependency order. CASCADE handles this,
# but we drop the watermarked source explicitly to ensure the MV chain is clean.
DROP_STATEMENTS = [
    "DROP MATERIALIZED VIEW IF EXISTS mv_seller_health_alert_candidates CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_fulfillment_sla_5min CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_seller_health_1hour CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_late_shipment_alert CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_delivery_zone_status CASCADE",
    "DROP SOURCE IF EXISTS delivery_update_source CASCADE",
    "DROP SOURCE IF EXISTS order_placed_source CASCADE",
    "DROP SOURCE IF EXISTS shipment_created_source CASCADE",
    "DROP SOURCE IF EXISTS seller_activity_source CASCADE",
]


def _split_statements(sql: str) -> list[str]:
    """Split SQL into individual statements, stripping comments and blanks."""
    statements = []
    for raw in sql.split(";"):
        stripped = raw.strip()
        # Strip line comments
        lines = [line for line in stripped.splitlines() if not line.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if clean:
            statements.append(clean)
    return statements


def switch_watermark(
    mode: str,
    host: str = "localhost",
    port: int = 4566,
    broker: str | None = None,
) -> None:
    """Switch watermark mode on a live RisingWave instance.

    Args:
        mode: 'standard' (5-minute watermark) or 'fault' (6-hour watermark).
        host: RisingWave host.
        port: RisingWave SQL port.
        broker: Kafka broker address to substitute in source DDL.
                Defaults to 'redpanda:9092' (the compose default).
    """
    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit("psycopg2-binary is required: uv add psycopg2-binary") from exc

    if mode not in SQL_FILES:
        raise ValueError(f"mode must be 'standard' or 'fault', got '{mode}'")

    sources_sql = SQL_FILES[mode].read_text(encoding="utf-8")
    mvs_sql = MV_FILE.read_text(encoding="utf-8")

    if broker:
        sources_sql = sources_sql.replace("redpanda:9092", broker)

    conn = psycopg2.connect(host=host, port=port, user="root", dbname="dev")
    conn.autocommit = True
    cur = conn.cursor()

    print(f"[switch_watermark] Dropping MVs and sources (mode={mode})...")
    for stmt in DROP_STATEMENTS:
        cur.execute(stmt)

    print(f"[switch_watermark] Recreating sources from {SQL_FILES[mode].name}...")
    for stmt in _split_statements(sources_sql):
        cur.execute(stmt)

    print(f"[switch_watermark] Recreating MVs from {MV_FILE.name}...")
    for stmt in _split_statements(mvs_sql):
        cur.execute(stmt)

    cur.close()
    conn.close()
    print(f"[switch_watermark] Done. Watermark mode is now '{mode}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Switch RisingWave watermark mode.")
    parser.add_argument("--mode", required=True, choices=["standard", "fault"])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=4566)
    parser.add_argument(
        "--broker",
        default=None,
        help="Kafka broker address to substitute in source DDL (default: redpanda:9092)",
    )
    args = parser.parse_args()

    try:
        switch_watermark(mode=args.mode, host=args.host, port=args.port, broker=args.broker)
    except Exception as exc:
        print(f"[switch_watermark] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
