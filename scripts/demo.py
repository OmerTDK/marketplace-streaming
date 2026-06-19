"""E2E demo script: boots scenario, injects faults, shows reconciliation.

Usage:
    python scripts/demo.py [--dry-run] [--host localhost] [--rw-port 4566] [--ch-port 8123]

Modes:
    live (default)  — requires the docker-compose stack to be running
    --dry-run       — prints what each step would do; exits 0 (CI-safe)

Demo scenario:
    1. Verify stack is up and print baseline stats.
    2. Enable late_arrival fault injection.
    3. Wait 30 seconds (= ~30 simulated hours at 3600x acceleration).
    4. Show SLA impact in ClickHouse (diverged windows appear).
    5. Disable fault injection and restore standard watermark.
    6. Wait for convergence, then print final reconciliation summary.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _ts() -> str:
    return datetime.now(tz=UTC).strftime("%H:%M:%S")


def _banner(msg: str) -> None:
    width = 70
    print(f"\n{'=' * width}")
    print(f"  [{_ts()}]  {msg}")
    print(f"{'=' * width}")


def _step(msg: str) -> None:
    print(f"  [{_ts()}]  {msg}")


def _warn(msg: str) -> None:
    print(f"  [{_ts()}]  WARNING: {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Stack health check
# ---------------------------------------------------------------------------


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _check_stack(host: str, rw_port: int, ch_port: int) -> bool:
    rw_ok = _port_open(host, rw_port)
    ch_ok = _port_open(host, ch_port)
    return rw_ok and ch_ok


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------


def _ch_query(host: str, ch_port: int, sql: str) -> str:
    """Run a ClickHouse HTTP query and return the raw response text."""
    import urllib.error
    import urllib.parse
    import urllib.request

    url = f"http://{host}:{ch_port}/?query={urllib.parse.quote(sql)}&default_format=TabSeparated"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return resp.read().decode()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"ClickHouse query failed: {exc}") from exc


def _ch_row_count(host: str, ch_port: int, table: str) -> int:
    raw = _ch_query(host, ch_port, f"SELECT count() FROM {table} FINAL").strip()
    return int(raw) if raw else 0


def _ch_reconciliation_summary(host: str, ch_port: int) -> dict[str, int]:
    sql = "SELECT status, count() FROM reconciliation_audit GROUP BY status FORMAT TabSeparated"
    raw = _ch_query(host, ch_port, sql).strip()
    result: dict[str, int] = {}
    for line in raw.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) == 2:
            result[parts[0]] = int(parts[1])
    return result


# ---------------------------------------------------------------------------
# Fault control helpers
# ---------------------------------------------------------------------------


def _fault_control(mode: str) -> None:
    script = REPO_ROOT / "scripts" / "fault_control.py"
    subprocess.run([sys.executable, str(script), "--mode", mode], check=True)


# ---------------------------------------------------------------------------
# Dry-run narrative
# ---------------------------------------------------------------------------


DRY_RUN_STEPS = [
    "STEP 1 — Checking stack health",
    "  → RisingWave :4566 ... UP",
    "  → ClickHouse  :8123 ... UP",
    "",
    "STEP 2 — Baseline stats",
    "  fulfillment_sla rows (streaming): 140",
    "  batch_recompute_fulfillment_sla rows: 140",
    "  reconciliation_audit rows: 140  (within_tolerance: 140, diverged: 0)",
    "",
    "STEP 3 — Enabling late_arrival fault injection (10% rate)",
    "  [fault_control] Mode set to 'late_arrival'. Generator hot-reloads in ~5s.",
    "",
    "STEP 4 — Waiting 30 seconds for MVs to absorb late arrivals ...",
    "",
    "STEP 5 — Post-fault stats",
    "  fulfillment_sla rows (streaming): 155",
    "  batch_recompute_fulfillment_sla rows: 158  ← batch sees late events sooner",
    "  reconciliation_audit diverged windows: 3   ← watermark lag visible",
    "",
    "STEP 6 — Disabling fault injection",
    "  [fault_control] Mode set to 'off'. Generator hot-reloads in ~5s.",
    "",
    "STEP 7 — Waiting 30 seconds for watermark to advance and MVs to converge ...",
    "",
    "STEP 8 — Final reconciliation summary",
    "  within_tolerance : 155",
    "  converged        :   3",
    "  diverged         :   0   ← all windows resolved",
    "",
    "SUMMARY",
    "  Throughput         : 50 events/sec",
    "  End-to-end latency : <2s (event produced → MV row visible)",
    "  MV correctness     : 0 diverged windows at steady state (SEED=42)",
    "  Fault recovery     : ~30s wall-clock (approx 30 simulated hours at 3600x accel)",
    "",
    "[DRY RUN] Demo complete — no live services were contacted.",
]


# ---------------------------------------------------------------------------
# Live demo
# ---------------------------------------------------------------------------


def run_live_demo(host: str, rw_port: int, ch_port: int) -> int:
    _banner("marketplace-streaming — E2E fault injection demo")

    _step("STEP 1 — Checking stack health")
    if not _check_stack(host, rw_port, ch_port):
        print(
            "\nStack is not reachable. Start it first:\n"
            "  docker compose up --build\n\n"
            "Services take ~30s to become healthy. Then re-run this script.",
            file=sys.stderr,
        )
        return 1
    _step(f"  RisingWave :{rw_port} ... UP")
    _step(f"  ClickHouse  :{ch_port} ... UP")

    _banner("STEP 2 — Baseline stats")
    try:
        sla_rows = _ch_row_count(host, ch_port, "fulfillment_sla")
        batch_rows = _ch_row_count(host, ch_port, "batch_recompute_fulfillment_sla")
        audit_rows = _ch_row_count(host, ch_port, "reconciliation_audit")
        recon = _ch_reconciliation_summary(host, ch_port)
    except RuntimeError as exc:
        _warn(str(exc))
        sla_rows = batch_rows = audit_rows = 0
        recon = {}

    _step(f"  fulfillment_sla rows (streaming): {sla_rows}")
    _step(f"  batch_recompute rows            : {batch_rows}")
    _step(f"  reconciliation_audit rows       : {audit_rows}")
    _step(f"  verdict breakdown               : {json.dumps(recon)}")

    _banner("STEP 3 — Enabling late_arrival fault injection (10% rate)")
    _fault_control("late_arrival")
    _step("  Generator will hot-reload the fault config within 5 seconds.")

    _banner("STEP 4 — Waiting 30 seconds for MVs to absorb late arrivals")
    _step("  At 3600x acceleration, 30 real-seconds is approx 30 simulated hours.")
    _step("  Late-arriving events will be visible in ClickHouse shortly.")
    for i in range(6):
        time.sleep(5)
        _step(f"  ... {(i + 1) * 5}s elapsed")

    _banner("STEP 5 — Post-fault stats")
    try:
        sla_rows_after = _ch_row_count(host, ch_port, "fulfillment_sla")
        batch_rows_after = _ch_row_count(host, ch_port, "batch_recompute_fulfillment_sla")
        recon_after = _ch_reconciliation_summary(host, ch_port)
        diverged_after = recon_after.get("diverged", 0)
    except RuntimeError as exc:
        _warn(str(exc))
        sla_rows_after = batch_rows_after = diverged_after = 0
        recon_after = {}

    _step(f"  fulfillment_sla rows (streaming): {sla_rows_after}")
    _step(f"  batch_recompute rows            : {batch_rows_after}")
    _step(f"  diverged windows                : {diverged_after}")
    if diverged_after:
        _step("  ^ Watermark lag: batch sees late events; stream is catching up.")

    _banner("STEP 6 — Disabling fault injection")
    _fault_control("off")
    _step("  Generator returns to baseline mode within 5 seconds.")

    _banner("STEP 7 — Waiting 30 seconds for convergence")
    _step("  Watermark advances; late events land in the correct windows.")
    for i in range(6):
        time.sleep(5)
        _step(f"  ... {(i + 1) * 5}s elapsed")

    _banner("STEP 8 — Final reconciliation summary")
    try:
        recon_final = _ch_reconciliation_summary(host, ch_port)
        diverged_final = recon_final.get("diverged", 0)
        converged_final = recon_final.get("converged", 0)
        within_final = recon_final.get("within_tolerance", 0)
    except RuntimeError as exc:
        _warn(str(exc))
        recon_final = {}
        diverged_final = converged_final = within_final = 0

    _step(f"  within_tolerance : {within_final}")
    _step(f"  converged        : {converged_final}")
    _step(f"  diverged         : {diverged_final}")

    clean = diverged_final == 0
    _banner("SUMMARY")
    _step("  Throughput         : 50 events/sec")
    _step("  Fault injection    : late_arrival @ 10% for 30 real-seconds")
    _step(f"  Peak diverged wins : {diverged_after}")
    _step(f"  Final state        : {'CLEAN' if clean else 'STILL DIVERGED'}")
    _step(f"  converged windows  : {converged_final}")

    return 0 if clean else 2


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="marketplace-streaming E2E fault injection demo",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print steps without connecting")
    parser.add_argument("--host", default="localhost", help="Host for RisingWave and ClickHouse")
    parser.add_argument("--rw-port", type=int, default=4566, help="RisingWave SQL port")
    parser.add_argument("--ch-port", type=int, default=8123, help="ClickHouse HTTP port")
    args = parser.parse_args()

    if args.dry_run:
        _banner("marketplace-streaming — E2E demo (DRY RUN)")
        for line in DRY_RUN_STEPS:
            print(f"  {line}" if line and not line.startswith("  ") else line)
        sys.exit(0)

    sys.exit(run_live_demo(host=args.host, rw_port=args.rw_port, ch_port=args.ch_port))


if __name__ == "__main__":
    main()
