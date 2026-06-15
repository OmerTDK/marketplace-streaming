"""Hot-reload fault injection configuration.

Usage:
    python scripts/fault_control.py --mode late_arrival
    python scripts/fault_control.py --mode duplicate
    python scripts/fault_control.py --mode null_field
    python scripts/fault_control.py --mode zone_blackout
    python scripts/fault_control.py --mode off

Writes to shared/fault_injection.json, which the generator hot-reloads every 5 seconds.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FAULT_FILE = REPO_ROOT / "shared" / "fault_injection.json"

MODES: dict[str, dict] = {
    "off": {
        "active": False,
        "late_arrival_rate": 0.03,
        "late_arrival_max_delay_seconds": 300,
        "duplicate_rate": 0.01,
        "null_field_rate": 0.02,
        "null_field_targets": ["freight_value_brl", "days_to_pickup"],
        "requeue_rate": 0.005,
        "zone_blackout_prefix": None,
        "zone_blackout_duration_event_seconds": 7200,
    },
    "late_arrival": {
        "active": True,
        "late_arrival_rate": 0.10,
        "late_arrival_max_delay_seconds": 300,
        "duplicate_rate": 0.0,
        "null_field_rate": 0.0,
        "null_field_targets": [],
        "requeue_rate": 0.0,
        "zone_blackout_prefix": None,
        "zone_blackout_duration_event_seconds": 7200,
    },
    "duplicate": {
        "active": True,
        "late_arrival_rate": 0.0,
        "late_arrival_max_delay_seconds": 0,
        "duplicate_rate": 0.05,
        "null_field_rate": 0.0,
        "null_field_targets": [],
        "requeue_rate": 0.0,
        "zone_blackout_prefix": None,
        "zone_blackout_duration_event_seconds": 7200,
    },
    "null_field": {
        "active": True,
        "late_arrival_rate": 0.0,
        "late_arrival_max_delay_seconds": 0,
        "duplicate_rate": 0.0,
        "null_field_rate": 0.10,
        "null_field_targets": ["freight_value_brl", "days_to_pickup"],
        "requeue_rate": 0.0,
        "zone_blackout_prefix": None,
        "zone_blackout_duration_event_seconds": 7200,
    },
    "zone_blackout": {
        "active": True,
        "late_arrival_rate": 0.0,
        "late_arrival_max_delay_seconds": 0,
        "duplicate_rate": 0.0,
        "null_field_rate": 0.0,
        "null_field_targets": [],
        "requeue_rate": 0.0,
        "zone_blackout_prefix": "450",
        "zone_blackout_duration_event_seconds": 7200,
    },
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Control fault injection mode.")
    parser.add_argument("--mode", required=True, choices=list(MODES.keys()))
    args = parser.parse_args()

    config = MODES[args.mode]
    FAULT_FILE.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    print(f"[fault_control] Mode set to '{args.mode}'. Generator hot-reloads in ~5s.")


if __name__ == "__main__":
    main()
