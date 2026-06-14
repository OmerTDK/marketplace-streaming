"""Runtime entry point for the generator container.

Environment variables (all have sensible defaults):
  SEED                      RNG seed (default 42)
  EVENTS_PER_SECOND         Target throughput (default 50)
  SIM_START                 Simulated start time ISO 8601 (default 2024-01-08T00:00:00Z)
  TIME_ACCELERATION_FACTOR  Sim-seconds per real-second (default 3600)
  KAFKA_BOOTSTRAP_SERVERS   Broker address (default redpanda:9092)
  FAULT_CONTROL_FILE        Path to fault_injection.json (default /app/shared/fault_injection.json)

This module is NOT imported by tests. All generator logic lives in generator.py.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from generator.clock import SimClock
from generator.fault_injection import FaultConfig
from generator.generator import MarketplaceGenerator
from generator.sink import KafkaSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("generator.main")

SEED = int(os.environ.get("SEED", 42))
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", 50))
SIM_START = os.environ.get("SIM_START", "2024-01-08T00:00:00Z")
TIME_ACCELERATION_FACTOR = float(os.environ.get("TIME_ACCELERATION_FACTOR", 3600))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
FAULT_CONTROL_FILE = Path(os.environ.get("FAULT_CONTROL_FILE", "/app/shared/fault_injection.json"))

FAULT_RELOAD_INTERVAL_SECONDS = 5
LOG_INTERVAL_EVENTS = 1000


def load_fault_config(path: Path) -> FaultConfig:
    """Load FaultConfig from a JSON file. Returns inactive config on error."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return FaultConfig.from_dict(data)
    except Exception as exc:
        logger.warning("Failed to load fault config from %s: %s — using inactive", path, exc)
        return FaultConfig.inactive()


def main() -> None:
    """Run the generator loop until interrupted."""
    logger.info(
        "Starting generator: seed=%d, eps=%d, accel=%.0fx, broker=%s",
        SEED,
        EVENTS_PER_SECOND,
        TIME_ACCELERATION_FACTOR,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    sink = KafkaSink(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    clock = SimClock(
        sim_start=SIM_START,
        acceleration_factor=TIME_ACCELERATION_FACTOR,
    )
    fault_config = load_fault_config(FAULT_CONTROL_FILE)

    gen = MarketplaceGenerator(
        seed=SEED,
        sink=sink,
        fault_config=fault_config,
        clock=clock,
    )

    interval = 1.0 / max(1, EVENTS_PER_SECOND)
    last_reload = time.monotonic()
    emitted = 0

    logger.info("Generator running. Ctrl-C to stop.")

    try:
        while True:
            gen.generate_batch(n_events=1)
            emitted += 1

            now = time.monotonic()
            if now - last_reload >= FAULT_RELOAD_INTERVAL_SECONDS:
                new_config = load_fault_config(FAULT_CONTROL_FILE)
                gen.update_fault_config(new_config)
                last_reload = now

            if emitted % LOG_INTERVAL_EVENTS == 0:
                logger.info("Emitted %d events", emitted)

            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Generator interrupted after %d events.", emitted)
    finally:
        sink.flush()
        logger.info("Sink flushed. Exiting.")


if __name__ == "__main__":
    main()
