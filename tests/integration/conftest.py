"""Shared utilities for integration tests.

All integration tests are marked with @pytest.mark.integration so the fast
CI lane can exclude them with `pytest -m "not integration"`.

Container fixtures use scope="class" to amortize the 30s+ startup cost across
all test methods in a class. Each test class gets fresh containers.

The poll_until helper enforces the polling-not-sleeping discipline: never call
time.sleep() directly outside this function.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SQL_DIR = REPO_ROOT / "sql"

# Sentinel delivery zones for the kill-test: alphabetic, structurally distinct
# from the generator's 3-digit numeric CEP prefix format (e.g. "450").
KILL_TEST_ZONE = "KILL_TEST_ZONE"
ADVANCE_ZONE = "ADVANCE_ZONE"
BEYOND_TOLERANCE_ZONE = "BEYOND_TOLERANCE_ZONE"

# Image pins — single source of truth from docker-compose.yml.
REDPANDA_IMAGE = "redpandadata/redpanda:v23.3.18"
RISINGWAVE_IMAGE = "risingwavelabs/risingwave:v1.8.2"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.3-alpine"

# Topics matching the four event sources.
KAFKA_TOPICS = ["order_placed", "shipment_created", "delivery_update", "seller_activity"]


def poll_until(fn, timeout_s: float, interval_s: float = 2.0):
    """Poll fn() every interval_s until it returns a truthy value or timeout.

    Args:
        fn: Callable with no arguments. Returns falsy to keep polling, truthy to stop.
        timeout_s: Maximum seconds to wait.
        interval_s: Seconds between polls.

    Returns:
        The truthy return value of fn().

    Raises:
        TimeoutError: If fn() never returned truthy within timeout_s.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval_s)
    raise TimeoutError(f"poll_until timed out after {timeout_s}s")


def create_topics(bootstrap_servers: str, topics: list[str], num_partitions: int = 4) -> None:
    """Create Kafka topics via confluent-kafka AdminClient (idempotent).

    Args:
        bootstrap_servers: Broker address (host:port).
        topics: List of topic names to create.
        num_partitions: Number of partitions per topic.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topics = [NewTopic(t, num_partitions=num_partitions, replication_factor=1) for t in topics]
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as exc:
            # Topic already exists — idempotent.
            if "already exists" not in str(exc).lower() and "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"Failed to create topic '{topic}': {exc}") from exc


def init_risingwave(conn, sources_sql: str, mvs_sql: str) -> None:
    """Execute source and MV DDL on a live RisingWave connection.

    Uses autocommit=True and executes one statement at a time — RisingWave's
    psql wire protocol does not guarantee multi-statement string support.

    Args:
        conn: psycopg2 connection with autocommit=True.
        sources_sql: Content of a sources SQL file (01_sources.sql or fault variant).
        mvs_sql: Content of sql/02_mvs.sql.
    """
    for sql_block in [sources_sql, mvs_sql]:
        for stmt in _split_statements(sql_block):
            conn.cursor().execute(stmt)


def _split_statements(sql: str) -> list[str]:
    """Split SQL file content into individual executable statements."""
    statements = []
    for raw in sql.split(";"):
        # Strip comment lines and blank lines.
        lines = [line for line in raw.splitlines() if not line.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if clean:
            statements.append(clean)
    return statements


# ---------------------------------------------------------------------------
# Image pre-pull — testcontainers' raw DockerContainer.start() does not reliably
# pull a large absent image (RisingWave ~1.5 GB) before `create`, surfacing as
# docker.errors.ImageNotFound on a cold Docker cache (local first run AND fresh
# CI runners). The first-class Redpanda/ClickHouse modules pull themselves; the
# raw RisingWave container does not. Pull all three explicitly, once per session,
# so every fixture starts from a warm cache.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _prepull_container_images():
    import docker
    from docker.errors import ImageNotFound

    client = docker.from_env()
    for image in (REDPANDA_IMAGE, RISINGWAVE_IMAGE, CLICKHOUSE_IMAGE):
        try:
            client.images.get(image)
        except ImageNotFound:
            client.images.pull(image)


# ---------------------------------------------------------------------------
# pytest mark applied automatically to all tests in this package
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(items):
    """Auto-apply the 'integration' mark to all tests under tests/integration/."""
    integration_mark = pytest.mark.integration
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(integration_mark)
