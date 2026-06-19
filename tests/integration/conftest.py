"""Shared utilities and fixtures for integration tests.

All integration tests are marked with @pytest.mark.integration so the fast
CI lane can exclude them with `pytest -m "not integration"`.

Substrate: the repo's own docker-compose.yml is the test substrate (via
testcontainers' DockerCompose). The compose network makes `redpanda:9092`
resolve for RisingWave, so the SQL artifacts users run (sql/01_sources.sql,
sql/02_mvs.sql) are applied UNCHANGED — no broker-address substitution.

Redpanda advertises two listeners (see docker-compose.yml):
  - internal://redpanda:9092  → used by RisingWave's CREATE SOURCE (in-network)
  - external://localhost:19092 → used by the host test producer/consumer

Each test module gets its own compose topology under a distinct project name
(COMPOSE_PROJECT_NAME) so the standard-watermark suite and the 6-hour-watermark
kill-test never share a RisingWave instance. Modules run sequentially and the
module-scoped fixture tears each topology down before the next starts, so the
fixed published host ports (4566 / 19092 / 9000) never clash.

IMPORTANT — this suite must run SEQUENTIALLY. The distinct COMPOSE_PROJECT_NAMEs
isolate container/volume names, but every module publishes the SAME fixed host
ports. Running modules concurrently (pytest-xdist `-n auto` or any parallel
runner) would collide on those ports. The pinned pytest invocation in
.github/workflows/ci.yml has no parallelism flag; keep it that way, or switch to
ephemeral host ports first. See pyproject.toml [tool.pytest.ini_options].

The poll_until helper enforces the polling-not-sleeping discipline.
"""

from __future__ import annotations

import contextlib
import os
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

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
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.3.18.7-alpine"

# Topics matching the four event sources.
KAFKA_TOPICS = ["order_placed", "shipment_created", "delivery_update", "seller_activity"]

# Compose service names (must match docker-compose.yml).
SERVICE_REDPANDA = "redpanda"
SERVICE_RISINGWAVE = "risingwave"
SERVICE_CLICKHOUSE = "clickhouse"

# Published host ports (the compose `ports:` mappings publish these unchanged).
REDPANDA_EXTERNAL_PORT = 19092  # external listener — host producer/consumer
RISINGWAVE_PORT = 4566
CLICKHOUSE_NATIVE_PORT = 9000

RISINGWAVE_USER = "root"
RISINGWAVE_DB = "dev"

# Services the integration suite needs healthy. The `generator` and `dagster`
# services stay DOWN — the test produces its own events for determinism and the
# kill-test. `redpanda-init` is skipped too; topics are created from the host.
COMPOSE_SERVICES = [SERVICE_REDPANDA, SERVICE_RISINGWAVE, SERVICE_CLICKHOUSE]


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
        bootstrap_servers: Broker address (host:port) reachable from the host.
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

    The SQL is applied UNCHANGED: sources point at `redpanda:9092`, which
    resolves on the compose network. No broker-address substitution.

    Args:
        conn: psycopg2 connection with autocommit=True.
        sources_sql: Content of a sources SQL file (01_sources.sql or fault variant).
        mvs_sql: Content of sql/02_mvs.sql.
    """
    for sql_block in [sources_sql, mvs_sql]:
        for stmt in _split_statements(sql_block):
            conn.cursor().execute(stmt)


def _split_statements(sql: str) -> list[str]:
    """Split SQL file content into individual executable statements.

    Comments are stripped FIRST, then the text is split on ';'. Order matters:
    a '--' comment may itself contain a ';' (e.g. "...there; watermark..."), so
    splitting before stripping would tear a comment into a bogus statement.

    Both comment forms are stripped: full-line '--' comments AND inline trailing
    '--' comments (everything from ' --' to end-of-line). The SQL files in this
    repo never put '--' inside a string literal, so a literal-unaware strip is
    safe here; if that ever changes, switch to a real SQL parser (sqlparse).
    """
    cleaned_lines = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue  # full-line comment — drop entirely
        # Strip an inline trailing '--' comment (preceded by whitespace or at col 0).
        cleaned_lines.append(re.sub(r"(?:^|\s)--.*$", "", line))
    no_comments = "\n".join(cleaned_lines)
    # Split into statements on the terminator.
    statements = []
    for raw in no_comments.split(";"):
        clean = raw.strip()
        if clean:
            statements.append(clean)
    return statements


# ---------------------------------------------------------------------------
# Compose topology fixture
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def compose_topology(project_name: str) -> Iterator:
    """Bring up the repo's docker-compose.yml topology (selected services only).

    Uses a distinct COMPOSE_PROJECT_NAME so independent test modules never share
    a RisingWave instance or collide on container names. `wait=True` blocks on
    the compose healthchecks (redpanda rpk cluster health, risingwave TCP probe,
    clickhouse SELECT 1) before yielding.

    The ~1.5 GB RisingWave image is pulled by the session-scoped pre-pull fixture
    before this runs, so compose `up` starts from a warm image cache.

    Yields:
        The live DockerCompose instance.
    """
    from testcontainers.compose import DockerCompose

    prev = os.environ.get("COMPOSE_PROJECT_NAME")
    os.environ["COMPOSE_PROJECT_NAME"] = project_name
    compose = DockerCompose(
        context=str(REPO_ROOT),
        compose_file_name="docker-compose.yml",
        services=COMPOSE_SERVICES,
        pull=False,  # images pre-pulled by _prepull_container_images
        build=False,
        wait=True,  # block on healthchecks (docker compose up --wait)
        keep_volumes=False,
    )
    try:
        compose.start()
        yield compose
    finally:
        with contextlib.suppress(Exception):
            compose.stop()
        if prev is None:
            os.environ.pop("COMPOSE_PROJECT_NAME", None)
        else:
            os.environ["COMPOSE_PROJECT_NAME"] = prev


def kafka_bootstrap(compose) -> str:
    """Host-reachable Kafka bootstrap (external listener)."""
    host = compose.get_service_host(SERVICE_REDPANDA, REDPANDA_EXTERNAL_PORT)
    port = compose.get_service_port(SERVICE_REDPANDA, REDPANDA_EXTERNAL_PORT)
    return f"{host}:{port}"


def risingwave_endpoint(compose) -> tuple[str, int]:
    """Host-reachable RisingWave (host, port)."""
    host = compose.get_service_host(SERVICE_RISINGWAVE, RISINGWAVE_PORT)
    port = int(compose.get_service_port(SERVICE_RISINGWAVE, RISINGWAVE_PORT))
    return host, port


def clickhouse_endpoint(compose) -> tuple[str, int]:
    """Host-reachable ClickHouse native protocol (host, port)."""
    host = compose.get_service_host(SERVICE_CLICKHOUSE, CLICKHOUSE_NATIVE_PORT)
    port = int(compose.get_service_port(SERVICE_CLICKHOUSE, CLICKHOUSE_NATIVE_PORT))
    return host, port


def connect_risingwave(host: str, port: int):
    """Open an autocommit psycopg2 connection to RisingWave, polling until ready.

    The compose healthcheck is a bare TCP probe on 4566; RisingWave may accept
    TCP before the SQL frontend is fully ready, so poll on a real connection.
    """
    import psycopg2

    def _ready():
        try:
            c = psycopg2.connect(
                host=host,
                port=port,
                user=RISINGWAVE_USER,
                dbname=RISINGWAVE_DB,
                connect_timeout=3,
            )
            c.close()
            return True
        except Exception:
            return False

    poll_until(_ready, timeout_s=90, interval_s=2)
    conn = psycopg2.connect(host=host, port=port, user=RISINGWAVE_USER, dbname=RISINGWAVE_DB)
    conn.autocommit = True
    return conn


# ---------------------------------------------------------------------------
# Image pre-pull — compose `up` does not pull when pull=False, and the
# RisingWave image is ~1.5 GB. Pull all three once per session so every
# topology starts from a warm cache (local first run AND fresh CI runners).
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
