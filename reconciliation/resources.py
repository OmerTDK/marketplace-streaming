"""Dagster resources: connection factories for RisingWave and ClickHouse.

Resources own connection configuration only — they open short-lived
connections on demand and the caller closes them. Keeping config at the
resource boundary (not scattered through asset bodies) follows
standards/engineering-principles.md #10 (conditional logic at the boundaries).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

from dagster import ConfigurableResource

# Compose defaults (docker-compose.yml dagster service environment block).
DEFAULT_RISINGWAVE_HOST = "risingwave"
DEFAULT_RISINGWAVE_PORT = 4566
DEFAULT_RISINGWAVE_USER = "root"
DEFAULT_RISINGWAVE_DB = "dev"

DEFAULT_CLICKHOUSE_HOST = "clickhouse"
DEFAULT_CLICKHOUSE_NATIVE_PORT = 9000


class RisingWaveResource(ConfigurableResource):
    """Opens psycopg2 connections to RisingWave on demand."""

    host: str = DEFAULT_RISINGWAVE_HOST
    port: int = DEFAULT_RISINGWAVE_PORT
    user: str = DEFAULT_RISINGWAVE_USER
    database: str = DEFAULT_RISINGWAVE_DB

    @classmethod
    def from_env(cls) -> RisingWaveResource:
        """Build from RISINGWAVE_HOST / RISINGWAVE_PORT env vars (compose-wired)."""
        return cls(
            host=os.getenv("RISINGWAVE_HOST", DEFAULT_RISINGWAVE_HOST),
            port=int(os.getenv("RISINGWAVE_PORT", str(DEFAULT_RISINGWAVE_PORT))),
        )

    @contextmanager
    def connection(self) -> Any:
        """Yield an autocommit psycopg2 connection, closed on exit."""
        import psycopg2

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            dbname=self.database,
        )
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()


class ClickHouseResource(ConfigurableResource):
    """Opens clickhouse_driver clients on demand (native protocol)."""

    host: str = DEFAULT_CLICKHOUSE_HOST
    port: int = DEFAULT_CLICKHOUSE_NATIVE_PORT

    @classmethod
    def from_env(cls) -> ClickHouseResource:
        """Build from CLICKHOUSE_HOST / CLICKHOUSE_PORT env vars (compose-wired).

        CLICKHOUSE_PORT must be the NATIVE port (9000) — clickhouse_driver does
        not speak the HTTP protocol (8123).
        """
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", DEFAULT_CLICKHOUSE_HOST),
            port=int(os.getenv("CLICKHOUSE_PORT", str(DEFAULT_CLICKHOUSE_NATIVE_PORT))),
        )

    @contextmanager
    def client(self) -> Any:
        """Yield a clickhouse_driver Client, disconnected on exit."""
        from clickhouse_driver import Client

        ch_client = Client(host=self.host, port=self.port)
        try:
            yield ch_client
        finally:
            ch_client.disconnect()
