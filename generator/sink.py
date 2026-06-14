"""Sink abstraction for the event generator.

The generator writes events through the Sink interface. This decouples the
business logic (event generation, fault injection) from the transport layer.

At runtime: KafkaSink (confluent-kafka / redpanda-compatible).
In tests: InMemorySink — no broker required, CI stays container-free.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any


class Sink(ABC):
    """Abstract event sink. All randomness and event logic lives in the generator;
    the sink is purely responsible for transporting the serialised record."""

    @abstractmethod
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Emit one event record to the named topic.

        Args:
            topic: Destination topic name (e.g. 'order_placed').
            key: Kafka message key (partition routing).
            value: Event payload dict — will be JSON-serialised by this method.
        """

    @abstractmethod
    def flush(self) -> None:
        """Ensure all buffered records are delivered."""


class InMemorySink(Sink):
    """Recording sink for unit tests.

    Stores every emitted record in memory so tests can assert on the full
    event stream without starting a Kafka broker.

    Usage::

        sink = InMemorySink()
        run_generator(n_events=100, seed=42, sink=sink)
        orders = sink.records_for("order_placed")
        assert len(orders) == 25
    """

    def __init__(self) -> None:
        self._records: dict[str, list[dict[str, Any]]] = {}

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Buffer one record."""
        self._records.setdefault(topic, []).append({"key": key, "value": value})

    def flush(self) -> None:
        """No-op for in-memory sink — records are immediately durable."""

    def records_for(self, topic: str) -> list[dict[str, Any]]:
        """Return all value dicts emitted to *topic* (empty list if none)."""
        return [r["value"] for r in self._records.get(topic, [])]

    def all_records(self) -> dict[str, list[dict[str, Any]]]:
        """Return the full topic → [value, …] mapping."""
        return {topic: [r["value"] for r in records] for topic, records in self._records.items()}

    def total_count(self) -> int:
        """Total events emitted across all topics."""
        return sum(len(v) for v in self._records.values())

    def clear(self) -> None:
        """Reset the sink between test cases."""
        self._records.clear()


class KafkaSink(Sink):
    """Runtime Kafka/Redpanda sink using the confluent-kafka producer.

    Imported lazily so that tests (which import generator.sink) never trigger
    the confluent-kafka import — keeping CI container-free.

    Args:
        bootstrap_servers: Comma-separated broker addresses (e.g. 'redpanda:9092').
        producer_config: Extra config dict merged into the producer config.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        producer_config: dict[str, Any] | None = None,
    ) -> None:
        # Defer import so tests never trigger confluent_kafka resolution.
        try:
            from confluent_kafka import Producer  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "confluent-kafka is required for KafkaSink. Install it with: uv add confluent-kafka"
            ) from exc

        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 5,
            "batch.size": 65536,
        }
        if producer_config:
            config.update(producer_config)
        self._producer = Producer(config)

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Produce one record to Kafka."""
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, default=str).encode("utf-8"),
        )
        # Poll to trigger delivery callbacks and handle back-pressure.
        self._producer.poll(0)

    def flush(self) -> None:
        """Flush all in-flight messages (blocks until delivered)."""
        self._producer.flush()
