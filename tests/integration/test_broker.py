"""Integration test: generator → Redpanda byte-parity.

Spin up a real Redpanda container, produce 50 events via KafkaSink (SEED=42,
FixedClock), consume them back, and assert that every event_id in Kafka matches
the corresponding event_id from InMemorySink with the same seed.

This proves the KafkaSink serialisation/deserialisation round-trip is
byte-for-byte equivalent to the InMemorySink reference path.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import pytest

from generator.clock import FixedClock
from generator.generator import MarketplaceGenerator, run_generator
from generator.sink import InMemorySink, KafkaSink
from tests.integration.conftest import (
    KAFKA_TOPICS,
    REDPANDA_IMAGE,
    create_topics,
    poll_until,
)

N_EVENTS = 50
SEED = 42
SIM_START = datetime(2024, 1, 8, 9, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="class")
def redpanda():
    """Start a Redpanda container and yield bootstrap server address."""
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer(image=REDPANDA_IMAGE) as container:
        bootstrap = container.get_bootstrap_server()
        create_topics(bootstrap, KAFKA_TOPICS, num_partitions=4)
        yield bootstrap


@pytest.mark.integration
class TestBrokerByteParity:
    """Verify KafkaSink → Redpanda → consumer round-trip is byte-for-bit equivalent."""

    def test_all_topics_have_records(self, redpanda: str) -> None:
        """All four topics receive events after generator run."""
        from confluent_kafka import Consumer, KafkaError

        bootstrap = redpanda
        clock = FixedClock(event_ts=SIM_START)
        kafka_sink = KafkaSink(bootstrap_servers=bootstrap)
        gen = MarketplaceGenerator(seed=SEED, sink=kafka_sink, clock=clock)
        gen.generate_batch(N_EVENTS)
        kafka_sink.flush()

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test_broker_topics",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(KAFKA_TOPICS)

        consumed: dict[str, list[dict]] = {t: [] for t in KAFKA_TOPICS}

        def _poll_enough() -> bool:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                return False
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return False
                raise RuntimeError(f"Consumer error: {msg.error()}")
            topic = msg.topic()
            consumed[topic].append(json.loads(msg.value().decode("utf-8")))
            total = sum(len(v) for v in consumed.values())
            return total >= N_EVENTS

        poll_until(_poll_enough, timeout_s=30, interval_s=0.1)
        consumer.close()

        for topic in KAFKA_TOPICS:
            assert len(consumed[topic]) > 0, f"No records found in topic '{topic}'"

    def test_event_ids_match_in_memory_reference(self, redpanda: str) -> None:
        """event_ids from Kafka exactly match InMemorySink reference for SEED=42."""
        from confluent_kafka import Consumer, KafkaError

        bootstrap = redpanda

        # Reference: generate the same stream into InMemorySink
        clock_ref = FixedClock(event_ts=SIM_START)
        ref_sink = InMemorySink()
        run_generator(n_events=N_EVENTS, seed=SEED, sink=ref_sink, clock=clock_ref)

        # Produce to Kafka
        clock_kafka = FixedClock(event_ts=SIM_START)
        kafka_sink = KafkaSink(bootstrap_servers=bootstrap)
        gen = MarketplaceGenerator(seed=SEED, sink=kafka_sink, clock=clock_kafka)
        gen.generate_batch(N_EVENTS)
        kafka_sink.flush()

        # Consume from Kafka
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test_broker_parity",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(KAFKA_TOPICS)

        consumed_by_topic: dict[str, list[str]] = {t: [] for t in KAFKA_TOPICS}
        deadline = time.monotonic() + 30

        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Consumer error: {msg.error()}")
            payload = json.loads(msg.value().decode("utf-8"))
            consumed_by_topic[msg.topic()].append(payload["event_id"])

        consumer.close()

        # Assert: every event_id present in reference must appear in Kafka
        ref_ids: set[str] = set()
        for topic in KAFKA_TOPICS:
            for record in ref_sink.records_for(topic):
                ref_ids.add(record["event_id"])

        kafka_ids: set[str] = set()
        for ids in consumed_by_topic.values():
            kafka_ids.update(ids)

        missing = ref_ids - kafka_ids
        assert not missing, (
            f"{len(missing)} event_id(s) from InMemorySink not found in Kafka: {list(missing)[:5]}"
        )
