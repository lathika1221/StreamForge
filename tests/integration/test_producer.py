"""Integration tests for the M3 producer against the running local Redpanda.

Requires: docker compose -f infra/docker-compose.yml up -d --wait
Run with: python -m pytest -m integration

Exact key/value checks publish to a temporary 3-partition topic that is deleted
afterwards. The CLI test publishes a small number of fresh (unseeded) simulated
events to the configured events topic, which is the producer's real target.
Consumption here is test-only; the consumer itself is M5.
"""

import dataclasses
import json
import os
import re
import time
import uuid
from collections import defaultdict

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streamforge.common.config import load_kafka_settings
from streamforge.common.events import parse_event
from streamforge.producer.publisher import (
    EventPublisher,
    build_producer_config,
    main,
    message_key,
)
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def settings():
    return load_kafka_settings(dict(os.environ))


@pytest.fixture(scope="module")
def temp_topic(settings):
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    name = f"streamforge.it.producer.{uuid.uuid4().hex[:10]}"
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result(15)
    yield name
    admin.delete_topics([name])[name].result(15)


def _high_watermarks(bootstrap: str, topic: str) -> dict[int, int]:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-{uuid.uuid4()}"})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        return {
            p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
            for p in partitions
        }
    finally:
        consumer.close()


def _consume_from(bootstrap: str, topic: str, start: dict[int, int], expected: int) -> list:
    """Read exactly the messages written after ``start`` offsets (test-only consumer)."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"it-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(topic, p, offset) for p, offset in start.items()])
    messages, deadline = [], time.monotonic() + 20
    try:
        while len(messages) < expected and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is not None and msg.error() is None:
                messages.append(msg)
    finally:
        consumer.close()
    return messages


def _publish(settings, topic, records):
    topic_settings = dataclasses.replace(settings, events_topic=topic)
    publisher = EventPublisher(Producer(build_producer_config(topic_settings)), topic)
    publisher.publish_all(records)
    assert publisher.flush(30) == 0
    return publisher.stats


def test_valid_events_arrive_with_order_id_key_and_exact_value(settings, temp_topic):
    records = list(EventSimulator(SimulatorConfig()).iter_records(8))  # fresh ids
    start = _high_watermarks(settings.bootstrap_servers, temp_topic)

    stats = _publish(settings, temp_topic, records)
    assert stats.ok and stats.delivered == len(records) and stats.null_keys == 0

    messages = _consume_from(settings.bootstrap_servers, temp_topic, start, len(records))
    assert len(messages) == len(records)
    assert sorted((m.key(), m.value()) for m in messages) == sorted(
        (r.data["order_id"].encode(), r.value.encode()) for r in records
    )
    partitions_per_key = defaultdict(set)
    for m in messages:
        assert m.topic() == temp_topic
        partitions_per_key[m.key()].add(m.partition())
        event = parse_event(m.value())  # still a valid contract event after the round trip
        assert event.order_id.encode() == m.key()
    assert all(len(p) == 1 for p in partitions_per_key.values())  # an order stays in order
    # per-order sequence is preserved in partition order
    by_key = defaultdict(list)
    for m in sorted(messages, key=lambda m: (m.partition(), m.offset())):
        by_key[m.key()].append(json.loads(m.value())["sequence_number"])
    assert all(seq == sorted(seq) for seq in by_key.values())


@pytest.mark.parametrize("defect", ["missing_order_id", "malformed_json"])
def test_invalid_record_published_raw_with_null_key(settings, temp_topic, defect):
    sim = EventSimulator(SimulatorConfig(invalid_rate=1.0))
    record = next(r for r in sim.iter_records(50) if r.defect == defect)
    assert message_key(record) is None
    start = _high_watermarks(settings.bootstrap_servers, temp_topic)

    stats = _publish(settings, temp_topic, [record])
    assert stats.ok and stats.null_keys == 1

    (message,) = _consume_from(settings.bootstrap_servers, temp_topic, start, 1)
    assert message.key() is None
    assert message.value() == record.value.encode("utf-8")  # byte-for-byte raw payload


def test_cli_publishes_to_configured_events_topic_and_not_the_dlq(
    settings, tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)  # no developer .env; process env still applies
    events_start = _high_watermarks(settings.bootstrap_servers, settings.events_topic)
    dlq_before = _high_watermarks(settings.bootstrap_servers, settings.dlq_topic)

    assert main(["--orders", "2", "--invalid-rate", "0.5"]) == 0
    out = capsys.readouterr().out
    delivered = int(re.search(r"delivered:\s+(\d+)", out).group(1))
    assert delivered > 0 and "failed:     0" in out and "pending:    0" in out

    messages = _consume_from(
        settings.bootstrap_servers, settings.events_topic, events_start, delivered
    )
    assert len(messages) == delivered
    assert {m.topic() for m in messages} == {settings.events_topic}
    assert _high_watermarks(settings.bootstrap_servers, settings.dlq_topic) == dlq_before
