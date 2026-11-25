"""Consumer integration tests against the local Redpanda (M5). No Snowflake.

Every test creates its own temporary 3-partition events topic and 1-partition DLQ
topic (deleted afterwards) and a unique consumer group, so ``ecommerce.events`` and
the ``streamforge-consumer`` group are never read or modified. Valid events go to an
in-memory sink that behaves like the RAW sink (duplicate-safe per Kafka offset).

Run with: python -m pytest -m integration
"""

import dataclasses
import os
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streamforge.common.config import load_kafka_settings
from streamforge.consumer.consumer import EventConsumer, ProcessingError, build_consumer_config
from streamforge.consumer.dlq import DlqWriter, recent_dead_letters, record_fingerprint
from streamforge.consumer.records import KafkaRecord
from streamforge.consumer.snowflake_sink import SinkResult
from streamforge.producer.publisher import EventPublisher, build_producer_config
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

pytestmark = pytest.mark.integration

FAILURE_TYPES = {"empty_value", "malformed_json", "missing_field", "schema_validation"}


class MemorySink:
    def __init__(self, fail=False):
        self.rows, self.fail, self.calls = {}, fail, 0  # (partition, offset) -> ValidRecord

    def write_batch(self, records, batch):
        self.calls += 1
        if self.fail:
            raise RuntimeError("simulated Snowflake outage")
        new = 0
        for r in records:
            key = (r.record.partition, r.record.offset)
            if key not in self.rows:
                self.rows[key] = r
                new += 1
        return SinkResult(new, len(records) - new)


@pytest.fixture
def env():
    """Fresh temporary topics and a unique group for each test; topics deleted after."""
    base = load_kafka_settings(dict(os.environ))
    suffix = uuid.uuid4().hex[:10]
    settings = dataclasses.replace(
        base,
        events_topic=f"streamforge.it.consumer.{suffix}",
        dlq_topic=f"streamforge.it.consumer.{suffix}.dlq",
        consumer_group=f"streamforge-it-consumer-{suffix}",
    )
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    created = admin.create_topics(
        [
            NewTopic(settings.events_topic, num_partitions=3, replication_factor=1),
            NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1),
        ]
    )
    for f in created.values():
        f.result(15)
    yield settings
    for f in admin.delete_topics([settings.events_topic, settings.dlq_topic]).values():
        f.result(15)


def _publish(env):
    """Fresh valid + invalid records, plus a tombstone and non-UTF-8 bytes."""
    records = list(EventSimulator(SimulatorConfig(invalid_rate=0.4)).iter_records(8))
    publisher = EventPublisher(Producer(build_producer_config(env)), env.events_topic)
    publisher.publish_all(records)
    assert publisher.flush(30) == 0 and publisher.stats.ok
    extra = Producer({"bootstrap.servers": env.bootstrap_servers})
    extra.produce(env.events_topic, value=None, key=b"tombstone")
    extra.produce(env.events_topic, value=b"\xff\xfe not utf-8", key=b"\x00\xffraw-key")
    assert extra.flush(30) == 0
    return records


def _run(env, sink, *, dead_lettered=None, max_attempts=3):
    consumer = Consumer(build_consumer_config(env, offset_reset="earliest"))
    dlq_producer = Producer({**build_producer_config(env), "client.id": "it-dlq"})
    ec = EventConsumer(
        consumer,
        sink,
        DlqWriter(dlq_producer, env.dlq_topic),
        topic=env.events_topic,
        consumer_group=env.consumer_group,
        batch_size=50,
        batch_timeout=1.0,
        max_attempts=max_attempts,
        backoff_seconds=0.1,
        dead_lettered=dead_lettered,
    )
    try:
        ec.run(idle_timeout=8)
    finally:
        consumer.close()
    return ec.stats


def _read_all(bootstrap, topic):
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"it-read-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    out, empty = [], 0
    while empty < 6:
        m = c.poll(0.5)
        if m is None:
            empty += 1
        elif m.error() is None:
            out.append(m)
    c.close()
    return out


def _committed_and_ends(env):
    probe = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": env.consumer_group,
            "enable.auto.commit": False,
        }
    )
    try:
        topic = env.events_topic
        parts = probe.list_topics(topic, timeout=10).topics[topic].partitions
        committed = probe.committed([TopicPartition(topic, p) for p in parts], timeout=10)
        ends = {
            p: probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts
        }
    finally:
        probe.close()
    return {tp.partition: tp.offset for tp in committed}, ends


def _dlq_headers(msg):
    return {k: v.decode("utf-8") for k, v in msg.headers()}


def test_consume_route_commit_and_redelivery(env):
    published = _publish(env)
    sink = MemorySink()
    stats = _run(env, sink)

    valid = [r for r in published if r.is_valid]
    invalid = [r for r in published if not r.is_valid]
    assert stats.consumed == len(published) + 2  # + tombstone + non-UTF-8
    assert stats.valid == len(valid) and stats.dlq == len(invalid) + 2

    # valid events land in the sink with exact bytes and their Kafka coordinates
    assert sorted(v.record.value for v in sink.rows.values()) == sorted(
        r.value.encode() for r in valid
    )
    for (partition, offset), v in sink.rows.items():
        assert (v.record.topic, v.record.partition, v.record.offset) == (
            env.events_topic,
            partition,
            offset,
        )
        assert v.record.key == v.event.order_id.encode()

    # every DLQ message equals its source message byte-for-byte (key AND value)
    source = {
        (m.partition(), m.offset()): m for m in _read_all(env.bootstrap_servers, env.events_topic)
    }
    dlq = _read_all(env.bootstrap_servers, env.dlq_topic)
    assert len(dlq) == len(invalid) + 2
    for m in dlq:
        h = _dlq_headers(m)
        assert h["streamforge.source_topic"] == env.events_topic
        assert h["streamforge.consumer_group"] == env.consumer_group
        assert h["streamforge.failure_type"] in FAILURE_TYPES and h["streamforge.failure_reason"]
        original = source[
            (int(h["streamforge.source_partition"]), int(h["streamforge.source_offset"]))
        ]
        assert m.key() == original.key() and m.value() == original.value()
        assert h["streamforge.source_sha256"] == record_fingerprint(
            KafkaRecord.from_message(original)
        )
    assert {m.key() for m in dlq} >= {b"tombstone", b"\x00\xffraw-key"}

    # committed offsets are the next offsets = end of every partition
    committed, ends = _committed_and_ends(env)
    assert committed == ends

    # a second run resumes from the committed offsets: nothing is reprocessed
    again = _run(env, sink)
    assert again.consumed == 0 and again.batches == 0


def test_dlq_is_not_duplicated_when_snowflake_fails_and_consumer_restarts(env):
    """DLQ publish succeeds, the RAW load fails until retries are exhausted, the process
    stops without committing; after a restart the batch is redelivered and the DLQ
    tail scan prevents dead-lettering the same records twice."""
    published = _publish(env)
    n_invalid = sum(not r.is_valid for r in published) + 2

    failing = MemorySink(fail=True)
    with pytest.raises(ProcessingError, match="Snowflake RAW load"):
        _run(env, failing, max_attempts=2)
    assert failing.calls == 2  # bounded retries
    committed, _ = _committed_and_ends(env)
    assert all(offset < 0 for offset in committed.values())  # nothing committed
    assert len(_read_all(env.bootstrap_servers, env.dlq_topic)) == n_invalid

    reader = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": f"it-scan-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        seen = recent_dead_letters(
            reader,
            env.dlq_topic,
            source_topic=env.events_topic,
            consumer_group=env.consumer_group,
            window=100,
        )
    finally:
        reader.close()
    assert len(seen) == n_invalid

    sink = MemorySink()
    stats = _run(env, sink, dead_lettered=seen)
    assert stats.dlq == 0 and stats.dlq_already_present == n_invalid
    assert len(sink.rows) == sum(r.is_valid for r in published)
    assert len(_read_all(env.bootstrap_servers, env.dlq_topic)) == n_invalid  # no duplicates
    committed, ends = _committed_and_ends(env)
    assert committed == ends
