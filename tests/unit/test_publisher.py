"""Unit tests for streamforge.producer.publisher. No broker: a fake producer is used."""

import ast
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

import streamforge.producer.publisher as publisher_module
from streamforge.common.config import KafkaSettings
from streamforge.producer.publisher import (
    EventPublisher,
    PublishError,
    QueueFullError,
    build_producer_config,
    ensure_topic_exists,
    main,
    message_key,
    message_value,
)
from streamforge.producer.simulator import EventSimulator, SimulatedRecord, SimulatorConfig

START = datetime(2026, 11, 23, 10, 0, tzinfo=UTC)
TOPIC = "ecommerce.events"


# ---------------------------------------------------------------- fakes


class FakeMessage:
    def __init__(self, topic, key, value):
        self._topic, self._key, self._value = topic, key, value

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value


class FakeProducer:
    """Mimics confluent_kafka.Producer: callbacks fire only from poll()/flush()."""

    def __init__(
        self,
        *,
        buffer_errors=0,
        always_full=False,
        fail_all=False,
        stuck=False,
        deliver_on_poll=False,
        topics=None,
        unreachable=False,
        interrupt_after=None,
    ):
        self.buffer_errors = buffer_errors
        self.always_full = always_full
        self.fail_all = fail_all
        self.stuck = stuck
        self.deliver_on_poll = deliver_on_poll
        self.topics = {TOPIC: 3} if topics is None else topics
        self.unreachable = unreachable
        self.interrupt_after = interrupt_after
        self.produced = []
        self.pending = []
        self.poll_calls = []
        self.flush_calls = []
        self.config = None

    def produce(self, topic, value=None, key=None, on_delivery=None):
        if self.interrupt_after is not None and len(self.produced) >= self.interrupt_after:
            raise KeyboardInterrupt
        if self.always_full:
            raise BufferError("Local: Queue full")
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("Local: Queue full")
        self.produced.append((topic, key, value))
        self.pending.append((FakeMessage(topic, key, value), on_delivery))

    def _deliver_all(self):
        fired = 0
        while self.pending:
            msg, callback = self.pending.pop(0)
            err = KafkaError(KafkaError._MSG_TIMED_OUT) if self.fail_all else None
            callback(err, msg)
            fired += 1
        return fired

    def poll(self, timeout=0):
        self.poll_calls.append(timeout)
        return self._deliver_all() if self.deliver_on_poll else 0

    def flush(self, timeout=None):
        self.flush_calls.append(timeout)
        if self.stuck:
            return len(self.pending)
        self._deliver_all()
        return 0

    def list_topics(self, topic=None, timeout=None):
        if self.unreachable:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        if topic in self.topics:
            entry = SimpleNamespace(error=None, partitions=dict.fromkeys(range(self.topics[topic])))
        else:
            entry = SimpleNamespace(
                error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART), partitions={}
            )
        return SimpleNamespace(topics={topic: entry})


def _records(n_orders=20, **overrides):
    config = SimulatorConfig(**{"seed": 42, "start_time": START, **overrides})
    return list(EventSimulator(config).iter_records(n_orders))


@pytest.fixture(scope="module")
def mixed_records():
    return _records(400, invalid_rate=0.5)


def _first(records, defect):
    return next(r for r in records if r.defect == defect)


# ------------------------------------------------------------ key / value


def test_valid_record_is_keyed_by_order_id(mixed_records):
    record = next(r for r in mixed_records if r.is_valid)
    assert message_key(record) == record.data["order_id"].encode("utf-8")


@pytest.mark.parametrize("defect", ["missing_order_id", "malformed_json"])
def test_record_without_usable_order_id_gets_null_key(mixed_records, defect):
    assert message_key(_first(mixed_records, defect)) is None


@pytest.mark.parametrize("bad_order_id", ["", "   ", "order-1", "ORD-123", 123, None, ["ORD-X"]])
def test_unusable_order_id_values_get_null_key(bad_order_id):
    record = SimulatedRecord(value="{}", data={"order_id": bad_order_id}, is_valid=False)
    assert message_key(record) is None


def test_invalid_record_with_wellformed_order_id_keeps_its_key(mixed_records):
    record = _first(mixed_records, "unexpected_field")
    assert message_key(record) == record.data["order_id"].encode("utf-8")


def test_value_is_canonical_text_unchanged(mixed_records):
    for record in mixed_records:
        assert message_value(record) == record.value.encode("utf-8")
    valid = next(r for r in mixed_records if r.data and r.data["event_type"] == "order_created")
    text = message_value(valid).decode("utf-8")
    assert f'"order_total":"{valid.data["payload"]["order_total"]}"' in text  # Decimal as string
    assert '"event_timestamp":"' in text and 'Z"' in text  # UTC with Z suffix


def test_malformed_json_is_published_as_raw_text(mixed_records):
    record = _first(mixed_records, "malformed_json")
    assert message_value(record) == record.value.encode("utf-8")


def test_publisher_never_parses_or_reserializes_records():
    tree = ast.parse(Path(publisher_module.__file__).read_text("utf-8"))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    names |= {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    assert not names & {"parse_event", "to_json_line", "json", "TypeAdapter"}


# ----------------------------------------------------------------- config


def test_producer_config_reliability_defaults():
    settings = KafkaSettings("broker:9092", "events", "events.dlq", "group")
    config = build_producer_config(settings)
    assert config["bootstrap.servers"] == "broker:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["partitioner"] == "murmur2_random"
    assert not any("password" in k or "sasl" in k for k in config)


def test_topic_check_returns_partition_count():
    assert ensure_topic_exists(FakeProducer(), TOPIC) == 3


def test_topic_check_fails_for_missing_topic():
    with pytest.raises(PublishError, match="does not exist"):
        ensure_topic_exists(FakeProducer(topics={}), TOPIC)


def test_topic_check_fails_when_broker_unreachable():
    with pytest.raises(PublishError, match="cannot reach"):
        ensure_topic_exists(FakeProducer(unreachable=True), TOPIC)


# ------------------------------------------------------ delivery accounting


def test_publish_sends_key_value_to_configured_topic_only(mixed_records):
    fake = FakeProducer()
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records)
    assert [p[0] for p in fake.produced] == [TOPIC] * len(mixed_records)
    assert [(p[1], p[2]) for p in fake.produced] == [
        (message_key(r), message_value(r)) for r in mixed_records
    ]


def test_success_is_counted_only_after_callback(mixed_records):
    fake = FakeProducer()
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records[:10])
    assert publisher.stats.attempted == 10
    assert publisher.stats.delivered == 0 and publisher.stats.pending == 10
    assert not publisher.stats.ok
    assert publisher.flush(5) == 0
    assert publisher.stats.delivered == 10 and publisher.stats.pending == 0
    assert publisher.stats.ok


def test_callbacks_served_by_poll_during_publishing(mixed_records):
    fake = FakeProducer(deliver_on_poll=True)
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records[:5])
    assert publisher.stats.delivered == 5
    assert fake.poll_calls == [0] * 5


def test_failed_deliveries_are_counted_and_described(mixed_records):
    fake = FakeProducer(fail_all=True)
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records[:15])
    publisher.flush(5)
    stats = publisher.stats
    assert (stats.attempted, stats.delivered, stats.failed, stats.pending) == (15, 0, 15, 0)
    assert stats.errors == {"_MSG_TIMED_OUT": 15}
    assert len(stats.failure_samples) == 10  # capped
    assert not stats.ok


def test_null_keys_are_counted_not_rejected(mixed_records):
    fake = FakeProducer()
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records)
    expected = sum(1 for r in mixed_records if message_key(r) is None)
    assert expected > 0
    assert publisher.stats.null_keys == expected
    assert len(fake.produced) == len(mixed_records)


def test_queue_full_polls_and_retries_without_dropping(mixed_records):
    fake = FakeProducer(buffer_errors=3)
    publisher = EventPublisher(fake, TOPIC, queue_full_poll_interval=0.25)
    publisher.publish(mixed_records[0])
    assert len(fake.produced) == 1  # retried until accepted, sent exactly once
    assert fake.poll_calls == [0.25, 0.25, 0.25, 0]
    publisher.flush(5)
    assert publisher.stats.delivered == 1 and publisher.stats.ok


def test_queue_full_beyond_timeout_raises_and_counts_failure(mixed_records):
    now = [0.0]

    def clock():
        now[0] += 1.0
        return now[0]

    fake = FakeProducer(always_full=True)
    publisher = EventPublisher(fake, TOPIC, queue_full_timeout=3.0, clock=clock)
    with pytest.raises(QueueFullError):
        publisher.publish(mixed_records[0])
    assert fake.produced == []
    assert publisher.stats.failed == 1 and publisher.stats.errors == {"QUEUE_FULL_TIMEOUT": 1}
    assert publisher.stats.pending == 0


def test_local_produce_error_is_counted_and_publishing_continues(mixed_records):
    class RejectingProducer(FakeProducer):
        def produce(self, topic, value=None, key=None, on_delivery=None):
            if not self.produced and not getattr(self, "rejected", False):
                self.rejected = True
                raise KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
            super().produce(topic, value, key, on_delivery)

    fake = RejectingProducer()
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records[:3])
    publisher.flush(5)
    stats = publisher.stats
    assert (stats.attempted, stats.delivered, stats.failed) == (3, 2, 1)
    assert stats.errors == {"MSG_SIZE_TOO_LARGE": 1}


def test_flush_leaves_unconfirmed_messages_pending(mixed_records):
    fake = FakeProducer(stuck=True)
    publisher = EventPublisher(fake, TOPIC)
    publisher.publish_all(mixed_records[:4])
    assert publisher.flush(0.5) == 4
    assert fake.flush_calls == [0.5]
    assert publisher.stats.pending == 4 and not publisher.stats.ok


# --------------------------------------------------------------------- CLI


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    """No developer .env and no inherited KAFKA_* overrides."""
    monkeypatch.chdir(tmp_path)
    for name in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_EVENTS_TOPIC", "KAFKA_DLQ_TOPIC"):
        monkeypatch.delenv(name, raising=False)


CLI = ["--orders", "30", "--seed", "42", "--start-time", "2026-11-23T10:00:00Z"]


def _run(fake, argv):
    def factory(config):
        fake.config = config
        return fake

    return main(argv, producer_factory=factory)


def test_cli_publishes_simulator_records_raw_and_reports(isolated_env, capsys):
    fake = FakeProducer()
    argv = CLI + ["--invalid-rate", "0.3"]
    assert _run(fake, argv) == 0
    expected = _records(30, invalid_rate=0.3)
    assert [p[2] for p in fake.produced] == [r.value.encode("utf-8") for r in expected]
    assert {p[0] for p in fake.produced} == {TOPIC}
    assert fake.config["bootstrap.servers"] == "localhost:19092"
    assert fake.flush_calls == [30.0]
    out = capsys.readouterr().out
    n_valid = sum(r.is_valid for r in expected)
    assert f"attempted:  {len(expected)} (valid {n_valid}, invalid {len(expected) - n_valid}" in out
    assert f"delivered:  {len(expected)}" in out and "failed:     0" in out


def test_cli_uses_configured_topic_and_never_the_dlq(isolated_env, monkeypatch):
    monkeypatch.setenv("KAFKA_EVENTS_TOPIC", "custom.events")
    fake = FakeProducer(topics={"custom.events": 1})
    assert _run(fake, CLI) == 0
    assert {p[0] for p in fake.produced} == {"custom.events"}


def test_cli_exits_nonzero_on_delivery_failure(isolated_env, capsys):
    assert _run(FakeProducer(fail_all=True), CLI) == 1
    captured = capsys.readouterr()
    assert "delivered:  0" in captured.out and "_MSG_TIMED_OUT" in captured.out
    assert "failure: _MSG_TIMED_OUT" in captured.err


def test_cli_exits_nonzero_when_deliveries_unconfirmed(isolated_env, capsys):
    assert _run(FakeProducer(stuck=True), CLI + ["--flush-timeout", "0.5"]) == 1
    assert "pending:    " in capsys.readouterr().out


def test_cli_missing_topic_publishes_nothing(isolated_env, capsys):
    fake = FakeProducer(topics={})
    assert _run(fake, CLI) == 1
    assert fake.produced == []
    assert "does not exist" in capsys.readouterr().err


def test_cli_interrupt_flushes_queued_messages(isolated_env, capsys):
    fake = FakeProducer(interrupt_after=5)
    assert _run(fake, CLI) == 130
    assert fake.flush_calls == [30.0]
    assert "delivered:  5" in capsys.readouterr().out


def test_cli_config_error_exits_2(isolated_env, monkeypatch, capsys):
    monkeypatch.setenv("KAFKA_EVENTS_TOPIC", "same")
    monkeypatch.setenv("KAFKA_DLQ_TOPIC", "same")
    assert _run(FakeProducer(), CLI) == 2
    assert "must be different" in capsys.readouterr().err


@pytest.mark.parametrize("bad", [["--flush-timeout", "0"], ["--orders", "0"]])
def test_cli_rejects_bad_arguments(isolated_env, bad):
    with pytest.raises(SystemExit) as exc:
        _run(FakeProducer(), CLI + bad)
    assert exc.value.code == 2
