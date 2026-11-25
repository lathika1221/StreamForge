"""Unit tests for the M5 consumer: classification, DLQ, batch loop, CLI. No broker/Snowflake."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from streamforge.common.config import KafkaSettings
from streamforge.consumer import cli
from streamforge.consumer.consumer import (
    ConsumerConfigError,
    EventConsumer,
    ProcessingError,
    build_consumer_config,
    next_offsets,
    offset_ranges,
)
from streamforge.consumer.dlq import (
    DlqError,
    DlqWriter,
    dlq_headers,
    dlq_identity,
    recent_dead_letters,
    record_fingerprint,
)
from streamforge.consumer.records import (
    MAX_REASON_CHARS,
    FailureType,
    InvalidRecord,
    KafkaRecord,
    ValidRecord,
    classify,
    failure_reason,
)
from streamforge.consumer.snowflake_sink import SinkResult
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

TOPIC, GROUP = "ecommerce.events", "streamforge-consumer"
START = datetime(2026, 11, 23, 10, 0, tzinfo=UTC)


# ----------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def sim_records():
    sim = EventSimulator(SimulatorConfig(seed=5, start_time=START, invalid_rate=0.5))
    return list(sim.iter_records(60))


def _first(records, defect):
    return next(r for r in records if r.defect == defect)


def krec(value, *, partition=0, offset=0, key=b"ORD-000000000001", ts=1_795_000_000_000):
    return KafkaRecord(TOPIC, partition, offset, key, value, ts)


class FakeMsg:
    def __init__(self, record: KafkaRecord, error=None):
        self.r, self._error = record, error

    def error(self):
        return self._error

    def topic(self):
        return self.r.topic

    def partition(self):
        return self.r.partition

    def offset(self):
        return self.r.offset

    def key(self):
        return self.r.key

    def value(self):
        return self.r.value

    def timestamp(self):
        return (1, self.r.timestamp_ms) if self.r.timestamp_ms is not None else (0, -1)


class FakeKafkaConsumer:
    def __init__(self, batches, *, fail_commits=0, events=None):
        self.batches = list(batches)
        self.fail_commits = fail_commits
        self.commits, self.subscribed, self.closed = [], None, False
        self.events = events if events is not None else []
        self.topics = {
            TOPIC: SimpleNamespace(error=None),
            "ecommerce.events.dlq": SimpleNamespace(error=None),
        }

    def subscribe(self, topics, **_):
        self.subscribed = topics

    def consume(self, num_messages=1, timeout=1.0):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets=None, asynchronous=True):
        assert asynchronous is False
        if self.fail_commits:
            self.fail_commits -= 1
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        self.events.append("commit")
        self.commits.append([(tp.topic, tp.partition, tp.offset) for tp in offsets])

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics=self.topics)

    def close(self):
        self.closed = True


class FakeSink:
    """Duplicate-safe by Kafka coordinates, like the real RAW sink."""

    def __init__(self, fail=0, events=None):
        self.fail, self.batches, self.seen = fail, [], set()
        self.events = events if events is not None else []

    def write_batch(self, records, batch):
        if self.fail:
            self.fail -= 1
            raise RuntimeError("warehouse unavailable")
        self.events.append("sink")
        self.batches.append((list(records), batch))
        new = [r for r in records if (r.record.partition, r.record.offset) not in self.seen]
        self.seen |= {(r.record.partition, r.record.offset) for r in records}
        return SinkResult(len(new), len(records) - len(new))


class FakeDlq:
    def __init__(self, fail=0, events=None):
        self.fail, self.sent = fail, []
        self.events = events if events is not None else []

    def send(self, records, consumer_group):
        if self.fail:
            self.fail -= 1
            raise DlqError("broker down")
        self.events.append("dlq")
        self.sent.extend(records)


def _consumer(batches, *, sink=None, dlq=None, kafka=None, clock=None, **kw):
    events = []
    kafka = kafka or FakeKafkaConsumer(batches, events=events)
    sink = sink or FakeSink(events=events)
    dlq = dlq or FakeDlq(events=events)
    sleeps = []
    ec = EventConsumer(
        kafka,
        sink,
        dlq,
        topic=TOPIC,
        consumer_group=GROUP,
        sleep=sleeps.append,
        clock=clock or (lambda: 0.0),
        batch_id_factory=lambda: "batch-1",
        **kw,
    )
    return ec, kafka, sink, dlq, sleeps, events


def _mixed_batch(sim_records, n=12, partition=0, start=0):
    return [
        FakeMsg(krec(r.value.encode(), partition=partition, offset=start + i))
        for i, r in enumerate(sim_records[:n])
    ]


# ------------------------------------------------------------- classification


def test_valid_record_is_classified_with_event_and_original_bytes(sim_records):
    raw = next(r for r in sim_records if r.is_valid).value.encode()
    result = classify(krec(raw))
    assert isinstance(result, ValidRecord)
    assert result.record.value == raw and str(result.event.event_id) in raw.decode()


@pytest.mark.parametrize(
    "defect,expected",
    [
        ("malformed_json", FailureType.MALFORMED_JSON),
        ("missing_order_id", FailureType.MISSING_FIELD),
        ("missing_event_id", FailureType.MISSING_FIELD),
        ("unknown_event_type", FailureType.SCHEMA_VALIDATION),
        ("unexpected_field", FailureType.SCHEMA_VALIDATION),
        ("unsupported_schema_version", FailureType.SCHEMA_VALIDATION),
        ("naive_timestamp", FailureType.SCHEMA_VALIDATION),
    ],
)
def test_invalid_records_are_classified(sim_records, defect, expected):
    raw = _first(sim_records, defect).value.encode()
    result = classify(krec(raw))
    assert isinstance(result, InvalidRecord)
    assert result.failure_type is expected and result.reason
    assert result.record.value == raw  # original bytes kept


@pytest.mark.parametrize("value", [None, b""])
def test_empty_value_is_invalid(value):
    result = classify(krec(value))
    assert isinstance(result, InvalidRecord) and result.failure_type is FailureType.EMPTY_VALUE


def test_non_utf8_bytes_are_malformed_json():
    result = classify(krec(b"\xff\xfe{not json"))
    assert isinstance(result, InvalidRecord)
    assert result.failure_type is FailureType.MALFORMED_JSON


def test_reason_never_contains_payload_values(sim_records):
    raw = _first(sim_records, "invalid_currency").value
    result = classify(krec(raw.encode()))
    assert "payload.currency" in result.reason and "XYZ" not in result.reason


def test_reason_is_capped():
    class Many:
        def errors(self, **_):
            return [{"type": "x", "loc": ("field",), "msg": "m" * 100}] * 20

    assert len(failure_reason(Many())) == MAX_REASON_CHARS


def test_record_from_message_handles_missing_timestamp():
    r = KafkaRecord.from_message(FakeMsg(krec(b"{}", ts=None)))
    assert r.timestamp_ms is None


# ------------------------------------------------------------------- offsets


def test_next_offsets_are_last_plus_one_per_partition():
    records = [krec(b"x", partition=p, offset=o) for p, o in [(0, 5), (1, 2), (0, 7), (1, 3)]]
    assert [(t.partition, t.offset) for t in next_offsets(records)] == [(0, 8), (1, 4)]
    assert offset_ranges(records) == [
        {"partition": 0, "first_offset": 5, "last_offset": 7},
        {"partition": 1, "first_offset": 2, "last_offset": 3},
    ]


def test_consumer_config_disables_auto_commit_and_refuses_to_guess_start():
    settings = KafkaSettings("broker:9092", "events", "events.dlq", "my-group")
    config = build_consumer_config(settings)
    assert config["group.id"] == "my-group"
    assert config["enable.auto.commit"] is False and config["enable.auto.offset.store"] is False
    assert config["auto.offset.reset"] == "error"
    assert build_consumer_config(settings, "earliest")["auto.offset.reset"] == "earliest"
    with pytest.raises(ValueError):
        build_consumer_config(settings, "beginning")


# --------------------------------------------------------------- batch loop


def test_batch_routes_valid_to_sink_invalid_to_dlq_then_commits(sim_records):
    batch = _mixed_batch(sim_records)
    ec, kafka, sink, dlq, _, events = _consumer([batch])
    ec.run(max_batches=1)
    n_valid = sum(r.is_valid for r in sim_records[:12])
    assert len(sink.batches[0][0]) == n_valid and len(dlq.sent) == 12 - n_valid
    assert events == ["dlq", "sink", "commit"]  # commit strictly last
    assert kafka.commits == [[(TOPIC, 0, 12)]]
    info = sink.batches[0][1]
    assert info.records_consumed == 12 and info.dlq_records == 12 - n_valid
    assert info.offset_ranges == [{"partition": 0, "first_offset": 0, "last_offset": 11}]
    s = ec.stats
    assert (s.batches, s.consumed, s.valid, s.dlq, s.rows_inserted) == (
        1,
        12,
        n_valid,
        12 - n_valid,
        n_valid,
    )
    assert s.committed == {f"{TOPIC}[0]": 12}


def test_dlq_receives_original_bytes(sim_records):
    batch = _mixed_batch(sim_records)
    ec, _, _, dlq, _, _ = _consumer([batch])
    ec.run(max_batches=1)
    originals = {m.r.offset: m.r.value for m in batch}
    assert all(inv.record.value == originals[inv.record.offset] for inv in dlq.sent)


def test_sink_failure_retries_with_backoff_then_never_commits(sim_records):
    events = []
    ec, kafka, sink, _, sleeps, _ = _consumer(
        [_mixed_batch(sim_records)], sink=FakeSink(fail=99, events=events), max_attempts=3
    )
    with pytest.raises(ProcessingError, match="offsets NOT committed"):
        ec.run(max_batches=1)
    assert kafka.commits == [] and sleeps == [1.0, 2.0]
    assert ec.stats.batches == 0


def test_transient_sink_failure_recovers_and_commits(sim_records):
    ec, kafka, _, _, sleeps, _ = _consumer([_mixed_batch(sim_records)], sink=FakeSink(fail=1))
    ec.run(max_batches=1)
    assert sleeps == [1.0] and kafka.commits == [[(TOPIC, 0, 12)]]


def test_dlq_failure_blocks_sink_and_commit(sim_records):
    ec, kafka, sink, _, _, _ = _consumer([_mixed_batch(sim_records)], dlq=FakeDlq(fail=99))
    with pytest.raises(ProcessingError, match="DLQ publish"):
        ec.run(max_batches=1)
    assert sink.batches == [] and kafka.commits == []


def test_commit_failure_is_retried_then_raises(sim_records):
    kafka = FakeKafkaConsumer([_mixed_batch(sim_records)], fail_commits=99)
    ec, _, _, _, _, _ = _consumer([], kafka=kafka)
    with pytest.raises(ProcessingError, match="offset commit"):
        ec.run(max_batches=1)
    assert kafka.commits == []


def test_invalid_only_batch_skips_snowflake(sim_records):
    invalid = [
        m for m in _mixed_batch(sim_records, 30) if not isinstance(classify(m.r), ValidRecord)
    ]
    ec, kafka, sink, dlq, _, _ = _consumer([invalid])
    ec.run(max_batches=1)
    assert sink.batches == [] and len(dlq.sent) == len(invalid) and kafka.commits


def test_redelivered_batch_inserts_nothing_twice(sim_records):
    batch = _mixed_batch(sim_records)
    ec, kafka, _, _, _, _ = _consumer([batch, batch])
    ec.run(max_batches=2)
    n_valid = sum(r.is_valid for r in sim_records[:12])
    assert ec.stats.rows_inserted == n_valid and ec.stats.duplicates_skipped == n_valid
    assert len(kafka.commits) == 2


def test_multiple_partitions_commit_separately(sim_records):
    batch = _mixed_batch(sim_records, 4, partition=0) + _mixed_batch(
        sim_records, 3, partition=2, start=40
    )
    ec, kafka, _, _, _, _ = _consumer([batch])
    ec.run(max_batches=1)
    assert kafka.commits == [[(TOPIC, 0, 4), (TOPIC, 2, 43)]]


def test_idle_timeout_stops_without_touching_sink():
    ticks = iter(range(0, 100, 5))
    ec, kafka, sink, _, _, _ = _consumer([], clock=lambda: float(next(ticks)))
    ec.run(idle_timeout=10)
    assert sink.batches == [] and kafka.commits == [] and kafka.subscribed == [TOPIC]


def test_stop_finishes_current_batch_then_exits(sim_records):
    ec, kafka, _, _, _, _ = _consumer(
        [_mixed_batch(sim_records), _mixed_batch(sim_records, start=12)]
    )
    original = ec.process_batch

    def process_then_stop(records):
        original(records)
        ec.stop()

    ec.process_batch = process_then_stop
    ec.run()
    assert len(kafka.commits) == 1 and kafka.batches  # second batch never consumed


def test_partition_eof_is_ignored_and_offset_reset_error_is_explained():
    eof = FakeMsg(krec(b"x"), error=KafkaError(KafkaError._PARTITION_EOF))
    ec, kafka, _, _, _, _ = _consumer([[eof]], clock=iter([0.0, 0.0, 99.0, 99.0]).__next__)
    ec.run(idle_timeout=1)
    assert kafka.commits == []

    reset = FakeMsg(krec(b"x"), error=KafkaError(KafkaError._AUTO_OFFSET_RESET))
    ec2, _, _, _, _, _ = _consumer([[reset]])
    with pytest.raises(ConsumerConfigError, match="no committed offset"):
        ec2.run(max_batches=1)


def test_fatal_kafka_error_raises():
    fatal = FakeMsg(krec(b"x"), error=KafkaError(KafkaError._FATAL, fatal=True))
    ec, _, _, _, _, _ = _consumer([[fatal]])
    with pytest.raises(KafkaException):
        ec.run(max_batches=1)


# ----------------------------------------------------------------- DLQ writer


class FakeProducer:
    def __init__(self, *, fail=False, stuck=False, buffer_errors=0):
        self.fail, self.stuck, self.buffer_errors = fail, stuck, buffer_errors
        self.produced, self.pending, self.polls = [], [], 0

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("full")
        self.produced.append({"topic": topic, "value": value, "key": key, "headers": headers})
        self.pending.append(on_delivery)

    def poll(self, timeout=0):
        self.polls += 1
        return 0

    def flush(self, timeout=None):
        if self.stuck:
            return len(self.pending)
        for cb in self.pending:
            cb(KafkaError(KafkaError._MSG_TIMED_OUT) if self.fail else None, None)
        self.pending = []
        return 0


def _invalid(sim_records, defect="malformed_json"):
    raw = _first(sim_records, defect).value.encode()
    return classify(krec(raw, partition=1, offset=42, key=None))


def test_dlq_message_keeps_original_bytes_and_adds_failure_headers(sim_records):
    producer = FakeProducer()
    inv = _invalid(sim_records)
    DlqWriter(producer, "ecommerce.events.dlq").send([inv], GROUP)
    (msg,) = producer.produced
    assert msg["topic"] == "ecommerce.events.dlq"
    assert msg["value"] == inv.record.value and msg["key"] is None
    headers = {k: v.decode() for k, v in msg["headers"]}
    assert headers["streamforge.failure_type"] == "malformed_json"
    assert headers["streamforge.failure_reason"] == inv.reason
    assert (
        headers["streamforge.source_topic"],
        headers["streamforge.source_partition"],
        headers["streamforge.source_offset"],
    ) == (TOPIC, "1", "42")
    assert headers["streamforge.consumer_group"] == GROUP
    assert headers["streamforge.failed_at"].endswith("Z")


def test_dlq_headers_handle_missing_timestamp():
    inv = classify(krec(None, ts=None))
    headers = dict(dlq_headers(inv, GROUP, START))
    assert headers["streamforge.source_timestamp_ms"] == b""


@pytest.mark.parametrize("producer", [FakeProducer(fail=True), FakeProducer(stuck=True)])
def test_dlq_unconfirmed_delivery_raises(sim_records, producer):
    with pytest.raises(DlqError):
        DlqWriter(producer, "dlq").send([_invalid(sim_records)], GROUP)


def test_dlq_queue_full_is_retried(sim_records):
    producer = FakeProducer(buffer_errors=2)
    DlqWriter(producer, "dlq").send([_invalid(sim_records)], GROUP)
    assert len(producer.produced) == 1 and producer.polls == 2


def test_dlq_empty_send_is_noop():
    producer = FakeProducer()
    DlqWriter(producer, "dlq").send([], GROUP)
    assert producer.produced == []


# ----------------------------------------------------------------------- CLI


@pytest.fixture
def cli_env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # never read the developer's .env
    key = tmp_path / "k.p8"
    key.write_text("x")
    for k, v in {
        "SNOWFLAKE_ACCOUNT": "org-acct",
        "SNOWFLAKE_USER": "U",
        "SNOWFLAKE_WAREHOUSE": "WH",
        "SNOWFLAKE_DATABASE": "DB",
        "SNOWFLAKE_ROLE": "STREAMFORGE_ROLE",
        "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
    }.items():
        monkeypatch.setenv(k, v)
    for k in ("SNOWFLAKE_TOKEN", "SNOWFLAKE_PASSWORD", "KAFKA_EVENTS_TOPIC", "KAFKA_DLQ_TOPIC"):
        monkeypatch.delenv(k, raising=False)


class FakeDlqReader:
    """Stands in for the Kafka consumer used by ``recent_dead_letters`` (assign only)."""

    def __init__(self, messages=(), *, low=0, stall=False):
        self.messages = list(messages)
        self.low, self.stall = low, stall
        self.assigned, self.closed, self.config = None, False, None

    def list_topics(self, topic=None, timeout=None):
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions={0: None})})

    def get_watermark_offsets(self, tp, timeout=None):
        return (self.low, self.low + len(self.messages))

    def assign(self, tps):
        self.assigned = [(tp.topic, tp.partition, tp.offset) for tp in tps]
        self._queue = [] if self.stall else self.messages[tps[0].offset - self.low :]
        self._next = tps[0].offset

    def poll(self, timeout=None):
        if not self._queue:
            return None
        headers = self._queue.pop(0)
        msg = SimpleNamespace(
            error=lambda: None,
            partition=lambda: 0,
            offset=lambda o=self._next: o,
            headers=lambda h=headers: h,
        )
        self._next += 1
        return msg

    def close(self):
        self.closed = True


def _dlq_headers_for(topic, partition, offset, group=GROUP, sha="f" * 64):
    headers = [
        ("streamforge.consumer_group", group.encode()),
        ("streamforge.source_topic", topic.encode()),
        ("streamforge.source_partition", str(partition).encode()),
        ("streamforge.source_offset", str(offset).encode()),
    ]
    if sha is not None:
        headers.append(("streamforge.source_sha256", sha.encode()))
    return headers


def _run_cli(argv, kafka, sink=None, producer=None, reader=None):
    configs = {}

    def consumer_factory(config):
        configs["consumer"] = config
        return kafka

    def reader_factory(config):
        configs["reader"] = config
        return reader or FakeDlqReader()

    return cli.main(
        argv,
        consumer_factory=consumer_factory,
        producer_factory=lambda c: producer or FakeProducer(),
        sink_factory=lambda: sink or FakeSink(),
        dlq_reader_factory=reader_factory,
    ), configs


def test_cli_happy_path(cli_env, sim_records, capsys):
    kafka = FakeKafkaConsumer([_mixed_batch(sim_records)])
    code, configs = _run_cli(["--stop-when-idle", "0"], kafka)
    assert code == 0 and kafka.closed and kafka.commits == [[(TOPIC, 0, 12)]]
    assert configs["consumer"]["group.id"] == GROUP
    assert configs["consumer"]["auto.offset.reset"] == "error"
    out = capsys.readouterr().out
    assert "consumed:            12" in out and f"{TOPIC}[0]=12" in out


def test_cli_refuses_admin_role(cli_env, monkeypatch, capsys):
    monkeypatch.setenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    code, _ = _run_cli([], FakeKafkaConsumer([]))
    assert code == 2 and "STREAMFORGE_ROLE" in capsys.readouterr().err


def test_cli_missing_topic(cli_env, capsys):
    kafka = FakeKafkaConsumer([])
    kafka.topics.pop("ecommerce.events.dlq")
    code, _ = _run_cli([], kafka)
    assert code == 1 and "do not exist" in capsys.readouterr().err and kafka.closed


def test_cli_processing_error_exits_1_without_commit(cli_env, sim_records, capsys):
    kafka = FakeKafkaConsumer([_mixed_batch(sim_records)])
    code, _ = _run_cli(["--max-attempts", "1"], kafka, sink=FakeSink(fail=9))
    assert code == 1 and kafka.commits == [] and kafka.closed
    assert "offsets NOT committed" in capsys.readouterr().err


# ------------------------------------- DLQ duplicate safety (item 4)


def test_dlq_sent_once_when_sink_fails_then_recovers_within_retries(sim_records):
    batch = _mixed_batch(sim_records)
    n_invalid = sum(not r.is_valid for r in sim_records[:12])
    ec, kafka, sink, dlq, sleeps, _ = _consumer([batch], sink=FakeSink(fail=2), max_attempts=3)
    ec.run(max_batches=1)
    assert len(dlq.sent) == n_invalid  # DLQ step not repeated by sink retries
    assert sleeps == [1.0, 2.0] and kafka.commits == [[(TOPIC, 0, 12)]]


def test_same_process_batch_retry_does_not_resend_dlq(sim_records):
    batch = [m.r for m in _mixed_batch(sim_records)]
    sink = FakeSink(fail=99)
    ec, kafka, _, dlq, _, _ = _consumer([], sink=sink, max_attempts=1)
    with pytest.raises(ProcessingError):
        ec.process_batch(batch)
    first = len(dlq.sent)
    assert first > 0 and kafka.commits == []
    sink.fail = 0
    ec.process_batch(batch)  # caller retries the whole batch
    assert len(dlq.sent) == first and ec.stats.dlq_already_present == first
    assert kafka.commits == [[(TOPIC, 0, 12)]]


def test_restart_after_failed_batch_does_not_duplicate_dlq(sim_records):
    """DLQ succeeded, Snowflake failed, process stops; the restarted consumer is seeded
    from the DLQ tail scan and does not dead-letter the same records again."""
    batch = _mixed_batch(sim_records)
    first, kafka1, _, dlq1, _, _ = _consumer([batch], sink=FakeSink(fail=99), max_attempts=2)
    with pytest.raises(ProcessingError, match="Snowflake RAW load"):
        first.run(max_batches=1)
    assert kafka1.commits == [] and dlq1.sent

    scanned = {dlq_identity(i.record) for i in dlq1.sent}
    second, kafka2, sink2, dlq2, _, _ = _consumer([batch], dead_lettered=scanned)
    second.run(max_batches=1)
    assert dlq2.sent == [] and second.stats.dlq_already_present == len(dlq1.sent)
    assert len(sink2.batches[0][0]) == sum(r.is_valid for r in sim_records[:12])
    assert kafka2.commits == [[(TOPIC, 0, 12)]]


def test_recent_dead_letters_reads_only_the_tail_and_filters_by_group_and_topic():
    from streamforge.consumer.dlq import recent_dead_letters

    messages = [
        _dlq_headers_for(TOPIC, 0, 1),
        _dlq_headers_for(TOPIC, 0, 2, group="other-group"),
        _dlq_headers_for("other.topic", 1, 3),
        _dlq_headers_for(TOPIC, 2, 40),
        [("unrelated", b"x")],
    ]
    reader = FakeDlqReader(messages, low=100)
    found = recent_dead_letters(reader, "dlq", source_topic=TOPIC, consumer_group=GROUP, window=4)
    assert reader.assigned == [("dlq", 0, 101)]  # max(low, high - window)
    assert found == {(TOPIC, 2, 40, "f" * 64)}  # message at offset 100 is outside the window


def test_recent_dead_letters_on_empty_dlq_and_timeout():
    from streamforge.consumer.dlq import recent_dead_letters

    empty = recent_dead_letters(
        FakeDlqReader([]), "dlq", source_topic=TOPIC, consumer_group=GROUP, window=10
    )
    assert empty == set()
    ticks = iter(range(100))
    with pytest.raises(DlqError, match="could not read the DLQ tail"):
        recent_dead_letters(
            FakeDlqReader([_dlq_headers_for(TOPIC, 0, 1)], stall=True),
            "dlq",
            source_topic=TOPIC,
            consumer_group=GROUP,
            window=10,
            timeout=3,
            clock=lambda: float(next(ticks)),
        )


def test_cli_dlq_scan_suppresses_resend(cli_env, sim_records):
    batch = _mixed_batch(sim_records)
    invalid = [m.r for m in batch if isinstance(classify(m.r), InvalidRecord)]
    reader = FakeDlqReader(
        [_dlq_headers_for(TOPIC, 0, r.offset, sha=record_fingerprint(r)) for r in invalid]
    )
    producer = FakeProducer()
    code, configs = _run_cli(
        ["--stop-when-idle", "0"], FakeKafkaConsumer([batch]), producer=producer, reader=reader
    )
    assert code == 0 and producer.produced == [] and reader.closed
    assert configs["reader"]["enable.auto.commit"] is False
    assert configs["reader"]["group.id"].startswith("streamforge-dlq-scan-")


# ---------------------------- commit ordering and next offsets (items 5, 6)


@pytest.mark.parametrize("failing", ["dlq", "sink"])
def test_no_commit_unless_both_dlq_and_sink_succeed(sim_records, failing):
    kw = {"dlq": FakeDlq(fail=99)} if failing == "dlq" else {"sink": FakeSink(fail=99)}
    ec, kafka, _, _, _, _ = _consumer([_mixed_batch(sim_records)], max_attempts=2, **kw)
    with pytest.raises(ProcessingError):
        ec.run(max_batches=1)
    assert kafka.commits == [] and ec.stats.committed == {}


def test_committed_offsets_are_last_plus_one_with_gaps_and_unordered_offsets(sim_records):
    raw = [r.value.encode() for r in sim_records[:6]]
    layout = [(1, 10), (1, 7), (1, 12), (0, 3), (2, 99), (0, 0)]  # gaps, out of order
    batch = [FakeMsg(krec(v, partition=p, offset=o)) for v, (p, o) in zip(raw, layout, strict=True)]
    ec, kafka, _, _, _, _ = _consumer([batch])
    ec.run(max_batches=1)
    assert kafka.commits == [[(TOPIC, 0, 4), (TOPIC, 1, 13), (TOPIC, 2, 100)]]


# ------------------------- malformed records keep key and value bytes (item 7)


@pytest.mark.parametrize(
    "key,value",
    [
        (b"ORD-0123456789AB", b'{"event_id": "x", "truncated'),
        (b"\x00\xffbinary-key", b"\xff\xfe\x00 not utf-8"),
        (None, b"not json at all"),
        (b"k", None),
    ],
)
def test_malformed_records_reach_dlq_with_identical_key_and_value(key, value):
    producer = FakeProducer()
    batch = [FakeMsg(krec(value, key=key, offset=5))]
    ec, kafka, _, _, _, _ = _consumer([batch], dlq=DlqWriter(producer, "dlq"))
    ec.run(max_batches=1)
    (msg,) = producer.produced
    assert msg["key"] == key and msg["value"] == value  # byte-for-byte
    assert kafka.commits == [[(TOPIC, 0, 6)]]


# ------------------------------------- no secrets or payload in logs (item 8)


def test_logs_and_errors_never_contain_payload_values(sim_records, caplog):
    batch = _mixed_batch(sim_records)
    payload_text = [m.r.value.decode("utf-8", "replace") for m in batch]
    customer = next(r for r in sim_records[:12] if r.is_valid and "customer_id" in r.value)
    cust_id = customer.data["payload"]["customer_id"]

    class LeakySink(FakeSink):
        def write_batch(self, records, batch_info):
            # Databases echo offending values in quotes; they must not reach logs.
            raise RuntimeError(f"Numeric value '{cust_id}' is not recognized; token 'pat-SECRET-1'")

    caplog.set_level("DEBUG")
    ec, _, _, _, _, _ = _consumer([batch], sink=LeakySink(), max_attempts=2)
    with pytest.raises(ProcessingError) as exc:
        ec.run(max_batches=1)
    joined = "\n".join([r.getMessage() for r in caplog.records] + [str(exc.value)])
    assert "RuntimeError" in joined and "<redacted>" in joined
    assert cust_id not in joined and "pat-SECRET-1" not in joined
    assert not any(p in joined for p in payload_text)
    assert exc.value.__cause__ is None


def test_success_logs_contain_only_counts_and_offsets(sim_records, caplog):
    caplog.set_level("DEBUG")
    batch = _mixed_batch(sim_records)
    ec, _, _, _, _, _ = _consumer([batch])
    ec.run(max_batches=1)
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "consumed=12" in joined
    for m in batch:
        assert m.r.value.decode("utf-8", "replace") not in joined
    order_ids = {r.data.get("order_id") for r in sim_records[:12] if r.data}
    assert not any(str(o) in joined for o in order_ids if o)


def test_cli_output_never_contains_credentials(cli_env, monkeypatch, sim_records, capsys, caplog):
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "pp-SECRET-passphrase")
    caplog.set_level("DEBUG")
    _run_cli(["--stop-when-idle", "0"], FakeKafkaConsumer([_mixed_batch(sim_records)]))
    captured = capsys.readouterr()
    logs = "\n".join(r.getMessage() for r in caplog.records)
    assert "pp-SECRET-passphrase" not in captured.out + captured.err + logs


def test_safe_error_text_scrubs_quoted_values_and_caps_length():
    from streamforge.consumer.records import MAX_ERROR_CHARS, safe_error_text

    text = safe_error_text(ValueError("bad 'CUST-000123' and \"x\"\n\n  more " + "z" * 500))
    assert "CUST-000123" not in text and text.startswith("ValueError: bad '<redacted>'")
    assert len(text) == MAX_ERROR_CHARS


# ---------------------------- no silent loss on replay / reused offsets (item 2)


def test_fingerprint_distinguishes_content_null_and_empty():
    base = krec(b"value", key=b"key")
    assert record_fingerprint(base) == record_fingerprint(krec(b"value", key=b"key"))
    variants = [
        krec(b"value2", key=b"key"),
        krec(b"value", key=b"key2"),
        krec(b"keyvalue", key=b""),  # boundary shift must not collide
        krec(None, key=b"key"),
        krec(b"", key=b"key"),
        krec(b"value", key=None),
    ]
    prints = {record_fingerprint(v) for v in variants} | {record_fingerprint(base)}
    assert len(prints) == len(variants) + 1


def test_dlq_header_carries_the_content_fingerprint(sim_records):
    inv = _invalid(sim_records)
    headers = {k: v.decode() for k, v in dlq_headers(inv, GROUP, START)}
    assert headers["streamforge.source_sha256"] == record_fingerprint(inv.record)


def test_different_record_at_a_reused_offset_is_dead_lettered_not_skipped():
    """E.g. the source topic was recreated and offsets restarted: the DLQ already has an
    entry for (topic, 0, 5) but with other content. The new record must not be lost."""
    old = krec(b"old malformed", offset=5)
    new = krec(b"new malformed", offset=5)
    ec, kafka, _, dlq, _, _ = _consumer([[FakeMsg(new)]], dead_lettered={dlq_identity(old)})
    ec.run(max_batches=1)
    assert [i.record.value for i in dlq.sent] == [b"new malformed"]
    assert ec.stats.dlq == 1 and ec.stats.dlq_already_present == 0
    assert kafka.commits == [[(TOPIC, 0, 6)]]


def test_dlq_entries_without_fingerprint_never_suppress_a_record():
    reader = FakeDlqReader([_dlq_headers_for(TOPIC, 0, 5, sha=None)])
    found = recent_dead_letters(reader, "dlq", source_topic=TOPIC, consumer_group=GROUP, window=10)
    assert found == set()


def test_replay_of_old_offsets_dead_letters_again_rather_than_losing():
    """Records whose DLQ entries are outside the scanned tail are re-dead-lettered
    (a duplicate is possible), never skipped."""
    rec = krec(b"bad", offset=0)
    ec, kafka, _, dlq, _, _ = _consumer([[FakeMsg(rec)]], dead_lettered=set())
    ec.run(max_batches=1)
    assert len(dlq.sent) == 1 and kafka.commits == [[(TOPIC, 0, 1)]]


def test_inconsistent_sink_accounting_blocks_commit(sim_records):
    class ShortSink(FakeSink):
        def write_batch(self, records, batch):
            return SinkResult(len(records) - 1, 0)  # one record unaccounted for

    ec, kafka, _, _, _, _ = _consumer([_mixed_batch(sim_records)], sink=ShortSink())
    with pytest.raises(ProcessingError, match="accounted for"):
        ec.run(max_batches=1)
    assert kafka.commits == []


def test_partial_dlq_delivery_is_not_marked_and_blocks_commit(sim_records):
    """If DLQ delivery fails for any record, nothing is recorded as dead-lettered and
    the offsets are not committed, so the redelivered batch dead-letters it again."""
    producer = FakeProducer(fail=True)
    ec, kafka, _, _, _, _ = _consumer(
        [_mixed_batch(sim_records)], dlq=DlqWriter(producer, "dlq"), max_attempts=1
    )
    with pytest.raises(ProcessingError, match="DLQ publish"):
        ec.run(max_batches=1)
    assert kafka.commits == [] and ec._dead_lettered == set()
