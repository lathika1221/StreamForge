"""M8 failure-injection and recovery tests against the local Redpanda. No Snowflake.

Each test uses its own temporary events topic (3 partitions), DLQ topic and consumer
group, all deleted afterwards: ``ecommerce.events``, its DLQ and the
``streamforge-consumer`` group are never read or modified. Kafka is real (offsets,
commits, rebalances, DLQ delivery); Snowflake is replaced by ``RawStore``, an in-memory
RAW table that survives "restarts" and, like the real sink, MERGEs on
(topic, partition, offset). A process crash is modelled by ``Killed`` (a BaseException,
so the consumer's retry logic cannot catch it); the consumer is then closed, which
commits nothing (auto-commit is off), exactly as when the process dies.

For every scenario the test checks: is the source message still recoverable, was an
offset committed, what reached RAW / the DLQ, were duplicates created, and does a
plain restart recover without manual repair (``assert_exactly_once``).

Run with: python -m pytest -m integration tests/integration/test_consumer_recovery.py
"""

import dataclasses
import os
import threading
import uuid
from collections import Counter
from datetime import UTC, datetime

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streamforge.common.config import load_kafka_settings
from streamforge.common.snowflake import SnowflakeConnectionError
from streamforge.consumer import cli
from streamforge.consumer.consumer import EventConsumer, ProcessingError, build_consumer_config
from streamforge.consumer.dlq import (
    DlqIndex,
    DlqWriter,
    dlq_identity,
    identity_from_headers,
    recent_dead_letters,
)
from streamforge.consumer.records import KafkaRecord, ValidRecord, classify
from streamforge.consumer.snowflake_sink import SinkError, SinkResult
from streamforge.producer.publisher import EventPublisher, build_producer_config
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

pytestmark = pytest.mark.integration

START = datetime(2026, 11, 25, 9, 0, tzinfo=UTC)


class Killed(BaseException):
    """The process dies here (not an Exception: retries cannot swallow it)."""


# --------------------------------------------------------------- fake RAW


class RawStore:
    """Persistent stand-in for the RAW tables: MERGE on (topic, partition, offset)."""

    def __init__(self):
        self.rows, self.lock = {}, threading.Lock()
        self.inserts = 0  # rows ever inserted; > len(rows) would mean a RAW duplicate

    def merge(self, records):
        with self.lock:
            new = 0
            for r in records:
                key = (r.record.topic, r.record.partition, r.record.offset)
                if key not in self.rows:
                    self.rows[key] = r.record.value
                    new += 1
            self.inserts += new
            return SinkResult(new, len(records) - new)


class FakeRawSink:
    """``mode``: ok | connect (connection fails) | txn (rows staged, then rollback) |
    kill (process dies during the load). ``failures`` limits how often it fails."""

    def __init__(self, store, mode="ok", failures=10**9):
        self.store, self.mode, self.failures, self.calls = store, mode, failures, 0

    def write_batch(self, records, batch):
        self.calls += 1
        if self.mode != "ok" and self.failures > 0:
            self.failures -= 1
            if self.mode == "connect":
                raise SnowflakeConnectionError("simulated: Snowflake unreachable")
            if self.mode == "txn":
                staged = [r.record.offset for r in records]  # never committed
                assert staged
                raise SinkError("simulated: MERGE failed, transaction rolled back")
            if self.mode == "kill":
                raise Killed()
        return self.store.merge(records)


class KafkaProxy:
    """Real consumer with an injected fault on the n-th offset commit."""

    def __init__(self, consumer, kill_on_commit=None):
        self._c, self._kill_on, self.commit_calls = consumer, kill_on_commit, 0

    def __getattr__(self, name):
        return getattr(self._c, name)

    def commit(self, *args, **kwargs):
        self.commit_calls += 1
        if self._kill_on is not None and self.commit_calls >= self._kill_on:
            raise Killed()  # RAW/DLQ done, offsets not committed
        return self._c.commit(*args, **kwargs)


# ------------------------------------------------------------ environment


@pytest.fixture
def env():
    base = load_kafka_settings(dict(os.environ))
    suffix = uuid.uuid4().hex[:10]
    settings = dataclasses.replace(
        base,
        events_topic=f"streamforge.it.recovery.{suffix}",
        dlq_topic=f"streamforge.it.recovery.{suffix}.dlq",
        consumer_group=f"streamforge-it-recovery-{suffix}",
    )
    # safety: never the real topics or group
    assert settings.events_topic.startswith("streamforge.it.")
    assert settings.consumer_group.startswith("streamforge-it-")
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    for f in admin.create_topics(
        [
            NewTopic(settings.events_topic, num_partitions=3, replication_factor=1),
            NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1),
        ]
    ).values():
        f.result(15)
    yield settings
    for f in admin.delete_topics([settings.events_topic, settings.dlq_topic]).values():
        f.result(15)


def publish(env, orders=12, seed=8, invalid_rate=0.4):
    """Deterministic mix of valid and invalid events, keyed by order_id (3 partitions)."""
    cfg = SimulatorConfig(seed=seed, start_time=START, invalid_rate=invalid_rate)
    records = list(EventSimulator(cfg).iter_records(orders))
    publisher = EventPublisher(Producer(build_producer_config(env)), env.events_topic)
    publisher.publish_all(records)
    assert publisher.flush(30) == 0 and publisher.stats.ok
    return records


def run(
    env,
    store,
    *,
    sink=None,
    dlq_writer=None,
    kill_on_commit=None,
    batch_size=50,
    max_batches=None,
    idle=4.0,
    max_attempts=2,
    stop_after_batches=None,
):
    """One consumer "process": start (DLQ index full scan), consume, stop or die.

    Returns (EventConsumer, error) where error is the ProcessingError or Killed that
    ended the process, or None.
    """
    kafka = KafkaProxy(
        Consumer(build_consumer_config(env, offset_reset="earliest")), kill_on_commit
    )
    producer = Producer({**build_producer_config(env), "client.id": "it-dlq"})
    reader = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": f"it-dlq-index-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    index = DlqIndex(
        reader, env.dlq_topic, source_topic=env.events_topic, consumer_group=env.consumer_group
    )
    ec = EventConsumer(
        kafka,
        sink or FakeRawSink(store),
        dlq_writer or DlqWriter(producer, env.dlq_topic),
        topic=env.events_topic,
        consumer_group=env.consumer_group,
        batch_size=batch_size,
        batch_timeout=1.0,
        max_attempts=max_attempts,
        backoff_seconds=0.1,
        dlq_index=index,
    )
    if stop_after_batches is not None:  # graceful stop request (as on SIGINT/SIGTERM)
        inner = ec.process_batch

        def process_then_maybe_stop(records):
            inner(records)
            if ec.stats.batches >= stop_after_batches:
                ec.stop()

        ec.process_batch = process_then_maybe_stop
    error = None
    try:
        index.refresh()
        ec.run(max_batches=max_batches, idle_timeout=idle)
    except (ProcessingError, Killed) as exc:
        error = exc
    finally:
        kafka.close()  # commits nothing: auto-commit is off
        producer.flush(10)
        reader.close()
    return ec, error


# ------------------------------------------------------------- inspection


def read_topic(env, topic):
    c = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": f"it-read-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
        c.assign([TopicPartition(topic, p, 0) for p in parts])
        out, pos, idle = [], dict.fromkeys(parts, 0), 0
        while any(pos[p] < ends[p] for p in parts) and idle < 40:
            m = c.poll(0.5)
            if m is None:
                idle += 1
            elif m.error() is None:
                out.append(m)
                pos[m.partition()] = m.offset() + 1
        assert all(pos[p] >= ends[p] for p in parts), "could not read the whole topic"
        return out
    finally:
        c.close()


def committed_and_ends(env):
    probe = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": env.consumer_group,
            "enable.auto.commit": False,
        }
    )
    try:
        t = env.events_topic
        parts = probe.list_topics(t, timeout=10).topics[t].partitions
        committed = probe.committed([TopicPartition(t, p) for p in parts], timeout=10)
        ends = {p: probe.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1] for p in parts}
    finally:
        probe.close()
    return {tp.partition: tp.offset for tp in committed}, ends


def nothing_committed(env):
    committed, _ = committed_and_ends(env)
    return all(o < 0 for o in committed.values())


def rewind_group(env):
    """Replay: reset the *temporary* group to offset 0 (never the real group)."""
    assert env.consumer_group.startswith("streamforge-it-")
    c = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": env.consumer_group,
            "enable.auto.commit": False,
        }
    )
    try:
        t = env.events_topic
        parts = c.list_topics(t, timeout=10).topics[t].partitions
        c.commit(offsets=[TopicPartition(t, p, 0) for p in parts], asynchronous=False)
    finally:
        c.close()


def source(env):
    """Every source message, classified: (valid coords, invalid identities)."""
    valid, invalid = set(), set()
    for m in read_topic(env, env.events_topic):
        rec = KafkaRecord.from_message(m)
        if isinstance(classify(rec), ValidRecord):
            valid.add((rec.topic, rec.partition, rec.offset))
        else:
            invalid.add(dlq_identity(rec))
    return valid, invalid


def dlq_counts(env):
    counts = Counter()
    for m in read_topic(env, env.dlq_topic):
        identity = identity_from_headers(m.headers(), env.events_topic, env.consumer_group)
        assert identity is not None, "DLQ message without a complete source identity"
        counts[identity] += 1
    return counts


def assert_exactly_once(env, store):
    """The recovered end state: every valid message once in RAW, every invalid message
    once in the DLQ, nothing else, and committed offsets = end of every partition."""
    valid, invalid = source(env)
    assert set(store.rows) == valid
    assert store.inserts == len(valid), "a RAW row was inserted twice"
    counts = dlq_counts(env)
    assert set(counts) == invalid and all(n == 1 for n in counts.values()), counts
    committed, ends = committed_and_ends(env)
    assert committed == ends
    return valid, invalid


# ===================================================================== scenarios


def test_01_kafka_unavailable_at_startup_then_recovers(env, tmp_path, monkeypatch, capsys):
    """CLI start with the broker unreachable: clean exit 1, nothing consumed or
    committed. Same command once Kafka is reachable: full recovery."""
    publish(env)
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
        "KAFKA_EVENTS_TOPIC": env.events_topic,
        "KAFKA_DLQ_TOPIC": env.dlq_topic,
        "KAFKA_CONSUMER_GROUP": env.consumer_group,
    }.items():
        monkeypatch.setenv(k, v)
    for k in ("SNOWFLAKE_TOKEN", "SNOWFLAKE_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    store = RawStore()
    argv = ["--offset-reset", "earliest", "--stop-when-idle", "4", "--batch-timeout", "1"]

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:1")  # nothing listens here
    code = cli.main(argv, sink_factory=lambda: FakeRawSink(store))
    assert code == 1 and "error:" in capsys.readouterr().err
    assert store.rows == {} and nothing_committed(env)  # source intact, recoverable

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", env.bootstrap_servers)
    code = cli.main(argv, sink_factory=lambda: FakeRawSink(store))
    assert code == 0
    assert_exactly_once(env, store)


def test_02_dlq_delivery_failure_commits_nothing_then_recovers(env):
    """Real DLQ delivery failure (topic does not exist, auto-create disabled): the batch
    stops before RAW, no offset is committed; a restart delivers everything once."""
    publish(env)
    store = RawStore()
    broken = Producer(
        {
            **build_producer_config(env),
            "client.id": "it-dlq-broken",
            "delivery.timeout.ms": 3000,
            "topic.metadata.propagation.max.ms": 1000,
        }
    )
    ec, error = run(env, store, dlq_writer=DlqWriter(broken, env.dlq_topic + ".missing"))
    assert isinstance(error, ProcessingError) and "DLQ publish" in str(error)
    assert ec.stats.retries.get("DLQ publish") == 1  # bounded retry happened
    assert store.rows == {} and nothing_committed(env)
    assert dlq_counts(env) == Counter()

    run(env, store)
    assert_exactly_once(env, store)


def test_03_snowflake_connection_failure_then_recovers(env):
    publish(env)
    store = RawStore()
    sink = FakeRawSink(store, mode="connect")
    ec, error = run(env, store, sink=sink, max_attempts=3)
    assert isinstance(error, ProcessingError) and "Snowflake RAW load" in str(error)
    assert sink.calls == 3 and ec.stats.retries == {"Snowflake RAW load": 2}
    assert store.rows == {} and nothing_committed(env)
    first_dlq = dlq_counts(env)  # the DLQ step ran before the RAW load
    assert first_dlq and all(n == 1 for n in first_dlq.values())

    ec2, error = run(env, store)
    assert error is None
    assert ec2.stats.dlq_already_present >= len(first_dlq)  # not dead-lettered again
    assert_exactly_once(env, store)


def test_04_snowflake_transaction_failure_rolls_back_then_recovers(env):
    publish(env)
    store = RawStore()
    # permanent failure: rolled back, nothing persisted, nothing committed
    _, error = run(env, store, sink=FakeRawSink(store, mode="txn"))
    assert isinstance(error, ProcessingError) and store.rows == {} and nothing_committed(env)
    # transient failure on restart: the retry succeeds inside the same process
    ec, error = run(env, store, sink=FakeRawSink(store, mode="txn", failures=1))
    assert error is None and ec.stats.retries == {"Snowflake RAW load": 1}
    assert_exactly_once(env, store)


def test_05_killed_after_raw_commit_before_offset_commit(env):
    publish(env)
    store = RawStore()
    ec, error = run(env, store, kill_on_commit=1)
    assert isinstance(error, Killed) and ec.stats.commits == 0
    persisted = len(store.rows)
    assert persisted > 0 and nothing_committed(env)  # RAW committed, offsets not

    ec2, error = run(env, store)
    assert error is None
    # the redelivered batch is recognised by the RAW MERGE: no second row
    assert ec2.stats.duplicates_skipped == persisted
    assert ec2.stats.rows_inserted == len(store.rows) - persisted
    assert_exactly_once(env, store)


def test_06_killed_after_dlq_delivery_before_offset_commit(env):
    publish(env)
    store = RawStore()
    ec, error = run(env, store, sink=FakeRawSink(store, mode="kill"))
    assert isinstance(error, Killed)
    delivered = dlq_counts(env)
    assert delivered and store.rows == {} and nothing_committed(env)

    ec2, error = run(env, store)
    assert error is None
    assert ec2.stats.dlq_already_present == sum(delivered.values())  # not re-sent
    assert_exactly_once(env, store)


def test_07_duplicate_message_delivery(env):
    """The same bytes published twice are two Kafka messages: RAW keeps both (it is
    per Kafka message; dbt STAGING dedupes by event_id), the DLQ gets one entry per
    source message. Redelivering the same offsets adds nothing anywhere."""
    records = publish(env, orders=4, invalid_rate=0.5)
    good = next(r for r in records if r.is_valid)
    bad = next(r for r in records if not r.is_valid)
    p = Producer({"bootstrap.servers": env.bootstrap_servers})
    for r in (good, bad):
        p.produce(env.events_topic, value=r.value.encode(), key=b"dup", partition=0)
    assert p.flush(10) == 0
    store = RawStore()
    run(env, store)
    valid, invalid = assert_exactly_once(env, store)
    assert sum(v == good.value.encode() for v in store.rows.values()) == 2
    bad_ids = [
        i
        for i in invalid
        if i[3] == dlq_identity(KafkaRecord("", 0, 0, b"dup", bad.value.encode(), None))[3]
    ]
    assert len(bad_ids) == 1  # the copy keyed b"dup"; its other copy has another key

    rewind_group(env)
    ec, error = run(env, store)
    assert error is None and ec.stats.rows_inserted == 0 and ec.stats.dlq == 0
    assert_exactly_once(env, store)


def test_08_graceful_stop_and_restart_with_uncommitted_messages(env):
    """A stop request finishes the current batch and commits only that batch, even
    though the client has already fetched more; a restart resumes exactly there."""
    publish(env)
    store = RawStore()
    ec, error = run(env, store, batch_size=5, stop_after_batches=1)
    assert error is None and ec.stats.batches == 1 and ec.stats.consumed <= 5
    committed, ends = committed_and_ends(env)
    done = sum(max(o, 0) for o in committed.values())
    assert done == ec.stats.consumed < sum(ends.values())  # nothing beyond the batch

    ec2, error = run(env, store, batch_size=5)
    assert error is None
    assert ec2.stats.consumed == sum(ends.values()) - done  # resumed, no reprocessing
    assert ec2.stats.duplicates_skipped == 0 and ec2.stats.dlq_already_present == 0
    assert_exactly_once(env, store)


def test_09_replaying_committed_messages_creates_no_duplicates(env):
    """Replay everything after a reset of the (temporary) group: RAW and DLQ are
    unchanged, including DLQ entries far outside the old 2 x batch_size tail window."""
    publish(env, orders=20)
    store = RawStore()
    run(env, store, batch_size=5)
    valid, invalid = assert_exactly_once(env, store)

    # the superseded startup scan (window 2 x batch_size) would miss older entries
    reader = Consumer(
        {
            "bootstrap.servers": env.bootstrap_servers,
            "group.id": f"it-tail-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    try:
        tail = recent_dead_letters(
            reader,
            env.dlq_topic,
            source_topic=env.events_topic,
            consumer_group=env.consumer_group,
            window=10,
        )
    finally:
        reader.close()
    assert len(tail) < len(invalid)

    rewind_group(env)
    ec, error = run(env, store, batch_size=5)
    assert error is None
    assert ec.stats.rows_inserted == 0 and ec.stats.duplicates_skipped == len(valid)
    assert ec.stats.dlq == 0 and ec.stats.dlq_already_present == len(invalid)
    assert_exactly_once(env, store)


def test_10a_per_partition_offsets_after_a_mid_run_crash(env):
    """Kill on the 3rd commit: the broker holds exactly the per-partition offsets of
    the two committed batches; a restart completes every partition."""
    publish(env, orders=15)
    store = RawStore()
    ec, error = run(env, store, batch_size=8, kill_on_commit=3)
    assert isinstance(error, Killed) and ec.stats.commits == 2
    committed, ends = committed_and_ends(env)
    expected = {int(k.split("[")[1].rstrip("]")): v for k, v in ec.stats.committed.items()}
    assert {p: o for p, o in committed.items() if o >= 0} == expected
    assert all(o <= ends[p] for p, o in committed.items())

    ec2, error = run(env, store, batch_size=8)
    assert error is None
    # every partition was committed by one of the two runs (a partition finished before
    # the crash has nothing left for the restart); broker offsets are checked below
    assert len(ec.stats.committed.keys() | ec2.stats.committed.keys()) == 3
    assert_exactly_once(env, store)


def test_10b_two_concurrent_consumers_in_one_group(env):
    """Two live members of the group (rebalance on join): partitions are split, every
    message lands once in RAW or the DLQ, and every partition is fully committed."""
    publish(env, orders=40)
    store = RawStore()
    results = []

    def member():
        results.append(run(env, store, batch_size=10, idle=6.0))

    threads = [threading.Thread(target=member) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert len(results) == 2 and all(err is None for _, err in results)
    assert all(ec.stats.rebalances >= 1 for ec, _ in results)
    assert sum(ec.stats.consumed for ec, _ in results) >= len(source(env)[0])
    assert_exactly_once(env, store)
