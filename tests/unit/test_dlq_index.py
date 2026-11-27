"""M8 unit tests: persistent DLQ de-duplication (``DlqIndex``) and DLQ retry safety.

No broker: ``DlqLog`` is an in-memory, multi-partition DLQ topic shared by a fake DLQ
writer and fake readers, so several "processes" (EventConsumer + DlqIndex instances)
see the same DLQ exactly as they would on a real broker.
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from confluent_kafka import TopicPartition

from streamforge.consumer.consumer import EventConsumer, ProcessingError
from streamforge.consumer.dlq import (
    DlqError,
    DlqIndex,
    dlq_headers,
    dlq_identity,
    identity_from_headers,
    recent_dead_letters,
)
from streamforge.consumer.records import InvalidRecord, KafkaRecord, classify
from streamforge.consumer.snowflake_sink import SinkResult

TOPIC, DLQ, GROUP = "ecommerce.events", "ecommerce.events.dlq", "streamforge-consumer"
NOW = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)


# ------------------------------------------------------------------- fakes


def bad(offset, partition=0):
    """An invalid (malformed JSON) source record at the given coordinates."""
    return KafkaRecord(TOPIC, partition, offset, b"ORD-1", b"{bad %d" % offset, 1)


def invalid(record):
    result = classify(record)
    assert isinstance(result, InvalidRecord)
    return result


class DlqLog:
    """In-memory DLQ topic: per partition, a list of (offset, headers) plus watermarks."""

    def __init__(self, partitions=1):
        self.entries = {p: [] for p in range(partitions)}
        self.low = {p: 0 for p in range(partitions)}
        self.high = {p: 0 for p in range(partitions)}

    def append(self, headers, partition=0):
        self.entries[partition].append((self.high[partition], headers))
        self.high[partition] += 1

    def dead_letter(self, record, partition=0, group=GROUP):
        self.append(dlq_headers(invalid(record), group, NOW), partition)

    def gap(self, partition, n):
        """Offsets that are never delivered as messages (e.g. transaction markers)."""
        self.high[partition] += n

    def expire(self, partition, new_low):
        """Retention: delete everything below ``new_low``."""
        self.low[partition] = new_low
        self.entries[partition] = [e for e in self.entries[partition] if e[0] >= new_low]

    def count(self, identity):
        return sum(
            identity_from_headers(h, TOPIC, GROUP) == identity
            for entries in self.entries.values()
            for _, h in entries
        )


class LogReader:
    """Kafka-consumer stand-in used by DlqIndex (assign/seek/poll/position only)."""

    def __init__(self, log, *, stall_polls=0):
        self.log, self.stall_polls = log, stall_polls
        self.assignments, self.seek_calls, self.closed, self.polls = [], [], False, 0
        self.pos, self.cursors = {}, {}

    def list_topics(self, topic=None, timeout=None):
        parts = {p: None for p in self.log.entries}
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions=parts, error=None)})

    def get_watermark_offsets(self, tp, timeout=None):
        return self.log.low[tp.partition], self.log.high[tp.partition]

    def _cursor(self, partition, offset):
        entries = self.log.entries[partition]
        return next((i for i, (off, _) in enumerate(entries) if off >= offset), len(entries))

    def assign(self, tps):
        self.assignments.append({tp.partition: tp.offset for tp in tps})
        self.pos, self.cursors = {}, {}
        for tp in tps:
            start = max(tp.offset, self.log.low[tp.partition])
            self.pos[tp.partition] = start
            self.cursors[tp.partition] = self._cursor(tp.partition, start)

    def seek(self, tp):
        self.seek_calls.append((tp.partition, tp.offset))
        start = max(tp.offset, self.log.low[tp.partition])
        self.pos[tp.partition] = start
        self.cursors[tp.partition] = self._cursor(tp.partition, start)

    def poll(self, timeout=None):
        self.polls += 1
        if self.stall_polls:
            self.stall_polls -= 1
            return None
        candidates = []
        for p, cursor in self.cursors.items():
            entries = self.log.entries[p]
            if cursor < len(entries):
                off, headers = entries[cursor]
                candidates.append((off, p, headers))
        if not candidates:
            return None
        off, p, headers = min(candidates)
        self.cursors[p] += 1
        self.pos[p] = off + 1
        return SimpleNamespace(
            error=lambda: None, partition=lambda: p, offset=lambda: off, headers=lambda: headers
        )

    def position(self, tps):
        # Like librdkafka: once a partition's messages are consumed, the position moves
        # past trailing offsets that carry no message.
        out = []
        for tp in tps:
            pending = self.cursors[tp.partition] < len(self.log.entries[tp.partition])
            pos = self.pos[tp.partition] if pending else self.log.high[tp.partition]
            out.append(TopicPartition(tp.topic, tp.partition, pos))
        return out

    def close(self):
        self.closed = True


class Ticker:
    def __init__(self, step=1.0):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


def index(log, **kw):
    kw.setdefault("timeout", 30.0)
    reader = kw.pop("reader", None) or LogReader(log)
    return DlqIndex(reader, DLQ, source_topic=TOPIC, consumer_group=GROUP, **kw)


class CountingIndex:
    """Wrap a DlqIndex to assert refresh frequency without changing its semantics."""

    def __init__(self, inner):
        self.inner = inner
        self.refresh_calls = 0

    def refresh(self):
        self.refresh_calls += 1
        return self.inner.refresh()

    def __contains__(self, identity):
        return identity in self.inner

    def update(self, identities):
        self.inner.update(identities)


class LogDlqWriter:
    """DLQ writer that appends to the shared log; can deliver only some records, then fail."""

    def __init__(self, log, *, deliver_then_fail=None, fail=0, before_send=None):
        self.log, self.fail, self.deliver_then_fail = log, fail, deliver_then_fail
        self.before_send, self.calls = before_send, 0

    def send(self, records, consumer_group):
        self.calls += 1
        if self.before_send:
            hook, self.before_send = self.before_send, None
            hook()
        if self.fail:
            self.fail -= 1
            raise DlqError("broker unavailable")
        if self.deliver_then_fail is not None:
            n, self.deliver_then_fail = self.deliver_then_fail, None
            for r in records[:n]:
                self.log.append(dlq_headers(r, consumer_group, NOW))
            raise DlqError(f"DLQ delivery not confirmed for {len(records) - n} records")
        for r in records:
            self.log.append(dlq_headers(r, consumer_group, NOW))


class Msg:
    def __init__(self, r):
        self.r = r

    def error(self):
        return None

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
        return (1, self.r.timestamp_ms)


class Kafka:
    def __init__(self, batches, fail_commits=0):
        self.batches, self.fail_commits, self.commits = list(batches), fail_commits, []

    def subscribe(self, topics, **_):
        pass

    def consume(self, num_messages=1, timeout=1.0):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets=None, asynchronous=True):
        if self.fail_commits:
            self.fail_commits -= 1
            raise RuntimeError("process killed before the offset commit")
        self.commits.append({(tp.partition): tp.offset for tp in offsets})

    def close(self):
        pass


class Sink:
    def write_batch(self, records, batch):
        return SinkResult(len(records), 0)


def consumer(log, batches, *, writer=None, idx=None, max_attempts=3, fail_commits=0):
    kafka = Kafka(batches, fail_commits=fail_commits)
    ec = EventConsumer(
        kafka,
        Sink(),
        writer or LogDlqWriter(log),
        topic=TOPIC,
        consumer_group=GROUP,
        max_attempts=max_attempts,
        sleep=lambda _s: None,
        dlq_index=idx if idx is not None else index(log),
    )
    return ec, kafka


# ------------------------------------------------- DlqIndex: reading the DLQ


def test_first_refresh_reads_the_whole_retained_dlq_not_a_tail():
    log = DlqLog()
    for o in range(1_000):
        log.dead_letter(bad(o))
    idx = index(log)
    assert idx.refresh() == 1_000
    assert idx._reader.assignments == [{0: 0}]  # from the low watermark
    assert all(dlq_identity(bad(o)) in idx for o in (0, 1, 500, 999))
    # the superseded tail scan would only have seen the last 2 x batch_size entries
    tail = recent_dead_letters(
        LogReader(log), DLQ, source_topic=TOPIC, consumer_group=GROUP, window=20
    )
    assert dlq_identity(bad(0)) not in tail and len(tail) == 20


def test_incremental_refresh_reads_only_new_messages():
    log = DlqLog()
    for o in range(5):
        log.dead_letter(bad(o))
    idx = index(log)
    assert idx.refresh() == 5 and idx.messages_read == 5
    assert idx.refresh() == 0 and idx.messages_read == 5
    assert len(idx._reader.assignments) == 1  # nothing new: no assign, no read
    log.dead_letter(bad(5))
    log.dead_letter(bad(6))
    assert idx.refresh() == 2
    assert idx._reader.assignments == [{0: 0}]  # stays assigned; polls continue at offset 5
    assert idx._reader.seek_calls == []
    assert idx._positions == {0: 7}
    assert idx.messages_read == 7 and len(idx) == 7


def test_multiple_dlq_partitions_are_all_read():
    log = DlqLog(partitions=3)
    log.dead_letter(bad(1, partition=0), partition=0)
    log.dead_letter(bad(2, partition=1), partition=2)
    log.dead_letter(bad(3, partition=2), partition=2)
    idx = index(log)
    assert idx.refresh() == 3
    assert idx._reader.assignments == [{0: 0, 1: 0, 2: 0}]  # keep empty partitions assigned
    log.dead_letter(bad(4, partition=0), partition=1)
    assert idx.refresh() == 1
    assert idx._reader.assignments == [{0: 0, 1: 0, 2: 0}]
    assert idx._reader.seek_calls == []
    assert {dlq_identity(bad(4, partition=0)), dlq_identity(bad(3, partition=2))} <= set(
        idx._identities
    )


def test_retention_advancing_the_low_watermark():
    log = DlqLog()
    for o in range(10):
        log.dead_letter(bad(o))
    # retention removed offsets 0-3 before the first refresh: start at the new low
    log.expire(0, 4)
    idx = index(log)
    assert idx.refresh() == 6 and idx._reader.assignments == [{0: 4}]
    # retention later moves past the index position: seek the existing assignment
    for o in range(10, 15):
        log.dead_letter(bad(o))
    log.expire(0, 12)
    assert idx.refresh() == 3
    assert idx._reader.assignments == [{0: 4}]
    assert idx._reader.seek_calls == [(0, 12)]
    # identities learnt earlier stay known (retention never causes a resend of those)
    assert dlq_identity(bad(5)) in idx


def test_offset_gaps_without_messages_do_not_stall_the_read():
    log = DlqLog()
    log.dead_letter(bad(1))
    log.gap(0, 2)  # e.g. transaction markers at the end of the partition
    idx = index(log, clock=Ticker(0.5))
    assert idx.refresh() == 1
    log.dead_letter(bad(2))
    assert idx.refresh() == 1
    assert idx._reader.assignments == [{0: 0}]  # persistent assignment, no reassign
    assert idx._positions == {0: 4}


@pytest.mark.parametrize(
    "headers",
    [
        None,
        [],
        [("streamforge.consumer_group", None)],
        # wrong group / wrong source topic
        "other-group",
        "other-topic",
        # incomplete or malformed identity parts
        "no-sha",
        "bad-partition",
        "bad-offset",
    ],
)
def test_malformed_or_foreign_headers_are_never_used_to_skip(headers):
    good = dict(dlq_headers(invalid(bad(7)), GROUP, NOW))
    if headers == "other-group":
        headers = list(dlq_headers(invalid(bad(7)), "someone-else", NOW))
    elif headers == "other-topic":
        headers = [*good.items()]
        headers = [(k, b"x.events" if k.endswith("source_topic") else v) for k, v in headers]
    elif headers == "no-sha":
        headers = [(k, v) for k, v in good.items() if not k.endswith("source_sha256")]
    elif headers == "bad-partition":
        headers = [(k, b"p0" if k.endswith("source_partition") else v) for k, v in good.items()]
    elif headers == "bad-offset":
        headers = [(k, b"" if k.endswith("source_offset") else v) for k, v in good.items()]
    assert identity_from_headers(headers, TOPIC, GROUP) is None
    log = DlqLog()
    log.append(headers)
    log.dead_letter(bad(8))  # a well-formed entry after it is still read
    idx = index(log)
    assert idx.refresh() == 1 and dlq_identity(bad(8)) in idx
    assert dlq_identity(bad(7)) not in idx


def test_missing_dlq_topic_raises_instead_of_reporting_empty():
    reader = LogReader(DlqLog())
    reader.list_topics = lambda topic=None, timeout=None: SimpleNamespace(topics={})
    with pytest.raises(DlqError, match="missing or unavailable"):
        index(DlqLog(), reader=reader).refresh()


def test_refresh_timeout_raises_and_leaves_the_index_unchanged():
    log = DlqLog()
    log.dead_letter(bad(1))
    reader = LogReader(log, stall_polls=10_000)
    idx = index(log, reader=reader, timeout=5.0, clock=Ticker(1.0))
    with pytest.raises(DlqError, match="could not read the DLQ"):
        idx.refresh()
    assert len(idx) == 0 and idx.messages_read == 0
    assert reader.seek_calls == [(0, 0)]  # timeout rewinds the persistent assignment
    reader.stall_polls = 0
    assert idx.refresh() == 1  # the next refresh re-reads from the same position
    assert reader.assignments == [{0: 0}]


# ---------------------------------------- EventConsumer + DlqIndex behaviour


def test_old_dlq_entry_outside_any_tail_window_is_not_resent():
    log = DlqLog()
    log.dead_letter(bad(3))  # dead-lettered long ago
    for o in range(1_000, 3_000):  # ...followed by 2000 other DLQ entries
        log.dead_letter(bad(o))
    ec, kafka = consumer(log, [[Msg(bad(3))]])  # replay of offset 3 (e.g. after a seek)
    ec.run(max_batches=1)
    assert log.count(dlq_identity(bad(3))) == 1
    assert ec.stats.dlq == 0 and ec.stats.dlq_already_present == 1
    assert kafka.commits == [{0: 4}]


def test_refresh_failure_is_retried_then_the_batch_proceeds():
    log = DlqLog()
    log.dead_letter(bad(1))
    reader = LogReader(log, stall_polls=4)  # one attempt = 4 polls: first refresh times out
    idx = index(log, reader=reader, timeout=5.0, clock=Ticker(1.0))
    ec, kafka = consumer(log, [[Msg(bad(1)), Msg(bad(2))]], idx=idx)
    ec.run(max_batches=1)
    assert ec.stats.retries == {"DLQ index refresh": 1}
    assert log.count(dlq_identity(bad(1))) == 1 and log.count(dlq_identity(bad(2))) == 1
    assert kafka.commits == [{0: 3}]


def test_refresh_that_keeps_failing_publishes_and_commits_nothing():
    log = DlqLog()
    reader = LogReader(log, stall_polls=10**9)
    log.dead_letter(bad(0))
    idx = index(log, reader=reader, timeout=2.0, clock=Ticker(1.0))
    writer = LogDlqWriter(log)
    ec, kafka = consumer(log, [[Msg(bad(1))]], idx=idx, writer=writer, max_attempts=2)
    with pytest.raises(ProcessingError, match="DLQ index refresh failed"):
        ec.run(max_batches=1)
    assert writer.calls == 0 and kafka.commits == []


def test_replay_after_restart_dead_letters_each_record_once():
    log = DlqLog()
    batch = [Msg(bad(o)) for o in range(4)]
    # process 1: DLQ delivery confirmed, then killed before the offset commit
    first, kafka1 = consumer(log, [batch], fail_commits=99, max_attempts=1)
    with pytest.raises(ProcessingError, match="offset commit"):
        first.run(max_batches=1)
    assert kafka1.commits == [] and sum(len(e) for e in log.entries.values()) == 4
    # process 2: fresh index (full scan), same batch redelivered
    second, kafka2 = consumer(log, [batch])
    second.run(max_batches=1)
    assert second.stats.dlq == 0 and second.stats.dlq_already_present == 4
    assert all(log.count(dlq_identity(bad(o))) == 1 for o in range(4))
    assert kafka2.commits == [{0: 4}]


def test_partial_dlq_delivery_retry_does_not_duplicate_delivered_records():
    log = DlqLog()
    writer = LogDlqWriter(log, deliver_then_fail=2)  # 2 of 3 confirmed, then an error
    idx = CountingIndex(index(log))
    ec, kafka = consumer(log, [[Msg(bad(o)) for o in range(3)]], writer=writer, idx=idx)
    ec.run(max_batches=1)
    assert all(log.count(dlq_identity(bad(o))) == 1 for o in range(3))
    assert ec.stats.dlq == 3 and ec.stats.retries == {"DLQ publish": 1}
    assert idx.refresh_calls == 2  # one before the first send, one before the retry
    assert kafka.commits == [{0: 3}]


def test_successful_dlq_batch_uses_one_refresh_before_the_first_publish():
    log = DlqLog()
    idx = CountingIndex(index(log))
    ec, kafka = consumer(log, [[Msg(bad(1)), Msg(bad(2))]], idx=idx)
    ec.run(max_batches=1)
    assert idx.refresh_calls == 1
    assert all(log.count(dlq_identity(bad(o))) == 1 for o in (1, 2))
    assert kafka.commits == [{0: 3}]


def test_dlq_entry_with_same_coordinates_but_other_content_does_not_suppress():
    log = DlqLog()
    log.dead_letter(KafkaRecord(TOPIC, 0, 5, b"k", b"{old content", 1))  # topic recreated
    ec, _ = consumer(log, [[Msg(bad(5))]])
    ec.run(max_batches=1)
    assert ec.stats.dlq == 1 and log.count(dlq_identity(bad(5))) == 1


# --------------------------------------- concurrent consumers (documented race)


def test_partition_handoff_after_confirmed_dlq_delivery_is_duplicate_free():
    """Consumer A dead-letters, then dies before committing; B takes the partition over
    (rebalance) with an index built before A's write: the refresh before publishing
    sees A's entry, so nothing is duplicated."""
    log = DlqLog()
    b_index = index(log)
    b_index.refresh()  # B started earlier (empty DLQ then)
    a, _ = consumer(log, [[Msg(bad(9))]], fail_commits=99, max_attempts=1)
    with pytest.raises(ProcessingError):
        a.run(max_batches=1)
    b, kafka_b = consumer(log, [[Msg(bad(9))]], idx=b_index)
    b.run(max_batches=1)
    assert log.count(dlq_identity(bad(9))) == 1 and kafka_b.commits == [{0: 10}]


def test_concurrent_publish_between_refresh_and_send_can_duplicate_but_never_loses():
    """Known limitation (docs/reliability.md): if two consumers process the same record
    at the same time - only possible when a stalled consumer has been evicted from the
    group (max.poll.interval.ms) between its index refresh and its DLQ publish - both
    may publish. The DLQ then holds a duplicate; no record is lost. Kafka-transaction
    fencing would be needed to rule this out."""
    log = DlqLog()
    b, _ = consumer(log, [[Msg(bad(9))]])
    # A refreshes (DLQ empty), then stalls; meanwhile B processes the same record.
    a_writer = LogDlqWriter(log, before_send=lambda: b.run(max_batches=1))
    a, _ = consumer(log, [[Msg(bad(9))]], writer=a_writer)
    a.run(max_batches=1)
    assert log.count(dlq_identity(bad(9))) == 2  # duplicate, not loss


# ------------------------------------------------------ commit / rebalance metrics


def test_commit_retry_and_rebalance_metrics_are_counted_and_serialisable():
    import json

    log = DlqLog()
    calls = {}

    class TrackingKafka(Kafka):
        def subscribe(self, topics, **callbacks):
            calls.update(callbacks)

    kafka = TrackingKafka([[Msg(bad(0)), Msg(bad(1, partition=1))], [Msg(bad(2))]], fail_commits=1)
    ec = EventConsumer(
        kafka,
        Sink(),
        LogDlqWriter(log),
        topic=TOPIC,
        consumer_group=GROUP,
        sleep=lambda _s: None,
        dlq_index=index(log),
    )
    ec.run(max_batches=2)
    # rebalance callbacks are registered; each assignment is counted
    calls["on_assign"](None, [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)])
    calls["on_revoke"](None, [TopicPartition(TOPIC, 0)])
    s = ec.stats
    assert s.rebalances == 1
    # one commit per batch, after the failed first attempt was retried
    assert s.commits == 2 and s.retries == {"offset commit": 1}
    assert kafka.commits == [{0: 1, 1: 2}, {0: 3}]  # per partition, last + 1
    assert s.committed == {f"{TOPIC}[0]": 3, f"{TOPIC}[1]": 2}
    out = json.loads(json.dumps(s.as_dict()))
    assert out["commits"] == 2 and out["dlq"] == 3 and out["retries"] == {"offset commit": 1}
