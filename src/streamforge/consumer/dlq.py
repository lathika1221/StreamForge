"""Dead-letter publishing for records that fail validation (M5).

The DLQ message keeps the **original key and value bytes unchanged**; why and where it
failed travels in Kafka headers (all ``streamforge.*``, UTF-8):

==================================  ==============================================
``streamforge.failure_type``        empty_value | malformed_json | missing_field |
                                    schema_validation
``streamforge.failure_reason``      ``loc: message`` list, no payload values
``streamforge.source_topic``        topic the record was consumed from
``streamforge.source_partition``    its partition
``streamforge.source_offset``       its offset
``streamforge.source_timestamp_ms`` its Kafka timestamp (empty if unavailable)
``streamforge.source_sha256``       fingerprint of the original key + value bytes
``streamforge.consumer_group``      the consumer group that rejected it
``streamforge.failed_at``           UTC ISO-8601 time of rejection
==================================  ==============================================

``send`` returns only after every DLQ message is confirmed by its delivery callback;
any failure raises ``DlqError`` so the caller does not commit the source offsets.

Duplicate safety without loss: a record is treated as "already dead-lettered" only if
the DLQ holds an entry with the same source coordinates **and** the same content
fingerprint (``DlqIdentity``). A different record at a reused offset (e.g. after the
source topic was recreated) never matches, so it is dead-lettered, not skipped.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Message, TopicPartition

from streamforge.consumer.records import InvalidRecord, KafkaRecord
from streamforge.producer.publisher import ProducerLike

HEADER_PREFIX = "streamforge."

# (source topic, partition, offset, sha256 of key+value): identifies one source record.
DlqIdentity = tuple[str, int, int, str]


class DlqError(RuntimeError):
    """A DLQ message could not be confirmed as delivered."""


def _part(data: bytes | None) -> bytes:
    # Length-prefixed, with null distinct from empty, so key/value boundaries are unambiguous.
    return b"N" if data is None else b"B" + len(data).to_bytes(8, "big") + data


def record_fingerprint(record: KafkaRecord) -> str:
    return hashlib.sha256(_part(record.key) + _part(record.value)).hexdigest()


def dlq_identity(record: KafkaRecord) -> DlqIdentity:
    return (record.topic, record.partition, record.offset, record_fingerprint(record))


def dlq_headers(
    invalid: InvalidRecord, consumer_group: str, failed_at: datetime
) -> list[tuple[str, bytes]]:
    r = invalid.record
    values = {
        "failure_type": invalid.failure_type.value,
        "failure_reason": invalid.reason,
        "source_topic": r.topic,
        "source_partition": str(r.partition),
        "source_offset": str(r.offset),
        "source_timestamp_ms": "" if r.timestamp_ms is None else str(r.timestamp_ms),
        "source_sha256": record_fingerprint(r),
        "consumer_group": consumer_group,
        "failed_at": failed_at.isoformat().replace("+00:00", "Z"),
    }
    return [(HEADER_PREFIX + k, v.encode("utf-8")) for k, v in values.items()]


def identity_from_headers(
    headers: list[tuple[str, bytes | None]] | None, source_topic: str, consumer_group: str
) -> DlqIdentity | None:
    """The source identity recorded in a DLQ message, or None if the message belongs to
    another group/topic or lacks any part of the identity (never used to skip a record)."""
    h = {k: (v or b"").decode("utf-8", "replace") for k, v in (headers or [])}
    if (
        h.get(HEADER_PREFIX + "consumer_group") != consumer_group
        or h.get(HEADER_PREFIX + "source_topic") != source_topic
    ):
        return None
    try:
        return (
            source_topic,
            int(h[HEADER_PREFIX + "source_partition"]),
            int(h[HEADER_PREFIX + "source_offset"]),
            h[HEADER_PREFIX + "source_sha256"],
        )
    except (KeyError, ValueError):
        return None


def _read_identities(
    reader: Any,
    dlq_topic: str,
    starts: dict[int, int],
    ends: dict[int, int],
    *,
    source_topic: str,
    consumer_group: str,
    timeout: float,
    clock: Callable[[], float],
) -> set[DlqIdentity]:
    """Read DLQ partitions from ``starts`` (inclusive) to ``ends`` (exclusive)."""
    found: set[DlqIdentity] = set()
    todo = {p for p in ends if ends[p] > starts[p]}
    if not todo:
        return found
    reader.assign([TopicPartition(dlq_topic, p, starts[p]) for p in sorted(todo)])
    positions = {p: starts[p] for p in todo}
    deadline = clock() + timeout
    while any(positions[p] < ends[p] for p in todo) and clock() < deadline:
        msg = reader.poll(0.5)
        if msg is None or msg.error() is not None:
            _advance_from_position(reader, dlq_topic, positions)
            continue
        positions[msg.partition()] = msg.offset() + 1
        identity = identity_from_headers(msg.headers(), source_topic, consumer_group)
        if identity is not None:
            found.add(identity)
    if any(positions[p] < ends[p] for p in todo):
        raise DlqError(f"could not read the DLQ {dlq_topic} within {timeout}s")
    return found


def _advance_from_position(reader: Any, dlq_topic: str, positions: dict[int, int]) -> None:
    """Advance over offsets that are never delivered as messages (transaction control
    records, compacted gaps): the reader's position is past them although no message
    was returned. Without this such a gap would stall the read until the timeout."""
    position = getattr(reader, "position", None)
    if position is None:
        return
    for tp in position([TopicPartition(dlq_topic, p) for p in positions]):
        if tp.offset >= 0 and tp.partition in positions:
            positions[tp.partition] = max(positions[tp.partition], tp.offset)


def _watermarks(reader: Any, dlq_topic: str, timeout: float) -> dict[int, tuple[int, int]]:
    metadata = reader.list_topics(dlq_topic, timeout=timeout)
    found = metadata.topics.get(dlq_topic)
    if found is None or getattr(found, "error", None) is not None or not found.partitions:
        # Never report "nothing dead-lettered" for a DLQ that cannot be read.
        raise DlqError(f"DLQ topic {dlq_topic} is missing or unavailable")
    partitions = list(found.partitions)
    return {
        p: reader.get_watermark_offsets(TopicPartition(dlq_topic, p), timeout=timeout)
        for p in partitions
    }


def recent_dead_letters(
    reader: Any,
    dlq_topic: str,
    *,
    source_topic: str,
    consumer_group: str,
    window: int,
    timeout: float = 15.0,
    clock: Callable[[], float] = time.monotonic,
) -> set[DlqIdentity]:
    """Identities in the last ``window`` messages of each DLQ partition.

    Superseded by :class:`DlqIndex` (which reads the whole retained DLQ and refreshes
    incrementally); kept for callers that only need a bounded tail.
    """
    marks = _watermarks(reader, dlq_topic, timeout)
    starts = {p: max(low, high - window) for p, (low, high) in marks.items()}
    ends = {p: high for p, (_, high) in marks.items()}
    return _read_identities(
        reader,
        dlq_topic,
        starts,
        ends,
        source_topic=source_topic,
        consumer_group=consumer_group,
        timeout=timeout,
        clock=clock,
    )


def _read_assigned_identities(
    reader: Any,
    dlq_topic: str,
    positions: dict[int, int],
    ends: dict[int, int],
    *,
    source_topic: str,
    consumer_group: str,
    timeout: float,
    clock: Callable[[], float],
) -> set[DlqIdentity]:
    """Read to a watermark snapshot using an already-assigned reader.

    Positions are local to one refresh. The caller commits them to the index only
    after the complete snapshot is read; on failure it can seek back to the saved
    starting positions and retry without losing identities from a partial scan.
    """
    found: set[DlqIdentity] = set()
    todo = {p for p in ends if positions[p] < ends[p]}
    if not todo:
        return found
    deadline = clock() + timeout
    while any(positions[p] < ends[p] for p in todo) and clock() < deadline:
        msg = reader.poll(0.1)
        if msg is None or msg.error() is not None:
            _advance_from_position(reader, dlq_topic, positions)
            continue
        partition = msg.partition()
        positions[partition] = max(positions[partition], msg.offset() + 1)
        identity = identity_from_headers(msg.headers(), source_topic, consumer_group)
        if identity is not None:
            found.add(identity)
    if any(positions[p] < ends[p] for p in todo):
        raise DlqError(f"could not read the DLQ {dlq_topic} within {timeout}s")
    return found


class DlqIndex:
    """Persistent DLQ de-duplication index, backed by the DLQ topic itself.

    The first :meth:`refresh` reads every retained DLQ message (from each partition's
    low watermark); later refreshes read only messages appended since the previous one.
    The reader stays assigned between refreshes, so incremental reads continue from its
    existing positions instead of reassigning every batch. If retention advances a low
    watermark, the reader seeks forward to the new low. The consumer refreshes once
    before the first DLQ publish attempt in a batch, and again before each retry after a
    failed send, so partial deliveries are found without a redundant pre-send scan.

    Completeness: a DLQ message is produced after its source record, so with equal
    retention on both topics (today both use the broker default) a source record that
    can still be (re)delivered normally still has its DLQ entry; segment-based deletion
    can expire a DLQ entry slightly earlier (a duplicate on replay, never a loss).
    No extra storage or schema is needed. ``reader`` is a Kafka consumer used only
    with ``assign``: it joins no group and commits nothing.
    """

    def __init__(
        self,
        reader: Any,
        dlq_topic: str,
        *,
        source_topic: str,
        consumer_group: str,
        timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._reader = reader
        self._topic = dlq_topic
        self._source_topic = source_topic
        self._group = consumer_group
        self._timeout = timeout
        self._clock = clock
        self._positions: dict[int, int] = {}
        self._assigned_partitions: set[int] = set()
        self._identities: set[DlqIdentity] = set()
        self.messages_read = 0

    def refresh(self) -> int:
        """Read DLQ messages appended since the last refresh; return new identities."""
        marks = _watermarks(self._reader, self._topic, self._timeout)
        partitions = set(marks)

        # Assign once and keep the reader on these partitions between refreshes. If
        # partitions are added/removed, reassign at the last confirmed positions.
        if partitions != self._assigned_partitions:
            starts = {p: max(low, self._positions.get(p, low)) for p, (low, _) in marks.items()}
            self._reader.assign(
                [TopicPartition(self._topic, p, starts[p]) for p in sorted(partitions)]
            )
            self._assigned_partitions = partitions
        else:
            starts = {}
            for p, (low, _) in marks.items():
                current = self._positions.get(p, low)
                starts[p] = max(low, current)
                # Retention can move the low watermark beyond the reader's position.
                if starts[p] > current:
                    self._reader.seek(TopicPartition(self._topic, p, starts[p]))

        ends = {p: high for p, (_, high) in marks.items()}
        positions = dict(starts)
        try:
            found = _read_assigned_identities(
                self._reader,
                self._topic,
                positions,
                ends,
                source_topic=self._source_topic,
                consumer_group=self._group,
                timeout=self._timeout,
                clock=self._clock,
            )
        except Exception:
            # A timed-out/failed scan may have advanced the Kafka reader without
            # completing the index update. Rewind to the last confirmed positions so
            # the next retry sees every record from this snapshot again.
            try:
                for p, offset in starts.items():
                    self._reader.seek(TopicPartition(self._topic, p, offset))
            except Exception:
                # If a seek fails, force a clean assign on the next refresh.
                self._assigned_partitions.clear()
            raise

        self.messages_read += sum(max(0, ends[p] - starts[p]) for p in ends)
        self._positions.update({p: max(positions[p], ends[p]) for p in ends})
        before = len(self._identities)
        self._identities |= found
        return len(self._identities) - before

    def __contains__(self, identity: object) -> bool:
        return identity in self._identities

    def __len__(self) -> int:
        return len(self._identities)

    def update(self, identities: Iterable[DlqIdentity]) -> None:
        """Record identities this process has just dead-lettered (confirmed delivery)."""
        self._identities.update(identities)

    def close(self) -> None:
        self._reader.close()


class DlqWriter:
    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        *,
        flush_timeout: float = 30.0,
        queue_full_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout
        self._queue_full_timeout = queue_full_timeout
        self._clock = clock

    @property
    def topic(self) -> str:
        return self._topic

    def send(self, records: Sequence[InvalidRecord], consumer_group: str) -> None:
        if not records:
            return
        failures: list[str] = []
        delivered = 0

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            nonlocal delivered
            if err is None:
                delivered += 1
            else:
                failures.append(f"{err.name()}: {err.str()}")

        failed_at = datetime.now(UTC)
        for invalid in records:
            self._produce(
                value=invalid.record.value,
                key=invalid.record.key,
                headers=dlq_headers(invalid, consumer_group, failed_at),
                on_delivery=on_delivery,
            )
        remaining = self._producer.flush(self._flush_timeout)
        if failures or remaining or delivered != len(records):
            raise DlqError(
                f"DLQ delivery not confirmed for {len(records) - delivered} of {len(records)} "
                f"records ({remaining} still queued; errors: {failures[:3]})"
            )

    def _produce(self, **kwargs: Any) -> None:
        deadline: float | None = None
        while True:
            try:
                self._producer.produce(self._topic, **kwargs)
                return
            except BufferError:
                now = self._clock()
                deadline = now + self._queue_full_timeout if deadline is None else deadline
                if now >= deadline:
                    raise DlqError("DLQ producer queue stayed full") from None
                self._producer.poll(0.1)

    def close(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
