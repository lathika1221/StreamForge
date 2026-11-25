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
from collections.abc import Callable, Sequence
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
    """Identities (coordinates + fingerprint) already dead-lettered by ``consumer_group``,
    read from the last ``window`` messages of each DLQ partition.

    Used at startup so a batch that failed after its DLQ step (and is therefore
    redelivered) is not dead-lettered twice. ``reader`` is a Kafka consumer used only
    with ``assign`` (it joins no group and commits nothing). Since one consumer
    instance writes the DLQ sequentially and a batch holds at most ``batch_size``
    records, a window of at least ``batch_size`` covers the uncommitted batch.
    Entries without a fingerprint header are ignored (never used to skip a record).
    """
    metadata = reader.list_topics(dlq_topic, timeout=timeout)
    partitions = list(metadata.topics[dlq_topic].partitions)
    assignments, ends = [], {}
    for p in partitions:
        low, high = reader.get_watermark_offsets(TopicPartition(dlq_topic, p), timeout=timeout)
        if high > low:
            ends[p] = high
            assignments.append(TopicPartition(dlq_topic, p, max(low, high - window)))
    found: set[DlqIdentity] = set()
    if not assignments:
        return found
    reader.assign(assignments)
    positions = {tp.partition: tp.offset for tp in assignments}
    deadline = clock() + timeout
    while any(positions[p] < ends[p] for p in ends) and clock() < deadline:
        msg = reader.poll(0.5)
        if msg is None or msg.error() is not None:
            continue
        positions[msg.partition()] = msg.offset() + 1
        h = {k: (v or b"").decode("utf-8", "replace") for k, v in (msg.headers() or [])}
        if (
            h.get(HEADER_PREFIX + "consumer_group") == consumer_group
            and h.get(HEADER_PREFIX + "source_topic") == source_topic
        ):
            try:
                found.add(
                    (
                        source_topic,
                        int(h[HEADER_PREFIX + "source_partition"]),
                        int(h[HEADER_PREFIX + "source_offset"]),
                        h[HEADER_PREFIX + "source_sha256"],
                    )
                )
            except (KeyError, ValueError):
                continue  # incomplete identity: never used to skip a record
    if any(positions[p] < ends[p] for p in ends):
        raise DlqError(f"could not read the DLQ tail of {dlq_topic} within {timeout}s")
    return found


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
