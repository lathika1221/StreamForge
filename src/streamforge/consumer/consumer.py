"""Kafka -> Snowflake RAW consumer with dead-letter routing (M5).

Per batch, strictly in this order:

1. ``consume`` up to ``batch_size`` messages from the events topic;
2. classify each against the event contract (``records.classify``);
3. publish invalid records to the DLQ topic and wait for confirmed delivery;
4. load valid records into Snowflake RAW in one transaction (duplicate-safe);
5. commit the next offsets **synchronously**, only after 3 and 4 succeeded.

Steps 3-5 each get bounded retries with exponential backoff. If a step still fails,
nothing is committed and ``ProcessingError`` is raised: on restart the batch is read
again from the last committed offsets. RAW loading is idempotent per Kafka offset; DLQ
publishing is at-least-once (headers carry the source coordinates for deduplication).

Auto-commit is disabled. Graceful shutdown (``stop()``, Ctrl+C, SIGTERM) finishes the
current batch, then closes the consumer without committing anything unprocessed.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, TypeVar

from confluent_kafka import KafkaError, KafkaException, TopicPartition

from streamforge.common.config import KafkaSettings
from streamforge.consumer.dlq import DlqIdentity, dlq_identity
from streamforge.consumer.records import (
    InvalidRecord,
    KafkaRecord,
    ValidRecord,
    classify,
    safe_error_text,
)
from streamforge.consumer.snowflake_sink import BatchInfo, SinkResult

log = logging.getLogger(__name__)
T = TypeVar("T")

OFFSET_RESET_CHOICES = ("error", "earliest", "latest")


class ConsumerLike(Protocol):
    def subscribe(self, topics: list[str], **kwargs: Any) -> None: ...
    def consume(self, num_messages: int = ..., timeout: float = ...) -> list[Any]: ...
    def commit(
        self, offsets: list[TopicPartition] | None = ..., asynchronous: bool = ...
    ) -> Any: ...
    def close(self) -> None: ...


class SinkLike(Protocol):
    def write_batch(self, records: Sequence[ValidRecord], batch: BatchInfo) -> SinkResult: ...


class DlqLike(Protocol):
    def send(self, records: Sequence[InvalidRecord], consumer_group: str) -> None: ...


class ProcessingError(RuntimeError):
    """A batch could not be fully processed; its offsets were not committed."""


class ConsumerConfigError(RuntimeError):
    """Kafka reported a configuration-level problem (e.g. no committed offsets)."""


def build_consumer_config(settings: KafkaSettings, offset_reset: str = "error") -> dict[str, Any]:
    """librdkafka consumer settings. ``offset_reset="error"`` refuses to guess a start
    position when the group has no committed offset (set one explicitly first)."""
    if offset_reset not in OFFSET_RESET_CHOICES:
        raise ValueError(f"offset_reset must be one of {OFFSET_RESET_CHOICES}")
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.consumer_group,
        "client.id": "streamforge-consumer",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": offset_reset,
        "isolation.level": "read_committed",
        "max.poll.interval.ms": 300_000,
    }


def next_offsets(records: Sequence[KafkaRecord]) -> list[TopicPartition]:
    """Offsets to commit: last processed offset + 1, per topic-partition."""
    last: dict[tuple[str, int], int] = {}
    for r in records:
        key = (r.topic, r.partition)
        last[key] = max(last.get(key, -1), r.offset)
    return [TopicPartition(t, p, o + 1) for (t, p), o in sorted(last.items())]


def _coords(invalid: InvalidRecord) -> DlqIdentity:
    """Coordinates + content fingerprint: a different record at a reused offset never
    matches, so it is dead-lettered instead of being skipped."""
    return dlq_identity(invalid.record)


def offset_ranges(records: Sequence[KafkaRecord]) -> list[dict[str, int]]:
    ranges: dict[int, list[int]] = {}
    for r in records:
        lo_hi = ranges.setdefault(r.partition, [r.offset, r.offset])
        lo_hi[0], lo_hi[1] = min(lo_hi[0], r.offset), max(lo_hi[1], r.offset)
    return [
        {"partition": p, "first_offset": lo, "last_offset": hi}
        for p, (lo, hi) in sorted(ranges.items())
    ]


@dataclass
class ConsumerStats:
    batches: int = 0
    consumed: int = 0
    valid: int = 0
    rows_inserted: int = 0
    duplicates_skipped: int = 0
    dlq: int = 0  # newly dead-lettered
    dlq_already_present: int = 0  # invalid records skipped because already in the DLQ
    commits: int = 0  # successful synchronous offset commits
    retries: dict[str, int] = field(default_factory=dict)  # step -> failed attempts retried
    rebalances: int = 0  # partition assignments received
    committed: dict[str, int] = field(default_factory=dict)  # "topic[partition]" -> offset

    def as_dict(self) -> dict[str, Any]:
        return {
            "batches": self.batches,
            "consumed": self.consumed,
            "valid": self.valid,
            "rows_inserted": self.rows_inserted,
            "duplicates_skipped": self.duplicates_skipped,
            "dlq": self.dlq,
            "dlq_already_present": self.dlq_already_present,
            "commits": self.commits,
            "retries": dict(self.retries),
            "rebalances": self.rebalances,
            "committed": dict(self.committed),
        }


class DlqIndexLike(Protocol):
    def refresh(self) -> int: ...
    def __contains__(self, identity: object) -> bool: ...
    def update(self, identities: Any) -> None: ...


class EventConsumer:
    def __init__(
        self,
        consumer: ConsumerLike,
        sink: SinkLike,
        dlq: DlqLike,
        *,
        topic: str,
        consumer_group: str,
        batch_size: int = 500,
        batch_timeout: float = 5.0,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        batch_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
        dead_lettered: set[DlqIdentity] | None = None,
        dlq_index: DlqIndexLike | None = None,
    ) -> None:
        """De-duplication of the DLQ step: ``dlq_index`` (normally a ``dlq.DlqIndex``)
        is refreshed before every DLQ publish, so records already in the DLQ - from any
        earlier attempt, process or partition owner - are not dead-lettered again.
        ``dead_lettered`` is a static alternative used when no index is given."""
        if batch_size < 1 or max_attempts < 1:
            raise ValueError("batch_size and max_attempts must be >= 1")
        if dlq_index is not None and dead_lettered:
            dlq_index.update(dead_lettered)
        self._dlq_index = dlq_index
        self._dead_lettered: DlqIndexLike | set[DlqIdentity] = (
            dlq_index if dlq_index is not None else set(dead_lettered or ())
        )
        self._consumer = consumer
        self._sink = sink
        self._dlq = dlq
        self._topic = topic
        self._group = consumer_group
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds
        self._sleep = sleep
        self._clock = clock
        self._new_batch_id = batch_id_factory
        self._stopping = False
        self.stats = ConsumerStats()

    def stop(self) -> None:
        """Request a graceful stop after the current batch."""
        self._stopping = True

    @property
    def stopping(self) -> bool:
        return self._stopping

    def add_dead_lettered(self, coordinates: set[DlqIdentity]) -> None:
        """Register source coordinates already present in the DLQ (startup scan)."""
        self._dead_lettered.update(coordinates)

    # --------------------------------------------------------------- loop

    def run(self, *, max_batches: int | None = None, idle_timeout: float | None = None) -> None:
        """Process batches until ``stop()``, ``max_batches`` or ``idle_timeout`` seconds
        without messages. Raises ``ProcessingError`` / ``ConsumerConfigError``."""
        self._consumer.subscribe(
            [self._topic], on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        idle_since = self._clock()
        while not self._stopping:
            messages = self._consumer.consume(
                num_messages=self._batch_size, timeout=self._batch_timeout
            )
            records = self._records(messages)
            if not records:
                if idle_timeout is not None and self._clock() - idle_since >= idle_timeout:
                    log.info("idle for %.0fs: stopping", idle_timeout)
                    return
                continue
            self.process_batch(records)
            idle_since = self._clock()
            if max_batches is not None and self.stats.batches >= max_batches:
                return

    # ------------------------------------------------------------ rebalance

    def _on_assign(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        # Committed offsets are used as-is (the next consume() starts there). The DLQ
        # index is refreshed before every DLQ publish, so DLQ entries written by a
        # previous owner of these partitions are seen without extra work here.
        self.stats.rebalances += 1
        log.info("partitions assigned: %s", sorted(p.partition for p in partitions))

    def _on_revoke(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        # Batches are processed and committed synchronously between consume() calls,
        # and callbacks only run inside consume(): nothing is in flight here.
        log.info("partitions revoked: %s", sorted(p.partition for p in partitions))

    def _records(self, messages: Sequence[Any]) -> list[KafkaRecord]:
        records = []
        for msg in messages:
            err = msg.error()
            if err is None:
                records.append(KafkaRecord.from_message(msg))
            elif err.code() == KafkaError._PARTITION_EOF:
                continue
            elif err.code() == KafkaError._AUTO_OFFSET_RESET:
                raise ConsumerConfigError(
                    f"consumer group {self._group!r} has no committed offset for "
                    f"{msg.topic()}[{msg.partition()}]; set a starting boundary first "
                    "(see docs/consumer.md) or pass --offset-reset earliest|latest"
                )
            elif err.fatal():
                raise KafkaException(err)
            else:
                log.warning("consumer error (will retry): %s", err.str())
        return records

    # -------------------------------------------------------------- batch

    def process_batch(self, records: Sequence[KafkaRecord]) -> None:
        started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        classified = [classify(r) for r in records]
        valid = [c for c in classified if isinstance(c, ValidRecord)]
        invalid = [c for c in classified if isinstance(c, InvalidRecord)]
        batch = BatchInfo(
            batch_id=self._new_batch_id(),
            consumer_group=self._group,
            topic=self._topic,
            offset_ranges=offset_ranges(records),
            records_consumed=len(records),
            dlq_records=len(invalid),
            started_at=started_at,
        )

        # Skip records already dead-lettered (earlier attempt of this batch, in this
        # process, before a restart or by a previous partition owner): the index is
        # refreshed from the DLQ topic first, so the check covers the whole retained DLQ.
        new_dlq = 0
        if invalid:
            if self._dlq_index is not None:
                self._retry("DLQ index refresh", self._dlq_index.refresh)
            new_dlq = sum(1 for i in invalid if _coords(i) not in self._dead_lettered)
            if new_dlq:
                self._retry("DLQ publish", lambda: self._publish_missing(invalid))
        result = SinkResult(0, 0)
        if valid:
            result = self._retry("Snowflake RAW load", lambda: self._sink.write_batch(valid, batch))
            if result.rows_inserted + result.duplicates_skipped != len(valid):
                raise ProcessingError(
                    f"RAW load accounted for {result.rows_inserted + result.duplicates_skipped} "
                    f"of {len(valid)} valid records; offsets NOT committed"
                )
        # Every consumed record must be accounted for before its offset is committed:
        # valid (persisted) + invalid (now confirmed in, or already in, the DLQ).
        unaccounted = [i for i in invalid if _coords(i) not in self._dead_lettered]
        if len(valid) + len(invalid) != len(records) or unaccounted:
            raise ProcessingError(
                f"{len(unaccounted)} invalid record(s) not confirmed in the DLQ; "
                "offsets NOT committed"
            )
        offsets = next_offsets(records)
        self._retry(
            "offset commit", lambda: self._consumer.commit(offsets=offsets, asynchronous=False)
        )

        s = self.stats
        s.commits += 1
        s.batches += 1
        s.consumed += len(records)
        s.valid += len(valid)
        s.dlq += new_dlq
        s.dlq_already_present += len(invalid) - new_dlq
        s.rows_inserted += result.rows_inserted
        s.duplicates_skipped += result.duplicates_skipped
        for tp in offsets:
            s.committed[f"{tp.topic}[{tp.partition}]"] = tp.offset
        log.info(
            "batch %s: consumed=%d valid=%d inserted=%d duplicates=%d dlq_new=%d "
            "dlq_already_present=%d committed=%s",
            batch.batch_id,
            len(records),
            len(valid),
            result.rows_inserted,
            result.duplicates_skipped,
            new_dlq,
            len(invalid) - new_dlq,
            {f"{tp.partition}": tp.offset for tp in offsets},
        )

    def _publish_missing(self, invalid: Sequence[InvalidRecord]) -> None:
        """Publish the invalid records that are not yet in the DLQ (one retry attempt).

        Each attempt re-reads the DLQ first (when an index is used), so records that a
        failed attempt did deliver - partial delivery, or a delivery timeout after the
        broker had already stored the message - are found and not published twice.
        """
        if self._dlq_index is not None:
            self._dlq_index.refresh()
        pending = [i for i in invalid if _coords(i) not in self._dead_lettered]
        if pending:
            self._dlq.send(pending, self._group)
            self._dead_lettered.update(_coords(i) for i in pending)

    def _retry(self, what: str, fn: Callable[[], T]) -> T:
        for attempt in range(1, self._max_attempts + 1):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                detail = safe_error_text(exc)  # never log raw error text (may echo values)
                if attempt == self._max_attempts:
                    raise ProcessingError(
                        f"{what} failed after {attempt} attempts; offsets NOT committed: {detail}"
                    ) from None
                delay = self._backoff * 2 ** (attempt - 1)
                self.stats.retries[what] = self.stats.retries.get(what, 0) + 1
                log.warning(
                    "%s failed (attempt %d/%d), retrying in %.1fs: %s",
                    what,
                    attempt,
                    self._max_attempts,
                    delay,
                    detail,
                )
                self._sleep(delay)
        raise AssertionError("unreachable")
