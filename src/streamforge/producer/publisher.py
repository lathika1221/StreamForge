"""Kafka producer for simulated StreamForge events (M3).

Publishes ``SimulatedRecord``s from ``streamforge.producer.simulator`` to the events
topic (``KAFKA_EVENTS_TOPIC``). The DLQ topic is never written here; it is reserved
for consumer-side handling (M5).

* **Value**: ``SimulatedRecord.value`` encoded as UTF-8. That string is already the
  canonical JSON (``to_json_line``) for valid events, and the raw text for injected
  invalid records. It is published as-is, never re-serialized, parsed or "fixed".
* **Key**: ``order_id`` when it is a well-formed order id, otherwise ``None`` (null
  key). A fallback key is never invented; a missing key never blocks publishing.
* **Delivery**: asynchronous ``produce`` with a delivery callback. A message counts as
  delivered only when its callback reports success. A full local queue is drained by
  polling and retried (bounded by ``queue_full_timeout``), never silently dropped.
  ``flush`` waits for outstanding callbacks; anything still unconfirmed is "pending"
  and treated as a failure.

CLI (see ``docs/producer.md``):

    python -m streamforge.producer --orders 100 --seed 42 --invalid-rate 0.05
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from streamforge.common.config import ConfigError, KafkaSettings, load_kafka_settings
from streamforge.common.events import ORDER_ID_PATTERN
from streamforge.producer.simulator import (
    EventSimulator,
    SimulatedRecord,
    add_simulator_arguments,
    simulator_config_from_args,
)

_ORDER_ID = re.compile(ORDER_ID_PATTERN)
MAX_FAILURE_SAMPLES = 10


class ProducerLike(Protocol):
    """The subset of ``confluent_kafka.Producer`` used here (lets tests use a fake)."""

    def produce(self, topic: str, value: Any = ..., key: Any = ..., **kwargs: Any) -> None: ...
    def poll(self, timeout: float = ...) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...
    def list_topics(self, topic: str | None = ..., timeout: float = ...) -> Any: ...


class PublishError(RuntimeError):
    """The producer cannot publish (broker unreachable, topic missing, queue stuck)."""


class QueueFullError(PublishError):
    """The local producer queue stayed full for longer than ``queue_full_timeout``."""


# ------------------------------------------------------------ key / value


def message_key(record: SimulatedRecord) -> bytes | None:
    """``order_id`` as UTF-8 bytes if present and well-formed, else ``None`` (null key)."""
    if record.data is None:  # e.g. malformed_json: not a JSON object at all
        return None
    order_id = record.data.get("order_id")
    if isinstance(order_id, str) and _ORDER_ID.fullmatch(order_id):
        return order_id.encode("utf-8")
    return None


def message_value(record: SimulatedRecord) -> bytes:
    """The record's canonical JSON text, unchanged, as UTF-8 bytes."""
    return record.value.encode("utf-8")


# ----------------------------------------------------------------- config


def build_producer_config(settings: KafkaSettings) -> dict[str, Any]:
    """librdkafka settings for reliable, ordered, duplicate-free (per session) publishing."""
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": "streamforge-producer",
        "acks": "all",  # wait for all in-sync replicas
        "enable.idempotence": True,  # no duplicates/reordering from internal retries
        "partitioner": "murmur2_random",  # Java-compatible key hashing; null key -> random
        "linger.ms": 5,
        "delivery.timeout.ms": 30_000,  # give up (and report failure) after 30 s
    }


def ensure_topic_exists(producer: ProducerLike, topic: str, timeout: float = 10.0) -> int:
    """Fail fast with a clear error if the broker is unreachable or the topic is missing.

    Returns the topic's partition count. Topic auto-creation is disabled on the local
    broker, so a missing topic would otherwise only surface as delivery failures.
    """
    try:
        metadata = producer.list_topics(topic=topic, timeout=timeout)
    except KafkaException as exc:
        raise PublishError(f"cannot reach the Kafka broker: {exc}") from exc
    found = metadata.topics.get(topic)
    if found is None or found.error is not None or not found.partitions:
        raise PublishError(
            f"topic {topic!r} does not exist. Start the local stack with "
            "`docker compose -f infra/docker-compose.yml up -d --wait`."
        )
    return len(found.partitions)


# -------------------------------------------------------------- publishing


@dataclass
class DeliveryStats:
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    null_keys: int = 0
    errors: Counter[str] = field(default_factory=Counter)
    failure_samples: list[str] = field(default_factory=list)

    @property
    def pending(self) -> int:
        """Attempted but not yet confirmed either way."""
        return self.attempted - self.delivered - self.failed

    @property
    def ok(self) -> bool:
        return self.failed == 0 and self.pending == 0

    def record_failure(self, error_name: str, detail: str) -> None:
        self.failed += 1
        self.errors[error_name] += 1
        if len(self.failure_samples) < MAX_FAILURE_SAMPLES:
            self.failure_samples.append(f"{error_name}: {detail}")


class EventPublisher:
    """Publishes records to one topic and accounts for every delivery outcome."""

    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        *,
        queue_full_timeout: float = 30.0,
        queue_full_poll_interval: float = 0.1,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._queue_full_timeout = queue_full_timeout
        self._poll_interval = queue_full_poll_interval
        self._clock = clock
        self.stats = DeliveryStats()

    @property
    def topic(self) -> str:
        return self._topic

    def publish(self, record: SimulatedRecord) -> None:
        """Enqueue one record. Raises ``QueueFullError`` if the queue never drains."""
        key, value = message_key(record), message_value(record)
        self.stats.attempted += 1
        if key is None:
            self.stats.null_keys += 1
        deadline: float | None = None
        while True:
            try:
                self._producer.produce(
                    self._topic, value=value, key=key, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # Local queue full: serve delivery callbacks to free space, then retry.
                now = self._clock()
                if deadline is None:
                    deadline = now + self._queue_full_timeout
                if now >= deadline:
                    self.stats.record_failure(
                        "QUEUE_FULL_TIMEOUT",
                        f"key={key!r}: local queue still full after {self._queue_full_timeout}s",
                    )
                    raise QueueFullError(
                        f"producer queue full for {self._queue_full_timeout}s; is the broker up?"
                    ) from None
                self._producer.poll(self._poll_interval)
            except KafkaException as exc:  # rejected locally, e.g. message too large
                error = exc.args[0] if exc.args else None
                name = error.name() if isinstance(error, KafkaError) else type(exc).__name__
                self.stats.record_failure(name, f"key={key!r}: {exc}")
                return
        self._producer.poll(0)  # serve any callbacks that are already due

    def publish_all(self, records: Iterable[SimulatedRecord]) -> None:
        for record in records:
            self.publish(record)

    def flush(self, timeout: float) -> int:
        """Wait up to ``timeout`` s for outstanding deliveries; returns how many remain."""
        return self._producer.flush(timeout)

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is None:
            self.stats.delivered += 1
        else:
            self.stats.record_failure(
                err.name(), f"key={msg.key()!r} topic={msg.topic()}: {err.str()}"
            )


# --------------------------------------------------------------------- CLI


def _summary(
    stats: DeliveryStats, topic: str, partitions: int, bootstrap: str, valid: int, invalid: int
) -> str:
    lines = [
        "StreamForge producer summary",
        f"  topic:      {topic} ({partitions} partitions) @ {bootstrap}",
        f"  attempted:  {stats.attempted} "
        f"(valid {valid}, invalid {invalid}, null key {stats.null_keys})",
        f"  delivered:  {stats.delivered}",
        f"  failed:     {stats.failed}",
        f"  pending:    {stats.pending} (unconfirmed when flush ended)",
    ]
    if stats.errors:
        lines.append("  errors:     " + ", ".join(f"{k}={v}" for k, v in stats.errors.items()))
    return "\n".join(lines)


def main(
    argv: Sequence[str] | None = None,
    producer_factory: Callable[[dict[str, Any]], ProducerLike] = Producer,
) -> int:
    """Exit codes: 0 all delivered, 1 any failure or unconfirmed, 2 usage/config, 130 Ctrl+C."""
    parser = argparse.ArgumentParser(
        prog="python -m streamforge.producer",
        description="Publish simulated StreamForge events to Redpanda/Kafka.",
    )
    add_simulator_arguments(parser)
    parser.add_argument(
        "--flush-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for outstanding deliveries at the end (default 30)",
    )
    args = parser.parse_args(argv)
    sim_config = simulator_config_from_args(parser, args)
    if args.flush_timeout <= 0:
        parser.error("--flush-timeout must be greater than 0")

    try:
        settings = load_kafka_settings()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    producer = producer_factory(build_producer_config(settings))
    try:
        partitions = ensure_topic_exists(producer, settings.events_topic)
    except PublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    publisher = EventPublisher(producer, settings.events_topic)
    valid = invalid = 0
    exit_code = 0
    try:
        for record in EventSimulator(sim_config).iter_records(args.orders):
            publisher.publish(record)
            if record.is_valid:
                valid += 1
            else:
                invalid += 1
    except QueueFullError as exc:
        print(f"error: {exc} Stopped publishing.", file=sys.stderr)
        exit_code = 1
    except KeyboardInterrupt:
        print("interrupted: flushing messages already queued...", file=sys.stderr)
        exit_code = 130
    finally:
        publisher.flush(args.flush_timeout)

    stats = publisher.stats
    print(_summary(stats, publisher.topic, partitions, settings.bootstrap_servers, valid, invalid))
    for sample in stats.failure_samples:
        print(f"  failure: {sample}", file=sys.stderr)
    if exit_code:
        return exit_code
    return 0 if stats.ok else 1
