"""CLI: ``python -m streamforge.consumer`` (see docs/consumer.md)."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer

from streamforge.common.config import ConfigError, load_kafka_settings, load_snowflake_settings
from streamforge.common.snowflake import SnowflakeConnectionError, connect
from streamforge.consumer.consumer import (
    OFFSET_RESET_CHOICES,
    ConsumerConfigError,
    EventConsumer,
    ProcessingError,
    build_consumer_config,
)
from streamforge.consumer.dlq import DlqError, DlqWriter, recent_dead_letters
from streamforge.consumer.snowflake_sink import FORBIDDEN_ROLES, RawEventSink, SinkError
from streamforge.producer.publisher import build_producer_config

log = logging.getLogger("streamforge.consumer")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m streamforge.consumer",
        description="Consume ecommerce events into Snowflake RAW; route invalid ones to the DLQ.",
    )
    p.add_argument("--batch-size", type=int, default=500, help="max messages per batch")
    p.add_argument("--batch-timeout", type=float, default=5.0, help="seconds to fill a batch")
    p.add_argument(
        "--stop-when-idle",
        type=float,
        default=None,
        metavar="SECONDS",
        help="exit after this many seconds without messages (default: run until stopped)",
    )
    p.add_argument("--max-batches", type=int, default=None)
    p.add_argument(
        "--offset-reset",
        choices=OFFSET_RESET_CHOICES,
        default="error",
        help="when the group has no committed offset: error (default), earliest, latest",
    )
    p.add_argument("--max-attempts", type=int, default=3, help="attempts per step before stopping")
    p.add_argument("--log-level", default="INFO")
    return p


def _summary(ec: EventConsumer) -> str:
    s = ec.stats
    committed = ", ".join(f"{k}={v}" for k, v in sorted(s.committed.items())) or "none"
    return "\n".join(
        [
            "StreamForge consumer summary",
            f"  batches:             {s.batches}",
            f"  consumed:            {s.consumed} (valid {s.valid}, dlq {s.dlq}, "
            f"already in dlq {s.dlq_already_present})",
            f"  RAW rows inserted:   {s.rows_inserted}",
            f"  duplicates skipped:  {s.duplicates_skipped}",
            f"  committed offsets:   {committed}",
        ]
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
    producer_factory: Callable[[dict[str, Any]], Any] = Producer,
    sink_factory: Callable[[], Any] | None = None,
    dlq_reader_factory: Callable[[dict[str, Any]], Any] = Consumer,
) -> int:
    """Exit codes: 0 ok, 1 processing/infrastructure error, 2 usage/config, 130 interrupted."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.batch_size < 1 or args.max_attempts < 1 or args.batch_timeout <= 0:
        parser.error("--batch-size/--max-attempts must be >= 1 and --batch-timeout > 0")
    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # The connector's INFO logs are not needed and must never carry query text.
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

    try:
        kafka = load_kafka_settings()
        snowflake = load_snowflake_settings()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if (snowflake.role or "").upper() in FORBIDDEN_ROLES or not snowflake.role:
        print(
            "error: SNOWFLAKE_ROLE must be the application role (STREAMFORGE_ROLE), "
            f"not {snowflake.role or '<user default>'}",
            file=sys.stderr,
        )
        return 2

    consumer = consumer_factory(build_consumer_config(kafka, args.offset_reset))
    producer = producer_factory({**build_producer_config(kafka), "client.id": "streamforge-dlq"})
    dlq = DlqWriter(producer, kafka.dlq_topic)
    sink = (
        sink_factory()
        if sink_factory
        else RawEventSink(lambda: connect(snowflake), database=snowflake.database)
    )
    ec = EventConsumer(
        consumer,
        sink,
        dlq,
        topic=kafka.events_topic,
        consumer_group=kafka.consumer_group,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout,
        max_attempts=args.max_attempts,
    )

    interrupted = False

    def _graceful(signum: int, _frame: Any) -> None:
        nonlocal interrupted
        interrupted = True
        log.warning("signal %s received: finishing the current batch, then stopping", signum)
        ec.stop()

    previous = {s: signal.signal(s, _graceful) for s in (signal.SIGINT, signal.SIGTERM)}
    code = 0
    try:
        metadata = consumer.list_topics(timeout=10)
        missing = [
            t
            for t in (kafka.events_topic, kafka.dlq_topic)
            if t not in metadata.topics or metadata.topics[t].error is not None
        ]
        if missing:
            print(
                f"error: topic(s) {missing} do not exist; start the local stack first "
                "(docs/redpanda.md)",
                file=sys.stderr,
            )
            return 1
        reader = dlq_reader_factory(
            {
                "bootstrap.servers": kafka.bootstrap_servers,
                "group.id": f"streamforge-dlq-scan-{uuid.uuid4().hex[:8]}",  # never committed
                "client.id": "streamforge-dlq-scan",
                "enable.auto.commit": False,
            }
        )
        try:
            already = recent_dead_letters(
                reader,
                kafka.dlq_topic,
                source_topic=kafka.events_topic,
                consumer_group=kafka.consumer_group,
                window=2 * args.batch_size,
            )
        finally:
            reader.close()
        ec.add_dead_lettered(already)
        log.info("DLQ tail scan: %d record(s) already dead-lettered by this group", len(already))
        if hasattr(sink, "ensure_ready"):
            sink.ensure_ready()  # fail fast: connection, role guard, tables
        log.info(
            "consuming %s as group %s (DLQ %s)",
            kafka.events_topic,
            kafka.consumer_group,
            kafka.dlq_topic,
        )
        ec.run(max_batches=args.max_batches, idle_timeout=args.stop_when_idle)
    except (
        ProcessingError,
        ConsumerConfigError,
        SinkError,
        SnowflakeConnectionError,
        KafkaException,
        DlqError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        code = 1
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)
        consumer.close()  # leaves the group; commits nothing (auto-commit is off)
        dlq.close()
        if hasattr(sink, "close"):
            sink.close()
    print(_summary(ec))
    if interrupted and code == 0:
        return 130
    return code
