"""End-to-end local pipeline demo without Snowflake (M9.3).

    python -m streamforge.demo [--orders 20] [--seed 42] [--invalid-rate 0.1]
                               [--output-dir DIR] [--keep-topics]

Runs the real pipeline code against the local Redpanda broker:

1. creates two **temporary** topics (``streamforge.demo.<id>`` with 3 partitions and its
   ``.dlq``) and a temporary consumer group - never ``ecommerce.events``, its DLQ or the
   ``streamforge-consumer`` group;
2. publishes simulated events with the real producer (``EventPublisher``);
3. consumes them with the real ``EventConsumer``: invalid records go to the temporary
   DLQ through ``DlqWriter`` and the ``DlqIndex``; valid ones are written by
   ``FileRawSink`` to JSON Lines files instead of Snowflake RAW; offsets are committed
   per batch exactly as in production;
4. prints an accounting summary, then deletes the temporary topics and group (unless
   ``--keep-topics``).

It needs only Docker/Redpanda: no Snowflake account, credentials or credits.
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streamforge.common.config import ConfigError, KafkaSettings, load_kafka_settings
from streamforge.consumer.consumer import EventConsumer, ProcessingError, build_consumer_config
from streamforge.consumer.dlq import DlqIndex, DlqWriter
from streamforge.demo.file_sink import FileRawSink
from streamforge.producer.publisher import EventPublisher, build_producer_config
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

TOPIC_PREFIX = "streamforge.demo."
GROUP_PREFIX = "streamforge-demo-"
PROTECTED_TOPICS = frozenset({"ecommerce.events", "ecommerce.events.dlq"})
PROTECTED_GROUPS = frozenset({"streamforge-consumer"})


def demo_settings(base: KafkaSettings, run_id: str) -> KafkaSettings:
    """Temporary topic/group names for one demo run; never the real ones."""
    settings = dataclasses.replace(
        base,
        events_topic=f"{TOPIC_PREFIX}{run_id}",
        dlq_topic=f"{TOPIC_PREFIX}{run_id}.dlq",
        consumer_group=f"{GROUP_PREFIX}{run_id}",
    )
    names = {settings.events_topic, settings.dlq_topic}
    if names & PROTECTED_TOPICS or settings.consumer_group in PROTECTED_GROUPS:
        raise AssertionError("the demo must never use the real topics or consumer group")
    assert all(n.startswith(TOPIC_PREFIX) for n in names)
    assert settings.consumer_group.startswith(GROUP_PREFIX)
    return settings


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m streamforge.demo",
        description="Run producer -> Redpanda -> consumer locally (temporary topics, "
        "file-backed RAW, no Snowflake).",
    )
    p.add_argument("--orders", type=int, default=20, help="simulated orders (default 20)")
    p.add_argument("--seed", type=int, default=42, help="simulator seed (default 42)")
    p.add_argument(
        "--invalid-rate", type=float, default=0.1, help="share of extra invalid records (0..1)"
    )
    p.add_argument("--batch-size", type=int, default=25, help="consumer batch size")
    p.add_argument(
        "--output-dir", type=Path, default=None, help="where RAW files go (default: a new temp dir)"
    )
    p.add_argument("--keep-topics", action="store_true", help="keep the temporary topics and group")
    return p


def _end_offsets(client: Consumer, topic: str) -> dict[int, int]:
    parts = client.list_topics(topic, timeout=10).topics[topic].partitions
    return {p: client.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}


def main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
    connect_timeout: float = 10.0,
) -> int:
    args = _parser().parse_args(argv)
    if args.orders < 1 or not 0 <= args.invalid_rate <= 1 or args.batch_size < 1:
        print(
            "error: --orders and --batch-size must be >= 1, --invalid-rate in 0..1", file=sys.stderr
        )
        return 2
    try:
        base = load_kafka_settings(environ)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    run_id = uuid.uuid4().hex[:10]
    settings = demo_settings(base, run_id)
    out_dir = args.output_dir or Path(tempfile.mkdtemp(prefix="streamforge-demo-"))

    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    try:
        admin.list_topics(timeout=connect_timeout)
    except KafkaException:
        print(
            f"error: no Kafka broker reachable at {settings.bootstrap_servers}. Start Redpanda "
            "first: docker compose -f infra/docker-compose.yml up -d --wait",
            file=sys.stderr,
        )
        return 2

    futures = admin.create_topics(
        [
            NewTopic(settings.events_topic, num_partitions=3, replication_factor=1),
            NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1),
        ]
    )
    for f in futures.values():
        f.result(15)
    print(f"Demo run {run_id}: temporary topics {settings.events_topic} (+ .dlq)")

    code = 1
    try:
        code = _run(settings, args, out_dir)
    finally:
        if args.keep_topics:
            print(
                f"Kept topics {settings.events_topic}, {settings.dlq_topic} "
                f"and group {settings.consumer_group}"
            )
        else:
            for f in admin.delete_topics([settings.events_topic, settings.dlq_topic]).values():
                f.result(15)
            for f in admin.delete_consumer_groups([settings.consumer_group]).values():
                try:
                    f.result(15)
                except KafkaException:
                    pass  # group already gone
            print("Temporary topics and consumer group deleted.")
    return code


def _run(settings: KafkaSettings, args: argparse.Namespace, out_dir: Path) -> int:
    # 1. publish
    sim = SimulatorConfig(seed=args.seed, invalid_rate=args.invalid_rate)
    records = list(EventSimulator(sim).iter_records(args.orders))
    publisher = EventPublisher(Producer(build_producer_config(settings)), settings.events_topic)
    publisher.publish_all(records)
    publisher.flush(30)
    if not publisher.stats.ok:
        print("error: not every message was confirmed by the broker", file=sys.stderr)
        return 1
    valid = sum(r.is_valid for r in records)
    invalid = len(records) - valid

    # 2. consume (the production EventConsumer; RAW goes to files instead of Snowflake)
    sink = FileRawSink(out_dir)
    consumer = Consumer(build_consumer_config(settings, offset_reset="earliest"))
    dlq_producer = Producer({**build_producer_config(settings), "client.id": "streamforge-demo"})
    reader = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"{GROUP_PREFIX}{uuid.uuid4().hex[:8]}-dlq-index",  # never committed
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    index = DlqIndex(
        reader,
        settings.dlq_topic,
        source_topic=settings.events_topic,
        consumer_group=settings.consumer_group,
    )
    ec = EventConsumer(
        consumer,
        sink,
        DlqWriter(dlq_producer, settings.dlq_topic),
        topic=settings.events_topic,
        consumer_group=settings.consumer_group,
        batch_size=args.batch_size,
        batch_timeout=1.0,
        dlq_index=index,
    )
    error = None
    try:
        index.refresh()
        ec.run(idle_timeout=5.0)
    except ProcessingError as exc:
        error = exc
    finally:
        consumer.close()
        dlq_producer.flush(10)
        reader.close()

    # 3. summary
    probe = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
        }
    )
    try:
        ends = _end_offsets(probe, settings.events_topic)
        dlq_end = sum(_end_offsets(probe, settings.dlq_topic).values())
        committed = {
            tp.partition: tp.offset
            for tp in probe.committed(
                [TopicPartition(settings.events_topic, p) for p in ends], timeout=10
            )
        }
    finally:
        probe.close()
    rows = sink.rows()
    s = ec.stats
    all_committed = all(committed.get(p, -1) == end for p, end in ends.items())
    ok = (
        error is None
        and s.consumed == len(records)
        and sum(rows.values()) == valid
        and dlq_end == invalid
        and all_committed
    )
    print(
        "\n".join(
            [
                "",
                "StreamForge local pipeline demo (no Snowflake)",
                f"  published:          {len(records)} ({valid} valid, {invalid} invalid)",
                f"  consumed:           {s.consumed} in {s.batches} batch(es)",
                f"  RAW rows (files):   {sum(rows.values())} "
                + ", ".join(f"{k}={v}" for k, v in rows.items()),
                f"  dead-lettered:      {dlq_end} (DLQ topic {settings.dlq_topic})",
                f"  duplicates skipped: {s.duplicates_skipped}",
                "  committed offsets:  "
                + ", ".join(f"p{p}={committed.get(p)}/{end}" for p, end in sorted(ends.items())),
                f"  RAW files:          {out_dir}",
                f"  result:             {'OK - every record accounted for' if ok else 'MISMATCH'}",
            ]
        )
    )
    if error is not None:
        print(f"error: {error}", file=sys.stderr)
    return 0 if ok else 1
