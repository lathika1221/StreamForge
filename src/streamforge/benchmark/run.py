"""Local Kafka-path benchmark (M9.4). No Snowflake: RAW writes go to an in-memory sink.

    python -m streamforge.benchmark [--events 2000 10000] [--batch-sizes 100 500]
                                    [--repeats 3] [--invalid-rate 0.05] [--json FILE]

For every workload size and repeat it:

1. pre-generates the events with the deterministic simulator (generation is not timed);
2. creates **temporary** topics ``streamforge.bench.<id>`` (3 partitions) and ``.dlq``
   (never ``ecommerce.events``, its DLQ or ``streamforge-consumer``);
3. **producer**: publishes all events with the real ``EventPublisher`` (``acks=all``,
   idempotent) and times publish + final flush until every delivery is confirmed;
4. **consumer**, once per batch size with a fresh consumer group: the real
   ``EventConsumer`` (validation, ``DlqWriter`` + ``DlqIndex``, synchronous per-partition
   offset commits) with :class:`MemorySink` instead of Snowflake RAW. Timed from the
   start of the first batch to the end of the last batch's commit (group join excluded);
5. checks that every record is accounted for (RAW rows = valid, DLQ = invalid) - a run
   that loses or duplicates records is reported as invalid, not as a result;
6. deletes the temporary topics and groups.

What it does NOT measure: Snowflake (one MERGE transaction per batch, likely the real
bottleneck in production), dbt, network latency to a remote broker, or multi-broker
replication. Results describe this code on one machine with a single local broker.
"""

from __future__ import annotations

import argparse
import dataclasses
import itertools
import json
import os
import platform
import statistics
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import confluent_kafka
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from streamforge.common.config import ConfigError, KafkaSettings, load_kafka_settings
from streamforge.consumer.consumer import EventConsumer, ProcessingError, build_consumer_config
from streamforge.consumer.dlq import DlqIndex, DlqWriter
from streamforge.consumer.records import ValidRecord
from streamforge.consumer.snowflake_sink import BatchInfo, SinkResult
from streamforge.producer.publisher import EventPublisher, build_producer_config
from streamforge.producer.simulator import EventSimulator, SimulatedRecord, SimulatorConfig

TOPIC_PREFIX = "streamforge.bench."
GROUP_PREFIX = "streamforge-bench-"
PROTECTED_TOPICS = frozenset({"ecommerce.events", "ecommerce.events.dlq"})
PROTECTED_GROUPS = frozenset({"streamforge-consumer"})
START = datetime(2026, 11, 20, tzinfo=UTC)


class BenchmarkError(RuntimeError):
    """The run cannot produce a valid measurement (records lost, duplicated, unconfirmed)."""


# ------------------------------------------------------------------ helpers


def bench_settings(base: KafkaSettings, run_id: str) -> KafkaSettings:
    settings = dataclasses.replace(
        base,
        events_topic=f"{TOPIC_PREFIX}{run_id}",
        dlq_topic=f"{TOPIC_PREFIX}{run_id}.dlq",
        consumer_group=f"{GROUP_PREFIX}{run_id}",
    )
    names = {settings.events_topic, settings.dlq_topic}
    if names & PROTECTED_TOPICS or settings.consumer_group in PROTECTED_GROUPS:
        raise AssertionError("the benchmark must never use the real topics or consumer group")
    return settings


def generate(events: int, invalid_rate: float, seed: int) -> list[SimulatedRecord]:
    """Exactly ``events`` simulated records (valid plus invalid), deterministic."""
    cfg = SimulatorConfig(seed=seed, start_time=START, invalid_rate=invalid_rate)
    return list(itertools.islice(EventSimulator(cfg).iter_records(None), events))


def summarize(values: Sequence[float]) -> dict[str, float]:
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def percentile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


class MemorySink:
    """In-memory stand-in for the Snowflake RAW sink: idempotent on Kafka coordinates."""

    def __init__(self) -> None:
        self.seen: set[tuple[str, int, int]] = set()

    def write_batch(self, records: Sequence[ValidRecord], batch: BatchInfo) -> SinkResult:
        before = len(self.seen)
        self.seen.update((v.record.topic, v.record.partition, v.record.offset) for v in records)
        inserted = len(self.seen) - before
        return SinkResult(inserted, len(records) - inserted)


def environment(bootstrap: str) -> dict[str, Any]:
    return {
        "timestamp_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown",
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "confluent_kafka": confluent_kafka.version()[0],
        "librdkafka": confluent_kafka.libversion()[0],
        "bootstrap_servers": bootstrap,
        "ci": bool(os.environ.get("GITHUB_ACTIONS")),
    }


# ------------------------------------------------------------------ phases


def produce(settings: KafkaSettings, records: list[SimulatedRecord]) -> float:
    publisher = EventPublisher(Producer(build_producer_config(settings)), settings.events_topic)
    start = time.perf_counter()
    publisher.publish_all(records)
    pending = publisher.flush(60)
    elapsed = time.perf_counter() - start
    if pending or not publisher.stats.ok or publisher.stats.delivered != len(records):
        raise BenchmarkError(
            f"producer: {publisher.stats.delivered}/{len(records)} confirmed, {pending} pending"
        )
    return elapsed


def consume(
    settings: KafkaSettings, expected: int, valid: int, invalid: int, batch_size: int
) -> dict[str, Any]:
    group = f"{settings.consumer_group}-b{batch_size}"
    s = dataclasses.replace(settings, consumer_group=group)
    sink = MemorySink()
    consumer = Consumer(build_consumer_config(s, offset_reset="earliest"))
    dlq_producer = Producer({**build_producer_config(s), "client.id": "streamforge-bench"})
    reader = Consumer(
        {
            "bootstrap.servers": s.bootstrap_servers,
            "group.id": f"{group}-dlq-index",  # assign only, never committed
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    index = DlqIndex(reader, s.dlq_topic, source_topic=s.events_topic, consumer_group=group)
    ec = EventConsumer(
        consumer,
        sink,
        DlqWriter(dlq_producer, s.dlq_topic),
        topic=s.events_topic,
        consumer_group=group,
        batch_size=batch_size,
        batch_timeout=1.0,
        dlq_index=index,
    )
    spans: list[tuple[float, float]] = []
    inner = ec.process_batch

    def timed(records):
        t0 = time.perf_counter()
        inner(records)
        spans.append((t0, time.perf_counter()))
        if ec.stats.consumed >= expected:
            ec.stop()

    ec.process_batch = timed
    try:
        index.refresh()
        ec.run(idle_timeout=30.0)  # safety net: stop if the topic stops delivering
    except ProcessingError as exc:
        raise BenchmarkError(f"consumer: {exc}") from None
    finally:
        consumer.close()
        dlq_producer.flush(10)
        reader.close()

    st = ec.stats
    if st.consumed != expected or len(sink.seen) != valid or st.dlq != invalid:
        raise BenchmarkError(
            f"consumer accounting failed: consumed {st.consumed}/{expected}, "
            f"RAW {len(sink.seen)}/{valid}, DLQ {st.dlq}/{invalid}"
        )
    elapsed = spans[-1][1] - spans[0][0]
    batch_ms = [(b - a) * 1000 for a, b in spans]
    return {
        "batch_size": batch_size,
        "seconds": elapsed,
        "events_per_second": expected / elapsed,
        "batches": st.batches,
        "batch_ms_p50": percentile(batch_ms, 0.5),
        "batch_ms_p95": percentile(batch_ms, 0.95),
    }


# ------------------------------------------------------------------ CLI


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m streamforge.benchmark",
        description="Benchmark producer and consumer throughput on local Redpanda "
        "(temporary topics, in-memory RAW sink, no Snowflake).",
    )
    p.add_argument("--events", type=int, nargs="+", default=[2000, 10000])
    p.add_argument("--batch-sizes", type=int, nargs="+", default=[100, 500])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--invalid-rate", type=float, default=0.05)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--json", type=Path, default=None, help="also write all results to FILE")
    return p


def _cleanup(admin: AdminClient, settings: KafkaSettings, groups: list[str]) -> None:
    for f in admin.delete_topics([settings.events_topic, settings.dlq_topic]).values():
        try:
            f.result(15)
        except KafkaException:
            pass
    if groups:
        for f in admin.delete_consumer_groups(groups).values():
            try:
                f.result(15)
            except KafkaException:
                pass


def run(base: KafkaSettings, args: argparse.Namespace, admin: AdminClient) -> list[dict[str, Any]]:
    results = []
    for events in args.events:
        records = generate(events, args.invalid_rate, args.seed)
        valid = sum(r.is_valid for r in records)
        for repeat in range(1, args.repeats + 1):
            settings = bench_settings(base, uuid.uuid4().hex[:10])
            for f in admin.create_topics(
                [
                    NewTopic(settings.events_topic, num_partitions=3, replication_factor=1),
                    NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1),
                ]
            ).values():
                f.result(15)
            groups: list[str] = []
            try:
                seconds = produce(settings, records)
                row = {
                    "events": events,
                    "valid": valid,
                    "invalid": events - valid,
                    "repeat": repeat,
                    "producer_seconds": seconds,
                    "producer_events_per_second": events / seconds,
                    "consumer": [],
                }
                for batch_size in args.batch_sizes:
                    groups.append(
                        f"{settings.consumer_group}-b{batch_size}"
                    )  # cleaned up even on failure
                    row["consumer"].append(
                        consume(settings, events, valid, events - valid, batch_size)
                    )
                results.append(row)
                print(
                    f"  {events:>6} events, run {repeat}: producer "
                    f"{row['producer_events_per_second']:,.0f} ev/s; consumer "
                    + ", ".join(
                        f"batch {c['batch_size']}: {c['events_per_second']:,.0f} ev/s"
                        for c in row["consumer"]
                    ),
                    flush=True,
                )
            finally:
                _cleanup(admin, settings, groups)
    return results


def report(results: list[dict[str, Any]], env: dict[str, Any]) -> str:
    lines = [
        "| Events | Measurement | Median | Min | Max |",
        "|---:|---|---:|---:|---:|",
    ]
    for events in sorted({r["events"] for r in results}):
        rows = [r for r in results if r["events"] == events]
        p = summarize([r["producer_events_per_second"] for r in rows])
        lines.append(
            f"| {events:,} | Producer, events/s | {p['median']:,.0f} | {p['min']:,.0f} "
            f"| {p['max']:,.0f} |"
        )
        for batch_size in sorted({c["batch_size"] for r in rows for c in r["consumer"]}):
            cs = [c for r in rows for c in r["consumer"] if c["batch_size"] == batch_size]
            t = summarize([c["events_per_second"] for c in cs])
            b = summarize([c["batch_ms_p50"] for c in cs])
            lines.append(
                f"| {events:,} | Consumer (batch {batch_size}), events/s | {t['median']:,.0f} "
                f"| {t['min']:,.0f} | {t['max']:,.0f} |"
            )
            lines.append(
                f"| {events:,} | Consumer (batch {batch_size}), median batch time, ms "
                f"| {b['median']:,.1f} | {b['min']:,.1f} | {b['max']:,.1f} |"
            )
    repeats = max(r["repeat"] for r in results)
    lines.append("")
    lines.append(
        f"{repeats} run(s) per size; {env['platform']}, {env['cpu_count']} logical CPUs, "
        f"Python {env['python']}, librdkafka {env['librdkafka']}; single local Redpanda "
        "broker; RAW sink in memory (no Snowflake)."
    )
    return "\n".join(lines)


def main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
    connect_timeout: float = 10.0,
) -> int:
    args = _parser().parse_args(argv)
    if (
        min(args.events) < 1
        or min(args.batch_sizes) < 1
        or args.repeats < 1
        or not 0 <= args.invalid_rate <= 1
    ):
        print(
            "error: --events, --batch-sizes and --repeats must be >= 1; --invalid-rate in 0..1",
            file=sys.stderr,
        )
        return 2
    try:
        base = load_kafka_settings(environ)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    admin = AdminClient({"bootstrap.servers": base.bootstrap_servers})
    try:
        admin.list_topics(timeout=connect_timeout)
    except KafkaException:
        print(
            f"error: no Kafka broker reachable at {base.bootstrap_servers}. Start Redpanda "
            "first: docker compose -f infra/docker-compose.yml up -d --wait",
            file=sys.stderr,
        )
        return 2

    env = environment(base.bootstrap_servers)
    print(
        f"StreamForge Kafka-path benchmark: events {args.events}, batch sizes "
        f"{args.batch_sizes}, {args.repeats} repeat(s), invalid rate {args.invalid_rate}",
        flush=True,
    )
    try:
        results = run(base, args, admin)
    except BenchmarkError as exc:
        print(f"error: invalid run, no results reported: {exc}", file=sys.stderr)
        return 1
    table = report(results, env)
    print("\n" + table)
    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "environment": env,
                    "arguments": vars(args) | {"json": str(args.json)},
                    "results": results,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\nAll results written to {args.json}")
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write("## StreamForge Kafka-path benchmark\n\n" + table + "\n")
    return 0
