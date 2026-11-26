"""M9.4 Kafka-path benchmark: helpers, safety and reporting (no broker needed)."""

import pytest

from streamforge.benchmark import run as bench
from streamforge.common.config import load_kafka_settings
from streamforge.consumer.records import KafkaRecord, ValidRecord, classify
from streamforge.consumer.snowflake_sink import BatchInfo, SinkResult


def test_generate_is_exact_and_deterministic():
    a = bench.generate(500, 0.05, seed=7)
    b = bench.generate(500, 0.05, seed=7)
    assert len(a) == 500
    assert [r.value for r in a] == [r.value for r in b]
    assert 0 < sum(not r.is_valid for r in a) < 500  # invalid records are included


def test_summary_and_percentile():
    assert bench.summarize([3.0, 1.0, 2.0]) == {"median": 2.0, "min": 1.0, "max": 3.0}
    values = list(range(1, 101))
    assert bench.percentile(values, 0.5) in (50, 51)
    assert bench.percentile(values, 0.95) == 95
    assert bench.percentile([4.0], 0.95) == 4.0


def test_memory_sink_is_idempotent_on_coordinates():
    rec = next(r for r in bench.generate(5, 0, seed=1) if r.is_valid)
    v = classify(KafkaRecord("t", 0, 7, None, rec.value.encode(), 0))
    assert isinstance(v, ValidRecord)
    sink = bench.MemorySink()
    info = BatchInfo("b", "g", "t", [], 1, 0, "2026-11-20T00:00:00+00:00")
    assert sink.write_batch([v], info) == SinkResult(1, 0)
    assert sink.write_batch([v, v], info) == SinkResult(0, 2)


def test_benchmark_never_uses_the_real_topics_or_group():
    s = bench.bench_settings(load_kafka_settings({}), "abc")
    assert s.events_topic == "streamforge.bench.abc" and s.dlq_topic.endswith(".dlq")
    assert s.consumer_group == "streamforge-bench-abc"
    assert {s.events_topic, s.dlq_topic}.isdisjoint(bench.PROTECTED_TOPICS)


def test_report_shows_median_min_max_and_the_caveats():
    results = [
        {
            "events": 2000,
            "repeat": i,
            "producer_events_per_second": p,
            "consumer": [{"batch_size": 100, "events_per_second": c, "batch_ms_p50": 10.0 + i}],
        }
        for i, (p, c) in enumerate([(1000.0, 500.0), (3000.0, 700.0), (2000.0, 600.0)], 1)
    ]
    env = {"platform": "TestOS", "cpu_count": 4, "python": "3.11", "librdkafka": "2.x"}
    table = bench.report(results, env)
    assert "| 2,000 | Producer, events/s | 2,000 | 1,000 | 3,000 |" in table
    assert "| 2,000 | Consumer (batch 100), events/s | 600 | 500 | 700 |" in table
    assert "3 run(s) per size" in table and "no Snowflake" in table


@pytest.mark.parametrize(
    "argv",
    [["--events", "0"], ["--batch-sizes", "0"], ["--repeats", "0"], ["--invalid-rate", "2"]],
)
def test_invalid_arguments_are_rejected(argv):
    assert bench.main(argv, environ={}) == 2


def test_unreachable_broker_fails_fast_with_a_hint(capsys):
    code = bench.main([], environ={"KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1"}, connect_timeout=1)
    assert code == 2
    assert "docker compose -f infra/docker-compose.yml up -d --wait" in capsys.readouterr().err
