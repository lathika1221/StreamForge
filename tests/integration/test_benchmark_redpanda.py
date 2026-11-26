"""M9.4: a small benchmark run end to end on Redpanda (temporary topics, no Snowflake)."""

import json
import os

import pytest
from confluent_kafka.admin import AdminClient

from streamforge.benchmark import run as bench
from streamforge.common.config import load_kafka_settings

pytestmark = pytest.mark.integration


def bench_topics(bootstrap):
    names = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics
    return {t for t in names if t.startswith(bench.TOPIC_PREFIX)}


def test_small_benchmark_run_reports_valid_results_and_cleans_up(tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)  # keep the CI summary clean
    environ = dict(os.environ)
    bootstrap = load_kafka_settings(environ).bootstrap_servers
    before = bench_topics(bootstrap)
    out = tmp_path / "bench.json"

    code = bench.main(
        ["--events", "300", "--batch-sizes", "50", "--repeats", "1", "--json", str(out)],
        environ=environ,
    )
    assert code == 0

    data = json.loads(out.read_text("utf-8"))
    (row,) = data["results"]
    assert row["events"] == 300 and row["valid"] + row["invalid"] == 300
    assert row["producer_events_per_second"] > 0
    (consumer,) = row["consumer"]
    assert consumer["batch_size"] == 50 and consumer["events_per_second"] > 0
    assert consumer["batches"] >= 300 // 50
    assert data["environment"]["librdkafka"]
    assert bench_topics(bootstrap) == before  # temporary topics deleted
