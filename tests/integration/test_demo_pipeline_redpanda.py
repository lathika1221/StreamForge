"""M9.3: the local pipeline demo end to end against Redpanda (temporary topics, no Snowflake)."""

import json
import os

import pytest
from confluent_kafka.admin import AdminClient

from streamforge.common.config import load_kafka_settings
from streamforge.demo import pipeline
from streamforge.demo.file_sink import AUDIT_FILE

pytestmark = pytest.mark.integration


def demo_topics(bootstrap):
    names = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics
    return {t for t in names if t.startswith(pipeline.TOPIC_PREFIX)}


def test_demo_accounts_for_every_record_and_cleans_up(tmp_path, capsys):
    environ = dict(os.environ)
    bootstrap = load_kafka_settings(environ).bootstrap_servers
    before = demo_topics(bootstrap)

    code = pipeline.main(
        [
            "--orders",
            "10",
            "--invalid-rate",
            "0.3",
            "--batch-size",
            "7",
            "--output-dir",
            str(tmp_path),
        ],
        environ=environ,
    )
    out = capsys.readouterr().out
    assert code == 0, out
    assert "OK - every record accounted for" in out

    raw = [
        json.loads(line)
        for path in tmp_path.glob("raw_*.jsonl")
        for line in path.read_text("utf-8").splitlines()
    ]
    assert raw and len({(r["partition"], r["offset"]) for r in raw}) == len(raw)
    audit = [json.loads(x) for x in (tmp_path / AUDIT_FILE).read_text("utf-8").splitlines()]
    assert sum(a["rows_inserted"] for a in audit) == len(raw)
    assert all(a["topic"].startswith(pipeline.TOPIC_PREFIX) for a in audit)
    assert demo_topics(bootstrap) == before  # temporary topics deleted
