"""M9.3 local pipeline demo: file-backed RAW sink and safety checks (no broker needed)."""

import json
from datetime import UTC, datetime

import pytest

from streamforge.common.config import load_kafka_settings
from streamforge.consumer.records import KafkaRecord, ValidRecord, classify
from streamforge.consumer.snowflake_sink import BatchInfo
from streamforge.demo import pipeline
from streamforge.demo.file_sink import AUDIT_FILE, FileRawSink
from streamforge.producer.simulator import EventSimulator, SimulatorConfig


def valid_records(n_orders=3, topic="t"):
    cfg = SimulatorConfig(seed=11, start_time=datetime(2026, 11, 20, tzinfo=UTC))
    out = []
    for offset, rec in enumerate(r for r in EventSimulator(cfg).iter_records(n_orders)):
        result = classify(KafkaRecord(topic, offset % 3, offset, None, rec.value.encode(), 0))
        assert isinstance(result, ValidRecord)
        out.append(result)
    return out


def batch(batch_id="b1", n=0):
    return BatchInfo(batch_id, "g", "t", [], n, 0, "2026-11-20T00:00:00+00:00")


def test_file_sink_writes_one_file_per_event_type_with_unchanged_bytes(tmp_path):
    records = valid_records()
    result = FileRawSink(tmp_path).write_batch(records, batch(n=len(records)))
    assert (result.rows_inserted, result.duplicates_skipped) == (len(records), 0)
    rows = [
        json.loads(line)
        for path in tmp_path.glob("raw_*.jsonl")
        for line in path.read_text("utf-8").splitlines()
    ]
    assert len(rows) == len(records)
    by_coords = {(r["partition"], r["offset"]): r for r in rows}
    for v in records:
        row = by_coords[(v.record.partition, v.record.offset)]
        assert row["raw_value"].encode("utf-8") == v.record.value  # bytes stored unchanged
        assert row["event_type"] == v.event.event_type
        assert tmp_path.joinpath(f"raw_{row['event_type']}.jsonl").exists()


def test_file_sink_is_idempotent_on_kafka_coordinates_across_restarts(tmp_path):
    records = valid_records()
    FileRawSink(tmp_path).write_batch(records[:5], batch("b1"))
    # redelivery after a "crash": a new sink instance reads what is already stored
    again = FileRawSink(tmp_path).write_batch(records, batch("b2"))
    assert (again.rows_inserted, again.duplicates_skipped) == (len(records) - 5, 5)
    # the same coordinates twice within one batch count once
    dup = FileRawSink(tmp_path).write_batch(records[:2] + records[:2], batch("b3"))
    assert (dup.rows_inserted, dup.duplicates_skipped) == (0, 4)
    assert sum(FileRawSink(tmp_path).rows().values()) == len(records)
    audit = [json.loads(x) for x in (tmp_path / AUDIT_FILE).read_text("utf-8").splitlines()]
    assert [a["batch_id"] for a in audit] == ["b1", "b2", "b3"]
    assert all(a["valid_records"] == a["rows_inserted"] + a["duplicates_skipped"] for a in audit)


def test_demo_never_uses_the_real_topics_or_group():
    real = load_kafka_settings({})
    s = pipeline.demo_settings(real, "abc123")
    assert s.events_topic == "streamforge.demo.abc123"
    assert s.dlq_topic == "streamforge.demo.abc123.dlq"
    assert s.consumer_group == "streamforge-demo-abc123"
    assert {s.events_topic, s.dlq_topic}.isdisjoint(pipeline.PROTECTED_TOPICS)
    assert s.bootstrap_servers == real.bootstrap_servers


def test_unreachable_broker_fails_fast_with_a_hint(capsys):
    code = pipeline.main(
        ["--output-dir", "unused"],
        environ={"KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1"},
        connect_timeout=1.0,
    )
    assert code == 2
    assert "docker compose -f infra/docker-compose.yml up -d --wait" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv", [["--orders", "0"], ["--invalid-rate", "1.5"], ["--batch-size", "0"]]
)
def test_invalid_arguments_are_rejected(argv):
    assert pipeline.main(argv, environ={}) == 2
