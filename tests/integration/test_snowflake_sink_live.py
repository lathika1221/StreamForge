"""RAW sink against the real Snowflake account (M5), as STREAMFORGE_ROLE.

Writes to uniquely prefixed tables (``RAW.IT_<hex>_*`` / ``OPS.IT_<hex>_*``) that are
dropped afterwards, so the real RAW tables are never touched. Resumes STREAMFORGE_WH
for a few seconds (about 0.017 credits, the 60 s minimum).

Run with: python -m pytest -m snowflake
"""

import logging
import uuid
from datetime import UTC, datetime

import pytest

from streamforge.admin.snowflake_setup import DATABASE, ROLE, Session
from streamforge.common.config import load_snowflake_settings
from streamforge.common.snowflake import _secrets, connect
from streamforge.consumer.records import KafkaRecord, ValidRecord, classify
from streamforge.consumer.snowflake_sink import BatchInfo, RawEventSink
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

pytestmark = pytest.mark.snowflake


@pytest.fixture(scope="module")
def settings():
    s = load_snowflake_settings()
    assert s.role == ROLE and s.database == DATABASE
    return s


@pytest.fixture(scope="module")
def sink(settings):
    prefix = f"IT_{uuid.uuid4().hex[:8].upper()}_"
    raw_sink = RawEventSink(lambda: connect(settings), database=DATABASE, table_prefix=prefix)
    yield raw_sink
    raw_sink.close()
    cleanup = Session(connect(settings), _secrets(settings))
    try:
        for table in raw_sink.all_tables():  # only the tables this test created
            cleanup.run(f"DROP TABLE IF EXISTS {table}")
    finally:
        cleanup.conn.close()


@pytest.fixture(scope="module")
def records():
    sim = EventSimulator(SimulatorConfig(start_time=datetime(2026, 11, 23, tzinfo=UTC)))
    out = []
    for i, r in enumerate(sim.iter_records(4)):
        rec = KafkaRecord(
            "it.topic",
            i % 3,
            1000 + i,
            r.data["order_id"].encode(),
            r.value.encode(),
            1_795_000_000_000 + i,
        )
        out.append(classify(rec))
    assert all(isinstance(v, ValidRecord) for v in out)
    return out


def _batch(records, batch_id):
    return BatchInfo(
        batch_id,
        "it-group",
        "it.topic",
        [{"partition": 0, "first_offset": 1000, "last_offset": 1000 + len(records)}],
        len(records),
        0,
        datetime.now(UTC).isoformat(),
    )


def test_load_is_duplicate_safe_and_traceable(settings, sink, records):
    first = sink.write_batch(records, _batch(records, "it-batch-1"))
    assert first.rows_inserted == len(records) and first.duplicates_skipped == 0

    again = sink.write_batch(records, _batch(records, "it-batch-2"))
    assert again.rows_inserted == 0 and again.duplicates_skipped == len(records)

    s = Session(connect(settings), _secrets(settings))
    try:
        s.run("USE WAREHOUSE STREAMFORGE_WH")
        total = 0
        for event_type in {r.event.event_type for r in records}:
            (row,) = s.rows(
                f"SELECT COUNT(*) AS n, COUNT(DISTINCT KAFKA_OFFSET) AS d "
                f"FROM {sink.table(event_type)}"
            )
            assert row["n"] == row["d"]
            total += row["n"]
        assert total == len(records)

        sample = records[0]
        (row,) = s.rows(
            f"SELECT EVENT_ID, RAW_VALUE, PAYLOAD:customer_id::STRING AS customer, KAFKA_TOPIC, "
            f"KAFKA_PARTITION, KAFKA_OFFSET, KAFKA_KEY, BATCH_ID, INGESTED_AT, EVENT_TIMESTAMP "
            f"FROM {sink.table(sample.event.event_type)} WHERE KAFKA_OFFSET = 1000"
        )
        assert row["raw_value"].encode() == sample.record.value
        assert row["event_id"] == str(sample.event.event_id)
        assert row["customer"] == sample.event.payload.customer_id
        assert (row["kafka_topic"], row["kafka_partition"], row["kafka_offset"]) == (
            "it.topic",
            0,
            1000,
        )
        assert row["kafka_key"] == sample.event.order_id and row["batch_id"] == "it-batch-1"
        assert row["ingested_at"] is not None
        assert row["event_timestamp"] == sample.event.event_timestamp

        audit = s.rows(
            f"SELECT BATCH_ID, ROWS_INSERTED, DUPLICATES_SKIPPED FROM {sink.audit_table} "
            "ORDER BY LOADED_AT"
        )
        assert [(a["batch_id"], a["rows_inserted"], a["duplicates_skipped"]) for a in audit] == [
            ("it-batch-1", len(records), 0),
            ("it-batch-2", 0, len(records)),
        ]
        s.fails("ALTER WAREHOUSE STREAMFORGE_WH SUSPEND")  # tolerate "already suspended"
    finally:
        s.conn.close()


def test_connector_logs_never_contain_row_values(settings, sink, caplog):
    """A real RAW load with every Snowflake connector logger at DEBUG: no message text,
    customer id or order id may appear in any log record (query-text logging is
    pinned off; client-side binding would otherwise put row values into SQL text)."""
    sim = EventSimulator(SimulatorConfig(start_time=datetime(2026, 11, 24, tzinfo=UTC)))
    fresh = []
    for i, r in enumerate(sim.iter_records(2)):
        rec = KafkaRecord(
            "it.topic", 0, 5000 + i, r.data["order_id"].encode(), r.value.encode(), None
        )
        fresh.append(classify(rec))
    caplog.set_level(logging.DEBUG, logger="snowflake")
    caplog.set_level(logging.DEBUG, logger="streamforge")

    result = sink.write_batch(fresh, _batch(fresh, "it-batch-logs"))
    assert result.rows_inserted == len(fresh)

    logged = "\n".join(rec.getMessage() for rec in caplog.records)
    for v in fresh:
        assert v.record.value.decode() not in logged
        assert v.event.order_id not in logged
        if v.event.event_type == "order_created":
            assert v.event.payload.customer_id not in logged
    passphrase = settings.private_key_passphrase or ""
    assert not passphrase or passphrase not in logged

    s = Session(connect(settings), _secrets(settings))
    try:
        s.fails("ALTER WAREHOUSE STREAMFORGE_WH SUSPEND")
    finally:
        s.conn.close()
