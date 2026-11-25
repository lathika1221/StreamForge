"""Unit tests for the Snowflake RAW sink, with a fake connection (no Snowflake)."""

from datetime import UTC, datetime

import pytest

from streamforge.consumer.records import KafkaRecord, ValidRecord, classify
from streamforge.consumer.snowflake_sink import (
    BUFFER_COLUMNS,
    EVENT_TABLES,
    BatchInfo,
    RawEventSink,
    SinkError,
    buffer_row,
    merge_sql,
)
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

START = datetime(2026, 11, 23, 10, 0, tzinfo=UTC)
BATCH = BatchInfo(
    "b1",
    "grp",
    "ecommerce.events",
    [{"partition": 0, "first_offset": 0, "last_offset": 9}],
    10,
    1,
    "2026-11-23T10:00:00Z",
)


@pytest.fixture(scope="module")
def valid_records():
    sim = EventSimulator(SimulatorConfig(seed=9, start_time=START))
    out = []
    for i, r in enumerate(sim.iter_records(10)):
        rec = KafkaRecord(
            "ecommerce.events",
            i % 3,
            i,
            r.data["order_id"].encode(),
            r.value.encode(),
            1_795_000_000_000 + i,
        )
        out.append(classify(rec))
    assert all(isinstance(v, ValidRecord) for v in out)
    return out


class FakeCursor:
    def __init__(self, conn):
        self.conn, self.description, self._rows = conn, None, []

    def execute(self, sql, params=None):
        sql_flat = " ".join(sql.split())
        self.conn.log.append(sql_flat)
        for needle in self.conn.fail_on:
            if needle in sql_flat:
                raise RuntimeError(f"boom on {needle}")
        if sql_flat.startswith("SELECT CURRENT_ROLE()"):
            self.description, self._rows = [("ROLE",)], [(self.conn.role,)]
        elif sql_flat.startswith("MERGE"):
            self.description, self._rows = [("number of rows inserted",)], [(self.conn.merge_rows,)]
        else:
            self.description, self._rows = None, []

    def executemany(self, sql, rows):
        self.conn.log.append("EXECUTEMANY")
        self.conn.buffered.append(list(rows))

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, role="STREAMFORGE_ROLE", fail_on=(), merge_rows=2):
        self.role, self.fail_on, self.merge_rows = role, list(fail_on), merge_rows
        self.log, self.buffered, self.closed = [], [], False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def _sink(conn, **kw):
    conns = []

    def factory():
        conns.append(conn)
        return conn

    return RawEventSink(factory, database="ECOMMERCE_STREAMING_DB", **kw), conns


def test_ensure_ready_creates_tables_and_buffer_once():
    conn = FakeConn()
    sink, conns = _sink(conn)
    sink.ensure_ready()
    sink.ensure_ready()
    assert len(conns) == 1
    creates = [s for s in conn.log if s.startswith("CREATE")]
    assert len(creates) == 6
    for event_type in EVENT_TABLES:
        assert any(f"IF NOT EXISTS {sink.table(event_type)} " in s for s in creates)
    assert any("ECOMMERCE_STREAMING_DB.OPS.INGESTION_BATCHES" in s for s in creates)
    assert any(s.startswith("CREATE TEMPORARY TABLE IF NOT EXISTS") for s in creates)
    assert not any("OR REPLACE" in s or s.startswith("DROP") for s in conn.log)


@pytest.mark.parametrize("role", ["ACCOUNTADMIN", "SYSADMIN", "SECURITYADMIN"])
def test_admin_roles_are_refused(role):
    conn = FakeConn(role=role)
    sink, _ = _sink(conn)
    with pytest.raises(SinkError, match="refusing to ingest"):
        sink.ensure_ready()
    assert conn.closed and not any(s.startswith("CREATE") for s in conn.log)


def test_write_batch_is_one_transaction(valid_records):
    conn = FakeConn(merge_rows=3)
    sink, _ = _sink(conn, chunk_size=4)
    result = sink.write_batch(valid_records, BATCH)
    log = [s for s in conn.log if not s.startswith(("CREATE", "SELECT CURRENT_ROLE"))]
    types = sorted({v.event.event_type for v in valid_records})
    assert log[0] == "BEGIN" and log[-1] == "COMMIT"
    assert log[1].startswith("DELETE FROM ECOMMERCE_STREAMING_DB.RAW.STREAMFORGE_INGEST_BUFFER")
    assert log.count("EXECUTEMANY") == -(-len(valid_records) // 4)  # chunks of 4 rows
    merges = [s for s in log if s.startswith("MERGE")]
    assert len(merges) == len(types)
    assert any(
        s.startswith("INSERT INTO ECOMMERCE_STREAMING_DB.OPS.INGESTION_BATCHES") for s in log
    )
    assert result.rows_inserted == 3 * len(types)
    assert result.duplicates_skipped == len(valid_records) - result.rows_inserted
    assert sum(len(c) for c in conn.buffered) == len(valid_records)


def test_failure_rolls_back_and_raises(valid_records):
    conn = FakeConn(fail_on=["MERGE"])
    sink, _ = _sink(conn)
    with pytest.raises(SinkError, match="batch b1"):
        sink.write_batch(valid_records, BATCH)
    assert "ROLLBACK" in conn.log and "COMMIT" not in conn.log


def test_broken_session_is_reset_for_reconnect(valid_records):
    conn = FakeConn(fail_on=["MERGE", "ROLLBACK"])
    sink, conns = _sink(conn)
    with pytest.raises(SinkError):
        sink.write_batch(valid_records, BATCH)
    assert conn.closed
    conn.fail_on = []
    sink.write_batch(valid_records, BATCH)  # reconnects and recreates the buffer
    assert len(conns) == 2


def test_empty_batch_does_not_connect():
    conn = FakeConn()
    sink, conns = _sink(conn)
    assert sink.write_batch([], BATCH).rows_inserted == 0 and conns == []


def test_buffer_row_preserves_raw_text_and_coordinates(valid_records):
    v = valid_records[4]
    row = dict(zip(BUFFER_COLUMNS, buffer_row(v, "b1"), strict=True))
    assert row["RAW_VALUE"].encode("utf-8") == v.record.value
    assert row["EVENT_ID"] == str(v.event.event_id) and row["ORDER_ID"] == v.event.order_id
    assert (row["KAFKA_TOPIC"], row["KAFKA_PARTITION"], row["KAFKA_OFFSET"]) == (
        "ecommerce.events",
        1,
        4,
    )
    assert row["KAFKA_KEY"] == v.event.order_id and row["KAFKA_TIMESTAMP_MS"] == 1_795_000_000_004
    assert row["EVENT_TIMESTAMP"].endswith("+00:00") and row["BATCH_ID"] == "b1"


def test_merge_is_keyed_on_kafka_coordinates():
    sql = " ".join(merge_sql("DB.RAW.ORDER_CREATED", "order_created", "DB.RAW.BUF").split())
    assert (
        "ON t.KAFKA_TOPIC = s.KAFKA_TOPIC AND t.KAFKA_PARTITION = s.KAFKA_PARTITION "
        "AND t.KAFKA_OFFSET = s.KAFKA_OFFSET"
    ) in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql and "WHEN MATCHED" not in sql
    assert "PARSE_JSON(s.RAW_VALUE):payload" in sql and "EVENT_TYPE = 'order_created'" in sql
    with pytest.raises(ValueError):
        merge_sql("T", "order_refunded'; DROP TABLE X; --", "B")


def test_table_prefix_and_identifier_validation():
    sink, _ = _sink(FakeConn(), table_prefix="IT_ABC_")
    assert sink.table("order_created") == "ECOMMERCE_STREAMING_DB.RAW.IT_ABC_ORDER_CREATED"
    assert sink.audit_table == "ECOMMERCE_STREAMING_DB.OPS.IT_ABC_INGESTION_BATCHES"
    with pytest.raises(ValueError):
        RawEventSink(lambda: None, database="DB; DROP", table_prefix="")
    with pytest.raises(ValueError):
        RawEventSink(lambda: None, database="DB", table_prefix="x-y")
