"""Snowflake RAW sink for validated events (M5).

One append-only table per event type in ``ECOMMERCE_STREAMING_DB.RAW``:

    RAW.ORDER_CREATED  RAW.PAYMENT_PROCESSED  RAW.ORDER_CANCELLED  RAW.SHIPMENT_DISPATCHED

and a load-audit table ``OPS.INGESTION_BATCHES``. Tables are created on first use by
the application role (``CREATE TABLE IF NOT EXISTS``; no warehouse needed).

Each batch is loaded in **one transaction**: rows go into a session-temporary buffer,
then one ``MERGE`` per event type inserts rows whose Kafka coordinates
``(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)`` are not present yet, plus one audit
row. Redelivering a batch (e.g. after a crash before the Kafka offset commit) therefore
inserts nothing twice. Deduplication of *business* duplicates (same ``event_id`` at
different offsets) is deliberately left to the transformation layer (M6).

Every row keeps the exact message text (``RAW_VALUE``), the parsed ``PAYLOAD`` and the
Kafka coordinates, key and timestamp, plus ``BATCH_ID`` and ``INGESTED_AT``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from streamforge.consumer.records import ValidRecord, safe_error_text

EVENT_TABLES: dict[str, str] = {
    "order_created": "ORDER_CREATED",
    "payment_processed": "PAYMENT_PROCESSED",
    "order_cancelled": "ORDER_CANCELLED",
    "shipment_dispatched": "SHIPMENT_DISPATCHED",
}
AUDIT_TABLE = "INGESTION_BATCHES"
BUFFER_TABLE = "STREAMFORGE_INGEST_BUFFER"
# Roles that must never be used for normal ingestion.
FORBIDDEN_ROLES = frozenset({"ACCOUNTADMIN", "ORGADMIN", "SECURITYADMIN", "SYSADMIN", "USERADMIN"})
_IDENT = re.compile(r"^[A-Z_][A-Z0-9_]*$")

RAW_COLUMNS_DDL = """
    EVENT_ID          STRING        NOT NULL,
    EVENT_TYPE        STRING        NOT NULL,
    SCHEMA_VERSION    NUMBER(5,0)   NOT NULL,
    EVENT_TIMESTAMP   TIMESTAMP_TZ  NOT NULL,
    ORDER_ID          STRING        NOT NULL,
    SEQUENCE_NUMBER   NUMBER(10,0)  NOT NULL,
    SOURCE            STRING,
    PAYLOAD           VARIANT       NOT NULL,
    RAW_VALUE         STRING        NOT NULL,
    KAFKA_TOPIC       STRING        NOT NULL,
    KAFKA_PARTITION   NUMBER(10,0)  NOT NULL,
    KAFKA_OFFSET      NUMBER(20,0)  NOT NULL,
    KAFKA_KEY         STRING,
    KAFKA_TIMESTAMP   TIMESTAMP_TZ,
    BATCH_ID          STRING        NOT NULL,
    INGESTED_AT       TIMESTAMP_TZ  NOT NULL"""

AUDIT_COLUMNS_DDL = """
    BATCH_ID            STRING        NOT NULL,
    CONSUMER_GROUP      STRING        NOT NULL,
    TOPIC               STRING        NOT NULL,
    OFFSET_RANGES       VARIANT       NOT NULL,
    RECORDS_CONSUMED    NUMBER(10,0)  NOT NULL,
    VALID_RECORDS       NUMBER(10,0)  NOT NULL,
    ROWS_INSERTED       NUMBER(10,0)  NOT NULL,
    DUPLICATES_SKIPPED  NUMBER(10,0)  NOT NULL,
    DLQ_RECORDS         NUMBER(10,0)  NOT NULL,
    STARTED_AT          TIMESTAMP_TZ  NOT NULL,
    LOADED_AT           TIMESTAMP_TZ  NOT NULL"""

BUFFER_COLUMNS = (
    "EVENT_ID",
    "EVENT_TYPE",
    "SCHEMA_VERSION",
    "EVENT_TIMESTAMP",
    "ORDER_ID",
    "SEQUENCE_NUMBER",
    "SOURCE",
    "RAW_VALUE",
    "KAFKA_TOPIC",
    "KAFKA_PARTITION",
    "KAFKA_OFFSET",
    "KAFKA_KEY",
    "KAFKA_TIMESTAMP_MS",
    "BATCH_ID",
)
BUFFER_DDL = """
    EVENT_ID STRING, EVENT_TYPE STRING, SCHEMA_VERSION NUMBER(5,0), EVENT_TIMESTAMP STRING,
    ORDER_ID STRING, SEQUENCE_NUMBER NUMBER(10,0), SOURCE STRING, RAW_VALUE STRING,
    KAFKA_TOPIC STRING, KAFKA_PARTITION NUMBER(10,0), KAFKA_OFFSET NUMBER(20,0),
    KAFKA_KEY STRING, KAFKA_TIMESTAMP_MS NUMBER(20,0), BATCH_ID STRING"""


class SinkError(RuntimeError):
    """The batch could not be loaded; the transaction was rolled back."""


@dataclass(frozen=True)
class BatchInfo:
    batch_id: str
    consumer_group: str
    topic: str
    offset_ranges: list[dict[str, int]]  # [{"partition", "first_offset", "last_offset"}]
    records_consumed: int
    dlq_records: int
    started_at: str  # UTC ISO-8601


@dataclass(frozen=True)
class SinkResult:
    rows_inserted: int
    duplicates_skipped: int


def buffer_row(valid: ValidRecord, batch_id: str) -> tuple[Any, ...]:
    e, r = valid.event, valid.record
    assert r.value is not None  # valid records always have a value
    return (
        str(e.event_id),
        e.event_type,
        e.schema_version,
        e.event_timestamp.isoformat(),
        e.order_id,
        e.sequence_number,
        e.source,
        r.value.decode("utf-8"),  # valid JSON is valid UTF-8: exact original text
        r.topic,
        r.partition,
        r.offset,
        None if r.key is None else r.key.decode("utf-8", errors="replace"),
        r.timestamp_ms,
        batch_id,
    )


def merge_sql(target: str, event_type: str, buffer: str) -> str:
    """Insert buffered rows of one event type whose Kafka coordinates are new."""
    if event_type not in EVENT_TABLES:
        raise ValueError(f"unknown event_type {event_type!r}")
    return f"""MERGE INTO {target} t
USING (SELECT * FROM {buffer} WHERE EVENT_TYPE = '{event_type}') s
ON t.KAFKA_TOPIC = s.KAFKA_TOPIC AND t.KAFKA_PARTITION = s.KAFKA_PARTITION
   AND t.KAFKA_OFFSET = s.KAFKA_OFFSET
WHEN NOT MATCHED THEN INSERT (
    EVENT_ID, EVENT_TYPE, SCHEMA_VERSION, EVENT_TIMESTAMP, ORDER_ID, SEQUENCE_NUMBER, SOURCE,
    PAYLOAD, RAW_VALUE, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, KAFKA_KEY, KAFKA_TIMESTAMP,
    BATCH_ID, INGESTED_AT)
VALUES (
    s.EVENT_ID, s.EVENT_TYPE, s.SCHEMA_VERSION, TO_TIMESTAMP_TZ(s.EVENT_TIMESTAMP), s.ORDER_ID,
    s.SEQUENCE_NUMBER, s.SOURCE, PARSE_JSON(s.RAW_VALUE):payload, s.RAW_VALUE, s.KAFKA_TOPIC,
    s.KAFKA_PARTITION, s.KAFKA_OFFSET, s.KAFKA_KEY, TO_TIMESTAMP_TZ(s.KAFKA_TIMESTAMP_MS, 3),
    s.BATCH_ID, CURRENT_TIMESTAMP())"""


class RawEventSink:
    """Loads validated events into the RAW tables, duplicate-safe per Kafka offset."""

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        *,
        database: str,
        table_prefix: str = "",
        chunk_size: int = 200,
    ) -> None:
        for name in (database, table_prefix or "X"):
            if not _IDENT.fullmatch(name):
                raise ValueError(f"invalid identifier {name!r}")
        self._connect = connection_factory
        self._db = database
        self._prefix = table_prefix
        self._chunk = chunk_size
        self._conn: Any = None

    # ---------------------------------------------------------------- names

    def table(self, event_type: str) -> str:
        return f"{self._db}.RAW.{self._prefix}{EVENT_TABLES[event_type]}"

    @property
    def audit_table(self) -> str:
        return f"{self._db}.OPS.{self._prefix}{AUDIT_TABLE}"

    @property
    def buffer_table(self) -> str:
        return f"{self._db}.RAW.{self._prefix}{BUFFER_TABLE}"

    def all_tables(self) -> list[str]:
        return [self.table(t) for t in EVENT_TABLES] + [self.audit_table]

    # ------------------------------------------------------------- session

    def _execute(self, sql: str, params: Any = None) -> Any:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetchall() if cursor.description else []
        finally:
            cursor.close()

    def ensure_ready(self) -> None:
        """Connect (if needed), refuse admin roles, create tables and the session buffer."""
        if self._conn is not None:
            return
        self._conn = self._connect()
        try:
            ((role,),) = self._execute("SELECT CURRENT_ROLE()")
            if (role or "").upper() in FORBIDDEN_ROLES:
                raise SinkError(
                    f"refusing to ingest as {role}; configure SNOWFLAKE_ROLE=STREAMFORGE_ROLE"
                )
            for event_type in EVENT_TABLES:
                self._execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table(event_type)} ({RAW_COLUMNS_DDL})"
                    f" COMMENT = 'StreamForge RAW {event_type} events (append-only)'"
                )
            self._execute(
                f"CREATE TABLE IF NOT EXISTS {self.audit_table} ({AUDIT_COLUMNS_DDL})"
                " COMMENT = 'StreamForge consumer load audit (one row per loaded batch)'"
            )
            self._execute(
                f"CREATE TEMPORARY TABLE IF NOT EXISTS {self.buffer_table} ({BUFFER_DDL})"
            )
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    # --------------------------------------------------------------- write

    def write_batch(self, records: Sequence[ValidRecord], batch: BatchInfo) -> SinkResult:
        """Load ``records`` and one audit row in a single transaction."""
        if not records:
            return SinkResult(0, 0)
        self.ensure_ready()
        rows = [buffer_row(r, batch.batch_id) for r in records]
        insert = (
            f"INSERT INTO {self.buffer_table} ({', '.join(BUFFER_COLUMNS)}) "
            f"VALUES ({', '.join(['%s'] * len(BUFFER_COLUMNS))})"
        )
        try:
            self._execute("BEGIN")
            self._execute(f"DELETE FROM {self.buffer_table}")
            cursor = self._conn.cursor()
            try:
                for i in range(0, len(rows), self._chunk):
                    cursor.executemany(insert, rows[i : i + self._chunk])
            finally:
                cursor.close()
            inserted = 0
            for event_type in sorted({r.event.event_type for r in records}):
                result = self._execute(
                    merge_sql(self.table(event_type), event_type, self.buffer_table)
                )
                inserted += int(result[0][0]) if result else 0
            duplicates = len(records) - inserted
            self._execute(
                f"INSERT INTO {self.audit_table} (BATCH_ID, CONSUMER_GROUP, TOPIC, OFFSET_RANGES, "
                "RECORDS_CONSUMED, VALID_RECORDS, ROWS_INSERTED, DUPLICATES_SKIPPED, DLQ_RECORDS, "
                "STARTED_AT, LOADED_AT) "
                "SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s, "
                "TO_TIMESTAMP_TZ(%s), CURRENT_TIMESTAMP()",
                (
                    batch.batch_id,
                    batch.consumer_group,
                    batch.topic,
                    json.dumps(batch.offset_ranges),
                    batch.records_consumed,
                    len(records),
                    inserted,
                    duplicates,
                    batch.dlq_records,
                    batch.started_at,
                ),
            )
            self._execute("COMMIT")
            return SinkResult(inserted, duplicates)
        except Exception as exc:
            self._rollback_or_reset()
            # Scrubbed text and no exception chain: database errors can echo row values.
            raise SinkError(
                f"RAW load failed for batch {batch.batch_id}: {safe_error_text(exc)}"
            ) from None

    def _rollback_or_reset(self) -> None:
        try:
            self._execute("ROLLBACK")
        except Exception:  # noqa: BLE001 - session unusable: reconnect on next batch
            self.close()
