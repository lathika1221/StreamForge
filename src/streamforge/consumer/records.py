"""Classification of consumed Kafka messages (M5).

Every message is validated against the event contract with ``parse_event`` on its raw
bytes. The result is either a ``ValidRecord`` (typed event + Kafka metadata) or an
``InvalidRecord`` (failure type + human-readable reason). The original bytes are kept
untouched in both cases.

Failure reasons are built from pydantic error locations and messages only
(``include_input=False``), so payload values are never echoed into reasons, logs or
DLQ headers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from streamforge.common.events import Event, parse_event

MAX_REASON_CHARS = 500
MAX_ERROR_CHARS = 300
_TIMESTAMP_NOT_AVAILABLE = 0  # confluent_kafka.TIMESTAMP_NOT_AVAILABLE
_QUOTED = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")


def safe_error_text(exc: BaseException) -> str:
    """Exception text for logs: quoted literals (where databases echo offending
    values) are replaced, whitespace collapsed and the length capped."""
    text = _QUOTED.sub("'<redacted>'", re.sub(r"\s+", " ", str(exc)))
    text = f"{type(exc).__name__}: {text}"
    return text if len(text) <= MAX_ERROR_CHARS else text[: MAX_ERROR_CHARS - 3] + "..."


@dataclass(frozen=True)
class KafkaRecord:
    """A consumed message's raw bytes and coordinates."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None

    @classmethod
    def from_message(cls, msg: Any) -> KafkaRecord:
        ts_type, ts = msg.timestamp()
        return cls(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key(),
            value=msg.value(),
            timestamp_ms=None if ts_type == _TIMESTAMP_NOT_AVAILABLE else ts,
        )


class FailureType(StrEnum):
    EMPTY_VALUE = "empty_value"
    MALFORMED_JSON = "malformed_json"
    MISSING_FIELD = "missing_field"
    SCHEMA_VALIDATION = "schema_validation"


@dataclass(frozen=True)
class ValidRecord:
    record: KafkaRecord
    event: Event


@dataclass(frozen=True)
class InvalidRecord:
    record: KafkaRecord
    failure_type: FailureType
    reason: str


def classify(record: KafkaRecord) -> ValidRecord | InvalidRecord:
    if not record.value:
        return InvalidRecord(
            record, FailureType.EMPTY_VALUE, "message value is empty (null or zero bytes)"
        )
    try:
        event = parse_event(record.value)
    except ValidationError as exc:
        return InvalidRecord(record, failure_type(exc), failure_reason(exc))
    return ValidRecord(record, event)


def failure_type(exc: ValidationError) -> FailureType:
    types = {e["type"] for e in exc.errors(include_url=False, include_input=False)}
    if "json_invalid" in types:
        return FailureType.MALFORMED_JSON
    if "missing" in types:
        return FailureType.MISSING_FIELD
    return FailureType.SCHEMA_VALIDATION


def failure_reason(exc: ValidationError) -> str:
    """``loc: message`` for each error, without input values, capped in length."""
    parts = []
    for err in exc.errors(include_url=False, include_context=False, include_input=False):
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        parts.append(f"{loc}: {err['msg']}")
    reason = "; ".join(parts) or "validation failed"
    return reason if len(reason) <= MAX_REASON_CHARS else reason[: MAX_REASON_CHARS - 3] + "..."
