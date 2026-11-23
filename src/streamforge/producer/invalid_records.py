"""Deliberately malformed raw records for consumer and dead-letter testing.

This module is kept separate from valid-event generation on purpose. It works only on
plain JSON-compatible dicts and strings and never imports pydantic or the event
models (a unit test enforces this), so an invalid record is never built through,
or "repaired" by, model construction. It is exactly the kind of raw input a
consumer must reject.

``make_invalid_record`` takes the JSON dict of a valid event (``model_dump(mode="json")``),
deep-copies it, gives it a fresh ``event_id`` and applies exactly one defect.
The defect catalogue is documented in ``docs/event-schema.md``.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime
from decimal import Decimal
from typing import Any

DEFECTS: tuple[str, ...] = (
    "missing_event_id",
    "missing_order_id",
    "unknown_event_type",
    "naive_timestamp",
    "malformed_timestamp",
    "unsupported_schema_version",
    "negative_amount",
    "invalid_currency",
    "empty_items",
    "total_mismatch",
    "unexpected_field",
    "malformed_json",
)

_ANY_EVENT_TYPE: tuple[str, ...] = (
    "missing_event_id",
    "missing_order_id",
    "unknown_event_type",
    "naive_timestamp",
    "malformed_timestamp",
    "unsupported_schema_version",
    "unexpected_field",
    "malformed_json",
)
DEFECTS_BY_EVENT_TYPE: dict[str, tuple[str, ...]] = {
    "order_created": _ANY_EVENT_TYPE
    + ("negative_amount", "invalid_currency", "empty_items", "total_mismatch"),
    "payment_processed": _ANY_EVENT_TYPE + ("negative_amount", "invalid_currency"),
    "order_cancelled": _ANY_EVENT_TYPE,
    "shipment_dispatched": _ANY_EVENT_TYPE,
}


def applicable_defects(event_type: str) -> tuple[str, ...]:
    """Defects that can be applied to a valid event of ``event_type``."""
    try:
        return DEFECTS_BY_EVENT_TYPE[event_type]
    except KeyError:
        raise ValueError(f"unknown event_type {event_type!r}") from None


def make_invalid_record(
    valid_data: dict[str, Any], defect: str, new_event_id: str
) -> dict[str, Any] | str:
    """Return a raw invalid copy of ``valid_data`` with exactly one ``defect``.

    Returns a dict for every defect except ``malformed_json``, which returns
    truncated JSON text. ``valid_data`` is never mutated.
    """
    if defect not in applicable_defects(valid_data["event_type"]):
        raise ValueError(f"defect {defect!r} does not apply to {valid_data['event_type']!r}")

    bad = copy.deepcopy(valid_data)
    bad["event_id"] = new_event_id  # never collides with the valid event's id
    payload = bad["payload"]

    if defect == "missing_event_id":
        del bad["event_id"]
    elif defect == "missing_order_id":
        del bad["order_id"]
    elif defect == "unknown_event_type":
        bad["event_type"] = "order_refunded"
    elif defect == "naive_timestamp":
        ts = datetime.fromisoformat(bad["event_timestamp"])
        bad["event_timestamp"] = ts.replace(tzinfo=None).isoformat()
    elif defect == "malformed_timestamp":
        bad["event_timestamp"] = "not-a-timestamp"
    elif defect == "unsupported_schema_version":
        bad["schema_version"] = 99
    elif defect == "negative_amount":
        key = "amount" if bad["event_type"] == "payment_processed" else "order_total"
        payload[key] = "-10.00"
    elif defect == "invalid_currency":
        payload["currency"] = "XYZ"
    elif defect == "empty_items":
        payload["items"] = []
    elif defect == "total_mismatch":
        payload["order_total"] = f"{Decimal(payload['order_total']) + Decimal('1.00'):.2f}"
    elif defect == "unexpected_field":
        bad["unexpected_field"] = "not in schema_version 1"
    elif defect == "malformed_json":
        text = json.dumps(bad, separators=(",", ":"))
        return text[: len(text) // 2]  # truncated mid-object: not parseable JSON
    return bad
