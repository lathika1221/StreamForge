"""Unit tests for streamforge.producer.invalid_records (raw-only defect injection)."""

import ast
import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

import streamforge.producer.invalid_records as invalid_records
from streamforge.common.events import EVENT_TYPES, parse_event
from streamforge.producer.invalid_records import (
    DEFECTS,
    DEFECTS_BY_EVENT_TYPE,
    applicable_defects,
    make_invalid_record,
)
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

NEW_ID = "11111111-2222-4333-8444-555555555555"

# defect -> (expected pydantic error type or None, expected field in error loc or None)
EXPECTED_ERROR = {
    "missing_event_id": ("missing", "event_id"),
    "missing_order_id": ("missing", "order_id"),
    "unknown_event_type": ("union_tag_invalid", None),
    "naive_timestamp": ("timezone_aware", "event_timestamp"),
    "malformed_timestamp": (None, "event_timestamp"),
    "unsupported_schema_version": ("literal_error", "schema_version"),
    "negative_amount": (None, ("amount", "order_total")),
    "invalid_currency": ("enum", "currency"),
    "empty_items": ("too_short", "items"),
    "total_mismatch": ("value_error", "payload"),
    "unexpected_field": ("extra_forbidden", "unexpected_field"),
    "malformed_json": ("json_invalid", None),
}


@pytest.fixture(scope="module")
def valid_by_type() -> dict[str, dict]:
    """One valid JSON dict per event type, produced by the (valid) simulator."""
    sim = EventSimulator(SimulatorConfig(seed=3, start_time=datetime(2026, 11, 23, tzinfo=UTC)))
    found: dict[str, dict] = {}
    for record in sim.iter_records(200):
        found.setdefault(record.data["event_type"], record.data)
    assert set(found) == set(EVENT_TYPES)
    return found


def _cases():
    return [(t, d) for t in EVENT_TYPES for d in DEFECTS_BY_EVENT_TYPE[t]]


def _matches(err: dict, expected_type, expected_field) -> bool:
    if expected_type is not None and err["type"] != expected_type:
        return False
    if expected_field is None:
        return True
    fields = expected_field if isinstance(expected_field, tuple) else (expected_field,)
    return any(f in err["loc"] for f in fields)


def test_catalogue_covers_every_event_type_and_every_defect_is_used():
    assert set(DEFECTS_BY_EVENT_TYPE) == set(EVENT_TYPES)
    used = {d for defects in DEFECTS_BY_EVENT_TYPE.values() for d in defects}
    assert used == set(DEFECTS) == set(EXPECTED_ERROR)


@pytest.mark.parametrize("event_type,defect", _cases(), ids=lambda v: str(v))
def test_defect_is_rejected_for_the_declared_reason(valid_by_type, event_type, defect):
    raw = make_invalid_record(valid_by_type[event_type], defect, NEW_ID)
    as_text = raw if isinstance(raw, str) else json.dumps(raw)
    with pytest.raises(ValidationError) as exc:
        parse_event(as_text)
    expected_type, expected_field = EXPECTED_ERROR[defect]
    errors = exc.value.errors()
    assert any(_matches(e, expected_type, expected_field) for e in errors), errors


@pytest.mark.parametrize("event_type,defect", _cases(), ids=lambda v: str(v))
def test_input_is_never_mutated(valid_by_type, event_type, defect):
    original = valid_by_type[event_type]
    snapshot = copy.deepcopy(original)
    make_invalid_record(original, defect, NEW_ID)
    assert original == snapshot


def test_dict_defects_return_raw_dicts_with_fresh_event_id(valid_by_type):
    raw = make_invalid_record(valid_by_type["order_created"], "invalid_currency", NEW_ID)
    assert type(raw) is dict and raw["event_id"] == NEW_ID
    assert raw["event_id"] != valid_by_type["order_created"]["event_id"]


def test_malformed_json_is_unparseable_text(valid_by_type):
    raw = make_invalid_record(valid_by_type["payment_processed"], "malformed_json", NEW_ID)
    assert isinstance(raw, str) and raw.startswith("{")
    with pytest.raises(json.JSONDecodeError):
        json.loads(raw)


def test_unknown_or_inapplicable_defect_is_rejected(valid_by_type):
    with pytest.raises(ValueError):
        make_invalid_record(valid_by_type["order_created"], "no_such_defect", NEW_ID)
    with pytest.raises(ValueError):
        make_invalid_record(valid_by_type["order_cancelled"], "empty_items", NEW_ID)
    with pytest.raises(ValueError):
        applicable_defects("order_refunded")


def test_module_never_imports_pydantic_or_event_models():
    tree = ast.parse(Path(invalid_records.__file__).read_text("utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert not {m for m in imported if m.split(".")[0] in ("pydantic", "streamforge")}
