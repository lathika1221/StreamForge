"""Keeps docs/event-schema.md and streamforge.common.events in sync."""

import json
import re
from enum import StrEnum
from pathlib import Path

import pytest
from pydantic import BaseModel

from streamforge.common import events
from streamforge.common.events import EVENT_MODELS, EVENT_TYPES, OrderItem, parse_event
from streamforge.producer.invalid_records import DEFECTS

REPO = Path(__file__).resolve().parents[2]
DOC = (REPO / "docs" / "event-schema.md").read_text("utf-8")
ENUMS = [
    obj
    for obj in vars(events).values()
    if isinstance(obj, type) and issubclass(obj, StrEnum) and obj is not StrEnum
]


def _field_names(model: type[BaseModel]) -> set[str]:
    return set(model.model_fields)


def test_every_event_type_is_documented():
    for event_type in EVENT_TYPES:
        assert f"`{event_type}`" in DOC


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_every_envelope_and_payload_field_is_documented(event_type):
    model = EVENT_MODELS[event_type]
    payload_model = model.model_fields["payload"].annotation
    for name in _field_names(model) | _field_names(payload_model):
        assert f"`{name}`" in DOC, f"{event_type}: field {name} missing from doc"


def test_order_item_fields_are_documented():
    for name in _field_names(OrderItem):
        assert f"`items[].{name}`" in DOC


@pytest.mark.parametrize("enum", ENUMS, ids=lambda e: e.__name__)
def test_every_enum_value_is_documented(enum):
    for member in enum:
        assert f"`{member.value}`" in DOC, f"{enum.__name__}.{member.name} missing from doc"


def test_every_defect_kind_is_documented():
    for defect in DEFECTS:
        assert f"`{defect}`" in DOC


def test_event_timestamp_is_the_only_timestamp_name():
    """The legacy name `event_time` must not reappear in docs or source."""
    files = [*(REPO / "docs").glob("*.md"), *(REPO / "src").rglob("*.py")]
    pattern = re.compile(r"\bevent_time\b")
    offenders = [str(f.relative_to(REPO)) for f in files if pattern.search(f.read_text("utf-8"))]
    assert offenders == []
    assert "`event_timestamp`" in (REPO / "docs" / "architecture.md").read_text("utf-8")


def test_doc_examples_are_valid_and_cover_every_type():
    blocks = re.findall(r"```json\n(.*?)```", DOC, flags=re.DOTALL)
    parsed = [parse_event(json.loads(block)) for block in blocks]
    assert {e.event_type for e in parsed} == set(EVENT_TYPES)
