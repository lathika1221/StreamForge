"""Unit tests for the event contract in streamforge.common.events."""

import copy
import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from streamforge.common.events import (
    EVENT_TYPES,
    OrderCancelled,
    OrderCreated,
    PaymentProcessed,
    ShipmentDispatched,
    parse_event,
)

_DELETE = object()


def _envelope(event_type: str, event_id: str, seq: int) -> dict:
    return {
        "event_id": event_id,
        "schema_version": 1,
        "event_timestamp": "2026-11-23T10:00:00Z",
        "order_id": "ORD-0123456789AB",
        "sequence_number": seq,
        "source": "streamforge-simulator",
        "event_type": event_type,
    }


VALID = {
    "order_created": {
        **_envelope("order_created", "8f14e45f-ceea-467a-9a36-dedd4bea2543", 1),
        "payload": {
            "customer_id": "CUST-000042",
            "items": [
                {"sku": "SKU-1001", "product_name": "Mouse", "quantity": 2, "unit_price": "24.99"},
                {"sku": "SKU-1012", "product_name": "Cable", "quantity": 1, "unit_price": "8.49"},
            ],
            "currency": "GBP",
            "order_total": "58.47",
            "shipping_country": "GB",
        },
    },
    "payment_processed": {
        **_envelope("payment_processed", "c9f0f895-fb98-4b91-9d33-b1b5a8b7a2a1", 2),
        "payload": {
            "payment_id": "PAY-0123456789AB",
            "amount": "58.47",
            "currency": "GBP",
            "method": "card",
            "status": "succeeded",
            "failure_reason": None,
        },
    },
    "order_cancelled": {
        **_envelope("order_cancelled", "45c48cce-2e2d-4fbd-a9d3-0c3c2b1a5e10", 3),
        "payload": {"reason": "customer_request", "cancelled_by": "customer"},
    },
    "shipment_dispatched": {
        **_envelope("shipment_dispatched", "d3d94468-02a4-4c7d-8a1e-3c5b8f2e1d70", 3),
        "payload": {
            "shipment_id": "SHP-0123456789AB",
            "carrier": "Royal Mail",
            "tracking_number": "AB12CD34EF56",
            "estimated_delivery_date": "2026-11-25",
        },
    },
}
MODEL_FOR = {
    "order_created": OrderCreated,
    "payment_processed": PaymentProcessed,
    "order_cancelled": OrderCancelled,
    "shipment_dispatched": ShipmentDispatched,
}


def _mutate(event_type: str, path: tuple, value) -> dict:
    data = copy.deepcopy(VALID[event_type])
    target = data
    for key in path[:-1]:
        target = target[key]
    if value is _DELETE:
        del target[path[-1]]
    else:
        target[path[-1]] = value
    return data


# ------------------------------------------------------------------ valid


def test_valid_fixtures_cover_every_event_type():
    assert set(VALID) == set(EVENT_TYPES) == set(MODEL_FOR)


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_valid_event_parses_to_its_model_and_round_trips(event_type):
    event = parse_event(VALID[event_type])
    assert type(event) is MODEL_FOR[event_type]
    dumped = event.model_dump(mode="json")
    assert parse_event(dumped) == event
    assert parse_event(json.dumps(dumped)) == event
    assert parse_event(json.dumps(dumped).encode("utf-8")) == event


@pytest.mark.parametrize("currency", ["INR", "GBP", "EUR", "USD"])
def test_every_supported_currency_is_accepted(currency):
    order = parse_event(_mutate("order_created", ("payload", "currency"), currency))
    payment = parse_event(_mutate("payment_processed", ("payload", "currency"), currency))
    assert order.payload.currency == payment.payload.currency == currency


def test_inr_order_with_rupee_scale_prices():
    data = _mutate("order_created", ("payload", "currency"), "INR")
    data["payload"]["items"] = [
        {"sku": "SKU-1004", "product_name": "Monitor", "quantity": 2, "unit_price": "18999.00"}
    ]
    data["payload"]["order_total"] = "37998.00"
    assert parse_event(data).model_dump(mode="json")["payload"]["order_total"] == "37998.00"


def test_money_serializes_as_two_decimal_string():
    event = parse_event(_mutate("payment_processed", ("payload", "amount"), "58.5"))
    assert event.model_dump(mode="json")["payload"]["amount"] == "58.50"


def test_timestamp_offset_is_normalised_to_utc():
    event = parse_event(
        _mutate("order_cancelled", ("event_timestamp",), "2026-11-23T11:00:00+01:00")
    )
    assert event.event_timestamp == datetime(2026, 11, 23, 10, 0, tzinfo=UTC)
    assert event.model_dump(mode="json")["event_timestamp"] == "2026-11-23T10:00:00Z"


def test_failed_payment_with_reason_is_valid():
    data = _mutate("payment_processed", ("payload", "status"), "failed")
    data["payload"]["failure_reason"] = "card_declined"
    assert parse_event(data).payload.failure_reason == "card_declined"


def test_out_of_stock_cancellation_by_system_is_valid():
    data = _mutate("order_cancelled", ("payload", "reason"), "out_of_stock")
    data["payload"]["cancelled_by"] = "system"
    assert parse_event(data).payload.cancelled_by == "system"


# ---------------------------------------------------------------- invalid

INVALID_CASES = [
    ("missing_event_id", "order_created", ("event_id",), _DELETE),
    ("non_v4_event_id", "order_created", ("event_id",), "6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
    ("missing_order_id", "payment_processed", ("order_id",), _DELETE),
    ("bad_order_id", "order_created", ("order_id",), "order-1"),
    ("unknown_event_type", "order_created", ("event_type",), "order_refunded"),
    ("naive_timestamp", "order_created", ("event_timestamp",), "2026-11-23T10:00:00"),
    ("malformed_timestamp", "order_created", ("event_timestamp",), "not-a-timestamp"),
    ("schema_version_2", "order_created", ("schema_version",), 2),
    ("sequence_number_zero", "order_created", ("sequence_number",), 0),
    ("missing_payload", "order_cancelled", ("payload",), _DELETE),
    ("unexpected_top_level_field", "order_created", ("extra",), "x"),
    ("unexpected_payload_field", "payment_processed", ("payload", "extra"), "x"),
    ("payload_of_wrong_type", "shipment_dispatched", ("event_type",), "order_cancelled"),
    ("negative_amount", "payment_processed", ("payload", "amount"), "-10.00"),
    ("three_decimal_places", "payment_processed", ("payload", "amount"), "58.475"),
    ("invalid_currency", "order_created", ("payload", "currency"), "XYZ"),
    ("empty_items", "order_created", ("payload", "items"), []),
    ("total_mismatch", "order_created", ("payload", "order_total"), "59.47"),
    ("zero_quantity", "order_created", ("payload", "items", 0, "quantity"), 0),
    ("zero_unit_price", "order_created", ("payload", "items", 0, "unit_price"), "0.00"),
    ("bad_country", "order_created", ("payload", "shipping_country"), "gbr"),
    ("succeeded_with_reason", "payment_processed", ("payload", "failure_reason"), "card_declined"),
    ("failed_without_reason", "payment_processed", ("payload", "status"), "failed"),
    ("customer_request_by_system", "order_cancelled", ("payload", "cancelled_by"), "system"),
    ("unknown_carrier", "shipment_dispatched", ("payload", "carrier"), "Pigeon"),
    (
        "delivery_before_dispatch",
        "shipment_dispatched",
        ("payload", "estimated_delivery_date"),
        "2026-11-22",
    ),
]


@pytest.mark.parametrize(
    "event_type,path,value", [c[1:] for c in INVALID_CASES], ids=[c[0] for c in INVALID_CASES]
)
def test_invalid_event_is_rejected(event_type, path, value):
    with pytest.raises(ValidationError):
        parse_event(_mutate(event_type, path, value))


def test_malformed_json_is_rejected():
    with pytest.raises(ValidationError):
        parse_event('{"event_id": ')


def test_models_are_immutable():
    event = parse_event(VALID["order_cancelled"])
    with pytest.raises(ValidationError):
        event.order_id = "ORD-FFFFFFFFFFFF"
