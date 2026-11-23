"""Canonical StreamForge event contract, schema_version 1.

This module is the code half of the contract documented in ``docs/event-schema.md``;
``tests/unit/test_event_schema_doc.py`` keeps the two in sync. Change both together.

Every event shares an envelope (id, type, version, UTC timestamp, order correlation
id, per-order sequence number, source) and carries a type-specific ``payload``.
``event_type`` is the discriminator. Models are strict (unknown fields rejected)
and immutable.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    UUID4,
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    TypeAdapter,
    model_validator,
)

SCHEMA_VERSION = 1
DEFAULT_SOURCE = "streamforge-simulator"

ORDER_ID_PATTERN = r"^ORD-[0-9A-F]{12}$"
CUSTOMER_ID_PATTERN = r"^CUST-[0-9]{6}$"
PAYMENT_ID_PATTERN = r"^PAY-[0-9A-F]{12}$"
SHIPMENT_ID_PATTERN = r"^SHP-[0-9A-F]{12}$"
SKU_PATTERN = r"^SKU-[0-9]{4}$"
TRACKING_NUMBER_PATTERN = r"^[A-Z0-9]{10,20}$"
COUNTRY_CODE_PATTERN = r"^[A-Z]{2}$"

# ------------------------------------------------------------------ value types


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


UtcTimestamp = Annotated[AwareDatetime, AfterValidator(_to_utc)]
"""Timezone-aware datetime, normalised to UTC. Naive datetimes are rejected."""

Money = Annotated[
    Decimal,
    Field(max_digits=12, decimal_places=2),
    PlainSerializer(lambda v: f"{v:.2f}", return_type=str, when_used="json"),
]
"""Decimal amount with at most 2 decimal places, serialized to JSON as a string."""

PositiveMoney = Annotated[Money, Field(gt=0)]
NonNegativeMoney = Annotated[Money, Field(ge=0)]


class Currency(StrEnum):
    INR = "INR"
    GBP = "GBP"
    EUR = "EUR"
    USD = "USD"


class PaymentMethod(StrEnum):
    CARD = "card"
    PAYPAL = "paypal"
    BANK_TRANSFER = "bank_transfer"


class PaymentStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PaymentFailureReason(StrEnum):
    INSUFFICIENT_FUNDS = "insufficient_funds"
    CARD_DECLINED = "card_declined"
    FRAUD_SUSPECTED = "fraud_suspected"


class CancellationReason(StrEnum):
    CUSTOMER_REQUEST = "customer_request"
    PAYMENT_FAILED = "payment_failed"
    OUT_OF_STOCK = "out_of_stock"


class CancelledBy(StrEnum):
    CUSTOMER = "customer"
    SYSTEM = "system"


class Carrier(StrEnum):
    DHL = "DHL"
    UPS = "UPS"
    FEDEX = "FedEx"
    ROYAL_MAIL = "Royal Mail"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --------------------------------------------------------------------- payloads


class OrderItem(_Strict):
    sku: str = Field(pattern=SKU_PATTERN)
    product_name: str = Field(min_length=1, max_length=200)
    quantity: int = Field(ge=1, le=100)
    unit_price: PositiveMoney


class OrderCreatedPayload(_Strict):
    customer_id: str = Field(pattern=CUSTOMER_ID_PATTERN)
    items: list[OrderItem] = Field(min_length=1)
    currency: Currency
    order_total: PositiveMoney
    shipping_country: str = Field(pattern=COUNTRY_CODE_PATTERN)

    @model_validator(mode="after")
    def _total_matches_items(self) -> OrderCreatedPayload:
        expected = sum((i.unit_price * i.quantity for i in self.items), Decimal("0"))
        if self.order_total != expected:
            raise ValueError(f"order_total {self.order_total} != sum of items {expected}")
        return self


class PaymentProcessedPayload(_Strict):
    payment_id: str = Field(pattern=PAYMENT_ID_PATTERN)
    amount: NonNegativeMoney
    currency: Currency
    method: PaymentMethod
    status: PaymentStatus
    failure_reason: PaymentFailureReason | None = None

    @model_validator(mode="after")
    def _failure_reason_iff_failed(self) -> PaymentProcessedPayload:
        failed = self.status is PaymentStatus.FAILED
        if failed != (self.failure_reason is not None):
            raise ValueError(
                "failure_reason is required when status is 'failed' and null otherwise"
            )
        return self


class OrderCancelledPayload(_Strict):
    reason: CancellationReason
    cancelled_by: CancelledBy

    @model_validator(mode="after")
    def _actor_matches_reason(self) -> OrderCancelledPayload:
        by_customer = self.reason is CancellationReason.CUSTOMER_REQUEST
        if by_customer != (self.cancelled_by is CancelledBy.CUSTOMER):
            raise ValueError(
                "cancelled_by must be 'customer' for customer_request and 'system' otherwise"
            )
        return self


class ShipmentDispatchedPayload(_Strict):
    shipment_id: str = Field(pattern=SHIPMENT_ID_PATTERN)
    carrier: Carrier
    tracking_number: str = Field(pattern=TRACKING_NUMBER_PATTERN)
    estimated_delivery_date: date


# ----------------------------------------------------------------------- events


class _Envelope(_Strict):
    event_id: UUID4
    schema_version: Literal[1] = SCHEMA_VERSION
    event_timestamp: UtcTimestamp
    order_id: str = Field(pattern=ORDER_ID_PATTERN)
    sequence_number: int = Field(ge=1)
    source: str = Field(default=DEFAULT_SOURCE, min_length=1, max_length=100)


class OrderCreated(_Envelope):
    event_type: Literal["order_created"] = "order_created"
    payload: OrderCreatedPayload


class PaymentProcessed(_Envelope):
    event_type: Literal["payment_processed"] = "payment_processed"
    payload: PaymentProcessedPayload


class OrderCancelled(_Envelope):
    event_type: Literal["order_cancelled"] = "order_cancelled"
    payload: OrderCancelledPayload


class ShipmentDispatched(_Envelope):
    event_type: Literal["shipment_dispatched"] = "shipment_dispatched"
    payload: ShipmentDispatchedPayload

    @model_validator(mode="after")
    def _delivery_not_before_dispatch(self) -> ShipmentDispatched:
        if self.payload.estimated_delivery_date < self.event_timestamp.date():
            raise ValueError("estimated_delivery_date is before the dispatch date")
        return self


Event = Annotated[
    OrderCreated | PaymentProcessed | OrderCancelled | ShipmentDispatched,
    Field(discriminator="event_type"),
]

EVENT_MODELS: dict[str, type[_Envelope]] = {
    "order_created": OrderCreated,
    "payment_processed": PaymentProcessed,
    "order_cancelled": OrderCancelled,
    "shipment_dispatched": ShipmentDispatched,
}
EVENT_TYPES: tuple[str, ...] = tuple(EVENT_MODELS)

_EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)


def parse_event(data: dict[str, Any] | str | bytes) -> Event:
    """Validate a raw event (dict, or JSON text/bytes) into its typed model.

    Raises ``pydantic.ValidationError`` if the event violates the contract.
    """
    if isinstance(data, str | bytes | bytearray):
        return _EVENT_ADAPTER.validate_json(data)
    return _EVENT_ADAPTER.validate_python(data)
