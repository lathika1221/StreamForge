"""Deterministic e-commerce event simulator.

Generates correlated order lifecycles (see ``docs/event-schema.md``) on a simulated
clock and emits them in global ``event_timestamp`` order. All randomness comes from
one seeded ``random.Random``, so the same ``seed``, ``start_time`` and config give
byte-identical JSONL output; ``seed=None`` gives fresh IDs on every run.

Valid and invalid generation are separate:

* valid records are built through the Pydantic models, then dumped to JSON;
* invalid records (optional, ``invalid_rate``) are *extra* raw records produced by
  ``streamforge.producer.invalid_records`` from a valid record's JSON dict. They
  never go through model construction. The valid stream is never altered, so
  produced records = valid records + injected invalid records.

No Kafka here. CLI (inspection only):

    python -m streamforge.producer.simulator --orders 10 --seed 42
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import random
import sys
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from streamforge.common.events import (
    CancellationReason,
    CancelledBy,
    Carrier,
    Currency,
    Event,
    OrderCancelled,
    OrderCancelledPayload,
    OrderCreated,
    OrderCreatedPayload,
    OrderItem,
    PaymentFailureReason,
    PaymentMethod,
    PaymentProcessed,
    PaymentProcessedPayload,
    PaymentStatus,
    ShipmentDispatched,
    ShipmentDispatchedPayload,
)
from streamforge.producer.invalid_records import applicable_defects, make_invalid_record

# Fixed product catalogue, priced in INR: (sku, product_name, unit_price_inr).
CATALOGUE_INR: tuple[tuple[str, str, Decimal], ...] = (
    ("SKU-1001", "Wireless Mouse", Decimal("1299.00")),
    ("SKU-1002", "Mechanical Keyboard", Decimal("6499.00")),
    ("SKU-1003", "USB-C Hub", Decimal("2499.00")),
    ("SKU-1004", "27in Monitor", Decimal("18999.00")),
    ("SKU-1005", "Laptop Stand", Decimal("1899.00")),
    ("SKU-1006", "Noise-Cancelling Headphones", Decimal("12999.00")),
    ("SKU-1007", "Webcam 1080p", Decimal("3999.00")),
    ("SKU-1008", "Desk Lamp", Decimal("1499.00")),
    ("SKU-1009", "Ergonomic Chair", Decimal("15999.00")),
    ("SKU-1010", "Portable SSD 1TB", Decimal("7999.00")),
    ("SKU-1011", "Phone Charger", Decimal("999.00")),
    ("SKU-1012", "HDMI Cable 2m", Decimal("449.00")),
)
# Fixed, illustrative INR conversion factors for simulating other currencies.
# Not real FX data; chosen only to keep simulated prices at a plausible scale.
ILLUSTRATIVE_INR_RATES: dict[Currency, Decimal] = {
    Currency.INR: Decimal("1"),
    Currency.GBP: Decimal("0.0094"),
    Currency.EUR: Decimal("0.011"),
    Currency.USD: Decimal("0.012"),
}
# Weighted by repetition: an India-based store, mostly domestic orders.
SHIPPING_COUNTRIES: tuple[str, ...] = ("IN",) * 8 + ("AE", "SG", "GB", "US")

_RATE_FIELDS = (
    "pre_payment_cancel_rate",
    "payment_failure_rate",
    "post_payment_cancel_rate",
    "invalid_rate",
)


def to_json_line(data: dict[str, Any]) -> str:
    """Canonical serialization for one record: compact, ASCII-only, model field order."""
    return json.dumps(data, separators=(",", ":"), ensure_ascii=True)


def catalogue_for(currency: Currency) -> tuple[tuple[str, str, Decimal], ...]:
    """The catalogue priced in ``currency`` (2 dp, ROUND_HALF_UP)."""
    rate = ILLUSTRATIVE_INR_RATES[Currency(currency)]
    return tuple(
        (sku, name, (price * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        for sku, name, price in CATALOGUE_INR
    )


@dataclass(frozen=True)
class SimulatorConfig:
    """Simulator settings. Rates are probabilities; delays are simulated seconds."""

    seed: int | None = None
    start_time: datetime | None = None  # timezone-aware; None = now (UTC)
    orders_per_second: float = 5.0  # simulated mean order arrival rate
    pre_payment_cancel_rate: float = 0.03
    payment_failure_rate: float = 0.08
    post_payment_cancel_rate: float = 0.04
    invalid_rate: float = 0.0
    customer_pool_size: int = 500
    currency: Currency = Currency.INR
    payment_delay_seconds: tuple[float, float] = (1.0, 20.0)
    terminal_delay_seconds: tuple[float, float] = (30.0, 300.0)

    def __post_init__(self) -> None:
        for name in _RATE_FIELDS:
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.orders_per_second <= 0:
            raise ValueError("orders_per_second must be greater than 0")
        if not 1 <= self.customer_pool_size <= 999_999:
            raise ValueError("customer_pool_size must be between 1 and 999999")
        if self.start_time is not None and self.start_time.utcoffset() is None:
            raise ValueError("start_time must be timezone-aware")
        Currency(self.currency)  # raises ValueError for unsupported currencies
        for name in ("payment_delay_seconds", "terminal_delay_seconds"):
            low, high = getattr(self, name)
            if not 0.001 <= low <= high:
                raise ValueError(f"{name} must satisfy 0.001 <= min <= max")


@dataclass(frozen=True)
class SimulatedRecord:
    """One emitted record.

    ``value`` is the exact JSON text a producer publishes (and the CLI prints).
    ``data`` is the parsed dict, or ``None`` when the record is not valid JSON.
    """

    value: str
    data: dict[str, Any] | None
    is_valid: bool = True
    defect: str | None = None


class EventSimulator:
    def __init__(self, config: SimulatorConfig | None = None) -> None:
        self.config = config or SimulatorConfig()
        self._rng = random.Random(self.config.seed)
        self._catalogue = catalogue_for(self.config.currency)
        start = self.config.start_time or datetime.now(UTC)
        self._clock = start.astimezone(UTC)

    # ------------------------------------------------------------------ public

    def iter_records(self, n_orders: int | None = None) -> Iterator[SimulatedRecord]:
        """Yield records for ``n_orders`` orders (forever if ``None``), time-ordered."""
        if n_orders is not None and n_orders < 0:
            raise ValueError("n_orders must be >= 0")
        pending: list[tuple[datetime, int, Event]] = []
        tiebreak = itertools.count()
        orders = range(n_orders) if n_orders is not None else itertools.count()
        for _ in orders:
            self._clock += self._millis(self._rng.expovariate(self.config.orders_per_second))
            for event in self._order_lifecycle(self._clock):
                heapq.heappush(pending, (event.event_timestamp, next(tiebreak), event))
            # Later orders start at or after self._clock, so everything up to it is final.
            while pending and pending[0][0] <= self._clock:
                yield from self._emit(heapq.heappop(pending)[2])
        while pending:
            yield from self._emit(heapq.heappop(pending)[2])

    # ---------------------------------------------------- valid generation

    def _order_lifecycle(self, created_at: datetime) -> list[Event]:
        cfg, rng = self.config, self._rng
        order_id = f"ORD-{rng.getrandbits(48):012X}"
        seq = itertools.count(1)
        envelope = lambda ts: {  # noqa: E731
            "event_id": self._uuid(),
            "event_timestamp": ts,
            "order_id": order_id,
            "sequence_number": next(seq),
        }

        lines = rng.sample(self._catalogue, k=rng.randint(1, 4))
        items = [
            OrderItem(
                sku=sku,
                product_name=name,
                quantity=rng.choices((1, 2, 3, 4), weights=(70, 20, 7, 3))[0],
                unit_price=price,
            )
            for sku, name, price in lines
        ]
        total = sum((i.unit_price * i.quantity for i in items), Decimal("0"))
        events: list[Event] = [
            OrderCreated(
                **envelope(created_at),
                payload=OrderCreatedPayload(
                    customer_id=f"CUST-{rng.randint(1, cfg.customer_pool_size):06d}",
                    items=items,
                    currency=cfg.currency,
                    order_total=total,
                    shipping_country=rng.choice(SHIPPING_COUNTRIES),
                ),
            )
        ]

        t = created_at + self._delay(cfg.payment_delay_seconds)
        if rng.random() < cfg.pre_payment_cancel_rate:
            events.append(self._cancellation(envelope(t), CancellationReason.CUSTOMER_REQUEST))
            return events

        failed = rng.random() < cfg.payment_failure_rate
        events.append(
            PaymentProcessed(
                **envelope(t),
                payload=PaymentProcessedPayload(
                    payment_id=f"PAY-{rng.getrandbits(48):012X}",
                    amount=total,
                    currency=cfg.currency,
                    method=rng.choices(tuple(PaymentMethod), weights=(70, 25, 5))[0],
                    status=PaymentStatus.FAILED if failed else PaymentStatus.SUCCEEDED,
                    failure_reason=rng.choice(tuple(PaymentFailureReason)) if failed else None,
                ),
            )
        )

        t += self._delay(cfg.terminal_delay_seconds)
        if failed:
            events.append(self._cancellation(envelope(t), CancellationReason.PAYMENT_FAILED))
        elif rng.random() < cfg.post_payment_cancel_rate:
            reason = rng.choice(
                (CancellationReason.CUSTOMER_REQUEST, CancellationReason.OUT_OF_STOCK)
            )
            events.append(self._cancellation(envelope(t), reason))
        else:
            events.append(
                ShipmentDispatched(
                    **envelope(t),
                    payload=ShipmentDispatchedPayload(
                        shipment_id=f"SHP-{rng.getrandbits(48):012X}",
                        carrier=rng.choice(tuple(Carrier)),
                        tracking_number=f"{rng.getrandbits(64):016X}",
                        estimated_delivery_date=t.date() + timedelta(days=rng.randint(1, 5)),
                    ),
                )
            )
        return events

    @staticmethod
    def _cancellation(envelope: dict[str, Any], reason: CancellationReason) -> OrderCancelled:
        by = (
            CancelledBy.CUSTOMER
            if reason is CancellationReason.CUSTOMER_REQUEST
            else CancelledBy.SYSTEM
        )
        return OrderCancelled(
            **envelope, payload=OrderCancelledPayload(reason=reason, cancelled_by=by)
        )

    # ------------------------------------------------------------ emission

    def _emit(self, event: Event) -> Iterator[SimulatedRecord]:
        data = event.model_dump(mode="json")
        yield SimulatedRecord(value=to_json_line(data), data=data)
        if self.config.invalid_rate and self._rng.random() < self.config.invalid_rate:
            yield self._invalid_record(data)

    def _invalid_record(self, valid_data: dict[str, Any]) -> SimulatedRecord:
        """Raw invalid record: the simulator picks the defect, invalid_records builds it."""
        defect = self._rng.choice(applicable_defects(valid_data["event_type"]))
        raw = make_invalid_record(valid_data, defect, new_event_id=str(self._uuid()))
        if isinstance(raw, str):
            return SimulatedRecord(value=raw, data=None, is_valid=False, defect=defect)
        return SimulatedRecord(value=to_json_line(raw), data=raw, is_valid=False, defect=defect)

    # ----------------------------------------------------------------- helpers

    def _uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self._rng.getrandbits(128), version=4)

    def _delay(self, bounds: tuple[float, float]) -> timedelta:
        return self._millis(self._rng.uniform(*bounds))

    @staticmethod
    def _millis(seconds: float) -> timedelta:
        return timedelta(milliseconds=round(seconds * 1000))


# --------------------------------------------------------------------------- CLI


def add_simulator_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the simulator options (shared by this CLI and the Kafka producer CLI)."""
    parser.add_argument("--orders", type=int, default=10, help="number of orders (default 10)")
    parser.add_argument("--seed", type=int, default=None, help="seed for reproducible output")
    parser.add_argument(
        "--start-time",
        type=datetime.fromisoformat,
        default=None,
        help="simulated clock start, ISO-8601 with offset (e.g. 2026-11-23T10:00:00Z)",
    )
    parser.add_argument("--invalid-rate", type=float, default=0.0, help="0..1 (default 0)")
    parser.add_argument("--orders-per-second", type=float, default=5.0)
    parser.add_argument(
        "--currency", choices=[c.value for c in Currency], default=Currency.INR.value
    )


def simulator_config_from_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> SimulatorConfig:
    """Validate parsed simulator options; exits via ``parser.error`` (code 2) if invalid."""
    if args.orders < 1:
        parser.error("--orders must be at least 1")
    try:
        return SimulatorConfig(
            seed=args.seed,
            start_time=args.start_time,
            invalid_rate=args.invalid_rate,
            orders_per_second=args.orders_per_second,
            currency=Currency(args.currency),
        )
    except ValueError as exc:
        parser.error(str(exc))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m streamforge.producer.simulator",
        description="Print simulated StreamForge events as JSON lines (no Kafka).",
    )
    add_simulator_arguments(parser)
    args = parser.parse_args(argv)
    config = simulator_config_from_args(parser, args)
    # Write bytes with "\n" so output is byte-identical on every OS (no CRLF translation).
    out = sys.stdout.buffer
    valid = invalid = 0
    for record in EventSimulator(config).iter_records(args.orders):
        out.write(record.value.encode("utf-8") + b"\n")
        if record.is_valid:
            valid += 1
        else:
            invalid += 1
    out.flush()
    print(f"{valid + invalid} records ({valid} valid, {invalid} invalid)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
