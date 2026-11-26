"""Offline demo data for the dashboard (M9.3): no Snowflake, no network, no credentials.

Enabled only when the environment variable ``STREAMFORGE_DASHBOARD_DEMO`` is ``1``,
``true``, ``yes`` or ``on`` (set in the shell; ``.env`` is not consulted), so the demo can
never be switched on by accident in a configured environment. In demo mode
``data.get_runner()`` returns a :class:`DemoRunner` and no Snowflake connection is made.

How the data is produced:

1. The project's own deterministic simulator generates orders in several currencies
   (fixed seed and start time, so every run shows the same figures), including a small
   share of invalid records.
2. Each record gets Kafka-like coordinates (3 partitions, keyed by ``order_id``) and is
   classified by the consumer's real ``records.classify()``: invalid records count as
   dead-lettered, valid ones as loaded into RAW, in batches of ``batch_size``.
3. The seven dashboard query results are derived with pandas, mirroring the dbt models
   ``fct_orders`` (status from the highest ``sequence_number``), ``agg_daily_orders``,
   ``agg_daily_product_sales``, ``ops_ingestion_reconciliation`` and
   ``ops_order_anomalies``.

This approximates the dbt models for display purposes; it is not dbt output.
"""

from __future__ import annotations

import os
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from streamforge.common.events import Currency
from streamforge.consumer.records import KafkaRecord, ValidRecord, classify
from streamforge.dashboard import queries
from streamforge.producer.publisher import message_key, message_value
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

DEMO_ENV = "STREAMFORGE_DASHBOARD_DEMO"
DEMO_TOPIC = "ecommerce.events"  # a label for the simulated coordinates; nothing is published
PARTITIONS = 3

ORDER_COLUMNS = [
    "order_id",
    "currency",
    "order_total",
    "shipping_country",
    "created_at_utc",
    "order_date",
    "payment_status",
    "payment_amount",
    "payment_currency",
    "cancelled_at_utc",
    "shipped_at_utc",
    "current_status",
    "has_order_created",
    "has_sequence_gap",
    "event_count",
    "latest_sequence_number",
]
ITEM_COLUMNS = ["order_id", "sku", "product_name", "quantity", "line_total"]


def demo_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """True only when STREAMFORGE_DASHBOARD_DEMO is explicitly switched on."""
    value = (os.environ if environ is None else environ).get(DEMO_ENV, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DemoConfig:
    seed: int = 42
    start_time: datetime = datetime(2026, 11, 16, tzinfo=UTC)
    days: int = 7  # each currency's orders are spread over this many days
    orders: tuple[tuple[str, int], ...] = (("INR", 240), ("GBP", 60), ("EUR", 45), ("USD", 45))
    invalid_rate: float = 0.03
    batch_size: int = 100


# ------------------------------------------------------------------ simulation


def simulate(config: DemoConfig) -> list[KafkaRecord]:
    """Simulated records in consumption order, with Kafka-like coordinates."""
    streams: list[tuple[datetime, int, int, Any]] = []
    for stream, (code, n_orders) in enumerate(config.orders):
        cfg = SimulatorConfig(
            seed=config.seed + stream,
            start_time=config.start_time,
            orders_per_second=n_orders / (config.days * 86_400),
            invalid_rate=config.invalid_rate,
            currency=Currency(code),
        )
        last = config.start_time
        for position, record in enumerate(EventSimulator(cfg).iter_records(n_orders)):
            stamp = (record.data or {}).get("event_timestamp")
            if isinstance(stamp, str):
                try:
                    last = datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(UTC)
                except ValueError:
                    pass  # malformed timestamp: keep the stream's last known time
            streams.append((last, stream, position, record))
    streams.sort(key=lambda s: (s[0], s[1], s[2]))

    offsets = [0] * PARTITIONS
    out: list[KafkaRecord] = []
    for i, (stamp, _, _, record) in enumerate(streams):
        key = message_key(record)
        partition = zlib.crc32(key) % PARTITIONS if key else i % PARTITIONS
        out.append(
            KafkaRecord(
                DEMO_TOPIC,
                partition,
                offsets[partition],
                key,
                message_value(record),
                int(stamp.timestamp() * 1000),
            )
        )
        offsets[partition] += 1
    return out


# ------------------------------------------------------------------ derivation


def _event_time(record: KafkaRecord) -> datetime:
    return datetime.fromtimestamp((record.timestamp_ms or 0) / 1000, tz=UTC)


def _reconciliation(records: list[KafkaRecord], batch_size: int) -> pd.DataFrame:
    rows = []
    for n, start in enumerate(range(0, len(records), batch_size), start=1):
        batch = records[start : start + batch_size]
        valid = sum(isinstance(classify(r), ValidRecord) for r in batch)
        loaded = max(_event_time(r) for r in batch) + timedelta(seconds=5)
        rows.append(
            {
                "batch_id": f"demo-{n:04d}",
                "records_consumed": len(batch),
                "valid_records": valid,
                "rows_inserted": valid,
                "duplicates_skipped": 0,
                "dlq_records": len(batch) - valid,
                "raw_rows": valid,
                "rows_match": True,
                "accounting_ok": True,
                "load_accounting_ok": True,
                "started_at_utc": pd.Timestamp(loaded - timedelta(seconds=2)),
                "loaded_at_utc": pd.Timestamp(loaded),
            }
        )
    return pd.DataFrame(rows, columns=list(_RECON_COLUMNS))


_RECON_COLUMNS = (
    "batch_id",
    "records_consumed",
    "valid_records",
    "rows_inserted",
    "duplicates_skipped",
    "dlq_records",
    "raw_rows",
    "rows_match",
    "accounting_ok",
    "load_accounting_ok",
    "started_at_utc",
    "loaded_at_utc",
)


def _staged(records: list[KafkaRecord]) -> tuple[list[ValidRecord], list[tuple[str, str]]]:
    """Valid events deduplicated by event_id (earliest copy wins, as in STAGING), plus
    (event_id, order_id) pairs seen with conflicting content."""
    first: dict[str, ValidRecord] = {}
    conflicts: list[tuple[str, str]] = []
    for record in records:
        result = classify(record)
        if not isinstance(result, ValidRecord):
            continue
        event_id = str(result.event.event_id)
        if event_id not in first:
            first[event_id] = result
        elif first[event_id].record.value != record.value:
            conflicts.append((event_id, result.event.order_id))
    return list(first.values()), conflicts


def _order_key(v: ValidRecord) -> tuple:
    e = v.event
    return (e.sequence_number, e.event_timestamp, v.record.partition, v.record.offset)


def _orders(events: list[ValidRecord]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One row per order (as fct_orders) and one row per order line (as fct_order_items)."""
    by_order: dict[str, list[ValidRecord]] = {}
    for v in events:
        by_order.setdefault(v.event.order_id, []).append(v)

    orders, items = [], []
    for order_id, evs in sorted(by_order.items()):
        of_type = {t: [v for v in evs if v.event.event_type == t] for t in _STATUS}
        latest = max(evs, key=_order_key).event
        created = min(of_type["order_created"], key=_order_key, default=None)
        payment = max(of_type["payment_processed"], key=_order_key, default=None)
        cancel = min(of_type["order_cancelled"], key=_order_key, default=None)
        ship = min(of_type["shipment_dispatched"], key=_order_key, default=None)
        pay_status = payment.event.payload.status.value if payment else None
        status = _STATUS[latest.event_type]
        if latest.event_type == "payment_processed":
            status = "paid" if pay_status == "succeeded" else "payment_failed"
        sequences = [v.event.sequence_number for v in evs]
        c = created.event if created else None
        orders.append(
            {
                "order_id": order_id,
                "currency": c.payload.currency.value if c else None,
                "order_total": c.payload.order_total if c else None,
                "shipping_country": c.payload.shipping_country if c else None,
                "created_at_utc": c.event_timestamp if c else None,
                "order_date": c.event_timestamp.astimezone(UTC).date() if c else None,
                "payment_status": pay_status,
                "payment_amount": payment.event.payload.amount if payment else None,
                "payment_currency": payment.event.payload.currency.value if payment else None,
                "cancelled_at_utc": cancel.event.event_timestamp if cancel else None,
                "shipped_at_utc": ship.event.event_timestamp if ship else None,
                "current_status": status,
                "has_order_created": c is not None,
                "has_sequence_gap": min(sequences) != 1 or max(sequences) != len(set(sequences)),
                "event_count": len(evs),
                "latest_sequence_number": max(sequences),
            }
        )
        if c is not None:
            for line in c.payload.items:
                items.append(
                    {
                        "order_id": order_id,
                        "sku": line.sku,
                        "product_name": line.product_name,
                        "quantity": line.quantity,
                        "line_total": line.unit_price * line.quantity,
                    }
                )
    return pd.DataFrame(orders, columns=ORDER_COLUMNS), pd.DataFrame(items, columns=ITEM_COLUMNS)


_STATUS = {
    "order_created": "awaiting_payment",
    "payment_processed": "paid",
    "order_cancelled": "cancelled",
    "shipment_dispatched": "shipped",
}


def _has(value: Any) -> bool:
    """Null-safe presence test (pandas turns missing values into NaN/NaT, never None)."""
    return value is not None and not pd.isna(value)


def _anomalies(orders: pd.DataFrame, conflicts: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for o in orders.itertuples(index=False):
        if not o.has_order_created:
            rows.append((o.order_id, "missing_order_created", f"events seen: {o.event_count}"))
        if o.has_sequence_gap:
            rows.append(
                (
                    o.order_id,
                    "sequence_gap",
                    f"latest sequence {o.latest_sequence_number}, events {o.event_count}",
                )
            )
        if _has(o.shipped_at_utc) and o.payment_status != "succeeded":
            rows.append(
                (
                    o.order_id,
                    "shipped_without_successful_payment",
                    f"payment_status: {o.payment_status if _has(o.payment_status) else 'none'}",
                )
            )
        if _has(o.cancelled_at_utc) and _has(o.shipped_at_utc):
            rows.append((o.order_id, "cancelled_and_shipped", None))
        if _has(o.currency) and _has(o.payment_currency):
            if o.currency != o.payment_currency:
                rows.append(
                    (o.order_id, "currency_mismatch", f"{o.currency} vs {o.payment_currency}")
                )
            elif o.order_total != o.payment_amount:
                rows.append(
                    (
                        o.order_id,
                        "payment_amount_mismatch",
                        f"order {o.order_total} {o.currency} vs payment "
                        f"{o.payment_amount} {o.payment_currency}",
                    )
                )
    for event_id, order_id in conflicts:
        rows.append((order_id, "conflicting_duplicate_event", f"event {event_id} has 2 versions"))
    df = pd.DataFrame(rows, columns=["order_id", "anomaly", "detail"])
    return df[["anomaly", "order_id", "detail"]].sort_values(["anomaly", "order_id"])


# ------------------------------------------------------------------ runner


def _money_sum(values: pd.Series) -> Decimal:
    return sum((v for v in values if v is not None and not pd.isna(v)), Decimal("0"))


class DemoRunner:
    """Answers the dashboard's seven queries from simulated data (see the module doc)."""

    is_demo = True

    def __init__(
        self,
        orders: pd.DataFrame,
        items: pd.DataFrame,
        reconciliation: pd.DataFrame,
        anomalies: pd.DataFrame,
    ) -> None:
        self.orders = orders
        self.items = items
        self.reconciliation_rows = reconciliation
        self.anomaly_rows = anomalies
        self._handlers: dict[str, Callable[..., pd.DataFrame]] = {
            queries.DATE_BOUNDS_SQL: self._date_bounds,
            queries.DAILY_ORDERS_SQL: self._daily_orders,
            queries.ORDER_STATUS_SQL: self._order_status,
            queries.PRODUCT_SALES_SQL: self._product_sales,
            queries.COUNTRY_SQL: self._country,
            queries.RECONCILIATION_SQL: self._reconciliation,
            queries.ANOMALIES_SQL: self._anomalies,
        }

    @classmethod
    def build(cls, config: DemoConfig | None = None) -> DemoRunner:
        config = config or DemoConfig()
        records = simulate(config)
        staged, conflicts = _staged(records)
        orders, items = _orders(staged)
        return cls(
            orders,
            items,
            _reconciliation(records, config.batch_size),
            _anomalies(orders, conflicts),
        )

    def __call__(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        handler = self._handlers.get(sql)
        if handler is None:
            from streamforge.dashboard.data import DashboardDataError

            raise DashboardDataError("demo mode: unknown query")
        return handler(*params).reset_index(drop=True)

    def close(self) -> None:  # same interface as SnowflakeRunner
        pass

    # -- helpers

    def _created_in(self, start: date, end: date) -> pd.DataFrame:
        o = self.orders[self.orders["has_order_created"]]
        return o[(o["order_date"] >= start) & (o["order_date"] <= end)]

    @staticmethod
    def _net(orders: pd.DataFrame) -> pd.Series:
        return (orders["payment_status"] == "succeeded") & orders["cancelled_at_utc"].isna()

    # -- the seven queries

    def _date_bounds(self) -> pd.DataFrame:
        dates = self.orders.loc[self.orders["has_order_created"], "order_date"]
        lo, hi = (dates.min(), dates.max()) if len(dates) else (None, None)
        return pd.DataFrame([{"min_date": lo, "max_date": hi}])

    def _daily_orders(self, start: date, end: date) -> pd.DataFrame:
        rows = []
        for (day, currency), o in self._created_in(start, end).groupby(
            ["order_date", "currency"], sort=True
        ):
            paid = o["payment_status"] == "succeeded"
            rows.append(
                {
                    "order_date": day,
                    "currency": currency,
                    "orders_created": len(o),
                    "orders_paid": int(paid.sum()),
                    "orders_payment_failed": int((o["payment_status"] == "failed").sum()),
                    "orders_cancelled": int(o["cancelled_at_utc"].notna().sum()),
                    "orders_shipped": int(o["shipped_at_utc"].notna().sum()),
                    "orders_awaiting_payment": int(
                        (o["current_status"] == "awaiting_payment").sum()
                    ),
                    "gross_order_value": _money_sum(o["order_total"]),
                    "paid_value": _money_sum(o.loc[paid, "order_total"]),
                    "net_revenue": _money_sum(o.loc[self._net(o), "order_total"]),
                }
            )
        columns = [
            "order_date",
            "currency",
            "orders_created",
            "orders_paid",
            "orders_payment_failed",
            "orders_cancelled",
            "orders_shipped",
            "orders_awaiting_payment",
            "gross_order_value",
            "paid_value",
            "net_revenue",
        ]
        return pd.DataFrame(rows, columns=columns)

    def _order_status(self, start: date, end: date) -> pd.DataFrame:
        o = self._created_in(start, end).assign(
            payment_status=lambda d: d["payment_status"].fillna("no_payment")
        )
        out = o.groupby(["current_status", "payment_status", "currency"]).size()
        return out.rename("orders").reset_index()

    def _product_sales(self, start: date, end: date) -> pd.DataFrame:
        o = self._created_in(start, end)
        o = o.assign(is_net=self._net(o))[["order_id", "currency", "order_date", "is_net"]]
        lines = self.items.merge(o, on="order_id", how="inner")
        rows = []
        for (sku, currency), g in lines.groupby(["sku", "currency"], sort=True):
            rows.append(
                {
                    "sku": sku,
                    "currency": currency,
                    "product_name": g.sort_values("order_date")["product_name"].iloc[-1],
                    "units": int(g["quantity"].sum()),
                    "orders": int(g.groupby("order_date")["order_id"].nunique().sum()),
                    "gross_value": _money_sum(g["line_total"]),
                    "net_value": _money_sum(g.loc[g["is_net"], "line_total"]),
                }
            )
        columns = ["sku", "currency", "product_name", "units", "orders"]
        return pd.DataFrame(rows, columns=columns + ["gross_value", "net_value"])

    def _country(self, start: date, end: date) -> pd.DataFrame:
        o = self._created_in(start, end)
        o = o.assign(shipping_country=o["shipping_country"].fillna("UNKNOWN"), is_net=self._net(o))
        rows = []
        for (country, currency), g in o.groupby(["shipping_country", "currency"], sort=True):
            rows.append(
                {
                    "shipping_country": country,
                    "currency": currency,
                    "orders": len(g),
                    "gross_order_value": _money_sum(g["order_total"]),
                    "net_revenue": _money_sum(g.loc[g["is_net"], "order_total"]),
                }
            )
        columns = ["shipping_country", "currency", "orders", "gross_order_value", "net_revenue"]
        return pd.DataFrame(rows, columns=columns)

    def _reconciliation(self) -> pd.DataFrame:
        r = self.reconciliation_rows.sort_values("loaded_at_utc", ascending=False)
        return r.head(200)

    def _anomalies(self) -> pd.DataFrame:
        return self.anomaly_rows.head(500)
