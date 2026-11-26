"""Pure calculations and formatting for the dashboard (no Streamlit, no Snowflake).

Rules: monetary values are only ever aggregated within one currency; rates are
recomputed from summed counts (never by averaging daily rates); every function accepts
an empty or partially-null DataFrame and returns something displayable.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

CURRENCY_SYMBOLS = {"INR": "₹", "GBP": "£", "EUR": "€", "USD": "$"}
STATUS_ORDER = ["awaiting_payment", "paid", "payment_failed", "cancelled", "shipped"]
STATUS_LABELS = {
    "awaiting_payment": "Awaiting payment",
    "paid": "Paid",
    "payment_failed": "Payment failed",
    "cancelled": "Cancelled",
    "shipped": "Shipped",
    "succeeded": "Succeeded",
    "failed": "Failed",
    "no_payment": "No payment yet",
}

DAILY_COLUMNS = [
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
COUNT_COLUMNS = [c for c in DAILY_COLUMNS if c.startswith("orders_")]
MONEY_COLUMNS = ["gross_order_value", "paid_value", "net_revenue"]


# ------------------------------------------------------------------ formatting


def format_money(value: float | None, currency: str | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    symbol = CURRENCY_SYMBOLS.get(currency or "", "")
    text = f"{abs(value):,.2f}"
    prefix = "-" if value < 0 else ""
    return f"{prefix}{symbol}{text}" if symbol else f"{prefix}{text} {currency or ''}".strip()


def format_rate(value: float | None) -> str:
    return "—" if value is None or pd.isna(value) else f"{value * 100:.1f}%"


def format_count(value: float | None) -> str:
    return "—" if value is None or pd.isna(value) else f"{int(value):,}"


def label(status: str | None) -> str:
    if status is None or (isinstance(status, float) and pd.isna(status)):
        return "Unknown"
    return STATUS_LABELS.get(status, str(status).replace("_", " ").capitalize())


def ratio(numerator: float, denominator: float) -> float | None:
    return None if not denominator else numerator / denominator


# ---------------------------------------------------------------- normalising


def ensure_columns(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    """Lower-case columns, add any missing ones (as NA) and keep the expected order."""
    df = pd.DataFrame(columns=columns) if df is None else df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    for c in columns:
        if c not in df.columns:
            df[c] = pd.NA
    return df[columns + [c for c in df.columns if c not in columns]]


def to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(float)
    return df


def currencies(daily: pd.DataFrame) -> list[str]:
    if daily.empty or "currency" not in daily:
        return []
    return sorted(c for c in daily["currency"].dropna().unique())


# ------------------------------------------------------------------- KPIs


@dataclass(frozen=True)
class CurrencyKpis:
    currency: str
    orders: int
    gross_order_value: float
    paid_value: float
    net_revenue: float
    payment_success_rate: float | None
    cancellation_rate: float | None


def currency_kpis(daily: pd.DataFrame) -> list[CurrencyKpis]:
    """One KPI set per currency over the filtered period. Never mixes currencies."""
    daily = to_numeric(ensure_columns(daily, DAILY_COLUMNS), COUNT_COLUMNS + MONEY_COLUMNS)
    daily = daily.dropna(subset=["currency"])
    if daily.empty:
        return []
    grouped = daily.groupby("currency", sort=True)[COUNT_COLUMNS + MONEY_COLUMNS].sum()
    out = []
    for cur, row in grouped.iterrows():
        paid, failed = row["orders_paid"], row["orders_payment_failed"]
        out.append(
            CurrencyKpis(
                currency=str(cur),
                orders=int(row["orders_created"]),
                gross_order_value=float(row["gross_order_value"]),
                paid_value=float(row["paid_value"]),
                net_revenue=float(row["net_revenue"]),
                payment_success_rate=ratio(paid, paid + failed),
                cancellation_rate=ratio(row["orders_cancelled"], row["orders_created"]),
            )
        )
    return out


def kpis_for(kpis: list[CurrencyKpis], currency: str | None) -> CurrencyKpis | None:
    return next((k for k in kpis if k.currency == currency), None)


def kpi_table(kpis: list[CurrencyKpis]) -> pd.DataFrame:
    """Per-currency summary: one row per currency, amounts formatted in that currency."""
    return pd.DataFrame(
        [
            {
                "Currency": k.currency,
                "Orders": format_count(k.orders),
                "Gross order value": format_money(k.gross_order_value, k.currency),
                "Paid value": format_money(k.paid_value, k.currency),
                "Net revenue": format_money(k.net_revenue, k.currency),
                "Payment success": format_rate(k.payment_success_rate),
                "Cancellation rate": format_rate(k.cancellation_rate),
            }
            for k in kpis
        ],
        columns=[
            "Currency",
            "Orders",
            "Gross order value",
            "Paid value",
            "Net revenue",
            "Payment success",
            "Cancellation rate",
        ],
    )


# ------------------------------------------------------------ order analytics


def daily_trend(daily: pd.DataFrame) -> pd.DataFrame:
    """Orders per day and currency (counts), for the trend chart."""
    daily = to_numeric(ensure_columns(daily, DAILY_COLUMNS), COUNT_COLUMNS + MONEY_COLUMNS)
    daily = daily.dropna(subset=["order_date", "currency"])
    if daily.empty:
        return pd.DataFrame(columns=["order_date", "currency", "orders_created", "net_revenue"])
    daily["order_date"] = pd.to_datetime(daily["order_date"])
    return daily[["order_date", "currency", "orders_created", "net_revenue"]]


def status_counts(status: pd.DataFrame, column: str) -> pd.DataFrame:
    """Order counts by ``current_status`` or ``payment_status`` (counts may span currencies)."""
    status = ensure_columns(status, ["current_status", "payment_status", "currency", "orders"])
    status = to_numeric(status, ["orders"])
    if status.empty:
        return pd.DataFrame(columns=["status", "label", "orders"])
    out = status.groupby(status[column].fillna("unknown"))["orders"].sum().reset_index()
    out.columns = ["status", "orders"]
    out["label"] = out["status"].map(label)
    rank = {s: i for i, s in enumerate(STATUS_ORDER)}
    out["_rank"] = out["status"].map(lambda s: rank.get(s, len(rank)))
    return out.sort_values(["_rank", "status"]).drop(columns="_rank")[["status", "label", "orders"]]


# ---------------------------------------------------------- product analytics

PRODUCT_COLUMNS = ["sku", "currency", "product_name", "units", "orders", "gross_value", "net_value"]


def products_for(products: pd.DataFrame, currency: str | None) -> pd.DataFrame:
    """SKU performance in one currency, best sellers (by units, then net value) first."""
    products = to_numeric(
        ensure_columns(products, PRODUCT_COLUMNS), ["units", "orders", "gross_value", "net_value"]
    )
    products = products[products["currency"] == currency] if currency else products.iloc[0:0]
    products = products.assign(product_name=products["product_name"].fillna(products["sku"]))
    return products.sort_values(["units", "net_value", "sku"], ascending=[False, False, True])


# -------------------------------------------------------- geographic analytics

COUNTRY_COLUMNS = ["shipping_country", "currency", "orders", "gross_order_value", "net_revenue"]


def countries_for(countries: pd.DataFrame, currency: str | None) -> pd.DataFrame:
    countries = to_numeric(
        ensure_columns(countries, COUNTRY_COLUMNS), ["orders", "gross_order_value", "net_revenue"]
    )
    countries = countries[countries["currency"] == currency] if currency else countries.iloc[0:0]
    countries = countries.assign(shipping_country=countries["shipping_country"].fillna("UNKNOWN"))
    return countries.sort_values(["orders", "net_revenue"], ascending=False)


# ------------------------------------------------------------- pipeline health

RECON_COLUMNS = [
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
]
RECON_COUNTS = [
    "records_consumed",
    "valid_records",
    "rows_inserted",
    "duplicates_skipped",
    "dlq_records",
    "raw_rows",
]


@dataclass(frozen=True)
class PipelineSummary:
    batches: int
    records_consumed: int
    valid_records: int
    rows_inserted: int
    duplicates_skipped: int
    dlq_records: int
    failing_batches: int
    last_loaded_at: pd.Timestamp | None

    @property
    def dlq_rate(self) -> float | None:
        return ratio(self.dlq_records, self.records_consumed)

    @property
    def healthy(self) -> bool:
        return self.failing_batches == 0


def pipeline_summary(recon: pd.DataFrame) -> PipelineSummary:
    recon = to_numeric(ensure_columns(recon, RECON_COLUMNS), RECON_COUNTS)
    checks = recon[["rows_match", "accounting_ok", "load_accounting_ok"]].fillna(False).astype(bool)
    failing = int((~checks.all(axis=1)).sum()) if not recon.empty else 0
    last = pd.to_datetime(recon["loaded_at_utc"], utc=True, errors="coerce").max()
    return PipelineSummary(
        batches=len(recon),
        records_consumed=int(recon["records_consumed"].sum()),
        valid_records=int(recon["valid_records"].sum()),
        rows_inserted=int(recon["rows_inserted"].sum()),
        duplicates_skipped=int(recon["duplicates_skipped"].sum()),
        dlq_records=int(recon["dlq_records"].sum()),
        failing_batches=failing,
        last_loaded_at=None if pd.isna(last) else last,
    )


def anomaly_counts(anomalies: pd.DataFrame) -> pd.DataFrame:
    anomalies = ensure_columns(anomalies, ["anomaly", "order_id", "detail"])
    if anomalies.empty:
        return pd.DataFrame(columns=["anomaly", "orders"])
    out = anomalies.groupby(anomalies["anomaly"].fillna("unknown"))["order_id"].nunique()
    return out.reset_index().rename(columns={"order_id": "orders"})
