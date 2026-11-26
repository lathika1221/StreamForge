"""Read-only SQL for the dashboard (M7).

Reads only existing dbt outputs (ANALYTICS / OPS); no transformation logic is repeated
here beyond grouping. Rules: dates are bound parameters (never string-formatted), money
is always grouped by currency (never summed across currencies), and result sizes are
bounded. Each function takes a ``runner(sql, params) -> DataFrame`` so tests can inject
fake data and the app can add caching.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pandas as pd

DB = "ECOMMERCE_STREAMING_DB"
Runner = Callable[[str, tuple], pd.DataFrame]

DATE_BOUNDS_SQL = f"""
select min(order_date) as min_date, max(order_date) as max_date
from {DB}.ANALYTICS.AGG_DAILY_ORDERS
"""

DAILY_ORDERS_SQL = f"""
select order_date, currency, orders_created, orders_paid, orders_payment_failed,
       orders_cancelled, orders_shipped, orders_awaiting_payment,
       gross_order_value, paid_value, net_revenue
from {DB}.ANALYTICS.AGG_DAILY_ORDERS
where order_date between %s and %s
order by order_date, currency
"""

ORDER_STATUS_SQL = f"""
select current_status,
       coalesce(payment_status, 'no_payment') as payment_status,
       currency,
       count(*) as orders
from {DB}.ANALYTICS.FCT_ORDERS
where has_order_created and order_date between %s and %s
group by current_status, coalesce(payment_status, 'no_payment'), currency
"""

PRODUCT_SALES_SQL = f"""
select sku, currency,
       max_by(product_name, order_date) as product_name,
       sum(units) as units,
       sum(orders) as orders,
       sum(gross_value) as gross_value,
       sum(net_value) as net_value
from {DB}.ANALYTICS.AGG_DAILY_PRODUCT_SALES
where order_date between %s and %s
group by sku, currency
"""

COUNTRY_SQL = f"""
select coalesce(shipping_country, 'UNKNOWN') as shipping_country,
       currency,
       count(*) as orders,
       sum(order_total) as gross_order_value,
       sum(iff(payment_status = 'succeeded' and cancelled_at_utc is null, order_total, 0))
           as net_revenue
from {DB}.ANALYTICS.FCT_ORDERS
where has_order_created and order_date between %s and %s
group by coalesce(shipping_country, 'UNKNOWN'), currency
"""

RECONCILIATION_SQL = f"""
select batch_id, records_consumed, valid_records, rows_inserted, duplicates_skipped,
       dlq_records, raw_rows, rows_match, accounting_ok, load_accounting_ok,
       started_at_utc, loaded_at_utc
from {DB}.OPS.OPS_INGESTION_RECONCILIATION
order by loaded_at_utc desc
limit 200
"""

ANOMALIES_SQL = f"""
select anomaly, order_id, detail
from {DB}.OPS.OPS_ORDER_ANOMALIES
order by anomaly, order_id
limit 500
"""

ALL_SQL = {
    "date_bounds": DATE_BOUNDS_SQL,
    "daily_orders": DAILY_ORDERS_SQL,
    "order_status": ORDER_STATUS_SQL,
    "product_sales": PRODUCT_SALES_SQL,
    "country": COUNTRY_SQL,
    "reconciliation": RECONCILIATION_SQL,
    "anomalies": ANOMALIES_SQL,
}


def date_bounds(run: Runner) -> tuple[date | None, date | None]:
    df = run(DATE_BOUNDS_SQL, ())
    if df.empty:
        return None, None
    lo, hi = df.iloc[0]["min_date"], df.iloc[0]["max_date"]
    return (
        None if pd.isna(lo) else pd.Timestamp(lo).date(),
        None if pd.isna(hi) else pd.Timestamp(hi).date(),
    )


def daily_orders(run: Runner, start: date, end: date) -> pd.DataFrame:
    return run(DAILY_ORDERS_SQL, (start, end))


def order_status(run: Runner, start: date, end: date) -> pd.DataFrame:
    return run(ORDER_STATUS_SQL, (start, end))


def product_sales(run: Runner, start: date, end: date) -> pd.DataFrame:
    return run(PRODUCT_SALES_SQL, (start, end))


def country_sales(run: Runner, start: date, end: date) -> pd.DataFrame:
    return run(COUNTRY_SQL, (start, end))


def reconciliation(run: Runner) -> pd.DataFrame:
    return run(RECONCILIATION_SQL, ())


def anomalies(run: Runner) -> pd.DataFrame:
    return run(ANOMALIES_SQL, ())
