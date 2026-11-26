"""StreamForge business intelligence dashboard (M7).

    streamlit run src/streamforge/dashboard/app.py

Reads the dbt outputs in ECOMMERCE_STREAMING_DB.ANALYTICS / OPS as STREAMFORGE_ROLE
(key-pair auth from .env, secondary roles disabled). Query results are cached for
10 minutes; STREAMFORGE_WH auto-suspends after 60 s idle. Money is always shown per
currency; rates are recomputed from counts.
"""

from __future__ import annotations

from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from streamforge.dashboard import data, metrics, queries, theme

CACHE_TTL_SECONDS = 600
DEFAULT_WINDOW_DAYS = 30
MAX_WINDOW_DAYS = 366

st.set_page_config(page_title="StreamForge Analytics", page_icon="◆", layout="wide")
st.markdown(theme.CSS, unsafe_allow_html=True)


# ------------------------------------------------------------------ data access


@st.cache_resource(show_spinner=False)
def _runner():
    return data.get_runner()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def _query(sql: str, params: tuple) -> pd.DataFrame:
    return _runner()(sql, params)


def _load(fetch, *args) -> pd.DataFrame | None:
    """Run one cached query; on failure show a safe message and return None."""
    try:
        return fetch(_query, *args)
    except data.DashboardDataError as exc:
        st.error(f"Could not load data from Snowflake: {exc}")
        return None


def empty(message: str) -> None:
    st.markdown(f'<div class="sf-empty">{message}</div>', unsafe_allow_html=True)


def section(title: str) -> None:
    st.markdown(f'<div class="sf-section">{title}</div>', unsafe_allow_html=True)


def table(df: pd.DataFrame, money_columns: tuple[str, ...] = ()) -> None:
    """Show a table with every column wide enough for its header and values.

    Money columns keep 2-decimal formatting. Columns stretch to the container when they
    fit; otherwise the table scrolls horizontally instead of clipping a column.
    """
    config = {}
    for col in df.columns:
        values = df[col].head(500).tolist()
        if col in money_columns:
            texts = [f"{float(v):,.2f}" for v in values if v is not None and not pd.isna(v)]
            width = theme.column_width(col, texts)
            config[col] = st.column_config.NumberColumn(col, format="%.2f", width=width)
        else:
            texts = [
                "" if v is None or (not isinstance(v, str) and pd.isna(v)) else str(v)
                for v in values
            ]
            config[col] = st.column_config.Column(col, width=theme.column_width(col, texts))
    st.dataframe(df, hide_index=True, width="stretch", column_config=config)


def cards(items: list[tuple[str, str, str, str]]) -> None:
    for col, (title, value, subtitle, tone) in zip(st.columns(len(items)), items, strict=True):
        col.markdown(theme.card(title, value, subtitle, tone), unsafe_allow_html=True)


# ------------------------------------------------------------------- sidebar


def sidebar(bounds: tuple[date | None, date | None]) -> tuple[date, date]:
    lo, hi = bounds
    today = date.today()
    end = hi or today
    start = max(lo, end - timedelta(days=DEFAULT_WINDOW_DAYS - 1)) if lo else end
    with st.sidebar:
        st.markdown("### Filters")
        picked = st.date_input(
            "Order date (UTC)", value=(start, end), max_value=max(end, today), key="date_range"
        )
        if isinstance(picked, tuple | list) and len(picked) == 2:
            start, end = picked
        elif isinstance(picked, tuple | list) and len(picked) == 1:
            start = end = picked[0]
        if (end - start).days >= MAX_WINDOW_DAYS:
            start = end - timedelta(days=MAX_WINDOW_DAYS - 1)
            st.caption(f"Range limited to {MAX_WINDOW_DAYS} days.")
        st.markdown("---")
        if st.button("Refresh data", width="stretch"):
            st.cache_data.clear()
            st.rerun()
        st.caption(
            f"Data from ECOMMERCE_STREAMING_DB (ANALYTICS, OPS) as STREAMFORGE_ROLE. "
            f"Cached for {CACHE_TTL_SECONDS // 60} minutes."
        )
        if lo and hi:
            st.caption(f"Available order dates: {lo:%d %b %Y} – {hi:%d %b %Y}")
    return start, end


# ---------------------------------------------------------------------- tabs


def executive(kpis: list[metrics.CurrencyKpis], currency: str | None) -> None:
    k = metrics.kpis_for(kpis, currency)
    if k is None:
        empty("No orders in the selected period.")
        return
    section(f"Revenue · {k.currency}")
    cards(
        [
            (
                "Gross order value",
                metrics.format_money(k.gross_order_value, k.currency),
                "All created orders",
                "accent",
            ),
            (
                "Paid value",
                metrics.format_money(k.paid_value, k.currency),
                "Orders with a successful payment",
                "accent",
            ),
            (
                "Net revenue",
                metrics.format_money(k.net_revenue, k.currency),
                "Paid and not cancelled",
                "accent",
            ),
        ]
    )
    section(f"Orders · {k.currency}")
    cards(
        [
            ("Total orders", metrics.format_count(k.orders), "Created in period", "accent"),
            (
                "Payment success rate",
                metrics.format_rate(k.payment_success_rate),
                "Succeeded ÷ (succeeded + failed)",
                "accent",
            ),
            (
                "Cancellation rate",
                metrics.format_rate(k.cancellation_rate),
                "Cancelled ÷ created",
                "accent",
            ),
        ]
    )
    section("All currencies (never summed together)")
    table(metrics.kpi_table(kpis))


def bar(df: pd.DataFrame, x: str, y: str, x_title: str, y_title: str, horizontal=True):
    enc_value = alt.X(f"{x}:Q", title=x_title) if horizontal else alt.Y(f"{x}:Q", title=x_title)
    enc_cat = (
        alt.Y(f"{y}:N", title=y_title, sort="-x")
        if horizontal
        else alt.X(f"{y}:N", title=y_title, sort="-y")
    )
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=4, color=theme.ACCENT)
        .encode(
            enc_value,
            enc_cat,
            tooltip=[
                alt.Tooltip(f"{y}:N", title=y_title),
                alt.Tooltip(f"{x}:Q", title=x_title, format=",.2f"),
            ],
        )
    )
    return theme.style(chart, height=max(160, 36 * len(df)) if horizontal else 300)


def orders_tab(daily: pd.DataFrame, status: pd.DataFrame, currency: str | None) -> None:
    left, right = st.columns(2)
    with left:
        section("Orders by current status")
        s = metrics.status_counts(status, "current_status")
        if s.empty:
            empty("No orders in the selected period.")
        else:
            st.altair_chart(
                bar(s, "orders", "label", "Orders", "Status"), width="stretch", theme=None
            )
    with right:
        section("Payment status distribution")
        p = metrics.status_counts(status, "payment_status")
        if p.empty:
            empty("No payments in the selected period.")
        else:
            st.altair_chart(
                bar(p, "orders", "label", "Orders", "Payment status"),
                width="stretch",
                theme=None,
            )

    trend = metrics.daily_trend(daily)
    section("Orders per day")
    if trend.empty:
        empty("No orders in the selected period.")
    else:
        if trend["order_date"].nunique() == 1:
            st.markdown(
                '<div class="sf-note">Only one day of data in this range.</div>',
                unsafe_allow_html=True,
            )
        chart = (
            alt.Chart(trend)
            .mark_line(point=True, strokeWidth=2)
            .encode(
                x=alt.X("order_date:T", title="Order date (UTC)"),
                y=alt.Y("orders_created:Q", title="Orders"),
                color=alt.Color(
                    "currency:N", title="Currency", scale=alt.Scale(range=theme.SERIES)
                ),
                tooltip=["order_date:T", "currency:N", "orders_created:Q"],
            )
        )
        st.altair_chart(theme.style(chart), width="stretch", theme=None)

    section(f"Net revenue per day · {currency or '—'}")
    rev = trend[trend["currency"] == currency] if currency else trend.iloc[0:0]
    if rev.empty:
        empty("No revenue in the selected period for this currency.")
    else:
        chart = (
            alt.Chart(rev)
            .mark_bar(color=theme.ACCENT, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("yearmonthdate(order_date):O", title="Order date (UTC)"),
                y=alt.Y("net_revenue:Q", title=f"Net revenue ({currency})"),
                tooltip=[
                    alt.Tooltip("order_date:T", title="Date"),
                    alt.Tooltip("net_revenue:Q", title="Net revenue", format=",.2f"),
                ],
            )
        )
        st.altair_chart(theme.style(chart), width="stretch", theme=None)


def products_tab(products: pd.DataFrame, currency: str | None, top_n: int = 10) -> None:
    p = metrics.products_for(products, currency)
    if p.empty:
        empty("No product sales in the selected period for this currency.")
        return
    top = p.head(top_n).assign(label=lambda d: d["product_name"] + " (" + d["sku"] + ")")
    left, right = st.columns(2)
    with left:
        section("Top-selling products · units")
        st.altair_chart(
            bar(top, "units", "label", "Units sold", "Product"),
            width="stretch",
            theme=None,
        )
    with right:
        section(f"Product net revenue · {currency}")
        st.altair_chart(
            bar(top, "net_value", "label", f"Net revenue ({currency})", "Product"),
            width="stretch",
            theme=None,
        )
    section(f"SKU performance · {currency}")
    sku_table = p.rename(
        columns={
            "sku": "SKU",
            "product_name": "Product",
            "units": "Units",
            "orders": "Orders",
            "gross_value": f"Gross value ({currency})",
            "net_value": f"Net revenue ({currency})",
        }
    ).drop(columns=["currency"])
    table(sku_table, money_columns=tuple(c for c in sku_table.columns if "(" in c))


def geography_tab(countries: pd.DataFrame, currency: str | None) -> None:
    c = metrics.countries_for(countries, currency)
    if c.empty:
        empty("No orders in the selected period for this currency.")
        return
    left, right = st.columns(2)
    with left:
        section("Orders by shipping country")
        st.altair_chart(
            bar(c, "orders", "shipping_country", "Orders", "Country (ISO)"),
            width="stretch",
            theme=None,
        )
    with right:
        section(f"Net revenue by shipping country · {currency}")
        st.altair_chart(
            bar(c, "net_revenue", "shipping_country", f"Net revenue ({currency})", "Country (ISO)"),
            width="stretch",
            theme=None,
        )
    st.markdown(
        '<div class="sf-note">Countries are the ISO codes from the order events '
        "(shipping_country); no other location data exists.</div>",
        unsafe_allow_html=True,
    )
    country_table = c.rename(
        columns={
            "shipping_country": "Country",
            "orders": "Orders",
            "gross_order_value": f"Gross order value ({currency})",
            "net_revenue": f"Net revenue ({currency})",
        }
    ).drop(columns=["currency"])
    table(country_table, money_columns=tuple(c for c in country_table.columns if "(" in c))


def pipeline_tab(recon: pd.DataFrame, anomalies: pd.DataFrame) -> None:
    s = metrics.pipeline_summary(recon)
    if s.batches == 0:
        empty("No ingestion batches recorded yet.")
        return
    health_tone = "ok" if s.healthy else "bad"
    section("Ingestion")
    cards(
        [
            ("Batches loaded", metrics.format_count(s.batches), "Consumer audit rows", "accent"),
            ("Records consumed", metrics.format_count(s.records_consumed), "From Kafka", "accent"),
            (
                "Rows inserted",
                metrics.format_count(s.rows_inserted),
                f"{metrics.format_count(s.duplicates_skipped)} duplicates skipped",
                "accent",
            ),
            (
                "Reconciliation",
                "All OK" if s.healthy else f"{s.failing_batches} failing",
                "RAW rows vs. audit, per batch",
                health_tone,
            ),
        ]
    )
    section("Dead-letter queue")
    cards(
        [
            (
                "Records sent to DLQ",
                metrics.format_count(s.dlq_records),
                "As recorded by the consumer",
                "warn" if s.dlq_records else "ok",
            ),
            ("DLQ rate", metrics.format_rate(s.dlq_rate), "DLQ ÷ consumed", "accent"),
            (
                "Last load (UTC)",
                s.last_loaded_at.strftime("%d %b %Y %H:%M")
                if s.last_loaded_at is not None
                else "—",
                "Latest batch",
                "accent",
            ),
        ]
    )
    st.markdown(
        '<div class="sf-note">Dead-letter figures come from the consumer audit in '
        "Snowflake. The DLQ messages themselves stay in Kafka "
        "(ecommerce.events.dlq) and are not queried here.</div>",
        unsafe_allow_html=True,
    )

    section("Data quality anomalies")
    a = metrics.anomaly_counts(anomalies)
    if a.empty:
        st.success("No data quality anomalies detected.", icon="✅")
    else:
        table(a.rename(columns={"anomaly": "Anomaly", "orders": "Orders"}))
        with st.expander("Anomaly details"):
            table(anomalies)

    section("Recent batches")
    view = metrics.ensure_columns(recon, metrics.RECON_COLUMNS)[
        [
            "loaded_at_utc",
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
        ]
    ].rename(columns=lambda c: c.replace("_", " ").capitalize())
    table(view)


# ---------------------------------------------------------------------- page


def main() -> None:
    bounds = _load(queries.date_bounds) or (None, None)
    start, end = sidebar(bounds)

    daily = _load(queries.daily_orders, start, end)
    recon = _load(queries.reconciliation)
    summary = metrics.pipeline_summary(recon if recon is not None else pd.DataFrame())
    fresh = (
        summary.last_loaded_at.strftime("%d %b %Y %H:%M UTC")
        if summary.last_loaded_at is not None
        else "no loads yet"
    )
    st.markdown(
        '<div class="sf-header"><div><div class="sf-title">StreamForge <span>Analytics</span>'
        '</div><div class="sf-subtitle">Real-time e-commerce pipeline · Kafka → Snowflake → dbt'
        f'</div></div><div class="sf-badge">Last data load <b>{fresh}</b> · '
        f"{start:%d %b %Y} – {end:%d %b %Y}</div></div>",
        unsafe_allow_html=True,
    )
    if daily is None:
        st.stop()

    kpis = metrics.currency_kpis(daily)
    options = [k.currency for k in kpis]
    with st.sidebar:
        currency = (
            st.selectbox(
                "Currency",
                options,
                key="currency",
                help="Monetary figures are shown for one currency at a time.",
            )
            if options
            else None
        )

    tabs = st.tabs(["Executive Overview", "Orders", "Products", "Geography", "Pipeline Health"])
    with tabs[0]:
        executive(kpis, currency)
    with tabs[1]:
        status = _load(queries.order_status, start, end)
        orders_tab(daily, status if status is not None else pd.DataFrame(), currency)
    with tabs[2]:
        products = _load(queries.product_sales, start, end)
        products_tab(products if products is not None else pd.DataFrame(), currency)
    with tabs[3]:
        countries = _load(queries.country_sales, start, end)
        geography_tab(countries if countries is not None else pd.DataFrame(), currency)
    with tabs[4]:
        anomalies = _load(queries.anomalies)
        pipeline_tab(
            recon if recon is not None else pd.DataFrame(),
            anomalies if anomalies is not None else pd.DataFrame(),
        )


main()
