"""Headless UI tests for the Streamlit dashboard (AppTest) with fake data - no Snowflake."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit.testing.v1 import AppTest

from streamforge.dashboard import data, metrics, queries, theme

APP = str(Path(__file__).resolve().parents[2] / "src" / "streamforge" / "dashboard" / "app.py")
D = date(2026, 11, 25)


def frames(**overrides):
    """Fake query results keyed by SQL text (two currencies, one anomaly)."""
    base = {
        queries.DATE_BOUNDS_SQL: pd.DataFrame([{"min_date": D, "max_date": D}]),
        queries.DAILY_ORDERS_SQL: pd.DataFrame(
            [
                (
                    D,
                    "INR",
                    3,
                    3,
                    0,
                    0,
                    3,
                    0,
                    Decimal("74137.00"),
                    Decimal("74137.00"),
                    Decimal("74137.00"),
                ),
                (D, "GBP", 2, 1, 1, 1, 0, 0, Decimal("50.00"), Decimal("30.00"), Decimal("0.00")),
            ],
            columns=metrics.DAILY_COLUMNS,
        ),
        queries.ORDER_STATUS_SQL: pd.DataFrame(
            [
                ("shipped", "succeeded", "INR", 3),
                ("cancelled", "succeeded", "GBP", 1),
                ("payment_failed", "failed", "GBP", 1),
            ],
            columns=["current_status", "payment_status", "currency", "orders"],
        ),
        queries.PRODUCT_SALES_SQL: pd.DataFrame(
            [
                ("SKU-1001", "INR", "Wireless Mouse", 4, 2, Decimal("5196.00"), Decimal("5196.00")),
                ("SKU-1012", "INR", None, 1, 1, Decimal("449.00"), Decimal("449.00")),
                ("SKU-9", "GBP", "Cable", 2, 1, Decimal("50.00"), Decimal("0.00")),
            ],
            columns=metrics.PRODUCT_COLUMNS,
        ),
        queries.COUNTRY_SQL: pd.DataFrame(
            [
                ("IN", "INR", 3, Decimal("74137.00"), Decimal("74137.00")),
                ("UNKNOWN", "GBP", 2, Decimal("50.00"), Decimal("0.00")),
            ],
            columns=metrics.COUNTRY_COLUMNS,
        ),
        queries.RECONCILIATION_SQL: pd.DataFrame(
            [
                (
                    "b1",
                    10,
                    9,
                    9,
                    0,
                    1,
                    9,
                    True,
                    True,
                    True,
                    pd.Timestamp("2026-11-25 08:55:33", tz="UTC"),
                    pd.Timestamp("2026-11-25 08:55:34", tz="UTC"),
                )
            ],
            columns=metrics.RECON_COLUMNS,
        ),
        queries.ANOMALIES_SQL: pd.DataFrame(
            [("sequence_gap", "ORD-1", "latest sequence 3, events 2")],
            columns=["anomaly", "order_id", "detail"],
        ),
    }
    base.update(overrides)
    return base


class FakeRunner:
    def __init__(self, results, fail=False):
        self.results, self.fail, self.calls = results, fail, []

    def __call__(self, sql, params=()):
        self.calls.append((sql, params))
        if self.fail:
            raise data.DashboardDataError("query failed: RuntimeError: '<redacted>'")
        return self.results[sql].copy()


def run_app(monkeypatch, runner):
    st.cache_data.clear()
    st.cache_resource.clear()
    monkeypatch.setattr(data, "get_runner", lambda: runner)
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    return at


def text(at):
    return "\n".join(m.value for m in at.markdown)


def test_all_tabs_render_with_currency_scoped_kpis(monkeypatch):
    runner = FakeRunner(frames())
    at = run_app(monkeypatch, runner)
    assert not at.exception and not at.error
    assert [t.label for t in at.tabs] == [
        "Executive Overview",
        "Orders",
        "Products",
        "Geography",
        "Pipeline Health",
    ]
    page = text(at)
    # default currency is the first alphabetically (GBP); INR is available in the selector
    assert at.selectbox(key="currency").options == ["GBP", "INR"]
    assert "£50.00" in page and "₹" not in page.split("All currencies")[0]
    # per-currency table: one row each, never a combined total
    table = at.dataframe[0].value
    assert list(table["Currency"]) == ["GBP", "INR"]
    assert table.loc[table["Currency"] == "INR", "Net revenue"].item() == "₹74,137.00"
    # pipeline: totals, DLQ and anomaly surfaced
    assert "Records sent to DLQ" in page and "All OK" in page
    assert any("sequence_gap" in str(df.value.to_dict()) for df in at.dataframe)


def test_switching_currency_rescopes_money(monkeypatch):
    at = run_app(monkeypatch, FakeRunner(frames()))
    at.selectbox(key="currency").set_value("INR").run()
    assert not at.exception
    head = text(at).split("All currencies")[0]
    assert "₹74,137.00" in head and "£" not in head
    assert "100.0%" in head  # INR payment success rate


def test_queries_use_date_filter_and_are_cached(monkeypatch):
    runner = FakeRunner(frames())
    at = run_app(monkeypatch, runner)
    dated = [p for s, p in runner.calls if "between %s and %s" in s]
    assert dated and all(p == (D, D) for p in dated)  # default window clipped to available data
    calls = len(runner.calls)
    at.run()  # rerun: served from st.cache_data
    assert len(runner.calls) == calls


def test_empty_datasets_render_empty_states(monkeypatch):
    empty = {sql: pd.DataFrame() for sql in queries.ALL_SQL.values()}
    at = run_app(monkeypatch, FakeRunner(empty))
    assert not at.exception and not at.error
    page = text(at)
    assert "No orders in the selected period." in page
    assert "No ingestion batches recorded yet." in page


def test_missing_values_are_handled(monkeypatch):
    at = run_app(monkeypatch, FakeRunner(frames()))
    at.selectbox(key="currency").set_value("INR").run()
    assert not at.exception
    sku_tables = [df.value for df in at.dataframe if "SKU" in df.value.columns]
    names = dict(zip(sku_tables[0]["SKU"], sku_tables[0]["Product"], strict=True))
    assert names["SKU-1012"] == "SKU-1012"  # null product_name falls back to the SKU


def test_data_errors_show_a_safe_message_not_a_traceback(monkeypatch):
    at = run_app(monkeypatch, FakeRunner(frames(), fail=True))
    assert not at.exception
    assert at.error and "Could not load data from Snowflake" in at.error[0].value
    assert "Traceback" not in at.error[0].value


# ----------------------------------------------------- table layout (no clipping)


def test_column_width_fits_header_and_values():
    # header longer than every value (the clipped "Payment success" case)
    assert theme.column_width("Payment success", ["100.0%"]) >= round(
        len("Payment success") * theme.COL_CHAR_PX + theme.COL_PADDING_PX
    )
    # a value longer than the header wins
    long_value = "x" * 30
    assert theme.column_width("Id", [long_value]) >= round(30 * theme.COL_CHAR_PX)
    # clamped to sensible bounds; very long content scrolls instead of growing forever
    assert theme.column_width("A", []) == theme.COL_MIN_PX
    assert theme.column_width("A", ["y" * 500]) == theme.COL_MAX_PX


def test_every_table_gives_every_column_room_for_its_header(monkeypatch):
    """Executive, Products, Geography and Pipeline tables: each column has an explicit
    width that fits its full heading, so no heading is clipped at any viewport width
    (the table scrolls horizontally if the columns exceed the page)."""
    at = run_app(monkeypatch, FakeRunner(frames()))
    at.selectbox(key="currency").set_value("INR").run()
    assert not at.exception
    assert len(at.dataframe) >= 5  # summary, SKU, country, anomalies (+details), batches
    for df in at.dataframe:
        config = json.loads(df.proto.columns)
        for column in df.value.columns:
            assert column in config, f"column {column!r} has no width"
            needed = round(len(str(column)) * theme.COL_CHAR_PX + theme.COL_PADDING_PX)
            assert config[column]["width"] >= min(needed, theme.COL_MAX_PX), column
    summary = json.loads(at.dataframe[0].proto.columns)
    assert summary["Payment success"]["width"] >= round(
        len("Payment success") * theme.COL_CHAR_PX + theme.COL_PADDING_PX
    )


def test_money_columns_keep_two_decimal_format(monkeypatch):
    at = run_app(monkeypatch, FakeRunner(frames()))
    at.selectbox(key="currency").set_value("INR").run()
    sku = next(df for df in at.dataframe if "SKU" in df.value.columns)
    config = json.loads(sku.proto.columns)
    assert config["Net revenue (INR)"]["type_config"]["format"] == "%.2f"


def test_app_uses_no_deprecated_container_width_argument():
    assert "use_container_width" not in Path(APP).read_text("utf-8")


def test_production_mode_keeps_the_snowflake_captions(monkeypatch):
    """With a real (non-demo) runner the captions still describe the Snowflake source."""
    at = run_app(monkeypatch, FakeRunner(frames()))
    assert not at.exception
    sidebar = " ".join(c.value for c in at.sidebar.caption)
    assert "Data from ECOMMERCE_STREAMING_DB (ANALYTICS, OPS) as STREAMFORGE_ROLE." in sidebar
    assert "Demo data" not in sidebar
    assert "Dead-letter figures come from the consumer audit in Snowflake." in text(at)
    assert not at.warning  # no demo banner


def test_recent_batches_table_shows_every_batch(monkeypatch):
    batch = frames()[queries.RECONCILIATION_SQL].iloc[0].to_dict()
    many = pd.DataFrame([{**batch, "batch_id": f"{i:032x}"} for i in range(12)])
    at = run_app(monkeypatch, FakeRunner(frames(**{queries.RECONCILIATION_SQL: many})))
    view = next(df.value for df in at.dataframe if "Checks" in df.value.columns)
    assert len(view) == 12 and list(view.columns) == metrics.RECENT_BATCH_COLUMNS
