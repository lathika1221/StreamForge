"""M9.3 offline dashboard demo: simulated data, no Snowflake (dashboard/demo.py)."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from streamforge.consumer.records import KafkaRecord
from streamforge.dashboard import data, demo, metrics, queries
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

APP = str(Path(__file__).resolve().parents[2] / "src" / "streamforge" / "dashboard" / "app.py")
FULL = (date(2000, 1, 1), date(2100, 1, 1))


@pytest.fixture(scope="module")
def runner():
    return demo.DemoRunner.build()


# ------------------------------------------------------------------ switch


@pytest.mark.parametrize(
    ("value", "enabled"),
    [
        ("1", True),
        ("true", True),
        (" YES ", True),
        ("on", True),
        ("", False),
        ("0", False),
        ("false", False),
        ("demo", False),
    ],
)
def test_demo_mode_needs_an_explicit_switch(value, enabled):
    assert demo.demo_enabled({demo.DEMO_ENV: value}) is enabled
    assert demo.demo_enabled({}) is False


def test_demo_mode_never_creates_a_snowflake_runner(monkeypatch):
    def forbidden():
        raise AssertionError("Snowflake must not be used in demo mode")

    monkeypatch.setattr(data.SnowflakeRunner, "from_env", staticmethod(forbidden))
    monkeypatch.setenv(demo.DEMO_ENV, "1")
    assert isinstance(data.get_runner(), demo.DemoRunner)

    monkeypatch.delenv(demo.DEMO_ENV)
    with pytest.raises(AssertionError, match="must not be used"):
        data.get_runner()  # without the switch the real (Snowflake) runner is requested


# ------------------------------------------------------------------ query results


def test_every_dashboard_query_returns_its_columns(runner):
    expected = {
        "date_bounds": ["min_date", "max_date"],
        "daily_orders": metrics.DAILY_COLUMNS,
        "order_status": ["current_status", "payment_status", "currency", "orders"],
        "product_sales": metrics.PRODUCT_COLUMNS,
        "country": metrics.COUNTRY_COLUMNS,
        "reconciliation": metrics.RECON_COLUMNS,
        "anomalies": ["anomaly", "order_id", "detail"],
    }
    for name, sql in queries.ALL_SQL.items():
        df = runner(sql, FULL if "%s" in sql else ())
        assert list(df.columns) == list(expected[name]), name
        assert len(df) > 0 or name == "anomalies", name


def test_unknown_query_is_a_safe_dashboard_error(runner):
    with pytest.raises(data.DashboardDataError, match="demo mode"):
        runner("select 1", ())


def test_demo_data_is_deterministic():
    a, b = demo.DemoRunner.build(), demo.DemoRunner.build()
    for sql in queries.ALL_SQL.values():
        params = FULL if "%s" in sql else ()
        pd.testing.assert_frame_equal(a(sql, params), b(sql, params))


def test_batches_account_for_every_simulated_record(runner):
    records = demo.simulate(demo.DemoConfig())
    recon = runner(queries.RECONCILIATION_SQL, ())
    assert recon["records_consumed"].sum() == len(records)
    assert (recon["records_consumed"] == recon["valid_records"] + recon["dlq_records"]).all()
    assert (recon["valid_records"] == recon["rows_inserted"] + recon["duplicates_skipped"]).all()
    assert recon["dlq_records"].sum() > 0  # invalid records are dead-lettered, not loaded
    assert recon["loaded_at_utc"].is_monotonic_decreasing  # newest first, as the query orders


def test_money_is_per_currency_and_matches_the_orders(runner):
    daily = runner(queries.DAILY_ORDERS_SQL, FULL)
    assert set(daily["currency"]) == {"INR", "GBP", "EUR", "USD"}
    assert not daily.duplicated(["order_date", "currency"]).any()
    orders = runner.orders[runner.orders["has_order_created"]]
    for currency, g in daily.groupby("currency"):
        expected = sum(orders.loc[orders["currency"] == currency, "order_total"], Decimal(0))
        assert sum(g["gross_order_value"], Decimal(0)) == expected
        assert (g["net_revenue"] <= g["paid_value"]).all()
        assert (g["paid_value"] <= g["gross_order_value"]).all()


def test_date_window_filters_every_dated_query(runner):
    bounds = runner(queries.DATE_BOUNDS_SQL, ()).iloc[0]
    day = bounds["min_date"]
    one_day = runner(queries.DAILY_ORDERS_SQL, (day, day))
    assert set(one_day["order_date"]) == {day}
    empty = (date(1999, 1, 1), date(1999, 1, 2))
    for sql in (
        queries.DAILY_ORDERS_SQL,
        queries.ORDER_STATUS_SQL,
        queries.PRODUCT_SALES_SQL,
        queries.COUNTRY_SQL,
    ):
        assert runner(sql, empty).empty


# ------------------------------------------------------------------ derivation rules


def _order_records(seed=5):
    """All valid events of the first simulated order, as (event_type, json bytes)."""
    cfg = SimulatorConfig(seed=seed, start_time=datetime(2026, 11, 20, tzinfo=UTC))
    recs = [r for r in EventSimulator(cfg).iter_records(3) if r.is_valid]
    first = recs[0].data["order_id"]
    return [
        (r.data["event_type"], r.data, r.value.encode())
        for r in recs
        if r.data["order_id"] == first
    ]


def _kafka(values):
    return [KafkaRecord("t", 0, i, None, v, 0) for i, v in enumerate(values)]


def test_status_follows_sequence_number_not_arrival_order():
    events = _order_records()
    assert events[-1][0] in ("shipment_dispatched", "order_cancelled")
    arrived = [v for _, _, v in reversed(events)]  # terminal event arrives first
    staged, conflicts = demo._staged(_kafka(arrived))
    orders, _ = demo._orders(staged)
    assert len(orders) == 1 and not conflicts
    expected = "shipped" if events[-1][0] == "shipment_dispatched" else "cancelled"
    assert orders.iloc[0]["current_status"] == expected
    assert not orders.iloc[0]["has_sequence_gap"]


def test_missing_order_created_gap_and_conflicting_duplicate_are_flagged():
    events = _order_records()
    without_created = [v for t, _, v in events if t != "order_created"]
    # same event_id, different content -> conflicting duplicate (first copy kept)
    changed = dict(events[1][1])
    changed["source"] = "replayed-elsewhere"
    values = without_created + [json.dumps(changed).encode()]
    staged, conflicts = demo._staged(_kafka(values))
    orders, items = demo._orders(staged)
    anomalies = demo._anomalies(orders, conflicts)
    kinds = set(anomalies["anomaly"])
    assert {"missing_order_created", "sequence_gap", "conflicting_duplicate_event"} <= kinds
    assert items.empty  # no order_created -> no item lines, kept out of money figures
    assert not orders.iloc[0]["has_order_created"]


# ------------------------------------------------------------------ app


def test_app_renders_in_demo_mode_with_a_visible_banner(monkeypatch):
    def forbidden():
        raise AssertionError("Snowflake must not be used in demo mode")

    st.cache_data.clear()
    st.cache_resource.clear()
    monkeypatch.setattr(data.SnowflakeRunner, "from_env", staticmethod(forbidden))
    monkeypatch.setenv(demo.DEMO_ENV, "1")
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    assert not at.exception and not at.error
    assert any("Demo mode" in w.value for w in at.warning)
    assert at.selectbox(key="currency").options == ["EUR", "GBP", "INR", "USD"]
    assert [t.label for t in at.tabs][-1] == "Pipeline Health"
    st.cache_data.clear()
    st.cache_resource.clear()


# ------------------------------------------------- demo captions and complete tables

# Widest table that fits the main column at a 1440 px viewport (1440 - 300 sidebar - 2 x 80
# padding ~ 980 px), with a margin.
FIT_PX = 960


def _run_demo_app(monkeypatch, heights=None):
    def forbidden():
        raise AssertionError("Snowflake must not be used in demo mode")

    st.cache_data.clear()
    st.cache_resource.clear()
    monkeypatch.setattr(data.SnowflakeRunner, "from_env", staticmethod(forbidden))
    monkeypatch.setenv(demo.DEMO_ENV, "1")
    if heights is not None:
        original = st.dataframe

        def spy(df, *args, **kwargs):
            heights.append((list(df.columns), kwargs.get("height")))
            return original(df, *args, **kwargs)

        monkeypatch.setattr(st, "dataframe", spy)
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    at.selectbox(key="currency").set_value("INR").run()
    assert not at.exception and not at.error
    return at


def test_demo_captions_describe_simulated_data_not_snowflake(monkeypatch):
    at = _run_demo_app(monkeypatch)
    sidebar = " ".join(c.value for c in at.sidebar.caption)
    assert "Demo data: simulated locally" in sidebar and "no Snowflake connection" in sidebar
    assert "ECOMMERCE_STREAMING_DB" not in sidebar and "STREAMFORGE_ROLE" not in sidebar
    page = "\n".join(m.value for m in at.markdown)
    assert "Demo data: dead-letter figures are derived from the simulated batches" in page
    assert "consumer audit in Snowflake" not in page
    st.cache_data.clear()
    st.cache_resource.clear()


def test_demo_tables_show_every_row_and_every_column(monkeypatch):
    heights = []
    at = _run_demo_app(monkeypatch, heights)
    tables = {tuple(df.value.columns)[0]: df for df in at.dataframe}

    sku = tables["SKU"].value
    assert len(sku) == 12  # all INR SKUs, not just the 10 a default-height table shows

    batches = tables["Loaded (UTC)"]
    assert list(batches.value.columns) == metrics.RECENT_BATCH_COLUMNS
    assert len(batches.value) == 12  # all demo batches
    widths = json.loads(batches.proto.columns)
    assert sum(widths[c]["width"] for c in metrics.RECENT_BATCH_COLUMNS) <= FIT_PX

    # every table on the page is sized to its rows (no inner scroll bar hiding rows)
    assert heights and all(h == "content" for _, h in heights), heights
    st.cache_data.clear()
    st.cache_resource.clear()
