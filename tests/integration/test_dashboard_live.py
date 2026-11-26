"""Live dashboard check (M7): render the real app headlessly against Snowflake.

Reads the existing dbt outputs as STREAMFORGE_ROLE (reads .env). One warehouse resume
(~7 sub-second queries); the warehouse is suspended at the end.
Run with: python -m pytest -m snowflake tests/integration/test_dashboard_live.py
"""

from pathlib import Path

import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from streamforge.dashboard import data, queries

pytestmark = pytest.mark.snowflake

APP = str(Path(__file__).resolve().parents[2] / "src" / "streamforge" / "dashboard" / "app.py")


@pytest.fixture(scope="module")
def runner():
    r = data.SnowflakeRunner.from_env()
    yield r
    r.suspend_warehouse()
    r.close()


def test_dashboard_renders_live_data_matching_dbt(runner, monkeypatch):
    # Expected values straight from the dbt aggregate (independent of the app code).
    lo, hi = queries.date_bounds(runner)
    expected = runner(queries.DAILY_ORDERS_SQL, (lo, hi))
    assert not expected.empty, "dbt models are empty; run the M6 build first"
    currency = sorted(expected["currency"].unique())[0]
    net = float(pd.to_numeric(expected.loc[expected["currency"] == currency, "net_revenue"]).sum())

    st.cache_data.clear()
    st.cache_resource.clear()
    monkeypatch.setattr(data, "get_runner", lambda: runner)
    at = AppTest.from_file(APP, default_timeout=120)
    at.run()

    assert not at.exception and not at.error
    assert [t.label for t in at.tabs] == [
        "Executive Overview",
        "Orders",
        "Products",
        "Geography",
        "Pipeline Health",
    ]
    page = "\n".join(m.value for m in at.markdown)
    assert at.selectbox(key="currency").value == currency
    assert f"{net:,.2f}" in page  # net revenue card equals the dbt aggregate
    assert "Records sent to DLQ" in page and "Reconciliation" in page
    tables = [df.value for df in at.dataframe]
    assert any("Currency" in t.columns for t in tables)
    assert any("SKU" in t.columns for t in tables)
    assert any("Country" in t.columns for t in tables)
