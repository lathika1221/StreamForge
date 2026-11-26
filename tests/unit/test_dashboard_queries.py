"""Unit tests for dashboard SQL and the Snowflake data layer (no Snowflake connection)."""

import re
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from streamforge.common.config import load_snowflake_settings
from streamforge.dashboard import data, queries

# ------------------------------------------------------------------------- SQL


@pytest.mark.parametrize("name,sql", queries.ALL_SQL.items())
def test_queries_are_read_only_and_scoped(name, sql):
    flat = " ".join(sql.split()).lower()
    assert flat.startswith("select ")
    assert not re.search(r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant)\b", flat)
    assert "ecommerce_streaming_db.analytics." in flat or "ecommerce_streaming_db.ops." in flat
    assert ".raw." not in flat and "latz" not in flat


@pytest.mark.parametrize("name", ["daily_orders", "order_status", "product_sales", "country"])
def test_date_filters_are_bound_parameters(name):
    sql = queries.ALL_SQL[name]
    assert "between %s and %s" in " ".join(sql.split())
    assert "{" not in sql  # nothing left to string-format


@pytest.mark.parametrize("name", ["product_sales", "country"])
def test_money_is_always_grouped_by_currency(name):
    flat = " ".join(queries.ALL_SQL[name].split()).lower()
    group_by = flat.split("group by", 1)[1]
    assert "currency" in group_by


def test_unbounded_tables_are_limited():
    assert "limit 200" in queries.RECONCILIATION_SQL
    assert "limit 500" in queries.ANOMALIES_SQL


def test_query_functions_pass_dates_as_params():
    calls = []

    def run(sql, params):
        calls.append((sql, params))
        return pd.DataFrame()

    start, end = date(2026, 11, 1), date(2026, 11, 30)
    queries.daily_orders(run, start, end)
    queries.country_sales(run, start, end)
    queries.reconciliation(run)
    assert [p for _, p in calls] == [(start, end), (start, end), ()]


def test_date_bounds_handles_empty_and_null():
    assert queries.date_bounds(lambda s, p: pd.DataFrame()) == (None, None)
    nulls = pd.DataFrame([{"min_date": None, "max_date": None}])
    assert queries.date_bounds(lambda s, p: nulls) == (None, None)
    real = pd.DataFrame([{"min_date": date(2026, 11, 25), "max_date": date(2026, 11, 25)}])
    assert queries.date_bounds(lambda s, p: real) == (date(2026, 11, 25), date(2026, 11, 25))


# ------------------------------------------------------------------ data layer


@pytest.fixture
def settings(tmp_path):
    key = tmp_path / "k.p8"
    key.write_text("x")
    return load_snowflake_settings(
        {
            "SNOWFLAKE_ACCOUNT": "org-acct",
            "SNOWFLAKE_USER": "U",
            "SNOWFLAKE_WAREHOUSE": "WH",
            "SNOWFLAKE_DATABASE": "DB",
            "SNOWFLAKE_ROLE": "STREAMFORGE_ROLE",
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "pp-SECRET",
        }
    )


class FakeCursor:
    def __init__(self, conn):
        self.conn, self.description, self._rows = conn, None, []

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if "boom" in sql:
            raise RuntimeError("Numeric value 'CUST-000123' is not recognized; pp-SECRET")
        if sql.startswith("select current_role()"):
            self.description, self._rows = [("ROLE",)], [(self.conn.role,)]
        else:
            self.description, self._rows = (
                [("ORDER_DATE",), ("CURRENCY",)],
                [(date(2026, 11, 25), "INR")],
            )

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, role="STREAMFORGE_ROLE"):
        self.role, self.executed, self.closed = role, [], False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def test_runner_returns_lowercase_dataframe_and_disables_secondary_roles(settings):
    conn = FakeConn()
    runner = data.SnowflakeRunner(settings, connect_fn=lambda **_: conn)
    df = runner(
        "select order_date, currency from t where d between %s and %s",
        (date(2026, 11, 1), date(2026, 11, 30)),
    )
    assert list(df.columns) == ["order_date", "currency"] and len(df) == 1
    assert conn.executed[0][0] == "USE SECONDARY ROLES NONE"  # from common.snowflake.connect
    assert conn.executed[-1][1] == (date(2026, 11, 1), date(2026, 11, 30))


@pytest.mark.parametrize("role", ["ACCOUNTADMIN", "SYSADMIN", None])
def test_runner_refuses_non_application_roles(settings, role):
    import dataclasses

    with pytest.raises(data.DashboardDataError, match="STREAMFORGE_ROLE"):
        data.SnowflakeRunner(dataclasses.replace(settings, role=role))


def test_runner_rejects_session_with_unexpected_role(settings):
    conn = FakeConn(role="PUBLIC")
    runner = data.SnowflakeRunner(settings, connect_fn=lambda **_: conn)
    with pytest.raises(data.DashboardDataError, match="expected STREAMFORGE_ROLE"):
        runner("select 1")
    assert conn.closed


def test_query_errors_are_scrubbed_and_reconnect(settings):
    conns = []

    def factory(**_):
        conns.append(FakeConn())
        return conns[-1]

    runner = data.SnowflakeRunner(settings, connect_fn=factory)
    with pytest.raises(data.DashboardDataError) as exc:
        runner("select boom")
    msg = str(exc.value)
    assert "CUST-000123" not in msg and "pp-SECRET" not in msg and "<redacted>" in msg
    assert exc.value.__cause__ is None and conns[0].closed
    runner("select ok")
    assert len(conns) == 2  # reconnected after the failure


def test_dashboard_code_never_references_credentials_or_admin_roles():
    pkg = Path(data.__file__).parent
    for f in pkg.glob("*.py"):
        text = f.read_text("utf-8")
        assert "PASSPHRASE" not in text and "PRIVATE_KEY" not in text
        assert "ACCOUNTADMIN" not in text
