"""Unit tests for streamforge.admin.snowflake_setup. No Snowflake connection is made."""

import re

import pytest

from streamforge.admin.snowflake_setup import (
    DATABASE,
    EXPECTED_ROLE_GRANTS,
    MONITOR,
    PROTECTED_DATABASE,
    ROLE,
    SCHEMAS,
    SETUP_SQL,
    WAREHOUSE,
    Session,
    apply,
    render,
    split_statements,
)

SQL = SETUP_SQL.read_text("utf-8")


def _statements(user="LATZ"):
    return split_statements(render(SQL, user))


def test_setup_sql_is_found_in_repo():
    assert SETUP_SQL.is_file() and SETUP_SQL.parts[-3:] == ("infra", "snowflake", "setup.sql")


def test_split_drops_comments_and_blank_statements():
    sql = "-- header; with semicolon\nUSE ROLE SYSADMIN;\n\n  -- note\nSELECT 1;\n;"
    assert split_statements(sql) == ["USE ROLE SYSADMIN", "SELECT 1"]


def test_render_substitutes_user_and_rejects_injection():
    assert "TO USER LATZ" in render(SQL, "LATZ")
    for bad in ("LATZ; DROP DATABASE X", "a b", '"LATZ"', "", "1abc"):
        with pytest.raises(ValueError):
            render(SQL, bad)


def test_every_create_is_idempotent_and_nothing_is_destructive():
    for stmt in _statements():
        upper = stmt.upper()
        if upper.startswith("CREATE"):
            assert "IF NOT EXISTS" in upper, stmt
        assert "OR REPLACE" not in upper, stmt
        assert not re.match(r"(DROP|DELETE|TRUNCATE|REVOKE|UNDROP)\b", upper), stmt


def test_setup_touches_only_streamforge_objects():
    allowed = {DATABASE, WAREHOUSE, ROLE, MONITOR, "SYSADMIN", "LATZ"}
    for stmt in _statements():
        upper = stmt.upper()
        assert PROTECTED_DATABASE not in upper
        assert "COMPUTE_WH" not in upper and "SNOWFLAKE_LEARNING" not in upper
        if upper.startswith(("ALTER WAREHOUSE", "GRANT")):
            assert any(name in upper for name in allowed), stmt


def test_warehouse_cost_settings():
    create = next(s for s in _statements() if s.startswith("CREATE WAREHOUSE"))
    for clause in (
        "WAREHOUSE_SIZE = XSMALL",
        "AUTO_SUSPEND = 60",
        "AUTO_RESUME = TRUE",
        "INITIALLY_SUSPENDED = TRUE",
    ):
        assert clause in create
    assert any(
        s == f"ALTER WAREHOUSE {WAREHOUSE} SET RESOURCE_MONITOR = {MONITOR}" for s in _statements()
    )


def test_resource_monitor_quota_and_triggers():
    monitor = next(s for s in _statements() if s.startswith("CREATE RESOURCE MONITOR"))
    flat = re.sub(r"\s+", " ", monitor)
    assert "CREDIT_QUOTA = 5" in flat and "FREQUENCY = MONTHLY" in flat
    for trigger in (
        "ON 50 PERCENT DO NOTIFY",
        "ON 80 PERCENT DO NOTIFY",
        "ON 100 PERCENT DO SUSPEND ",
        "ON 110 PERCENT DO SUSPEND_IMMEDIATE",
    ):
        assert trigger in flat + " "


def test_each_step_uses_the_narrowest_system_role():
    role = None
    for stmt in _statements():
        if stmt.startswith("USE ROLE"):
            role = stmt.split()[-1]
        elif stmt.startswith("CREATE ROLE"):
            assert role == "USERADMIN"
        elif stmt.startswith(("CREATE WAREHOUSE", "CREATE DATABASE", "CREATE SCHEMA")):
            assert role == "SYSADMIN"
        elif "RESOURCE MONITOR" in stmt or "RESOURCE_MONITOR" in stmt:
            assert role == "ACCOUNTADMIN"
        elif stmt.startswith("GRANT"):
            assert role == "SECURITYADMIN"
    assert _statements()[0] == "USE SECONDARY ROLES NONE"


def test_sql_grants_match_expected_grant_set():
    """setup.sql and the verifier's expectation must describe the same privileges."""
    granted = set()
    for stmt in _statements():
        m = re.match(r"GRANT (.+?) ON (WAREHOUSE|DATABASE|SCHEMA) (\S+) TO ROLE (\S+)", stmt)
        if m:
            assert m.group(4) == ROLE
            for priv in m.group(1).split(","):
                granted.add((priv.strip(), m.group(2), m.group(3)))
    assert granted == EXPECTED_ROLE_GRANTS
    assert not any("ACCOUNT" in s.upper() for s in _statements() if s.startswith("GRANT"))
    for schema in SCHEMAS:
        assert ("USAGE", "SCHEMA", f"{DATABASE}.{schema}") in granted


class FakeConn:
    def __init__(self, fail_on=None):
        self.fail_on, self.executed = fail_on, []

    def cursor(self):
        conn = self

        class _C:
            description = None

            def execute(self, sql):
                if conn.fail_on and conn.fail_on in sql:
                    raise RuntimeError(f"denied with secret-value for {sql}")
                conn.executed.append(sql)

            def fetchall(self):
                return []

            def close(self):
                pass

        return _C()


def test_apply_runs_statements_in_order():
    conn = FakeConn()
    statements = _statements()
    apply(Session(conn, []), statements, echo=lambda _: None)
    assert conn.executed == statements


def test_apply_stops_at_first_failure_with_redacted_message():
    conn = FakeConn(fail_on="CREATE DATABASE")
    with pytest.raises(RuntimeError) as exc:
        apply(Session(conn, ["secret-value"]), _statements(), echo=lambda _: None)
    assert "CREATE DATABASE" in str(exc.value)
    assert "secret-value" not in str(exc.value) and "***" in str(exc.value)
    assert not any(s.startswith("CREATE SCHEMA") for s in conn.executed)
