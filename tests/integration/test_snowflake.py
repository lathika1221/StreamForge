"""Snowflake checks for the M4 setup, run as STREAMFORGE_ROLE. Uses no warehouse (no credits).

Requires the M4 resources and credentials in .env (this test reads the real .env).
Run with: python -m pytest -m snowflake
Full verification, including one tiny warehouse query, is:
    python -m streamforge.admin.snowflake_setup verify
"""

import pytest

from streamforge.admin.snowflake_setup import (
    DATABASE,
    PROTECTED_DATABASE,
    ROLE,
    SCHEMAS,
    WAREHOUSE,
    Session,
)
from streamforge.common.config import load_snowflake_settings
from streamforge.common.snowflake import _secrets, connect

pytestmark = pytest.mark.snowflake


@pytest.fixture(scope="module")
def session():
    settings = load_snowflake_settings()
    assert settings.role == ROLE, "application config must use STREAMFORGE_ROLE"
    s = Session(connect(settings), _secrets(settings))
    yield s
    s.conn.close()


def test_runs_as_streamforge_role_without_secondary_roles(session):
    (row,) = session.rows("SELECT CURRENT_ROLE() AS r, CURRENT_SECONDARY_ROLES() AS sr")
    assert row["r"] == ROLE
    assert '"roles":""' in str(row["sr"]).replace(" ", "")


def test_sees_streamforge_database_and_schemas_only(session):
    databases = {r["name"] for r in session.rows("SHOW DATABASES")}
    assert DATABASE in databases and PROTECTED_DATABASE not in databases
    schemas = {r["name"] for r in session.rows(f"SHOW SCHEMAS IN DATABASE {DATABASE}")}
    assert set(SCHEMAS) <= schemas


def test_warehouse_configuration(session):
    (wh,) = session.rows(f"SHOW WAREHOUSES LIKE '{WAREHOUSE}'")
    assert wh["size"] in ("X-Small", "XSMALL")
    assert int(wh["auto_suspend"]) == 60 and str(wh["auto_resume"]).lower() == "true"


def test_forbidden_operations_fail(session):
    for sql in (
        f"SHOW SCHEMAS IN DATABASE {PROTECTED_DATABASE}",
        "USE WAREHOUSE COMPUTE_WH",
        "CREATE DATABASE STREAMFORGE_IT_SHOULD_FAIL",
    ):
        rejected, detail = session.fails(sql)
        assert rejected, f"{sql}: {detail}"


def test_can_create_and_drop_table_in_ops(session):
    session.run(f"CREATE TABLE {DATABASE}.OPS.STREAMFORGE_IT_CHECK (id INT)")
    session.run(f"DROP TABLE {DATABASE}.OPS.STREAMFORGE_IT_CHECK")
