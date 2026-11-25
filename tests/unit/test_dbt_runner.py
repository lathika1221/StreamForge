"""Unit tests for dbt/run_dbt.py (pure functions only; dbt is not needed or invoked)."""

import importlib.util
from pathlib import Path

import pytest

RUNNER = Path(__file__).resolve().parents[2] / "dbt" / "run_dbt.py"
_spec = importlib.util.spec_from_file_location("run_dbt", RUNNER)
run_dbt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_dbt)

PASSPHRASE = "pp-SECRET-do-not-print"


@pytest.fixture
def values(tmp_path):
    key = tmp_path / "k.p8"
    key.write_text("not a real key")
    return {
        "SNOWFLAKE_ACCOUNT": "org-acct",
        "SNOWFLAKE_USER": "LATZ",
        "SNOWFLAKE_ROLE": "STREAMFORGE_ROLE",
        "SNOWFLAKE_WAREHOUSE": "STREAMFORGE_WH",
        "SNOWFLAKE_DATABASE": "ECOMMERCE_STREAMING_DB",
        "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": PASSPHRASE,
        "SNOWFLAKE_TOKEN": "",
        "SNOWFLAKE_PASSWORD": "",
    }


def test_passphrase_is_only_passed_under_the_scrubbed_secret_name(values):
    env = run_dbt.build_dbt_env(values)
    assert env[run_dbt.SECRET_ENV] == PASSPHRASE
    assert run_dbt.SECRET_ENV.startswith("DBT_ENV_SECRET_")
    assert "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE" not in env
    assert [k for k, v in env.items() if v == PASSPHRASE] == [run_dbt.SECRET_ENV]


def test_env_points_dbt_at_project_and_disables_tracking(values):
    env = run_dbt.build_dbt_env(values)
    assert env["DBT_PROFILES_DIR"] == env["DBT_PROJECT_DIR"] == str(run_dbt.PROJECT_DIR)
    assert env["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false" and env["DO_NOT_TRACK"] == "1"
    assert env["SNOWFLAKE_ROLE"] == "STREAMFORGE_ROLE"


@pytest.mark.parametrize("role", ["ACCOUNTADMIN", "sysadmin", "SECURITYADMIN", "PUBLIC", ""])
def test_non_application_roles_are_refused(values, role):
    values["SNOWFLAKE_ROLE"] = role
    with pytest.raises(run_dbt.DbtConfigError):
        run_dbt.build_dbt_env(values)


@pytest.mark.parametrize("other", ["SNOWFLAKE_TOKEN", "SNOWFLAKE_PASSWORD"])
def test_only_key_pair_auth_is_accepted(values, other):
    values[other] = "some-secret-value"
    with pytest.raises(run_dbt.DbtConfigError) as exc:
        run_dbt.build_dbt_env(values)
    assert "some-secret-value" not in str(exc.value)


def test_missing_values_are_named_without_values(values):
    values["SNOWFLAKE_ACCOUNT"] = " "
    values["SNOWFLAKE_PRIVATE_KEY_PATH"] = ""
    with pytest.raises(run_dbt.DbtConfigError) as exc:
        run_dbt.build_dbt_env(values)
    assert "SNOWFLAKE_ACCOUNT" in str(exc.value) and PASSPHRASE not in str(exc.value)


def test_missing_key_file_is_rejected(values, tmp_path):
    values["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(tmp_path / "nope.p8")
    with pytest.raises(run_dbt.DbtConfigError, match="existing file"):
        run_dbt.build_dbt_env(values)


def test_dbt_args_always_pin_project_and_profiles_dir():
    args = run_dbt.dbt_args(["build", "--select", "fct_orders"])
    assert args[:3] == ["build", "--select", "fct_orders"]
    assert args[3:] == [
        "--project-dir",
        str(run_dbt.PROJECT_DIR),
        "--profiles-dir",
        str(run_dbt.PROJECT_DIR),
    ]
    with pytest.raises(run_dbt.DbtConfigError):
        run_dbt.dbt_args([])


def test_profiles_yml_contains_no_literal_credentials():
    profile = (run_dbt.PROJECT_DIR / "profiles.yml").read_text("utf-8")
    assert "env_var('DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')" in profile
    for key in ("account", "user", "role", "warehouse", "database", "private_key_path"):
        line = next(ln for ln in profile.splitlines() if ln.strip().startswith(f"{key}:"))
        assert "env_var(" in line, f"{key} must come from the environment"
    assert "password" not in profile.lower() and "token" not in profile.lower()
