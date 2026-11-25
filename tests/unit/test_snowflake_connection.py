"""Unit tests for streamforge.common.snowflake. No Snowflake connection is ever made."""

import dataclasses
import traceback
from pathlib import Path

import pytest

from streamforge.common.config import SnowflakeSettings, load_snowflake_settings
from streamforge.common.snowflake import (
    APPLICATION_NAME,
    SnowflakeConnectionError,
    connect,
    connection_params,
    redact,
)

BASE = {
    "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
    "SNOWFLAKE_USER": "STREAMFORGE_USER",
    "SNOWFLAKE_WAREHOUSE": "STREAMFORGE_WH",
    "SNOWFLAKE_DATABASE": "ECOMMERCE_STREAMING_DB",
    "SNOWFLAKE_ROLE": "STREAMFORGE_ROLE",
}
PAT = "pat-SECRET-0123456789"
PASSWORD = "pw-SECRET-abcdef"
PASSPHRASE = "pp-SECRET-xyz"


@pytest.fixture
def key_file(tmp_path: Path) -> Path:
    path = tmp_path / "rsa_key.p8"
    path.write_text("not a real key")
    return path


def _pat() -> SnowflakeSettings:
    return load_snowflake_settings({**BASE, "SNOWFLAKE_TOKEN": PAT})


# --------------------------------------------------------------- parameters


def test_pat_uses_official_programmatic_access_token_authenticator():
    params = connection_params(_pat())
    assert params["authenticator"] == "PROGRAMMATIC_ACCESS_TOKEN"
    assert params["token"] == PAT
    assert "password" not in params and "private_key_file" not in params


def test_key_pair_uses_snowflake_jwt(key_file):
    settings = load_snowflake_settings(
        {
            **BASE,
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(key_file),
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": PASSPHRASE,
        }
    )
    params = connection_params(settings)
    assert params["authenticator"] == "SNOWFLAKE_JWT"
    assert params["private_key_file"] == str(key_file)
    assert params["private_key_password"] == PASSPHRASE
    assert "token" not in params and "password" not in params


def test_key_pair_without_passphrase_omits_it(key_file):
    settings = load_snowflake_settings({**BASE, "SNOWFLAKE_PRIVATE_KEY_PATH": str(key_file)})
    assert "private_key_password" not in connection_params(settings)


def test_password_uses_default_authenticator():
    params = connection_params(load_snowflake_settings({**BASE, "SNOWFLAKE_PASSWORD": PASSWORD}))
    assert params["password"] == PASSWORD
    assert "authenticator" not in params and "token" not in params


def test_context_and_identity_parameters():
    params = connection_params(_pat())
    assert params["account"] == "myorg-myaccount" and params["user"] == "STREAMFORGE_USER"
    assert params["warehouse"] == "STREAMFORGE_WH"
    assert params["database"] == "ECOMMERCE_STREAMING_DB"
    assert params["role"] == "STREAMFORGE_ROLE"
    assert params["application"] == APPLICATION_NAME
    assert params["login_timeout"] == 30


def test_setup_mode_omits_warehouse_database_and_role():
    params = connection_params(_pat(), use_context=False)
    assert not {"warehouse", "database", "role"} & params.keys()


def test_role_omitted_when_not_configured():
    settings = dataclasses.replace(_pat(), role=None)
    assert "role" not in connection_params(settings)


def test_parameter_names_exist_in_installed_connector():
    """Guards against connector API drift without connecting."""
    from snowflake.connector._internal.authenticator_type import AuthenticatorType
    from snowflake.connector.connection_config import ConnectionConfig

    assert AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN.value == "PROGRAMMATIC_ACCESS_TOKEN"
    assert AuthenticatorType.KEY_PAIR.value == "SNOWFLAKE_JWT"
    fields = {f.name for f in dataclasses.fields(ConnectionConfig)}
    used = set(connection_params(_pat())) | {"password", "private_key_file", "private_key_password"}
    assert used <= fields


# ------------------------------------------------------------------ connect


class FakeConnection:
    def __init__(self, fail_on=None):
        self.executed, self.closed, self.fail_on = [], False, fail_on

    def cursor(self):
        conn = self

        class _Cursor:
            def execute(self, sql):
                if conn.fail_on and conn.fail_on in sql:
                    raise RuntimeError(f"cannot run {sql}")
                conn.executed.append(sql)

            def close(self):
                pass

        return _Cursor()

    def close(self):
        self.closed = True


def test_connect_passes_params_and_disables_secondary_roles():
    seen, conn = {}, FakeConnection()

    def fake_connect(**kwargs):
        seen.update(kwargs)
        return conn

    assert connect(_pat(), connect_fn=fake_connect) is conn
    assert seen == connection_params(_pat())
    assert conn.executed == ["USE SECONDARY ROLES NONE"]


def test_connect_can_keep_secondary_roles_when_asked():
    conn = FakeConnection()
    connect(_pat(), secondary_roles_none=False, connect_fn=lambda **_: conn)
    assert conn.executed == []


def test_connect_closes_session_if_secondary_roles_cannot_be_disabled():
    conn = FakeConnection(fail_on="SECONDARY")
    with pytest.raises(SnowflakeConnectionError, match="cannot run"):
        connect(_pat(), connect_fn=lambda **_: conn)
    assert conn.closed


def test_sessions_always_log_out_on_close():
    assert connection_params(_pat())["enable_server_session_keep_alive_auto_detection"] is False


@pytest.mark.parametrize(
    "env",
    [
        {"SNOWFLAKE_TOKEN": PAT},
        {"SNOWFLAKE_PASSWORD": PASSWORD},
    ],
)
def test_connect_failure_redacts_secrets_and_hides_original_exception(env):
    settings = load_snowflake_settings({**BASE, **env})
    secret = next(iter(env.values()))

    def failing_connect(**kwargs):
        raise RuntimeError(f"login failed for {kwargs.get('token') or kwargs.get('password')}")

    with pytest.raises(SnowflakeConnectionError) as exc:
        connect(settings, connect_fn=failing_connect)
    rendered = "".join(traceback.format_exception(exc.value))
    assert secret not in rendered
    assert "***" in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__suppress_context__
    assert "STREAMFORGE_USER" not in str(exc.value)  # identity not echoed either


def test_connect_failure_redacts_key_passphrase(key_file):
    settings = load_snowflake_settings(
        {
            **BASE,
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(key_file),
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": PASSPHRASE,
        }
    )

    def failing_connect(**kwargs):
        raise ValueError(f"bad passphrase {kwargs['private_key_password']}")

    with pytest.raises(SnowflakeConnectionError) as exc:
        connect(settings, connect_fn=failing_connect)
    assert PASSPHRASE not in str(exc.value)


def test_redact_handles_overlapping_secrets():
    assert redact("a-secret-long a-secret", ["a-secret", "a-secret-long"]) == "*** ***"
