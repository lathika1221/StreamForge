"""Snowflake connection helper.

Maps ``SnowflakeSettings`` to the official ``snowflake-connector-python`` authentication
parameters:

======== ================================================================
method   connector parameters
======== ================================================================
pat      ``authenticator="PROGRAMMATIC_ACCESS_TOKEN"``, ``token``
key_pair ``authenticator="SNOWFLAKE_JWT"``, ``private_key_file``, ``private_key_password``
password ``password`` (default authenticator)
======== ================================================================

Secrets never leave this module in readable form: the parameter dict is passed
straight to the connector and never logged, and connection errors are re-raised as
``SnowflakeConnectionError`` with every configured secret redacted and the original
exception chain suppressed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from streamforge.common.config import SnowflakeSettings

APPLICATION_NAME = "StreamForge"
LOGIN_TIMEOUT_SECONDS = 30
_REDACTED = "***"


class SnowflakeConnectionError(RuntimeError):
    """Connecting failed. The message never contains secret values."""


def connection_params(settings: SnowflakeSettings, *, use_context: bool = True) -> dict[str, Any]:
    """Connector keyword arguments for ``settings``. Contains secrets: never log it.

    ``use_context=False`` omits warehouse, database and role, for setup steps that
    run before those objects exist (the user's defaults apply instead).
    """
    params: dict[str, Any] = {
        "account": settings.account,
        "user": settings.user,
        "application": APPLICATION_NAME,
        "login_timeout": LOGIN_TIMEOUT_SECONDS,
        # Always log the session out on close (no lingering server sessions).
        "enable_server_session_keep_alive_auto_detection": False,
        # Never put SQL text in connector logs: client-side binding interpolates
        # row values into the statement text (pinned explicitly; default is False).
        "log_query_text": False,
    }
    if use_context:
        params["warehouse"] = settings.warehouse
        params["database"] = settings.database
        if settings.role:
            params["role"] = settings.role

    method = settings.auth_method
    if method == "pat":
        params["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
        params["token"] = settings.token
    elif method == "key_pair":
        params["authenticator"] = "SNOWFLAKE_JWT"
        params["private_key_file"] = str(settings.private_key_path)
        if settings.private_key_passphrase:
            params["private_key_password"] = settings.private_key_passphrase
    else:
        params["password"] = settings.password
    return params


def _secrets(settings: SnowflakeSettings) -> list[str]:
    values = (settings.token, settings.password, settings.private_key_passphrase)
    return [v for v in values if v]


def redact(text: str, secrets: list[str]) -> str:
    """Replace every occurrence of each secret in ``text`` (longest first)."""
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, _REDACTED)
    return text


def connect(
    settings: SnowflakeSettings,
    *,
    use_context: bool = True,
    secondary_roles_none: bool = True,
    connect_fn: Callable[..., Any] | None = None,
) -> Any:
    """Open a connection. Raises ``SnowflakeConnectionError`` with a redacted message.

    By default runs ``USE SECONDARY ROLES NONE`` so the session has only the privileges
    of its primary role (users may have ``DEFAULT_SECONDARY_ROLES = ('ALL')``, which
    would otherwise add every other granted role, e.g. ACCOUNTADMIN).
    """
    if connect_fn is None:
        import snowflake.connector  # imported lazily: unit tests never need it

        connect_fn = snowflake.connector.connect
    conn = None
    try:
        conn = connect_fn(**connection_params(settings, use_context=use_context))
        if secondary_roles_none:
            cursor = conn.cursor()
            try:
                cursor.execute("USE SECONDARY ROLES NONE")
            finally:
                cursor.close()
        return conn
    except Exception as exc:  # noqa: BLE001 - every failure must be redacted
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        detail = redact(f"{type(exc).__name__}: {exc}", _secrets(settings))
        raise SnowflakeConnectionError(
            f"Snowflake connection failed ({settings.auth_method} auth, "
            f"account {settings.account}): {detail}"
        ) from None
