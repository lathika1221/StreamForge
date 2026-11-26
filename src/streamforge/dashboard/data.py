"""Snowflake access for the dashboard: read-only, application role only, scrubbed errors.

Uses the existing key-pair settings (``load_snowflake_settings``) and ``connect()``, which
disables secondary roles. The session must be STREAMFORGE_ROLE; admin roles are refused.
Errors never carry credentials or query values to the UI or logs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from streamforge.common.config import ConfigError, SnowflakeSettings, load_snowflake_settings
from streamforge.common.snowflake import SnowflakeConnectionError, _secrets, connect, redact
from streamforge.consumer.records import safe_error_text

APP_ROLE = "STREAMFORGE_ROLE"


class DashboardDataError(RuntimeError):
    """Data could not be loaded. The message is safe to show to users."""


class SnowflakeRunner:
    """Runs read-only queries and returns DataFrames with lower-case column names."""

    def __init__(
        self,
        settings: SnowflakeSettings,
        connect_fn: Callable[..., Any] | None = None,
    ) -> None:
        if (settings.role or "").upper() != APP_ROLE:
            raise DashboardDataError(
                f"the dashboard must run as {APP_ROLE}; set SNOWFLAKE_ROLE={APP_ROLE}"
            )
        self._settings = settings
        self._connect_fn = connect_fn
        self._conn: Any = None

    @classmethod
    def from_env(cls) -> SnowflakeRunner:
        try:
            return cls(load_snowflake_settings())
        except ConfigError as exc:
            raise DashboardDataError(f"Snowflake is not configured: {exc}") from None

    def _connection(self) -> Any:
        if self._conn is None:
            try:
                kwargs = {"connect_fn": self._connect_fn} if self._connect_fn else {}
                self._conn = connect(self._settings, **kwargs)
            except SnowflakeConnectionError as exc:
                raise DashboardDataError(str(exc)) from None
            role = self._scalar("select current_role()")
            if str(role).upper() != APP_ROLE:
                self.close()
                raise DashboardDataError(f"connected with role {role}, expected {APP_ROLE}")
        return self._conn

    def _scalar(self, sql: str) -> Any:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def __call__(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        try:
            cursor = self._connection().cursor()
            try:
                cursor.execute(sql, params or None)
                columns = [d[0].lower() for d in cursor.description or []]
                return pd.DataFrame(cursor.fetchall(), columns=columns)
            finally:
                cursor.close()
        except DashboardDataError:
            raise
        except Exception as exc:  # noqa: BLE001 - never leak raw driver errors
            self.close()  # reconnect on the next query
            # Quoted literals (echoed values) are scrubbed, and any configured secret is
            # redacted as well, even if a driver ever echoed one unquoted.
            detail = redact(safe_error_text(exc), _secrets(self._settings))
            raise DashboardDataError(f"query failed: {detail}") from None

    def suspend_warehouse(self) -> None:
        """Suspend STREAMFORGE_WH now (the role holds OPERATE); ignores 'already suspended'."""
        try:
            self(f"alter warehouse {self._settings.warehouse} suspend")
        except DashboardDataError:
            pass

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


def get_runner() -> SnowflakeRunner:
    """Factory used by the app (tests replace it with a fake runner)."""
    return SnowflakeRunner.from_env()
