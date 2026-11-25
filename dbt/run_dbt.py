"""Run dbt for StreamForge with credentials from the repo's .env (never printed).

    .venv-dbt/Scripts/python.exe dbt/run_dbt.py debug
    .venv-dbt/Scripts/python.exe dbt/run_dbt.py build
    .venv-dbt/Scripts/python.exe dbt/run_dbt.py build --full-refresh

What it does before handing over to dbt's official programmatic entry point (dbtRunner):

* loads ``.env`` (real environment variables win, as in ``streamforge.common.config``);
* requires key-pair authentication (the profile uses ``private_key_path``);
* passes the key passphrase only as ``DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE``
  (dbt scrubs ``DBT_ENV_SECRET_*`` values from its logs) and removes the plain variable;
* refuses admin roles - StreamForge transforms only as STREAMFORGE_ROLE;
* disables dbt's anonymous usage tracking;
* points dbt at this directory for both the project and ``profiles.yml``.

Standard library + python-dotenv only, so the pure functions are testable from the main
virtualenv without dbt installed.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
REPO_DIR = PROJECT_DIR.parent
SECRET_ENV = "DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
APP_ROLE = "STREAMFORGE_ROLE"
FORBIDDEN_ROLES = frozenset({"ACCOUNTADMIN", "ORGADMIN", "SECURITYADMIN", "SYSADMIN", "USERADMIN"})
REQUIRED = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
)


class DbtConfigError(RuntimeError):
    """Configuration problem. Messages name variables, never their values."""


def _get(values: Mapping[str, str | None], name: str) -> str:
    return (values.get(name) or "").strip()


def build_dbt_env(values: Mapping[str, str | None]) -> dict[str, str]:
    """Environment variables for the dbt process, derived from ``values`` (.env + env)."""
    missing = [name for name in REQUIRED if not _get(values, name)]
    if missing:
        raise DbtConfigError(
            "Missing required variable(s) for dbt: " + ", ".join(missing) + " (see .env.example)"
        )
    if _get(values, "SNOWFLAKE_TOKEN") or _get(values, "SNOWFLAKE_PASSWORD"):
        raise DbtConfigError(
            "dbt uses key-pair authentication only: leave SNOWFLAKE_TOKEN and "
            "SNOWFLAKE_PASSWORD empty and set SNOWFLAKE_PRIVATE_KEY_PATH"
        )
    role = _get(values, "SNOWFLAKE_ROLE").upper()
    if role in FORBIDDEN_ROLES or role != APP_ROLE:
        raise DbtConfigError(f"SNOWFLAKE_ROLE must be {APP_ROLE} for dbt, not {role}")
    key_path = Path(_get(values, "SNOWFLAKE_PRIVATE_KEY_PATH")).expanduser()
    if not key_path.is_file():
        raise DbtConfigError("SNOWFLAKE_PRIVATE_KEY_PATH does not point to an existing file")

    env = {name: _get(values, name) for name in REQUIRED}
    env["SNOWFLAKE_ROLE"] = APP_ROLE
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(key_path)
    env[SECRET_ENV] = _get(values, "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    env["DBT_PROFILES_DIR"] = str(PROJECT_DIR)
    env["DBT_PROJECT_DIR"] = str(PROJECT_DIR)
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    env["DO_NOT_TRACK"] = "1"
    return env


def dbt_args(argv: Sequence[str]) -> list[str]:
    if not argv:
        raise DbtConfigError("usage: run_dbt.py <dbt command> [args...]  e.g. 'build'")
    return [*argv, "--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR)]


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    from dotenv import dotenv_values

    values = {**dotenv_values(REPO_DIR / ".env"), **os.environ}
    try:
        env = build_dbt_env(values)
        args = dbt_args(argv)
    except DbtConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    os.environ.update(env)
    os.environ.pop("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", None)  # only the scrubbed name remains
    os.chdir(PROJECT_DIR)

    from dbt.cli.main import dbtRunner  # imported late: only available in .venv-dbt

    result = dbtRunner().invoke(args)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
