"""StreamForge Snowflake setup and verification (M4).

    python -m streamforge.admin.snowflake_setup plan     # print the statements, run nothing
    python -m streamforge.admin.snowflake_setup apply    # run setup.sql, then verify
    python -m streamforge.admin.snowflake_setup verify   # verify only

``apply`` needs a user holding ACCOUNTADMIN; the SQL itself switches to the narrowest
system role per step. ``verify`` checks the objects as ACCOUNTADMIN (metadata only)
and then exercises STREAMFORGE_ROLE with secondary roles disabled: allowed operations
must succeed and forbidden ones (other databases, other warehouses, account-level
creation) must fail. It resumes STREAMFORGE_WH for one tiny query and suspends it
again. Credentials come from .env via ``load_snowflake_settings`` and are never printed.
"""

from __future__ import annotations

import argparse
import dataclasses
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from streamforge.common.config import ConfigError, SnowflakeSettings, load_snowflake_settings
from streamforge.common.snowflake import SnowflakeConnectionError, _secrets, connect, redact

DATABASE = "ECOMMERCE_STREAMING_DB"
SCHEMAS = ("RAW", "STAGING", "ANALYTICS", "OPS")
WAREHOUSE = "STREAMFORGE_WH"
ROLE = "STREAMFORGE_ROLE"
MONITOR = "STREAMFORGE_MONITOR"
PROTECTED_DATABASE = "LATZ_DATA_ENGINEERING"
OTHER_WAREHOUSE = "COMPUTE_WH"
CHECK_OBJECT = "STREAMFORGE_M4_CHECK"
SETUP_SQL = Path(__file__).resolve().parents[3] / "infra" / "snowflake" / "setup.sql"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")

EXPECTED_ROLE_GRANTS = {
    ("USAGE", "WAREHOUSE", WAREHOUSE),
    ("OPERATE", "WAREHOUSE", WAREHOUSE),
    ("USAGE", "DATABASE", DATABASE),
    *{
        (priv, "SCHEMA", f"{DATABASE}.{schema}")
        for schema in SCHEMAS
        for priv in ("USAGE", "CREATE TABLE", "CREATE VIEW")
    },
    ("CREATE STAGE", "SCHEMA", f"{DATABASE}.RAW"),
    ("CREATE FILE FORMAT", "SCHEMA", f"{DATABASE}.RAW"),
}


# ------------------------------------------------------------------ SQL file


def split_statements(sql: str) -> list[str]:
    """Split on ``;`` after dropping full-line ``--`` comments (setup.sql has no
    semicolons inside literals)."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def render(sql: str, app_user: str) -> str:
    if not _IDENTIFIER.fullmatch(app_user):
        raise ValueError("SNOWFLAKE_USER must be a plain identifier (letters, digits, _ or $)")
    return sql.replace("{{APP_USER}}", app_user)


def load_statements(settings: SnowflakeSettings, path: Path = SETUP_SQL) -> list[str]:
    return split_statements(render(path.read_text("utf-8"), settings.user))


def _label(statement: str) -> str:
    return re.sub(r"\s+", " ", statement)[:100]


# ---------------------------------------------------------------- execution


@dataclasses.dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


class Session:
    """Thin wrapper: executes SQL, returns rows as lower-cased dicts, redacts errors."""

    def __init__(self, conn: Any, secrets: list[str]) -> None:
        self.conn = conn
        self._secrets = secrets

    def rows(self, sql: str) -> list[dict[str, Any]]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            cols = [c[0].lower() for c in cursor.description or []]
            return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def run(self, sql: str) -> None:
        self.rows(sql)

    def fails(self, sql: str) -> tuple[bool, str]:
        """(True, reason) if ``sql`` is rejected, (False, ...) if it unexpectedly succeeds."""
        try:
            self.run(sql)
        except Exception as exc:  # noqa: BLE001
            return True, self.clean(exc)[:140]
        return False, "statement unexpectedly succeeded"

    def clean(self, exc: BaseException) -> str:
        return redact(re.sub(r"\s+", " ", str(exc)), self._secrets)


def apply(session: Session, statements: list[str], echo: Callable[[str], None] = print) -> None:
    for statement in statements:
        try:
            session.run(statement)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"failed: {_label(statement)} -> {session.clean(exc)}") from None
        echo(f"  ok  {_label(statement)}")


def _pct(value: Any) -> list[int]:
    return sorted(int(p) for p in re.findall(r"\d+", str(value or "")))


def verify_admin(s: Session, app_user: str) -> list[Check]:
    """Metadata checks as ACCOUNTADMIN. No warehouse is used."""
    s.run("USE SECONDARY ROLES NONE")
    s.run("USE ROLE ACCOUNTADMIN")
    checks: list[Check] = []

    dbs = s.rows(f"SHOW DATABASES LIKE '{DATABASE}'")
    checks.append(
        Check(
            "database exists, owned by SYSADMIN",
            len(dbs) == 1 and dbs[0]["owner"] == "SYSADMIN",
            f"owner={dbs[0]['owner']}" if dbs else "missing",
        )
    )
    schemas = {r["name"] for r in s.rows(f"SHOW SCHEMAS IN DATABASE {DATABASE}")}
    checks.append(
        Check(
            "schemas RAW, STAGING, ANALYTICS, OPS exist",
            set(SCHEMAS) <= schemas,
            ", ".join(sorted(schemas)),
        )
    )

    whs = s.rows(f"SHOW WAREHOUSES LIKE '{WAREHOUSE}'")
    wh = whs[0] if whs else {}
    checks.append(
        Check(
            "warehouse XSMALL, auto_suspend=60, auto_resume=true, owner SYSADMIN",
            bool(wh)
            and wh["size"] in ("X-Small", "XSMALL")
            and int(wh["auto_suspend"]) == 60
            and str(wh["auto_resume"]).lower() == "true"
            and wh["owner"] == "SYSADMIN",
            f"size={wh.get('size')} auto_suspend={wh.get('auto_suspend')} "
            f"auto_resume={wh.get('auto_resume')} state={wh.get('state')}",
        )
    )
    checks.append(
        Check(
            "warehouse assigned to resource monitor",
            wh.get("resource_monitor") == MONITOR,
            f"resource_monitor={wh.get('resource_monitor')}",
        )
    )

    rms = s.rows(f"SHOW RESOURCE MONITORS LIKE '{MONITOR}'")
    rm = rms[0] if rms else {}
    checks.append(
        Check(
            "monitor: 5 credits monthly, notify 50/80%, suspend 100%, immediate 110%",
            bool(rm)
            and float(rm["credit_quota"]) == 5
            and rm["frequency"] == "MONTHLY"
            and _pct(rm["notify_at"]) == [50, 80]
            and _pct(rm["suspend_at"]) == [100]
            and _pct(rm["suspend_immediately_at"]) == [110],
            f"quota={rm.get('credit_quota')} frequency={rm.get('frequency')} "
            f"notify_at={rm.get('notify_at')} suspend_at={rm.get('suspend_at')} "
            f"suspend_immediately_at={rm.get('suspend_immediately_at')} "
            f"used={rm.get('used_credits')}",
        )
    )

    grants = {
        (g["privilege"], g["granted_on"], g["name"].replace('"', ""))
        for g in s.rows(f"SHOW GRANTS TO ROLE {ROLE}")
    }
    missing = EXPECTED_ROLE_GRANTS - grants
    extra = {g for g in grants - EXPECTED_ROLE_GRANTS if g[0] != "OWNERSHIP"}
    checks.append(
        Check(
            "role has exactly the expected privileges",
            not missing and not extra,
            f"{len(grants)} grants; missing={sorted(missing)} extra={sorted(extra)}",
        )
    )
    checks.append(
        Check(
            "role has no account-level or protected-database privileges",
            not any(g[1] == "ACCOUNT" or PROTECTED_DATABASE in g[2] for g in grants),
        )
    )

    holders = {(r["granted_to"], r["grantee_name"]) for r in s.rows(f"SHOW GRANTS OF ROLE {ROLE}")}
    checks.append(
        Check(
            f"role granted to SYSADMIN and user {app_user}",
            {("ROLE", "SYSADMIN"), ("USER", app_user.upper())} <= holders,
            str(sorted(holders)),
        )
    )

    protected = s.rows(f"SHOW DATABASES LIKE '{PROTECTED_DATABASE}'")
    checks.append(Check(f"{PROTECTED_DATABASE} still present (untouched)", len(protected) == 1))
    return checks


def verify_app(s: Session) -> list[Check]:
    """Exercise STREAMFORGE_ROLE with secondary roles NONE (set by ``connect``)."""
    checks: list[Check] = []
    (who,) = s.rows("SELECT CURRENT_ROLE() AS r, CURRENT_SECONDARY_ROLES() AS sr")
    checks.append(Check("session primary role is STREAMFORGE_ROLE", who["r"] == ROLE, who["r"]))
    checks.append(
        Check(
            "secondary roles disabled",
            '"roles":""' in str(who["sr"]).replace(" ", ""),
            str(who["sr"]),
        )
    )

    visible = {r["name"] for r in s.rows("SHOW DATABASES")}
    checks.append(Check(f"{DATABASE} visible to role", DATABASE in visible))
    checks.append(
        Check(
            f"{PROTECTED_DATABASE} NOT visible to role",
            PROTECTED_DATABASE not in visible,
            ", ".join(sorted(visible)),
        )
    )
    for name, sql in (
        (f"cannot read {PROTECTED_DATABASE}", f"SHOW SCHEMAS IN DATABASE {PROTECTED_DATABASE}"),
        (f"cannot use {OTHER_WAREHOUSE}", f"USE WAREHOUSE {OTHER_WAREHOUSE}"),
        ("cannot create databases", "CREATE DATABASE STREAMFORGE_M4_SHOULD_FAIL"),
        ("cannot create roles", "CREATE ROLE STREAMFORGE_M4_SHOULD_FAIL"),
    ):
        ok, detail = s.fails(sql)
        checks.append(Check(name, ok, detail))
        if not ok and "DATABASE" in sql:  # never leave an accidental object behind
            s.run("DROP DATABASE IF EXISTS STREAMFORGE_M4_SHOULD_FAIL")

    s.run(f"USE DATABASE {DATABASE}")
    for schema in SCHEMAS:  # DDL only: no warehouse needed
        table = f"{schema}.{CHECK_OBJECT}"
        try:
            s.run(f"CREATE TABLE {table} (id INT)")
            s.run(f"DROP TABLE {table}")
            checks.append(Check(f"create/drop table in {schema}", True))
        except Exception as exc:  # noqa: BLE001
            checks.append(Check(f"create/drop table in {schema}", False, s.clean(exc)))
    try:
        s.run(f"CREATE VIEW ANALYTICS.{CHECK_OBJECT}_V AS SELECT 1 AS x")
        s.run(f"DROP VIEW ANALYTICS.{CHECK_OBJECT}_V")
        s.run(f"CREATE TEMPORARY STAGE RAW.{CHECK_OBJECT}_STAGE")
        s.run(f"DROP STAGE RAW.{CHECK_OBJECT}_STAGE")
        s.run(f"CREATE TEMPORARY FILE FORMAT RAW.{CHECK_OBJECT}_FF TYPE = JSON")
        s.run(f"DROP FILE FORMAT RAW.{CHECK_OBJECT}_FF")
        checks.append(Check("create/drop view, temp stage and file format", True))
    except Exception as exc:  # noqa: BLE001
        checks.append(Check("create/drop view, temp stage and file format", False, s.clean(exc)))

    # One tiny query on the StreamForge warehouse, then suspend it again.
    try:
        s.run(f"USE WAREHOUSE {WAREHOUSE}")
        s.run(f"CREATE TABLE OPS.{CHECK_OBJECT} (id INT, note STRING, checked_at TIMESTAMP_NTZ)")
        s.run(
            f"INSERT INTO OPS.{CHECK_OBJECT} VALUES (1, 'm4', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)"
        )
        (row,) = s.rows(f"SELECT COUNT(*) AS n, MAX(note) AS note FROM OPS.{CHECK_OBJECT}")
        checks.append(
            Check(
                "insert + select on STREAMFORGE_WH",
                row["n"] == 1 and row["note"] == "m4",
                f"rows={row['n']}",
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(Check("insert + select on STREAMFORGE_WH", False, s.clean(exc)))
    finally:
        s.run(f"DROP TABLE IF EXISTS OPS.{CHECK_OBJECT}")
        suspended, detail = s.fails(f"ALTER WAREHOUSE {WAREHOUSE} SUSPEND")
        # "fails" is inverted here: success of SUSPEND is what we want.
        checks.append(
            Check(
                "role can suspend STREAMFORGE_WH (OPERATE)",
                not suspended
                or "not in a valid state" in detail.lower()
                or "suspended" in detail.lower(),
                detail if suspended else "suspended",
            )
        )

    leftovers = s.rows(f"SHOW OBJECTS LIKE '{CHECK_OBJECT}%' IN DATABASE {DATABASE}")
    checks.append(Check("no verification objects left behind", not leftovers, str(len(leftovers))))
    return checks


def warehouse_suspended(s: Session, attempts: int = 10) -> Check:
    state = "?"
    for _ in range(attempts):
        whs = s.rows(f"SHOW WAREHOUSES LIKE '{WAREHOUSE}'")
        state = whs[0]["state"] if whs else "missing"
        if state in ("SUSPENDED", "SUSPENDING"):
            break
        time.sleep(2)
    return Check(
        "warehouse is suspended after verification",
        state in ("SUSPENDED", "SUSPENDING"),
        f"state={state}",
    )


# --------------------------------------------------------------------- CLI


def _print_checks(checks: list[Check]) -> bool:
    for c in checks:
        print(f"  {'PASS' if c.ok else 'FAIL'}  {c.name}" + (f"  [{c.detail}]" if c.detail else ""))
    return all(c.ok for c in checks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m streamforge.admin.snowflake_setup")
    parser.add_argument("command", choices=("plan", "apply", "verify"))
    args = parser.parse_args(argv)

    try:
        settings = load_snowflake_settings()
        statements = load_statements(settings)
    except (ConfigError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.command == "plan":
        for statement in statements:
            print(f"{statement};\n")
        return 0

    secrets = _secrets(settings)
    try:
        admin = Session(connect(settings, use_context=False), secrets)
    except SnowflakeConnectionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        if args.command == "apply":
            print(f"Applying {SETUP_SQL.name} ({len(statements)} statements):")
            try:
                apply(admin, statements)
            except RuntimeError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1

        print("\nAdmin checks (ACCOUNTADMIN, metadata only):")
        ok = _print_checks(verify_admin(admin, settings.user))

        app_settings = dataclasses.replace(
            settings, role=ROLE, warehouse=WAREHOUSE, database=DATABASE
        )
        print(f"\nApplication checks ({ROLE}, secondary roles NONE):")
        try:
            app = Session(connect(app_settings), secrets)
        except SnowflakeConnectionError as exc:
            print(f"  FAIL  connect as {ROLE}: {exc}")
            return 1
        try:
            ok = _print_checks(verify_app(app)) and ok
        finally:
            app.conn.close()

        print("\nFinal state:")
        ok = _print_checks([warehouse_suspended(admin)]) and ok
        config_ok = (settings.role or "").upper() == ROLE
        ok = (
            _print_checks(
                [
                    Check(
                        "app config (.env SNOWFLAKE_ROLE) is STREAMFORGE_ROLE, not ACCOUNTADMIN",
                        config_ok,
                        f"configured={settings.role}",
                    )
                ]
            )
            and ok
        )
    finally:
        admin.conn.close()
    print("\nRESULT:", "all checks passed" if ok else "some checks FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
