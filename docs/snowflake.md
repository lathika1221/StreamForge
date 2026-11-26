# Snowflake Setup (M4)

StreamForge uses one dedicated database, warehouse, role and resource monitor in the existing Snowflake account. Everything is created by the idempotent `infra/snowflake/setup.sql`, applied and verified by `python -m streamforge.admin.snowflake_setup`. No other database, warehouse or role is modified. In particular, `LATZ_DATA_ENGINEERING` is never touched, and `STREAMFORGE_ROLE` cannot see it.

## Resources

| Object | Configuration | Owner |
|---|---|---|
| Database `ECOMMERCE_STREAMING_DB` | `DATA_RETENTION_TIME_IN_DAYS = 1` | `SYSADMIN` |
| Schema `RAW` | Append-only events as ingested from Kafka (M5) | `SYSADMIN` |
| Schema `STAGING` | Validated, deduplicated, typed records (M6) | `SYSADMIN` |
| Schema `ANALYTICS` | Facts, dimensions and aggregates (M6, M7) | `SYSADMIN` |
| Schema `OPS` | Load audit, reconciliation and checks | `SYSADMIN` |
| Warehouse `STREAMFORGE_WH` | `XSMALL`, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`, `INITIALLY_SUSPENDED = TRUE`, `STATEMENT_TIMEOUT_IN_SECONDS = 900`, `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 300` | `SYSADMIN` |
| Resource monitor `STREAMFORGE_MONITOR` | 5 credits per month, starting immediately. Notifies at 50% and 80%, suspends at 100% (running queries finish), suspends immediately at 110% (running queries are cancelled). Assigned to `STREAMFORGE_WH` only. | `ACCOUNTADMIN` |
| Role `STREAMFORGE_ROLE` | Application role, granted to `SYSADMIN` (admin oversight) and to the application user | `USERADMIN` |

Each setup step runs as the narrowest system role able to do it: `USERADMIN` creates the role, `SYSADMIN` creates the warehouse, database and schemas, `ACCOUNTADMIN` handles only the resource monitor, and `SECURITYADMIN` manages the grants.

Tables are created by the application itself, as `STREAMFORGE_ROLE`, which therefore owns them. The M5 consumer creates `RAW.ORDER_CREATED`, `RAW.PAYMENT_PROCESSED`, `RAW.ORDER_CANCELLED`, `RAW.SHIPMENT_DISPATCHED` and `OPS.INGESTION_BATCHES` on first run (see `docs/consumer.md`).

## Grants to `STREAMFORGE_ROLE` (17 in total)

| Privilege | On |
|---|---|
| `USAGE`, `OPERATE` | Warehouse `STREAMFORGE_WH` (`OPERATE` lets the app suspend it explicitly) |
| `USAGE` | Database `ECOMMERCE_STREAMING_DB` |
| `USAGE`, `CREATE TABLE`, `CREATE VIEW` | Each of the schemas `RAW`, `STAGING`, `ANALYTICS` and `OPS` |
| `CREATE STAGE`, `CREATE FILE FORMAT` | Schema `RAW` (bulk loading with `write_pandas` uses a temporary stage and file format) |

The role holds no account-level privileges and nothing on any other database or warehouse. Tables and views it creates are owned by the role, so no future grants are needed.

## Secondary roles

The account user has `DEFAULT_SECONDARY_ROLES = ('ALL')`, which would silently add every other granted role, including `ACCOUNTADMIN`, to any session. `streamforge.common.snowflake.connect()` therefore runs `USE SECONDARY ROLES NONE` on every connection by default, so StreamForge sessions hold only `STREAMFORGE_ROLE`'s privileges. The setup SQL also starts with it.

## Authentication and configuration

The application connects with key-pair authentication (`SNOWFLAKE_JWT`):

| `.env` variable | Value |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Account identifier (`<org>-<account>`) |
| `SNOWFLAKE_USER` | The Snowflake user holding the registered public key |
| `SNOWFLAKE_ROLE` | `STREAMFORGE_ROLE`, the normal application role. Never `ACCOUNTADMIN`. |
| `SNOWFLAKE_WAREHOUSE` | `STREAMFORGE_WH` |
| `SNOWFLAKE_DATABASE` | `ECOMMERCE_STREAMING_DB` |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Encrypted PKCS#8 private key stored **outside** the repo (here `~/.snowflake/keys/streamforge_rsa_key.p8`, readable only by the owning Windows user) |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | The key's passphrase |
| `SNOWFLAKE_TOKEN`, `SNOWFLAKE_PASSWORD` | Empty. Exactly one auth method may be set. |

Programmatic access tokens are supported (`SNOWFLAKE_TOKEN`), but this account requires a network policy before a PAT can be used, so key-pair authentication is used instead.

To register a key pair, generate an RSA 2048 key as encrypted PKCS#8, keep the private key outside the repo, then run as `ACCOUNTADMIN` or `SECURITYADMIN`:

```sql
ALTER USER <user> SET RSA_PUBLIC_KEY='<public key body, without the BEGIN/END lines>';
DESC USER <user>;   -- RSA_PUBLIC_KEY_FP must equal SHA256:<base64 of the DER public key's SHA-256>
```

## Commands

Run from the repo root (credentials come from `.env` and are never printed):

```
.venv/Scripts/python.exe -m streamforge.admin.snowflake_setup plan     # show the SQL; runs nothing
.venv/Scripts/python.exe -m streamforge.admin.snowflake_setup apply    # create/refresh resources, then verify
.venv/Scripts/python.exe -m streamforge.admin.snowflake_setup verify   # verify only
.venv/Scripts/python.exe -m pytest -m snowflake                         # live checks as STREAMFORGE_ROLE (~0.017-0.034 credits, see below)
```

`apply` needs the connecting user to hold `ACCOUNTADMIN`; the SQL switches roles itself, so `.env` keeps `STREAMFORGE_ROLE`. It is safe to re-run: every `CREATE` uses `IF NOT EXISTS`, and the warehouse settings are re-asserted with `ALTER`. Nothing is ever dropped or replaced.

`verify` checks the following:

- **As `ACCOUNTADMIN`, metadata only:**
  - Database owner and schemas.
  - Warehouse size, auto-suspend, auto-resume and resource-monitor assignment.
  - Monitor quota and triggers.
  - The role's grants: exactly the 17 expected, none account-level, none on `LATZ_DATA_ENGINEERING`.
  - Who holds the role.
  - That `LATZ_DATA_ENGINEERING` still exists.
- **As `STREAMFORGE_ROLE` with secondary roles disabled:**
  - The role cannot see or read `LATZ_DATA_ENGINEERING`, cannot use `COMPUTE_WH`, and cannot create databases or roles.
  - It can create and drop a table in each schema, a view, a temporary stage and a file format.
  - One insert and select runs on `STREAMFORGE_WH`, then the warehouse is suspended again.
  - No check objects are left behind, and the warehouse ends `SUSPENDED`.
  - `.env` uses `STREAMFORGE_ROLE`.

Each `verify` run resumes the XS warehouse for one short query, which bills the 60-second minimum (about 0.017 credits). The `pytest -m snowflake` suite has 8 tests in three files. The M4 checks in `test_snowflake.py` (5 tests) use no warehouse: metadata, `SHOW` and DDL only. The M5 live sink test file `test_snowflake_sink_live.py` (2 tests) and the M7 dashboard test `test_dashboard_live.py` (1 test) each resume `STREAMFORGE_WH` and suspend it afterwards, about 0.017 credits each (60-second minimum), so the whole suite costs about 0.017–0.034 credits. CI never runs these tests.

## Cost controls and limitations

- **Spending cap:** the warehouse auto-suspends after 60 s idle, and the resource monitor caps it at 5 credits per month. The first verification runs used 0.04 credits.
- **Notification emails:** resource-monitor notifications reach account administrators only if they have enabled notifications and verified their email in Snowsight.
- **Pre-existing warehouses:** the monitor covers `STREAMFORGE_WH` only. `COMPUTE_WH`, `SNOWFLAKE_LEARNING_WH` and the Streamlit notebook warehouse have no resource monitor (unchanged; they pre-date StreamForge).
- **Service user:** StreamForge currently connects as the human user `LATZ` with `STREAMFORGE_ROLE`. A dedicated `TYPE = SERVICE` user would isolate the application further. It is not part of M4.

## Teardown (manual, not scripted)

Run as `ACCOUNTADMIN` only if StreamForge is to be removed completely:

```sql
DROP DATABASE ECOMMERCE_STREAMING_DB;
DROP WAREHOUSE STREAMFORGE_WH;
DROP RESOURCE MONITOR STREAMFORGE_MONITOR;
DROP ROLE STREAMFORGE_ROLE;
```
