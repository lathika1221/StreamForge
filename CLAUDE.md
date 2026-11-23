# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

StreamForge is an event-driven e-commerce data engineering platform (Python, Redpanda, SQL, Snowflake). MVP deadline: 2026-11-26. Agreed MVP scope excludes Airflow, S3 archival, Snowflake Streams/Tasks and required CI (kept as roadmap). Work proceeds milestone by milestone (M0 packaging/config → M1 schema+simulator → M2 Redpanda → M3 producer → M4 Snowflake setup → M5 consumer → M6 dbt → M7 Streamlit → M8 demo); the user approves each milestone before the next starts.

Done: M0 (packaging + `streamforge.common.config`). Producer, consumer, schema, infra, SQL, dbt, dashboard are not implemented yet; `docs/event-schema.md` is empty.

## Commands

Run from the repo root (`.env` is loaded from the current directory). Venv: `.venv` (Python 3.11, Windows).

Dependencies are declared in `pyproject.toml`. The package is installed editable in `.venv` with runtime deps (pydantic, python-dotenv, confluent-kafka, faker, snowflake-connector-python[pandas], streamlit) and the `dev` extra (pytest, ruff). dbt is **not** declared or installed: it is resolved in M6 against a stable release only — never a dbt-core pre-release (plain `dbt-snowflake` resolution pulled `dbt-core` 1.13.0b1, so dbt-core must be pinned explicitly).

```
.venv/Scripts/python.exe -m pip install -e ".[dev]"      # runtime deps + pytest/ruff
.venv/Scripts/python.exe -m pytest                         # all tests
.venv/Scripts/python.exe -m pytest tests/unit/test_config.py::test_kafka_defaults_for_local_dev
.venv/Scripts/python.exe -m pytest -m "not integration"    # skip tests needing Redpanda/Snowflake
.venv/Scripts/python.exe -m ruff check src tests
.venv/Scripts/python.exe -m ruff format src tests
```

## Configuration

- All settings come from environment variables via `streamforge.common.config` (`load_kafka_settings()`, `load_snowflake_settings()`); each component loads only what it needs. `.env.example` documents every variable; `.env` and `*.p8`/`*.pem` are gitignored.
- Snowflake auth is exactly one of key-pair (`SNOWFLAKE_PRIVATE_KEY_PATH`, preferred) or `SNOWFLAKE_PASSWORD` (password/PAT). Secret fields are `repr=False` and `ConfigError` messages name variables but never echo values — keep it that way.
- Loaders accept an explicit `env` mapping; tests must pass one (or chdir to a tmp dir) so they never read the developer's real `.env`.

## Intended architecture

`docs/architecture.md` is the design source of truth. The planned data flow:

```
Event simulator -> Python producer -> Redpanda (Kafka API) -> Python consumer
  -> validation (invalid events to dead-letter) -> Snowflake RAW
  -> SQL/dbt transformations -> STAGING -> ANALYTICS -> Streamlit dashboard
```

Package mapping: `streamforge.producer` (simulate + publish events to Redpanda topics), `streamforge.consumer` (consume, schema-validate, retry, dead-letter, load into Snowflake RAW), `streamforge.common` (shared code such as event models/config).

MVP supporting pieces: Docker Compose for local services (Redpanda, under `infra/`) and pytest. Roadmap only (out of MVP scope, described in the architecture doc): Snowflake Streams/Tasks, Airflow, S3 archival, GitHub Actions.

Design invariants to preserve as code is written:
- Every event gets a unique event ID and timestamp (plus version) — these drive deduplication and out-of-order reconciliation.
- RAW storage is append-only; dedup happens in transformations, not at ingest.
- Invalid or repeatedly failing events go to dead-letter handling rather than being dropped.
- Replay from Redpanda must be able to recover state after restart.
- Event types in scope: orders, payments, cancellations, shipments.
