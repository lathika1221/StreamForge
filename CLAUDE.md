# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

StreamForge is an event-driven e-commerce data engineering platform (Python, Redpanda, SQL, Snowflake). MVP deadline: 2026-11-26. Agreed MVP scope excludes Airflow, S3 archival, Snowflake Streams/Tasks and required CI (kept as roadmap). Work proceeds milestone by milestone (M0 packaging/config → M1 schema+simulator → M2 Redpanda → M3 producer → M4 Snowflake setup → M5 consumer → M6 dbt → M7 Streamlit → M8 demo); the user approves each milestone before the next starts.

Done: M0 (packaging + `streamforge.common.config`), M1 (event contract + simulator), M2 (local Redpanda via Docker Compose). Kafka publishing, consumer, Snowflake SQL, dbt and dashboard are not implemented yet.

## Local Redpanda

`infra/docker-compose.yml` runs one pinned Redpanda broker (`docs/redpanda.md` has all commands and troubleshooting). Host clients use `localhost:19092` (published on both `127.0.0.1` and `[::1]` — Windows resolves `localhost` to `::1` first); containers use `redpanda:9092`. Topics are created only by the idempotent `infra/redpanda/create-topics.sh` (`redpanda-init` service): `ecommerce.events` 3 partitions, `ecommerce.events.dlq` 1 partition. Topic auto-creation is disabled, so a misspelled topic fails with `UNKNOWN_TOPIC_OR_PARTITION`. `.gitattributes` forces LF for `*.sh` (CRLF breaks bash in the container). In Git Bash, prefix `docker run` commands containing container paths with `MSYS_NO_PATHCONV=1`.

M3 requirement (not implemented yet): key messages by `order_id`; records without a usable `order_id` (`missing_order_id`, `malformed_json`) are published with a null key.

## Event contract

`docs/event-schema.md` and `src/streamforge/common/events.py` are the two halves of one contract (schema_version 1) and must change together — `tests/unit/test_event_schema_doc.py` fails if a field, event type, enum value or defect kind is missing from the doc, or a doc JSON example stops validating. Use `parse_event()` to validate raw events (dict/str/bytes; raises `pydantic.ValidationError`). Money is `Decimal` serialized as a 2dp JSON string; timestamps are UTC with `Z`.

`streamforge.producer.simulator.EventSimulator` is deterministic: all randomness flows through one seeded `random.Random` and a simulated clock (no `uuid4()`/`datetime.now()` inside generation when `start_time` is set). Keep it that way — tests compare byte-identical JSONL (`SimulatedRecord.value`, produced only by `to_json_line()`; the CLI writes raw bytes with `\n` so Windows adds no CRLF). Default currency is INR; other currencies use fixed illustrative conversion factors, not real FX.

Invalid records are generated separately in `streamforge.producer.invalid_records`, which works only on raw JSON dicts/strings and must never import pydantic or `streamforge.common.events` (a test checks its imports). They are *extra* corrupted copies (fresh `event_id`), never replacements, so produced = valid + invalid. `malformed_json` records are text only (`SimulatedRecord.data is None`). Adding a defect means updating `DEFECTS_BY_EVENT_TYPE`, the defect table in `docs/event-schema.md`, and `EXPECTED_ERROR` in `tests/unit/test_invalid_records.py`.

## Commands

Run from the repo root (`.env` is loaded from the current directory). Venv: `.venv` (Python 3.11, Windows).

Dependencies are declared in `pyproject.toml`. The package is installed editable in `.venv` with runtime deps (pydantic, python-dotenv, confluent-kafka, snowflake-connector-python[pandas], streamlit) and the `dev` extra (pytest, ruff). dbt is **not** declared or installed: it is resolved in M6 against a stable release only — never a dbt-core pre-release (plain `dbt-snowflake` resolution pulled `dbt-core` 1.13.0b1, so dbt-core must be pinned explicitly).

```
.venv/Scripts/python.exe -m pip install -e ".[dev]"      # runtime deps + pytest/ruff
.venv/Scripts/python.exe -m pytest                         # unit tests (addopts deselects `integration`)
.venv/Scripts/python.exe -m pytest tests/unit/test_config.py::test_kafka_defaults_for_local_dev
.venv/Scripts/python.exe -m pytest -m integration          # needs the Redpanda stack running; fails if unreachable
docker compose -f infra/docker-compose.yml up -d --wait    # start Redpanda + create topics
docker compose -f infra/docker-compose.yml down [-v]       # stop (-v also deletes broker data)
.venv/Scripts/python.exe -m ruff check src tests
.venv/Scripts/python.exe -m ruff format src tests
.venv/Scripts/python.exe -m streamforge.producer.simulator --orders 10 --seed 42 --start-time 2026-11-23T10:00:00Z [--invalid-rate 0.1]   # JSONL to stdout, no Kafka
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
- Every event carries a unique `event_id`, a UTC `event_timestamp` (the only timestamp field name — never `event_time`), a per-order `sequence_number` and `schema_version` — these drive deduplication and out-of-order reconciliation.
- RAW storage is append-only; dedup happens in transformations, not at ingest.
- Invalid or repeatedly failing events go to dead-letter handling rather than being dropped.
- Replay from Redpanda must be able to recover state after restart.
- Event types in scope: orders, payments, cancellations, shipments.
