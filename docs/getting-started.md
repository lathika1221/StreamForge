# Getting Started

This guide runs the whole pipeline end to end on one machine, in this order:

**Docker → Redpanda topics → Snowflake setup → consumer offset initialisation → producer → consumer → dbt → dashboard**

Each step links to the detailed document for its component. Commands are written for Windows (Git Bash or PowerShell) and run **from the repository root**, because `.env` and `.streamlit/config.toml` are read from the current directory. On macOS or Linux, replace `.venv/Scripts/python.exe` with `.venv/bin/python` (and the same for `.venv-dbt`).

## 0. Prerequisites

| Need | Used for |
|---|---|
| Python 3.11 | The application (`.venv`) and dbt (`.venv-dbt`) |
| Docker Desktop (with about 3 GB of free RAM) | The local Redpanda broker (it reserves 1 GB) |
| A Snowflake account, and a user that holds `ACCOUNTADMIN` | One-time resource setup only; the pipeline itself runs as `STREAMFORGE_ROLE` |
| OpenSSL (or another tool that writes encrypted PKCS#8 keys) | Snowflake key-pair authentication |

Steps 1–3 and the unit and Redpanda tests are free. Steps 4 and 7–9 use Snowflake credits (estimates are given at each step; the M4 resource monitor caps usage at 5 credits per month).

## 1. Install

```
py -3.11 -m venv .venv
.venv/Scripts/python.exe -m pip install -e ".[dev]"     # runtime deps + pytest and ruff
.venv/Scripts/python.exe -m pytest                       # unit tests only; no broker, no Snowflake
```

`pytest` without a marker runs the unit tests only (`addopts` deselects `integration` and `snowflake`).

## 2. Configure `.env`

```
cp .env.example .env
```

Fill in the Snowflake values. Kafka defaults already match the local broker. Never commit `.env` (it is gitignored). Set exactly **one** authentication method; StreamForge uses key-pair authentication:

```
SNOWFLAKE_ACCOUNT=<org>-<account>
SNOWFLAKE_USER=<user>
SNOWFLAKE_ROLE=STREAMFORGE_ROLE
SNOWFLAKE_PRIVATE_KEY_PATH=<path outside the repo>/streamforge_rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=<passphrase>
```

Keep the private key outside the repository. Registering the public key with Snowflake is described in [`snowflake.md`](snowflake.md#authentication-and-configuration).

## 3. Start Redpanda and create the topics

```
docker compose -f infra/docker-compose.yml up -d --wait
docker exec streamforge-redpanda rpk cluster health      # Healthy: true
docker exec streamforge-redpanda rpk topic list          # ecommerce.events (3), ecommerce.events.dlq (1)
```

The one-shot `redpanda-init` service creates both topics and disables topic auto-creation, so a misspelled topic name fails instead of creating a new topic. The broker listens on `localhost:19092` (loopback only). Details and troubleshooting: [`redpanda.md`](redpanda.md).

Optional, free: `.venv/Scripts/python.exe -m pytest -m integration` runs the 20 Redpanda integration tests. They use temporary topics and groups only.

## 4. Create the Snowflake resources (one time)

```
.venv/Scripts/python.exe -m streamforge.admin.snowflake_setup plan     # show the SQL; runs nothing
.venv/Scripts/python.exe -m streamforge.admin.snowflake_setup apply    # create resources, then verify
```

`apply` creates `ECOMMERCE_STREAMING_DB` (schemas `RAW`, `STAGING`, `ANALYTICS`, `OPS`), the XS warehouse `STREAMFORGE_WH`, the `STREAMFORGE_MONITOR` resource monitor and `STREAMFORGE_ROLE`. It needs the connecting user to hold `ACCOUNTADMIN` (the SQL switches roles itself, so `.env` keeps `STREAMFORGE_ROLE`). It is idempotent: nothing is dropped or replaced. Its verification resumes the warehouse once (about 0.017 credits). Details: [`snowflake.md`](snowflake.md).

The RAW tables and `OPS.INGESTION_BATCHES` are created by the consumer on its first run.

## 5. Initialise the consumer group's offsets

With the default `--offset-reset error`, the consumer stops with an error on its first read if its group has no committed offsets, instead of guessing a starting point. Choose the starting point explicitly first.

**New, empty broker** (start from the beginning of the topic):

```
docker exec streamforge-redpanda rpk group seek streamforge-consumer --to start --topics ecommerce.events --allow-new-topics
docker exec streamforge-redpanda rpk group describe streamforge-consumer
```

`--allow-new-topics` is required for a group with no commits yet.

**Topic that already holds messages you do not want to ingest:** pin exact per-partition offsets with `rpk group seek … --to-file`, as described in [`consumer.md`](consumer.md#starting-boundary-and-replay). Do this while no consumer is running.

## 6. Publish events

```
.venv/Scripts/python.exe -m streamforge.producer --orders 100 --seed 42 --start-time 2026-11-23T10:00:00Z --invalid-rate 0.05
```

This publishes the events of 100 simulated orders to `ecommerce.events`, plus, with probability 0.05 after each valid event, an extra corrupted record that the consumer will route to the DLQ. `--seed` with `--start-time` makes the output reproducible. The exit code is 0 only when every message was confirmed delivered. Other options (`--currency`, `--orders-per-second`, `--flush-timeout`): `python -m streamforge.producer --help` and [`producer.md`](producer.md).

## 7. Consume into Snowflake RAW

```
.venv/Scripts/python.exe -m streamforge.consumer --stop-when-idle 30
```

Valid events are loaded into the RAW tables; invalid ones go to `ecommerce.events.dlq` with failure headers; offsets are committed after each batch is stored. It stops after 30 seconds without messages and prints a summary (batches, rows inserted, duplicates skipped, DLQ counts, retries, committed offsets). Each run resumes the warehouse (60-second minimum, about 0.017 credits). Inspect the DLQ:

```
docker exec streamforge-redpanda rpk topic consume ecommerce.events.dlq -n 5
```

Details: [`consumer.md`](consumer.md); guarantees and failure behaviour: [`reliability.md`](reliability.md).

## 8. Transform with dbt

dbt has its own virtualenv; installing it into `.venv` would downgrade `python-dateutil`.

```
py -3.11 -m venv .venv-dbt
.venv-dbt/Scripts/python.exe -m pip install -r dbt/requirements.txt
.venv-dbt/Scripts/python.exe dbt/run_dbt.py debug      # connection and profile check
.venv-dbt/Scripts/python.exe dbt/run_dbt.py build      # models + data tests + unit tests
```

Always run dbt through `dbt/run_dbt.py`: it loads `.env`, passes the key passphrase only as a scrubbed `DBT_ENV_SECRET_*` variable and refuses any role other than `STREAMFORGE_ROLE`. One build is about 0.03–0.05 credits. Details: [`dbt.md`](dbt.md).

## 9. Open the dashboard

```
.venv/Scripts/python.exe -m streamlit run src/streamforge/dashboard/app.py
```

It opens on `localhost` and reads the ANALYTICS and OPS models read-only. Data is as fresh as the last dbt build; query results are cached for 10 minutes (the sidebar's **Refresh data** clears the cache). A cold load resumes the warehouse once (about 0.017 credits). Details: [`dashboard.md`](dashboard.md).

## 10. Stop

```
docker compose -f infra/docker-compose.yml down        # stop; topics and messages are kept
```

`down -v` also deletes all broker data. The Snowflake warehouse suspends itself after 60 seconds idle.

## Running it again

Repeat steps 6–9. The consumer resumes from its committed offsets, so only new messages are loaded. To replay, stop the consumer and seek the group back:

```
docker exec streamforge-redpanda rpk group seek streamforge-consumer --to start --topics ecommerce.events
```

The RAW `MERGE` skips rows already loaded, and the DLQ index skips records still in the DLQ (see [`reliability.md`](reliability.md#known-limitations) for the exceptions).
