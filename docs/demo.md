# Offline Demo (M9.3)

Two ways to see StreamForge working **without a Snowflake account**:

| Demo | Needs | Shows |
|---|---|---|
| [Dashboard demo mode](#dashboard-demo-mode) | Python (`.venv`) only | The full Streamlit dashboard on simulated data |
| [Local pipeline demo](#local-pipeline-demo) | Python + Docker (local Redpanda) | Producer → Redpanda → consumer → DLQ and file-backed RAW, with an accounting summary |

Neither uses credentials, the network beyond `localhost`, or Snowflake credits.

## Dashboard demo mode

```
# Git Bash
STREAMFORGE_DASHBOARD_DEMO=1 .venv/Scripts/python.exe -m streamlit run src/streamforge/dashboard/app.py

# PowerShell
$env:STREAMFORGE_DASHBOARD_DEMO = "1"; .venv/Scripts/python.exe -m streamlit run src/streamforge/dashboard/app.py
```

Run it from the repository root (so `.streamlit/config.toml` applies). Every page shows a **Demo mode** banner.

**Safety.** Demo mode is on only when `STREAMFORGE_DASHBOARD_DEMO` is `1`, `true`, `yes` or `on` **in the shell environment**; `.env` is not consulted, so a configured machine never switches into it by accident. In demo mode `data.get_runner()` returns the demo runner and never creates a Snowflake connection (a unit test fails if it tries). Unset the variable to go back to live data (in PowerShell: `Remove-Item Env:STREAMFORGE_DASHBOARD_DEMO`).

**Where the data comes from** (`src/streamforge/dashboard/demo.py`):

1. The project's deterministic simulator generates 390 orders spread over about a week (240 INR, 60 GBP, 45 EUR, 45 USD; fixed seed and start date 2026-11-16, so the figures are identical on every run), plus about 3 % extra invalid records.
2. Each record gets Kafka-like coordinates (3 partitions, keyed by `order_id`) and is classified by the consumer's real `records.classify()`. Invalid records count as dead-lettered; valid ones as loaded, in batches of 100, which feeds **Pipeline Health**.
3. The dashboard's seven queries are answered with pandas, mirroring the dbt models: STAGING deduplication by `event_id`, `fct_orders` (status from the highest `sequence_number`, not arrival order), `agg_daily_orders`, `agg_daily_product_sales`, the reconciliation view and the anomaly rules. Money stays per currency.

This **approximates** the dbt models for display; it is not dbt output, and the numbers are simulated, not business data. Because the simulator's invalid records are extra copies that the consumer dead-letters, the demo data contains no order-level anomalies, so the anomalies table is empty. Unit tests (`tests/unit/test_dashboard_demo.py`) check the switch, the columns of every query, determinism, batch accounting, per-currency money, date filtering, status-from-sequence, the anomaly rules and the app rendering with the banner.

## Local pipeline demo

```
docker compose -f infra/docker-compose.yml up -d --wait
.venv/Scripts/python.exe -m streamforge.demo --orders 20 --invalid-rate 0.1
```

It runs the production code paths against the local broker:

1. creates **temporary** topics `streamforge.demo.<id>` (3 partitions) and `streamforge.demo.<id>.dlq`, and a temporary consumer group `streamforge-demo-<id>`. It never touches `ecommerce.events`, its DLQ or the `streamforge-consumer` group (enforced in code and tested);
2. publishes simulated events with the real producer (`EventPublisher`, `acks=all`);
3. consumes them with the real `EventConsumer`: invalid records go to the temporary DLQ through `DlqWriter` and the DLQ index; valid ones are written by `FileRawSink` to JSON Lines files (`raw_<event_type>.jsonl`, plus `ingestion_batches.jsonl` as the audit) instead of Snowflake RAW. The file sink is idempotent on (topic, partition, offset), like the RAW `MERGE`. Offsets are committed per batch exactly as in production;
4. prints a summary and deletes the temporary topics and group.

Example summary (counts depend on the options):

```
StreamForge local pipeline demo (no Snowflake)
  published:          …  (… valid, … invalid)
  consumed:           …  in … batch(es)
  RAW rows (files):   …  order_created=…, payment_processed=…, …
  dead-lettered:      …  (DLQ topic streamforge.demo.<id>.dlq)
  duplicates skipped: 0
  committed offsets:  p0=…/…, p1=…/…, p2=…/…
  RAW files:          <temp dir>
  result:             OK - every record accounted for
```

The run exits `0` only when every published record is accounted for: consumed, valid ones in the RAW files, invalid ones in the DLQ, and the committed offsets equal the partition ends. Options: `--orders`, `--seed`, `--invalid-rate`, `--batch-size`, `--output-dir` (default: a new temporary directory) and `--keep-topics` (to inspect the topics with `rpk` afterwards; delete them yourself later). Without a running broker it exits `2` with a hint.

Tested by `tests/unit/test_demo_pipeline.py` (file sink, safety, arguments; no broker) and `tests/integration/test_demo_pipeline_redpanda.py` (end to end on Redpanda; also runs in CI).

## Screenshots

The README's screenshots ([`README.md`](../README.md#screenshots)) are taken from the dashboard demo mode, so they never show real data. To re-take them:

1. Start the dashboard in demo mode (above) and open it at full width (about 1440 px).
2. Leave the default date range (16–23 Nov 2026); select the currency named below.
3. Save each tab as PNG to `docs/images/`:

| File | Tab | Currency |
|---|---|---|
| `docs/images/dashboard-executive.png` | Executive Overview | INR |
| `docs/images/dashboard-orders.png` | Orders | INR |
| `docs/images/dashboard-products.png` | Products | INR |
| `docs/images/dashboard-geography.png` | Geography | INR |
| `docs/images/dashboard-pipeline-health.png` | Pipeline Health | (any) |

Keep the **Demo mode** banner visible in each image. The current images were captured on 2026-11-27 with headless Chrome at a 1440 px viewport, full page height.
