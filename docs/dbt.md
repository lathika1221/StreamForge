# dbt Transformation Layer (M6)

dbt Core turns the append-only RAW tables written by the M5 consumer into deduplicated staging views, business-ready analytics tables, and operational checks. Project: `dbt/`.

```
RAW.ORDER_CREATED ─────► stg_order_created ──┬─► stg_order_items ─► fct_order_items ─► agg_daily_product_sales
RAW.PAYMENT_PROCESSED ─► stg_payment_processed ┤
RAW.ORDER_CANCELLED ───► stg_order_cancelled ──┼─► stg_order_events ─► fct_order_events ─► fct_orders ─► agg_daily_orders
RAW.SHIPMENT_DISPATCHED► stg_shipment_dispatched┘                                     └─► ops_order_anomalies
OPS.INGESTION_BATCHES + RAW.* ─────────────────────────────────────────────────────────► ops_ingestion_reconciliation
```

## Setup

dbt lives in its **own virtualenv**, `.venv-dbt`. Installing it into the main `.venv` would downgrade `python-dateutil` (4.0.0 → 2.9.0.post0), which the runtime packages use.

```
<Python 3.11>/python.exe -m venv .venv-dbt
.venv-dbt/Scripts/python.exe -m pip install -r dbt/requirements.txt   # dbt-core 1.12.6, dbt-snowflake 1.12.5
```

The versions are pinned to stable releases at least two weeks old. No dbt packages are used (no `dbt deps`); the two generic tests normally taken from `dbt_utils` are in `dbt/tests/generic/`.

## Running

Always go through the runner. It loads `.env`, passes the key passphrase only as `DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` (dbt scrubs `DBT_ENV_SECRET_*` values from its logs), refuses any role other than `STREAMFORGE_ROLE`, requires key-pair authentication, and disables dbt's usage tracking.

```
.venv-dbt/Scripts/python.exe dbt/run_dbt.py debug                  # connection + profile check
.venv-dbt/Scripts/python.exe dbt/run_dbt.py parse                  # no warehouse
.venv-dbt/Scripts/python.exe dbt/run_dbt.py build                  # models + tests + unit tests
.venv-dbt/Scripts/python.exe dbt/run_dbt.py build --full-refresh   # rebuild incremental models
.venv-dbt/Scripts/python.exe dbt/run_dbt.py source freshness       # RAW freshness (warn only)
```

`dbt/profiles.yml` holds no secrets. Every value comes from `env_var()`.

## Session safety

- **`on-run-start`** first runs `USE SECONDARY ROLES NONE`, then aborts unless `CURRENT_ROLE()` is `STREAMFORGE_ROLE` **and** `CURRENT_SECONDARY_ROLES()` is empty. The user's default secondary roles are `ALL`, which would add `ACCOUNTADMIN`.
- **Every model** runs `USE SECONDARY ROLES NONE` as the **first** statement on its connection, then `ALTER SESSION SET TIMEZONE = 'UTC'`, then the model SQL. dbt's debug log (`dbt/logs/dbt.log`) shows this order.
- **Data-test queries** have no hooks. They only read `ECOMMERCE_STREAMING_DB`.
- `generate_schema_name` writes to the existing `STAGING`, `ANALYTICS` and `OPS` schemas verbatim. No schemas are created, and the M4 grants are sufficient.
- Tables are transient (no Fail-safe storage).

## Models

| Model | Schema / type | Grain | Notes |
|---|---|---|---|
| `stg_order_created`, `stg_payment_processed`, `stg_order_cancelled`, `stg_shipment_dispatched` | STAGING / view | one row per `event_id` | Payload flattened and typed; timestamps converted to UTC; Kafka lineage and a `raw_value_sha256` fingerprint kept. **Deduplicated by `event_id`**: the earliest ingested copy wins, with the Kafka position as tie-break. |
| `stg_order_items` | STAGING / view | order × line | `LATERAL FLATTEN` of `payload.items`: `sku`, `product_name`, `quantity`, `unit_price`, `line_total` |
| `stg_order_events` | STAGING / view | one row per event | Lean union of the four event views |
| `fct_order_events` | ANALYTICS / incremental merge (`event_id`) | one row per event | See incremental strategy below |
| `fct_orders` | ANALYTICS / incremental merge (`order_id`) | one row per order | Lifecycle state and quality flags (below) |
| `fct_order_items` | ANALYTICS / table | order × line | Adds the order's currency, date, status and `is_net_revenue` |
| `agg_daily_orders` | ANALYTICS / table | UTC date × currency | Counts, values and rates. **Never sums across currencies.** |
| `agg_daily_product_sales` | ANALYTICS / table | UTC date × sku × currency | `units`, `orders`, `gross_value`, `net_value` |
| `ops_ingestion_reconciliation` | OPS / view | consumer batch | Audit versus RAW rows. Invalid records appear here as `dlq_records`; their contents stay in the Kafka DLQ. |
| `ops_order_anomalies` | OPS / view | order × anomaly | Data findings reported by warn-level tests |

**Only attributes present in the events are modelled.**
- **Customers:** only `customer_id`. There are no names, emails or segments.
- **Products:** only `sku`, `product_name` and `unit_price`. There are no categories or stock levels.

## Ordering, duplicates, missing events and invalid records

- **Event ordering:** `fct_orders.current_status` is taken from the event with the **highest `sequence_number`** (ties: event time, then Kafka position), never from arrival order:

  | Latest event | `current_status` |
  |---|---|
  | `order_created` | `awaiting_payment` |
  | `payment_processed`, succeeded | `paid` |
  | `payment_processed`, failed | `payment_failed` |
  | `order_cancelled` | `cancelled` |
  | `shipment_dispatched` | `shipped` |

- **Duplicate Kafka positions** cannot reach RAW (the consumer's MERGE prevents them). A source test asserts that `(kafka_topic, kafka_partition, kafka_offset)` is unique.
- **Duplicate events** (the same `event_id` at different offsets) are collapsed in staging. If the copies differ in content, `ops_order_anomalies` reports `conflicting_duplicate_event`.
- **Missing events:** every `order_id` seen in *any* event is kept. `has_order_created = false` marks an order whose `order_created` is missing, and `has_sequence_gap` marks sequences that aren't contiguous from 1. Both are reported as anomalies.
- **Invalid records** never reach Snowflake. They are counted per batch in `ops_ingestion_reconciliation.dlq_records`.

## Incremental strategy

- **`fct_order_events`:** merges events whose `ingested_at_utc` is after the current maximum minus `incremental_lookback_hours` (default 1). The look-back catches late arrivals, and MERGE makes re-reading the window harmless.
- **`fct_orders`:** recomputes only the orders touched since its own watermark (`last_ingested_at_utc` minus the look-back), each from its **complete** event history. A late payment or shipment therefore always corrects the order.
- **Aggregates:** rebuilt as tables on every run (one row per day). `--full-refresh` rebuilds everything from RAW and must give the same results.

Why this is safe for late-arriving events:

- **Arrival-time watermarks.** Both watermarks use `ingested_at` (arrival), not `event_timestamp` (business time). An event that happened long ago but arrives now is always selected.
- **Data-relative watermarks.** They're computed from the data already in the table, not from the clock, so dbt can be paused for any length of time without skipping events.
- **Look-back covers late commits.** The 1-hour look-back covers rows whose consumer transaction commits after dbt has read the table. Re-reading is harmless because both models merge.
- **Full-history recompute.** A touched order is recomputed from all its events and staging rows, not only those in the window.
- **Limitation.** Rows inserted into RAW with an *old* `INGESTED_AT`, such as a manual backfill, would fall outside the window. Run `build --full-refresh` after any such load. The consumer always stamps `CURRENT_TIMESTAMP()`, so normal operation is not affected.

## Metric definitions (`agg_daily_orders`, `agg_daily_product_sales`)

| Metric | Definition |
|---|---|
| `gross_order_value` | Σ `order_total` of created orders |
| `paid_value` | Σ `order_total` of orders whose latest payment succeeded |
| `net_revenue` / `net_value` | Paid orders that were **not** cancelled |
| `payment_success_rate` | paid ÷ (paid + payment failed) |
| `cancellation_rate` | cancelled ÷ created |

Order dates are UTC. Orders without an `order_created` event have no date or currency, so they're excluded from the aggregates and listed in `ops_order_anomalies`.

## Tests

| Level | What |
|---|---|
| Source (error) | Kafka position unique per RAW table; `event_id`, `order_id` and `sequence_number` not null; `event_type` accepted values; `batch_id` unique in the audit table |
| Staging (error) | `event_id` unique and not null; accepted values for currency, payment method and status, failure reason, cancellation reason, `cancelled_by` and carrier; amounts and quantities positive; failure reason present only for failed payments; line total = quantity × unit price; order total = Σ lines |
| Analytics (error) | `order_id` unique; status accepted values; relationships `fct_order_events` and `fct_order_items` → `fct_orders`; aggregate grains unique; counts and values consistent; rates within 0–1 |
| Reconciliation (error) | `assert_raw_rows_match_audit`: for every batch, RAW rows = `rows_inserted`, consumed = valid + DLQ, valid = inserted + duplicates; no RAW rows without an audit row |
| Business rules (warn) | Sequence gaps, missing `order_created`, shipment without successful payment, cancelled and shipped, payment or currency mismatch, conflicting duplicates |
| dbt unit tests (mocked data, 6) | `fct_orders_lifecycle_ordering_and_missing_events`: out-of-order arrival, failed payment then cancellation, missing `order_created`. `stg_order_created_keeps_earliest_copy_of_duplicate_event`: duplicates. `ops_order_anomalies_flags_conflicts_and_lifecycle_issues`: conflicting duplicates. `agg_daily_orders_never_mixes_currencies`: multi-currency. `fct_order_events_incremental_window_uses_arrival_time` and `fct_orders_late_event_recomputes_order_from_full_history`: incremental look-back and late arrivals. |
| pytest (`tests/unit/test_dbt_runner.py`) | Passphrase only under the scrubbed name, admin roles refused, key-pair only, no credentials in `profiles.yml` |

## Cost

`STREAMFORGE_WH` is an XS warehouse (1 credit/hour, billed per second with a 60 s minimum per resume, auto-suspend 60 s). `parse` needs no warehouse. A full `build` runs within one resume, about 0.03–0.05 credits. Staging models are views, so they have no storage and are computed when queried downstream.
