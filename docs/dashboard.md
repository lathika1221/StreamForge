# Business Intelligence Dashboard (M7)

A Streamlit dashboard over the dbt outputs (M6), with a dark navy theme and restrained blue accents. Code: `src/streamforge/dashboard/`. Theme: `.streamlit/config.toml`.

```
.venv/Scripts/python.exe -m streamlit run src/streamforge/dashboard/app.py
```

Run it from the repo root, so `.env` and `.streamlit/config.toml` are found. It needs no new dependencies (Streamlit, Altair and pandas are already in `.venv`) and no paid services.

**Without Snowflake:** set `STREAMFORGE_DASHBOARD_DEMO=1` in the shell to run on simulated data with a visible "Demo mode" banner and no Snowflake connection. See [`demo.md`](demo.md#dashboard-demo-mode).

## Access and safety

- **Snowflake access:** reads `ECOMMERCE_STREAMING_DB.ANALYTICS` and `OPS` as **STREAMFORGE_ROLE** with the existing key-pair settings from `.env`, through `streamforge.common.snowflake.connect()`, which disables secondary roles. The data layer refuses any other configured role and re-checks `CURRENT_ROLE()` after connecting.
- **Read-only:** a unit test checks that every query is a `SELECT` against ANALYTICS or OPS. It never touches RAW, Kafka, the warehouse settings or the resource monitor.
- **Errors:** errors shown in the UI are scrubbed. Quoted values are redacted, configured secrets are removed and the exception chain is dropped. Streamlit's `showErrorDetails = "none"` hides tracebacks, and Streamlit usage statistics are disabled.
- **HTML:** cards and the header take only values the code has formatted, and `card()` HTML-escapes its text. All data rows are shown with `st.dataframe` or Altair, never as raw HTML.

## Layout

- **Header:** the time of the latest ingestion batch and the selected date range.
- **Sidebar:**
  - **Order date range (UTC):** defaults to the 30 days ending at the latest data date, capped at 366 days.
  - **Currency:** all monetary figures are for one currency at a time.
  - **Refresh data:** clears the query cache.

| Tab | Content | Source (columns) |
|---|---|---|
| Executive Overview | Gross order value, paid value, net revenue, total orders, payment success rate and cancellation rate for the selected currency, plus a per-currency summary table (one row per currency, **no combined total**) | `ANALYTICS.AGG_DAILY_ORDERS` (`order_date, currency, orders_created, orders_paid, orders_payment_failed, orders_cancelled, gross_order_value, paid_value, net_revenue`) |
| Orders | Orders by current status; payment status distribution; orders per day by currency; net revenue per day for the selected currency | `ANALYTICS.FCT_ORDERS` (`current_status, payment_status, currency, order_date, has_order_created`) and `AGG_DAILY_ORDERS` |
| Products | Top-selling products by units; net revenue per product; SKU table (units, orders, gross, net) for the selected currency | `ANALYTICS.AGG_DAILY_PRODUCT_SALES` (`sku, product_name, currency, units, orders, gross_value, net_value`) |
| Geography | Orders and net revenue by `shipping_country` (ISO code), with a table | `ANALYTICS.FCT_ORDERS` (`shipping_country, currency, order_total, payment_status, cancelled_at_utc`) |
| Pipeline Health | Batches, records consumed, rows inserted and duplicates skipped; reconciliation status; dead-letter count and rate; last load time; data-quality anomalies; recent batches (every batch, in a compact table: time to the minute, short batch id, all counts, and one **Checks** column that reads `OK` or names the failing checks: `rows`, `accounting`, `load`) | `OPS.OPS_INGESTION_RECONCILIATION`, `OPS.OPS_ORDER_ANOMALIES` |

**Tables** are sized to their rows, so nothing is hidden behind an inner scroll bar (Streamlit's default shows at most ten rows); only the anomaly-details table inside its expander, which can hold up to 500 rows, keeps a scrolling height. Every column is wide enough for its heading and values.

**Demo mode captions:** with `STREAMFORGE_DASHBOARD_DEMO` on, the sidebar caption and the Pipeline Health dead-letter note say the data is simulated and that no Snowflake, Kafka or audit table is read. Otherwise they describe the Snowflake source (`ECOMMERCE_STREAMING_DB` ANALYTICS/OPS as `STREAMFORGE_ROLE`; dead-letter figures from the consumer audit).

## Metric rules

- **Money is never summed across currencies.** Every monetary query groups by currency, and the page shows one currency at a time.
- **Rates are recomputed from summed counts over the period, not averaged across days:**
  - payment success = succeeded ÷ (succeeded + failed);
  - cancellation = cancelled ÷ created.

  A rate with no denominator is shown as "—".
- Definitions of gross, paid and net come from `docs/dbt.md`.
- Orders without an `order_created` event have no date or currency. They are excluded from the time-filtered views and appear under Pipeline Health anomalies.
- **Missing values:** a missing product name falls back to the SKU; a missing country shows as `UNKNOWN`; a missing payment shows as "No payment yet". Empty periods show an explanatory empty state instead of an error.
- **Geography:** only `shipping_country` exists in the events. There are no country names, regions or coordinates, so no map is drawn.
- **Dead-letter figures** are the consumer's own counts (`dlq_records` in the audit table). The DLQ messages themselves stay in Kafka and are not queried from the dashboard.

## Queries and cost

- **Seven small queries per load**, with date filters sent to Snowflake as bound parameters. Pipeline tables are limited to the latest 200 batches and 500 anomalies.
- **Caching:** `st.cache_resource` keeps one connection, and `st.cache_data` caches every result for **10 minutes**. Reruns, tab switches and other viewers within that time don't query Snowflake.
- **Cost:** a cold load runs in one resume of the XS `STREAMFORGE_WH`, about 0.017 credits for the 60 s minimum. The warehouse auto-suspends after 60 s idle, and the 5-credit monthly monitor still applies.

## Tests

| Command | Covers |
|---|---|
| `pytest` | `tests/unit/test_dashboard_metrics.py`: KPIs per currency, pooled rates, formatting, empty and null inputs, status, product, country and pipeline summaries. `test_dashboard_queries.py`: queries read-only and scoped, dates bound, money grouped by currency, role refusal, scrubbed errors, reconnect. `test_dashboard_app.py`: headless `AppTest` of the real app with fake data (all tabs, currency switching, caching, empty datasets, missing values, safe error display, production captions, every recent batch shown). `test_dashboard_demo.py`: demo mode (switch, no Snowflake, query columns, accounting, banner, demo captions, every SKU and batch row shown, all columns within the page width, all tables sized to their rows). |
| `pytest -m snowflake tests/integration/test_dashboard_live.py` | Renders the app headlessly against the live models; the net-revenue card must equal the dbt aggregate. Suspends the warehouse afterwards. |

## Limitations

- **Little data so far:** 3 orders on one day, in one currency and one country. Trend charts show a single point, and the page says so.
- **Not live-streamed:** the dashboard is as fresh as the last dbt build plus the 10-minute cache. It doesn't refresh itself.
- **Local only:** there is no authentication on the Streamlit server, so run it on localhost only, as documented.
