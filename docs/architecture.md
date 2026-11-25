# StreamForge Architecture

## 1. Project Overview

StreamForge is a real-time e-commerce data engineering platform designed to ingest, process, validate, and analyse streaming business events.

**Objective:** Transform raw e-commerce events into reliable, actionable business intelligence.

## 2. High-Level Architecture

```text
E-Commerce Event Simulator
          |
          v
   Python Producer
          |
          v
   Redpanda (Kafka API)
          |
          v
   Python Consumer
          |
          v
   Event Validation
          |
          v
   Snowflake RAW Layer
          |
          v
   SQL / dbt Transformations
          |
          v
   Snowflake ANALYTICS
          |
          v
   Streamlit Dashboard
```

Supporting infrastructure:

- Docker Compose: Local service orchestration
- Snowflake Streams and Tasks: Incremental processing
- Apache Airflow: Workflow orchestration
- AWS S3: Raw event archival
- Pytest: Automated testing
- GitHub Actions: Continuous integration

## 3. Core Components

### 3.1 Event Simulator
Generates simulated e-commerce events, including orders, payments, cancellations, and shipments.

### 3.2 Event Producer
Creates structured events and publishes them to the events topic (`ecommerce.events`), keyed by `order_id` (null key when a record has no usable `order_id`), with `acks=all`, idempotence enabled and delivery confirmed per message by callback. Values are published exactly as generated; validation and dead-letter routing are left to the consumer. Implemented in `streamforge.producer.publisher` (`python -m streamforge.producer`); see `docs/producer.md`.

### 3.3 Redpanda Streaming Layer
Provides Kafka-compatible event streaming, topic management, partitioning, and consumer groups.

### 3.4 Event Consumer
Consumes events, validates schemas, handles retries, and forwards accepted events to Snowflake. Implemented in `streamforge.consumer` (`python -m streamforge.consumer`): batches are validated with `parse_event()`, invalid records go to `ecommerce.events.dlq` unchanged with failure headers, and valid events are loaded into one RAW table per event type in a single transaction via `MERGE` on the Kafka coordinates. Offsets are committed only afterwards. See `docs/consumer.md`.

### 3.5 Snowflake Data Warehouse

Three primary data layers:

- RAW: Original ingested events
- STAGING: Validated and transformed records
- ANALYTICS: Business-ready facts, dimensions, and aggregates

Implemented in M4 as database `ECOMMERCE_STREAMING_DB` (schemas `RAW`, `STAGING`, `ANALYTICS`, plus `OPS` for load audit and reconciliation), the XS warehouse `STREAMFORGE_WH` (60 s auto-suspend) capped by the `STREAMFORGE_MONITOR` resource monitor (5 credits per month), and the least-privilege role `STREAMFORGE_ROLE`. See `docs/snowflake.md`.

### 3.6 Transformation Layer
Uses SQL and dbt for deduplication, incremental transformations, and analytical modelling.

### 3.7 Dashboard
Uses Streamlit to present order metrics, revenue trends, payment performance, and fulfilment insights.

### 3.8 Orchestration and Archival
Airflow manages scheduled workflows and reconciliation. AWS S3 stores raw event archives.

## 4. Event Processing Lifecycle

1. Generate an e-commerce event.
2. Assign a unique `event_id` and a UTC `event_timestamp`.
3. Publish the event to a Redpanda topic.
4. Consume and validate the event.
5. Record invalid events for investigation.
6. Ingest accepted events into Snowflake RAW.
7. Transform and deduplicate records.
8. Publish analytical datasets.
9. Display business metrics through Streamlit.

## 5. Reliability and Fault Tolerance

- Unique event IDs support deduplication.
- `event_timestamp`, `sequence_number` and `schema_version` support out-of-order reconciliation.
- Retries handle transient processing failures.
- Dead-letter handling isolates invalid or repeatedly failing events.
- Append-only raw storage preserves source events.
- Replay mechanisms support recovery.
- Data quality checks identify missing, duplicate, and inconsistent records.

## 6. Security and Cost Controls

- Credentials are stored in environment variables.
- Secrets are excluded from Git.
- Snowflake warehouses use auto-suspend.
- Small compute resources are preferred during development.
- AWS services are introduced only when required.

## 7. Initial Performance Targets

These are engineering targets, not measured results:

| Metric | Target |
|---|---:|
| Event throughput | 100 events/sec |
| Dashboard freshness | Under 10 seconds |
| Duplicate business records | 0 |
| Event accounting | All accepted events reconciled |
| Recovery | Successful replay after restart |

Actual performance will be benchmarked and documented.

## 8. Implementation Strategy

The project will be developed incrementally, prioritising a working end-to-end MVP:

1. Event simulation
2. Redpanda infrastructure
3. Snowflake ingestion
4. Data transformation
5. Data quality and reliability
6. Dashboard
7. Orchestration and archival
8. Testing and performance evaluation