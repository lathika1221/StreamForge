# Event Consumer (M5)

`python -m streamforge.consumer` reads `ecommerce.events` as consumer group `streamforge-consumer`, validates every message against the event contract, writes valid events to Snowflake RAW and routes invalid ones to `ecommerce.events.dlq`. Kafka offsets are committed only after a batch has been fully processed.

## Processing guarantees

Each batch is processed in this order:

1. **Consume** up to `--batch-size` messages, waiting at most `--batch-timeout` seconds.
2. **Classify** each message by running `parse_event()` on its raw bytes:

   | Outcome | When |
   |---|---|
   | valid | The event conforms to `docs/event-schema.md` |
   | `empty_value` | The value is null (tombstone) or zero bytes |
   | `malformed_json` | The value is not parseable JSON, including non-UTF-8 bytes |
   | `missing_field` | A required field is absent |
   | `schema_validation` | Any other contract violation (unknown type, wrong version, bad values, extra fields) |

3. **Dead-letter** invalid records, and wait until every DLQ delivery is confirmed.
4. **Load** valid records into Snowflake RAW in one transaction.
5. **Commit** offsets (last processed offset + 1 per partition), synchronously.

Steps 3–5 each get up to `--max-attempts` attempts, with exponential backoff (1 s, 2 s, …). If a step still fails, **nothing is committed**: the consumer stops with exit code 1, and the next run re-reads the batch from the last committed offsets.

| Property | Guarantee |
|---|---|
| Offset commits | Manual only. Auto-commit and auto offset-store are disabled. Offsets are committed after DLQ delivery and the RAW transaction both succeed. Before committing, every consumed record must be accounted for: valid records must equal rows inserted plus duplicates skipped, and every invalid record must be confirmed in the DLQ. Otherwise the batch fails uncommitted. |
| RAW duplicates | **None per Kafka message.** Rows are inserted by `MERGE` keyed on `(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)`, so a redelivered batch inserts nothing twice. Business duplicates (the same `event_id` published at different offsets) are kept in RAW and removed in the transformation layer (M6). |
| DLQ | **Duplicate-safe on redelivery.** Step retries never repeat a DLQ step that already succeeded. A record already dead-lettered by this group is skipped (`dlq_already_present` in the summary), whether it was sent earlier in the same process or by a run that stopped before committing. In the second case the startup scan finds it: the consumer reads the last `2 × --batch-size` DLQ messages and collects their `streamforge.source_*` coordinates. **A record is skipped only if both its coordinates and its content fingerprint (`streamforge.source_sha256`) match.** A different record at a reused offset (for example, after the source topic was recreated) is always dead-lettered. DLQ entries without a fingerprint never cause a skip. On replay, records whose DLQ entries are older than the scanned tail are dead-lettered again: a duplicate is possible, but a loss is not. Limit: this assumes one consumer instance writes the DLQ, so a failed batch's DLQ records are within that tail. With several instances, deduplicate downstream on the source coordinates plus the fingerprint. |
| Ordering | Per partition, and therefore per `order_id`. |
| Start position | `--offset-reset error` (the default) refuses to guess when the group has no committed offset. Set an explicit boundary first (below). |
| Shutdown | Ctrl+C or SIGTERM finishes the current batch, commits it, then leaves the group. Nothing unprocessed is committed. |

## Snowflake RAW layout

The consumer connects as `STREAMFORGE_ROLE` with secondary roles disabled, and **refuses to run as `ACCOUNTADMIN`, `SYSADMIN`, `SECURITYADMIN`, `USERADMIN` or `ORGADMIN`**. On first use it creates these tables (`CREATE TABLE IF NOT EXISTS`, which needs no warehouse):

| Table | Contents |
|---|---|
| `RAW.ORDER_CREATED`, `RAW.PAYMENT_PROCESSED`, `RAW.ORDER_CANCELLED`, `RAW.SHIPMENT_DISPATCHED` | One append-only table per event type |
| `OPS.INGESTION_BATCHES` | One audit row per loaded batch: offset ranges, records consumed, valid, inserted, duplicates skipped, DLQ count, timings |

RAW columns:

| Column | Source |
|---|---|
| `EVENT_ID`, `EVENT_TYPE`, `SCHEMA_VERSION`, `EVENT_TIMESTAMP`, `ORDER_ID`, `SEQUENCE_NUMBER`, `SOURCE` | Envelope fields of the validated event |
| `PAYLOAD` (VARIANT) | `PARSE_JSON(RAW_VALUE):payload` |
| `RAW_VALUE` | The exact message text as consumed |
| `KAFKA_TOPIC`, `KAFKA_PARTITION`, `KAFKA_OFFSET`, `KAFKA_KEY`, `KAFKA_TIMESTAMP` | Kafka metadata |
| `BATCH_ID` | Links the row to `OPS.INGESTION_BATCHES` |
| `INGESTED_AT` | Snowflake time when the row was loaded |

Loading uses a session-temporary buffer table, then one `MERGE` per event type present, then the audit insert, all in one transaction. A batch containing only invalid records does not touch Snowflake. Empty polls never touch Snowflake either.

## DLQ message format

The value and key are the **original bytes, unchanged**. These headers (UTF-8) describe the failure:

| Header | Value |
|---|---|
| `streamforge.failure_type` | `empty_value`, `malformed_json`, `missing_field` or `schema_validation` |
| `streamforge.failure_reason` | `field.path: message` list, at most 500 characters. **Never contains payload values.** |
| `streamforge.source_topic`, `streamforge.source_partition`, `streamforge.source_offset`, `streamforge.source_timestamp_ms` | Where the record came from |
| `streamforge.source_sha256` | SHA-256 fingerprint of the original key and value bytes (null and empty are distinguished) |
| `streamforge.consumer_group` | The group that rejected it |
| `streamforge.failed_at` | UTC time of rejection |

To inspect: `docker exec streamforge-redpanda rpk topic consume ecommerce.events.dlq -n 5 -f '%h\n%v\n\n'`.

## Usage

```
.venv/Scripts/python.exe -m streamforge.consumer [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--batch-size N` | 500 | Maximum messages per batch, and per Snowflake transaction |
| `--batch-timeout S` | 5 | Seconds to wait while filling a batch |
| `--stop-when-idle S` | off | Exit after `S` seconds without messages. Useful for demos and catch-up runs. |
| `--max-batches N` | off | Exit after `N` batches |
| `--offset-reset` | `error` | What to do when the group has no committed offset: `error`, `earliest` or `latest` |
| `--max-attempts N` | 3 | Attempts per step before stopping without committing |
| `--log-level` | INFO | Python logging level |

Exit codes: `0` normal, `1` processing or infrastructure error (nothing uncommitted was committed), `2` usage or configuration error (including an admin role), `130` interrupted.

Startup checks, before anything is consumed: both topics exist, the DLQ tail is scanned for records this group already dead-lettered, Snowflake is reachable, the role is not an admin role, and the RAW tables exist (they are created if missing).

**Logging:**

- The consumer logs counts, batch IDs and offsets only, never message contents.
- Database and Kafka error texts are scrubbed before they reach logs or exceptions: quoted literals, where databases echo values, are replaced with `'<redacted>'`, and the length is capped. The original exception chain is dropped.
- Connections pin `log_query_text=False`, and the CLI sets the `snowflake.connector` logger to WARNING, so SQL text (which contains interpolated row values) never appears in logs.

## Starting boundary and replay

`ecommerce.events` already contains historical test messages. They are kept, not deleted. To start ingestion *after* them, commit the group's starting offsets once, while no consumer is running.

1. Inspect the current offsets and group state:
   ```
   docker exec streamforge-redpanda rpk topic describe ecommerce.events -p
   docker exec streamforge-redpanda rpk group describe streamforge-consumer
   ```
2. Pin the boundary to exact offsets. `--to-file` commits exactly what you specify and implies `--allow-new-topics`:
   ```
   printf 'ecommerce.events 0 <P0>\necommerce.events 1 <P1>\necommerce.events 2 <P2>\n' \
     | docker exec -i streamforge-redpanda sh -c \
       'cat > /tmp/streamforge-boundary.txt && rpk group seek streamforge-consumer --to-file /tmp/streamforge-boundary.txt'
   ```
   `rpk group seek streamforge-consumer --to end --topics ecommerce.events --allow-new-topics` also works. However, it uses whatever the end is at the moment it runs, so messages produced in between would be skipped. For a group with no commits, `--allow-new-topics` is required.
3. Confirm with `rpk group describe streamforge-consumer`.

**Replay:** stop the consumer, then run `rpk group seek streamforge-consumer --to start --topics ecommerce.events` (or `--to-file` with specific offsets). Messages already loaded are skipped by the `MERGE` (counted as duplicates). Invalid ones are dead-lettered again.

## Tests

| Command | What runs |
|---|---|
| `pytest` | Unit tests: classification, DLQ headers and delivery confirmation, batch ordering, retries and no-commit-on-failure, next-offset commits with gaps and out-of-order offsets, DLQ de-duplication (within retries, same-process batch retry, restart with tail scan), byte-identical keys and values for malformed records, no payload values or credentials in logs, redelivery, idle and stop, Snowflake SQL and transactions, admin-role refusal, CLI. No broker and no Snowflake. |
| `pytest -m integration` | `tests/integration/test_consumer_redpanda.py`: each test uses its own temporary topics (deleted afterwards) and a unique group. It checks real DLQ publishing with key and value byte-identical to the source message, committed offsets against partition ends, and a rerun that reprocesses nothing. A restart scenario (DLQ succeeded, Snowflake failed, nothing committed, restart) checks that the DLQ is not duplicated. It never touches `ecommerce.events` or `streamforge-consumer`. |
| `pytest -m snowflake` | `tests/integration/test_snowflake_sink_live.py`: real `MERGE` into uniquely prefixed `IT_…` tables (dropped afterwards), a duplicate-safe reload, and traceability columns and audit rows. Costs about 0.017 credits (the 60 s minimum). |

## Limitations

- **Throughput:** one consumer process and one Snowflake transaction per batch. Throughput has not been benchmarked yet.
- **Warehouse cost:** each batch with valid events resumes `STREAMFORGE_WH`. With continuous traffic the warehouse stays up, capped by the 5-credit monthly `STREAMFORGE_MONITOR`.
- **DLQ consumption:** the DLQ is only written; nothing consumes it automatically.
