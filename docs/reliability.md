# Reliability, failure handling and recovery (M8)

This page states what the ingestion path (`streamforge.consumer`) guarantees, where those guarantees stop, and how to recover after a failure. Every guarantee listed here is backed by a test; the tests are listed at the end.

**StreamForge does not provide end-to-end exactly-once processing.** Kafka delivery to the consumer is **at-least-once**. Duplicates from redelivery are absorbed by **idempotent writes**: a `MERGE` in Snowflake RAW, and a de-duplication index in front of the DLQ. RAW is duplicate-free per Kafka message. The DLQ is duplicate-free per source message, except in the two cases listed under [Known limitations](#known-limitations). In no failure case covered by the tests is a message lost.

## Message lifecycle

For each batch (`EventConsumer.process_batch`):

1. **`consume()`** returns up to `--batch-size` messages. Partition assignment and revocation callbacks run only inside this call.
2. **`classify()`** splits the messages into valid events and invalid records.
3. **DLQ index refresh** reads any DLQ messages appended since the last refresh (only when the batch has invalid records).
4. **DLQ publish** sends the invalid records that are not already in the DLQ, then waits for every delivery confirmation (`acks=all`, idempotent producer).
5. **RAW load** inserts the valid events and one audit row in **one Snowflake transaction**: a `MERGE` on `(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)`.
6. **Accounting check:** valid = inserted + duplicates, and every invalid record is confirmed in the DLQ.
7. **Synchronous offset commit** of the last processed offset + 1, per partition.

Steps 3, 4, 5 and 7 each get `--max-attempts` attempts with exponential backoff. Each DLQ publish attempt refreshes the index again first. If a step still fails, the consumer stops with exit code 1 and **no offset is committed** for that batch.

## Guarantees

| Area | Guarantee | Mechanism |
|---|---|---|
| Kafka delivery | At-least-once. A message is redelivered until a batch containing it is committed. | Auto-commit and auto offset-store off; commit only after steps 4–6 succeed. |
| Offset commits | **Never beyond what has been persisted.** A commit covers exactly the batch just processed, per partition. A graceful stop (Ctrl+C or SIGTERM) finishes and commits the current batch only. Closing the consumer commits nothing, even when the client has fetched further messages. | Synchronous commit at the end of `process_batch`; `next_offsets()` computes last + 1 per partition. |
| Snowflake RAW | **Idempotent per Kafka message.** Reloading a batch inserts nothing twice; rows are not lost on rollback. | `MERGE` keyed on `(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)` inside one transaction (M5). |
| Business duplicates | The same `event_id` published at two offsets gives two RAW rows. **dbt STAGING keeps the earliest copy.** | RAW is append-only by design; dedup happens in transformations (M6). |
| DLQ | **One entry per invalid source message** across retries, crashes, restarts, replays of old offsets, and partition handoffs between consumers. | `DlqIndex` (below). |
| Recovery | After any failure covered by the tests, a plain restart reaches the correct end state. No manual data repair is needed. | All of the above. |

### DLQ de-duplication (`DlqIndex`)

The index is built from the DLQ topic itself, so it needs no extra storage, table or schema.

- **Identity:** each DLQ message carries its source identity in headers: topic, partition, offset, and a SHA-256 of the original key and value bytes. A record is skipped only if the DLQ holds an entry **from the same consumer group** with the **same coordinates and fingerprint**.
  - A different record at a reused offset (for example, after the topic was recreated) is never suppressed.
  - Entries with incomplete headers are ignored, so they never cause a skip.
- **Startup:** a full scan of the retained DLQ, from each partition's low watermark. (The M5 version scanned only the last `2 × batch_size` messages, so replaying older offsets duplicated DLQ entries.)
- **During the run:** an incremental refresh before every DLQ publish attempt reads only newly appended DLQ messages. This picks up:
  - entries written by a crashed predecessor;
  - entries written by the previous owner of a partition after a rebalance;
  - entries actually delivered by a failed attempt (partial delivery, or a timeout after the broker stored the message). In M5 such a retry re-sent the whole list; that bug is fixed.
- **Retention:** if retention moves a partition's low watermark past the index position, the read continues from the new low watermark. Identities already learned stay known.
- **Offset gaps:** offsets that carry no message (transaction markers, compaction) are skipped by reading the reader's position, so they don't stall a refresh.
- **Failure handling:** a refresh that can't finish within its timeout raises `DlqError`, and is retried like any other step. A missing DLQ topic is an error, never an empty index. Until a refresh succeeds, nothing is published and nothing is committed.

## Concurrent consumers and rebalancing

Within one consumer group, Kafka gives each partition to exactly one member per group generation. The consumer processes and commits each batch synchronously between `consume()` calls, and the eager rebalance callbacks run only inside `consume()`. So when partitions are revoked from a healthy member, **nothing is in flight**: its last batch is already committed, and the new owner starts from that commit. Tested with two live members (`test_10b`).

**The remaining race.** Two processes can handle the same record at the same moment only when a member is **evicted while still running**. That happens when it exceeds `max.poll.interval.ms` (300 s), freezes (VM suspend, debugger, `SIGSTOP`), or loses its connection to the group coordinator but not to the partition leader. If the evicted member is between its DLQ index refresh and its DLQ publish while the new owner processes the same record, **both may publish**. The result is a duplicate DLQ entry, never a loss.

`test_concurrent_publish_between_refresh_and_send_can_duplicate_but_never_loses` reproduces this deliberately. There is no claim of duplicate-free *concurrent* publishing.

**How small the window is:**
- The refresh and the publish happen back to back, right after `consume()`.
- With a healthy broker the refresh and publish take milliseconds, far below the 300 s poll interval.
- **With a degraded broker the step is not guaranteed to fit inside 300 s.** There is no overall deadline per batch, only per-call timeouts (defaults, one DLQ partition):
  - One refresh can take up to about 90 s: 30 s metadata, plus 30 s watermarks per DLQ partition, plus 30 s reading.
  - The batch-level refresh is tried up to 3 times (≈ 270 s plus 3 s backoff).
  - Each publish attempt refreshes again (≈ 90 s) and then sends (up to 30 s with the producer queue full, plus a 30 s flush), so up to 3 × 150 s.
  - The RAW load retries also count towards the same poll interval.
  - So a member stuck in retries against a struggling broker **can** exceed `max.poll.interval.ms` and be evicted while still running. The race then needs the new owner to reach the same record while the old member is still between refresh and send. Bounding the whole batch below the poll interval (one deadline shared by all retries) would narrow this; it isn't implemented.
- An evicted member that reaches its RAW load is harmless: the `MERGE` is idempotent. Its offset commit is then rejected by the broker (stale generation).

**Why the race isn't fixed in M8.** Closing it fully needs Kafka transactions with group fencing (KIP-447):
- The DLQ messages and the source offsets are committed in one producer transaction (`send_offsets_to_transaction`), so a zombie's transaction is aborted and its DLQ messages become invisible to `read_committed` readers.
- The local Redpanda has transactions enabled, so **no new infrastructure or schema** is needed.
- But it **changes the M5 offset-commit mechanism**: offsets would be committed by the producer, not `consumer.commit()`. Every DLQ reader would need `isolation.level=read_committed`.
- That deserves its own reviewed change, so it is listed as an option, not implemented.

## Known limitations

1. **Concurrent publish race:** as described above. The consequence is a possible duplicate DLQ entry, never a loss. Consumers of the DLQ can de-duplicate on (`source_topic`, `source_partition`, `source_offset`, `source_sha256`).
2. **Retention:** neither topic sets `retention.ms`, so both inherit the broker's default (7 days). Nothing pins them to be equal: if someone shortened the DLQ's retention, old DLQ entries would expire first and replays would re-dead-letter them. Even with equal retention, where a DLQ entry is always written after its source message, retention deletes whole segments, so a DLQ entry can expire while its source message is still retained (segment boundaries differ). Replaying that source message would then dead-letter it again: a duplicate, not a loss. This needs a replay of messages close to the retention limit.
3. **Audit coverage:** `OPS.INGESTION_BATCHES` gets a row only for batches containing at least one valid record, because the sink is not called for all-invalid batches. DLQ counts in OPS and the dashboard therefore exclude all-invalid batches. Those are fully visible in the DLQ topic and the `consumer_summary` log.
   - Closing this needs a Snowflake write, and a warehouse resume, per DLQ-only batch. No schema change is needed.
   - It is proposed, not implemented, because it changes M5 sink behaviour and needs a paid live test.
4. **Retry and rebalance counts** are in the logs (`consumer_summary`), not in Snowflake. Storing them in OPS would need new audit columns, i.e. a schema migration.
5. **Process death:** the tests model a crash by closing the consumer without committing. A real crash leaves the group only after `session.timeout.ms` (45 s), so the replacement starts that much later. The data outcome is identical.
6. **Memory:** the DLQ index holds one identity per DLQ entry in the retention window. That is negligible at demo volume. It grows linearly with DLQ volume and is read in full at every start.

## Recovery procedures

**After the consumer exits with code 1** (Snowflake, DLQ or commit failure, retries exhausted):
1. Read the `error:` line and the `consumer_summary` log line: `retries` shows which step failed.
2. Fix the cause: the warehouse or resource monitor, network, broker, or credentials.
3. Start the consumer again with the same command. It resumes from the last committed offsets. Already-persisted RAW rows are skipped by the `MERGE`, and already-dead-lettered records by the DLQ index. No manual cleanup is needed.

**After a crash or kill:** same as above. If the restart is within about 45 s, the first `consume()` may wait for the old member's session to expire.

**Replaying source messages** (rebuilding RAW after a fix, or re-validating):
1. Stop the consumer. The group must be empty.
2. Move the group's offsets back with `rpk group seek streamforge-consumer --to-file <file>`. This is a deliberate operator action on the real group; see `docs/consumer.md` "Starting boundary and replay".
3. Start the consumer. RAW inserts nothing twice and the DLQ gets no second entry; the summary shows the replayed records as `duplicates skipped` and `already in dlq`. This is demonstrated on a temporary group in `test_09`, with DLQ entries far outside the old tail window.

**DLQ lifecycle:**
- **Written:** by the consumer only, never by the producer (M3).
- **Contents:** the original key and value bytes, plus the `streamforge.*` headers (`docs/consumer.md`).
- **Retention:** 7 days.
- **Inspection:** `rpk topic consume ecommerce.events.dlq -n 10 -f '%h %v\n'` inside the broker container (read-only; it uses no consumer group, commits nothing and removes nothing).
- **Re-ingest:** DLQ records are not replayed automatically: they failed the contract and would fail again. To re-ingest a corrected event, publish the corrected event to `ecommerce.events` (it gets a new offset and is validated normally), with the same `event_id` if it represents the same business event, so that STAGING keeps one copy.

## Failure-injection tests

The scenario tests use real Redpanda offsets, commits, rebalances and DLQ delivery on temporary topics and groups, which are deleted afterwards. Snowflake is replaced by an in-memory RAW store with the same `MERGE` semantics, so **no credits are used**.

Each scenario checks five things:
- the source message is still recoverable;
- whether an offset was committed;
- what reached RAW and the DLQ;
- that no duplicates were created;
- that a plain restart reaches the exactly-once end state (`assert_exactly_once`).

| # | Scenario (`tests/integration/test_consumer_recovery.py`) | Outcome checked |
|---|---|---|
| 1 | `test_01`: broker unreachable at CLI startup | Exit 1. Nothing consumed or committed. The same command later recovers fully. |
| 2 | `test_02`: real DLQ delivery failure (topic missing) | Bounded retry, then stop before RAW. Nothing committed and the DLQ empty. A restart recovers. |
| 3 | `test_03`: Snowflake connection failure | DLQ written, RAW empty, nothing committed. A restart adds no DLQ duplicates. |
| 4 | `test_04`: Snowflake transaction rolled back | Nothing persisted or committed. A transient failure is retried inside the same run. |
| 5 | `test_05`: killed after the RAW commit, before the offset commit | The restart reprocesses the batch; the `MERGE` skips every persisted row. |
| 6 | `test_06`: killed after DLQ delivery, before the offset commit | The restart finds every DLQ entry (`already in dlq`); none are re-sent. |
| 7 | `test_07`: the same bytes published twice, then redelivered | Two RAW rows (two Kafka messages), one DLQ entry per source message; redelivery adds nothing. |
| 8 | `test_08`: graceful stop, restart with uncommitted messages | Only the processed batch is committed. The restart resumes there with no reprocessing. |
| 9 | `test_09`: replay of all committed messages (group rewound) | Zero RAW inserts and zero new DLQ entries, including entries beyond the old tail window. |
| 10 | `test_10a`: crash on the 3rd commit, 3 partitions | Broker offsets equal the two committed batches per partition; the restart completes every partition. |
| 10 | `test_10b`: two concurrent group members | Partitions split with a rebalance; every message is handled once and every partition is fully committed. |

Unit tests in `tests/unit/test_dlq_index.py` cover the index and the retry path deterministically:
- full scan, incremental refresh, several partitions;
- retention and offset gaps;
- malformed or foreign headers, a missing topic, refresh timeout and retry;
- replay after restart, partial delivery, partition handoff;
- the concurrent-publish race.

```
.venv/Scripts/python.exe -m pytest tests/unit/test_dlq_index.py                     # no broker
docker compose -f infra/docker-compose.yml up -d --wait                              # local Redpanda
.venv/Scripts/python.exe -m pytest -m integration tests/integration/test_consumer_recovery.py   # ~4 min
```
