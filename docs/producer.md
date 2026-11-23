# Event Producer (M3)

`streamforge.producer.publisher` publishes simulator records (`docs/event-schema.md`) to Redpanda with `confluent-kafka`. It writes only to the events topic (`KAFKA_EVENTS_TOPIC`, default `ecommerce.events`). The dead-letter topic is reserved for the consumer (M5).

## Prerequisites

1. The local broker is running: `docker compose -f infra/docker-compose.yml up -d --wait` (see `docs/redpanda.md`).
2. The package is installed in `.venv`: `.venv/Scripts/python.exe -m pip install -e ".[dev]"`.
3. Optionally, `.env` sets `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_EVENTS_TOPIC`. The defaults match the local stack, and no credentials are needed.

## Usage

```
.venv/Scripts/python.exe -m streamforge.producer [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--orders N` | `10` | Number of simulated orders. Each order produces 2–3 events. |
| `--seed N` | none | Seed for reproducible records. Without it, IDs are fresh on every run. |
| `--start-time ISO` | now | Start of the simulated clock. Must include a UTC offset, e.g. `2026-11-23T10:00:00Z`. |
| `--invalid-rate R` | `0` | Probability (0–1) of injecting an extra invalid raw record after each valid event. |
| `--orders-per-second R` | `5` | Simulated order arrival rate. This affects `event_timestamp` spacing, not publishing speed. |
| `--currency C` | `INR` | `INR`, `GBP`, `EUR` or `USD`. |
| `--flush-timeout S` | `30` | Seconds to wait at the end for outstanding delivery confirmations. |

Examples:

```
# 100 orders of valid events
.venv/Scripts/python.exe -m streamforge.producer --orders 100

# reproducible run with ~5% invalid records for testing consumer/DLQ handling
.venv/Scripts/python.exe -m streamforge.producer --orders 100 --seed 42 --start-time 2026-11-23T10:00:00Z --invalid-rate 0.05

# another topic (must already exist; auto-creation is disabled)
KAFKA_EVENTS_TOPIC=my.topic .venv/Scripts/python.exe -m streamforge.producer --orders 5
```

Sample summary, printed to stdout (failure details go to stderr):

```
StreamForge producer summary
  topic:      ecommerce.events (3 partitions) @ localhost:19092
  attempted:  12 (valid 8, invalid 4, null key 0)
  delivered:  12
  failed:     0
  pending:    0 (unconfirmed when flush ended)
```

Exit codes: `0` when every message was confirmed delivered. `1` when any delivery failed or stayed unconfirmed, the broker is unreachable, or the topic is missing. `2` for invalid arguments or configuration. `130` when interrupted with Ctrl+C (already-queued messages are still flushed and reported).

To inspect what was published, use `docker exec streamforge-redpanda rpk topic consume ecommerce.events -n 5 -f '%k %v\n'`.

## Message format

| Part | Content |
|---|---|
| Key | The event's `order_id` (UTF-8) when it is a well-formed order id (`ORD-` + 12 hex). Otherwise a **null key**: records with the `missing_order_id` or `malformed_json` defect, or any unusable `order_id`. No fallback key is ever invented, and a missing key never stops a record from being published. |
| Value | `SimulatedRecord.value` as UTF-8 bytes, exactly as produced by the simulator: canonical compact JSON for valid events (money as 2-decimal-place strings, timestamps in UTC with `Z`), and the raw text for invalid records. The producer never parses, validates, re-serializes or repairs a record. Validation belongs to the consumer. |
| Partition | Hash of the key (`murmur2_random` partitioner, Java-client compatible), so all events of one order go to the same partition in order. Null-key records go to a random partition. |
| Timestamp | Kafka's default `CreateTime`, the wall-clock time of publishing. The business time is `event_timestamp` in the value. |

## Delivery semantics

- Producer settings are `acks=all` and `enable.idempotence=true`. Internal retries do not create duplicates or reorder messages within one producer session. `delivery.timeout.ms=30000` bounds how long a message may stay undelivered before it is reported as failed.
- Publishing is asynchronous. A message counts as **delivered only when its delivery callback confirms it**, never when `produce()` returns.
- When the local queue is full (`BufferError`), the producer serves delivery callbacks (`poll`) and retries the same record. If the queue stays full for 30 s, publishing stops with a `QUEUE_FULL_TIMEOUT` failure instead of dropping records.
- On exit, including Ctrl+C, the producer flushes. Messages still unconfirmed after `--flush-timeout` are reported as `pending` and make the exit code non-zero.
- Before publishing, the producer checks that the broker is reachable and the topic exists, and fails fast if not.
- Across separate runs there is no deduplication. Re-running with the same `--seed` and `--start-time` publishes identical events again. Downstream deduplication by `event_id` (M6) handles this, so treat the pipeline as at-least-once.

## Programmatic use

```python
from confluent_kafka import Producer
from streamforge.common.config import load_kafka_settings
from streamforge.producer.publisher import EventPublisher, build_producer_config
from streamforge.producer.simulator import EventSimulator, SimulatorConfig

settings = load_kafka_settings()
publisher = EventPublisher(Producer(build_producer_config(settings)), settings.events_topic)
publisher.publish_all(EventSimulator(SimulatorConfig(seed=1)).iter_records(10))
remaining = publisher.flush(30)
print(publisher.stats)  # attempted / delivered / failed / pending / errors
```

## Tests

- Unit tests (`tests/unit/test_publisher.py`) use a fake producer, so no broker is needed. They cover keys and null-key fallback, value bytes, callback accounting, queue-full retry and timeout, flush and pending handling, topic preflight, and CLI exit codes.
- Integration tests (`tests/integration/test_producer.py`, run with `pytest -m integration`) need the local stack. They check exact keys and values on a temporary 3-partition topic that is deleted afterwards, that null-key invalid records keep their raw payload, and that the CLI publishes to the configured events topic and not the DLQ. **Each run appends a few fresh simulated events to `ecommerce.events`.**

## Limitations

- There is no rate limiting: records are published as fast as the broker accepts them.
- Delivery statistics are per run only. No metrics are exported.
- No authentication or TLS settings, matching the local stack.
