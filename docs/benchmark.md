# Kafka-Path Benchmark (M9.4)

Measured throughput of the producer and consumer against a **single Redpanda broker**, with **no Snowflake**: the consumer's RAW writes go to an in-memory sink. Results are reported separately for the local laptop and a GitHub-hosted runner; neither is a production-capacity claim.

## How to run it

```
docker compose -f infra/docker-compose.yml up -d --wait
.venv/Scripts/python.exe -m streamforge.benchmark --events 2000 10000 --batch-sizes 100 500 --repeats 3 --json benchmark-results.json
```

Or on GitHub: **Actions → Benchmark → Run workflow** (`.github/workflows/benchmark.yml`, manual only). It runs the same command on a standard GitHub-hosted runner, prints the runner's hardware, writes the table to the job summary and uploads `benchmark-results.json` as an artifact. No secrets and no Snowflake.

## Method

For each workload size and repeat (`src/streamforge/benchmark/run.py`):

1. The deterministic simulator pre-generates exactly N records, about 5 % of them invalid (`--invalid-rate 0.05`). Generation is not timed.
2. Temporary topics `streamforge.bench.<id>` (3 partitions) and `.dlq` are created. The real topics and the `streamforge-consumer` group are refused in code.
3. **Producer:** the real `EventPublisher` (`acks=all`, idempotent) publishes all N records. Timed from the first publish to the final flush, when every delivery is confirmed.
4. **Consumer:** once per batch size, with a fresh consumer group, the real `EventConsumer` runs: validation, `DlqWriter` and `DlqIndex` for invalid records, an in-memory RAW sink for valid ones, and synchronous per-partition offset commits. Timed from the start of the first batch to the end of the last batch's commit, so group join and start-up are excluded.
5. Every run must account for every record (RAW rows = valid, DLQ = invalid, consumed = N). A run that loses or duplicates records fails instead of reporting a number.
6. The temporary topics and groups are deleted.

## Results: local laptop, 2026-11-26

Environment: Windows 11 (10.0.26300), AMD Ryzen 7 7435HS (8 cores, 16 logical CPUs), 15.8 GB RAM, Python 3.11.6, confluent-kafka 2.16.2 / librdkafka 2.16.0, Redpanda v26.2.6 in Docker Desktop (`--smp=1 --memory=1G`). 3 runs per size; laptop kept awake; 1.5–2.6 GB RAM free throughout.

| Events | Measurement | Median | Min | Max |
|---:|---|---:|---:|---:|
| 2,000 | Producer, events/s | 3,732 | 3,618 | 3,792 |
| 2,000 | Consumer, batch 100, events/s | 43 | 43 | 43 |
| 2,000 | Consumer, batch 500, events/s | 214 | 213 | 214 |
| 10,000 | Producer, events/s | 16,644 | 15,927 | 16,980 |
| 10,000 | Consumer, batch 100, events/s | 41 | 41 | 42 |
| 10,000 | Consumer, batch 500, events/s | 202 | 202 | 202 |

Median batch processing time was 2,509–2,510 ms in every consumer run, at both batch sizes.

## Results: GitHub-hosted runner, 2026-11-27

Environment: GitHub-hosted `ubuntu-latest` runner on Azure, AMD EPYC 9V74 (4 vCPUs allocated), 15,983 MiB RAM reported by `free -m` (12,778 MiB free at the hardware check), Linux 7.0.0-1020-azure x86_64 (glibc 2.43), Python 3.11.18, librdkafka 2.16.0, Redpanda v26.2.6. 3 runs per workload; 5 % invalid records. The uploaded `benchmark-results.json` artifact records the environment and all six runs.

| Events | Measurement | Median | Min | Max |
|---:|---|---:|---:|---:|
| 2,000 | Producer, events/s | 3,956 | 2,398 | 3,964 |
| 2,000 | Consumer, batch 100, events/s | 43 | 43 | 43 |
| 2,000 | Consumer, batch 500, events/s | 214 | 214 | 214 |
| 10,000 | Producer, events/s | 19,543 | 19,495 | 19,564 |
| 10,000 | Consumer, batch 100, events/s | 41 | 41 | 42 |
| 10,000 | Consumer, batch 500, events/s | 202 | 202 | 202 |

Median batch p50 stayed around 2,502 ms across all consumer runs, irrespective of batch size.

**Run:** [GitHub Actions Benchmark #1](https://github.com/lathika1221/StreamForge/actions/runs/45952350591) (success; 18m 24s). The `benchmark-results` artifact was uploaded successfully.

## Cross-environment comparison

| Events | Measurement | Local laptop median | GitHub runner median |
|---:|---|---:|---:|
| 2,000 | Producer, events/s | 3,732 | 3,956 |
| 2,000 | Consumer, batch 100, events/s | 43 | 43 |
| 2,000 | Consumer, batch 500, events/s | 214 | 214 |
| 10,000 | Producer, events/s | 16,644 | 19,543 |
| 10,000 | Consumer, batch 100, events/s | 41 | 41 |
| 10,000 | Consumer, batch 500, events/s | 202 | 202 |

The GitHub runner's producer median is about 6 % higher at 2,000 events and 17 % higher at 10,000 events. Consumer throughput is effectively the same on both machines. Together with the consistently ~2.5 s batch p50, this supports the measured DLQ index-refresh overhead as the current consumer bottleneck rather than a lack of CPU on the laptop. This comparison is specific to these two environments and this Kafka-path benchmark; it does not measure Snowflake or production capacity.

## What the numbers show

**Producer.** Several thousand events per second on one local broker. The 2,000-event runs are slower per event than the 10,000-event runs because fixed costs (connection set-up, the final flush) weigh more on a short run.

**Consumer: limited by the DLQ index refresh, not by validation or Kafka.** A diagnostic run of 1,000 events at batch size 100 isolated the cost:

| Invalid records | Consumer throughput | Median batch time | DLQ index refreshes |
|---|---:|---:|---|
| 0 % | 32,473 events/s | 3 ms | 1 (start-up), 6 ms |
| 5 % | 47 events/s | 2,509 ms | 21, median 1,003 ms each |

The DLQ publish itself takes about 2 ms. When a batch contains an invalid record, the M8 consumer refreshes the DLQ index before the duplicate check and again inside the publish step. A refresh that has new DLQ messages to read re-assigns its reader and waits about one fetch round (roughly a second). With 5 % invalid records almost every batch contains one, so each batch costs about 2.5 s regardless of its size, and throughput scales with batch size. Without invalid records, the same consumer processes over 30,000 events per second on this machine.

So the consumer currently falls **below** the original design target of 100 events/s at batch size 100 whenever invalid records are common, and meets it at batch size 500. Possible fixes (none implemented):
- skip the second refresh when the batch-level refresh has just run;
- keep the index reader assigned between refreshes instead of re-assigning;
- lower the reader's fetch wait.

Each changes M8 code and must keep the duplicate-safety tests passing.

## What is not measured

- **Snowflake.** In production every batch with valid records runs one `MERGE` transaction; its latency, likely seconds per batch on an XS warehouse, is not included. Measuring it costs credits.
- dbt, the dashboard, end-to-end latency from event creation to dashboard.
- Network latency to a remote or multi-broker cluster, replication, more than one consumer process.
- Long-running stability (runs are minutes, not hours).
