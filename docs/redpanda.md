# Local Redpanda (M2)

A single-node Redpanda broker for local development, defined in `infra/docker-compose.yml`. It is local and cost-free: no cloud resources and no credentials.

| Item | Value |
|---|---|
| Image | `docker.redpanda.com/redpandadata/redpanda:v26.2.6` (pinned) |
| Host access | `localhost:19092`, bound to the loopback interface only (`127.0.0.1` and `::1`). Matches `KAFKA_BOOTSTRAP_SERVERS` in `.env.example`. |
| Container access | `redpanda:9092` on the compose network `streamforge_default` |
| Topics | `ecommerce.events` (3 partitions), `ecommerce.events.dlq` (1 partition), replication factor 1 |
| Topic auto-creation | Disabled. Producing to an unknown topic fails with `UNKNOWN_TOPIC_OR_PARTITION`. |
| Data | Named volume `streamforge_redpanda-data`. It survives `down` and is deleted by `down -v`. |

Run every command from the repository root. In Git Bash on Windows, prefix `docker run` commands that contain container paths with `MSYS_NO_PATHCONV=1` (see Troubleshooting).

## Start

```
docker compose -f infra/docker-compose.yml up -d --wait
```

`--wait` returns once the broker is healthy and the one-shot `redpanda-init` service has finished. `redpanda-init` (`infra/redpanda/create-topics.sh`) is idempotent: it disables topic auto-creation, creates any missing topic, and verifies the partition counts. It exits with an error, without changing anything, if a topic exists with a different partition count.

## Status and verification

```
docker compose -f infra/docker-compose.yml ps -a                     # redpanda healthy, redpanda-init exited (0)
docker compose -f infra/docker-compose.yml logs redpanda-init        # "OK: ..." lines per topic
docker exec streamforge-redpanda rpk cluster health
docker exec streamforge-redpanda rpk topic list                      # names, partitions, replicas
docker exec streamforge-redpanda rpk topic describe ecommerce.events -p
docker exec streamforge-redpanda rpk cluster config get auto_create_topics_enabled   # false
```

Host reachability:

```
.venv/Scripts/python.exe -m pytest -m integration
```

`tests/integration/test_redpanda.py` reads only metadata. The producer integration tests (`tests/integration/test_producer.py`) publish to a temporary topic that is deleted afterwards, and **append a few simulated events to `ecommerce.events`** through the CLI test (see `docs/producer.md`).

Reachability from another container on the compose network:

```
MSYS_NO_PATHCONV=1 docker run --rm --network streamforge_default --entrypoint rpk \
  docker.redpanda.com/redpandadata/redpanda:v26.2.6 topic list -X brokers=redpanda:9092
```

Re-run topic setup on demand (safe to repeat):

```
docker compose -f infra/docker-compose.yml run --rm redpanda-init
```

## Inspect messages

```
docker exec streamforge-redpanda rpk topic consume ecommerce.events -n 5 -f '%k %v\n'
docker exec streamforge-redpanda rpk topic consume ecommerce.events.dlq -n 5
```

## Stop and reset

```
docker compose -f infra/docker-compose.yml down        # stop; topics and messages are kept
docker compose -f infra/docker-compose.yml down -v     # stop and delete all broker data
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `up --wait` reports the `redpanda` container is unhealthy | Check `docker logs streamforge-redpanda`. Redpanda is given `--memory=1G`, so Docker Desktop needs at least about 2 GB free. |
| `port is already allocated` for 19092 | Another process uses the port. Find it with `netstat -ano \| findstr :19092` (Windows), stop it, then start again. |
| `redpanda-init` fails with `$'\r': command not found` | The script was checked out with CRLF line endings. `.gitattributes` forces LF for `*.sh`. Re-checkout with `git checkout -- infra/redpanda/create-topics.sh`. |
| `redpanda-init` reports a partition mismatch | A topic exists with a different partition count from an earlier setup. Reset with `down -v`, then `up -d --wait`. |
| Host client logs `Connect to ipv6#[::1]:19092 failed` | The IPv6 loopback port binding is missing. Check that `docker port streamforge-redpanda` lists both `127.0.0.1:19092` and `[::1]:19092`. |
| Host client connects but then times out | The client must use `localhost:19092`, the advertised external listener. `redpanda:9092` only resolves inside Docker. |
| Git Bash: `stat C:/Program Files/Git/usr/bin/bash: no such file` | Git Bash rewrote a container path. Prefix the command with `MSYS_NO_PATHCONV=1`. |
| `UNKNOWN_TOPIC_OR_PARTITION` when producing | Expected for unknown topic names, because auto-creation is disabled. Check the topic name against `.env`. |

## Limitations

- Development only: one broker, replication factor 1, and `dev-container` mode, which skips fsync. Data can be lost on an unclean shutdown, and nothing here reflects production performance.
- No authentication or TLS. The broker is reachable only from this machine through the loopback interface.
- No Redpanda Console UI is included. Use `rpk` through `docker exec`.

## Message keys

The producer (`docs/producer.md`) keys messages by `order_id`, so each order's events stay ordered within one partition. Injected invalid records with the `missing_order_id` or `malformed_json` defect have no usable `order_id` and are published **with a null key**, which places them on a random partition. See `docs/event-schema.md` section 5.
