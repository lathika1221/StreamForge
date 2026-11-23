#!/usr/bin/env bash
# Idempotent StreamForge broker/topic setup (run by the redpanda-init compose service).
#
# 1. Disables topic auto-creation (cluster config; persists across restarts), so a
#    misspelled topic name fails instead of silently creating a topic.
# 2. Creates each topic if missing, then verifies its partition count.
# Safe to re-run.
# If a topic already exists with a different partition count it fails loudly
# instead of silently changing it (partitions cannot be reduced in place).
# Names match KAFKA_EVENTS_TOPIC / KAFKA_DLQ_TOPIC defaults in .env.example.
set -euo pipefail

BROKERS="${RPK_BROKERS:-redpanda:9092}"
ADMIN="${RPK_ADMIN_HOSTS:-redpanda:9644}"
REPLICAS=1 # single-node development broker

rpk cluster config set auto_create_topics_enabled false -X admin.hosts="$ADMIN" >/dev/null
auto_create="$(rpk cluster config get auto_create_topics_enabled -X admin.hosts="$ADMIN")"
if [[ "$auto_create" != "false" ]]; then
  echo "ERROR: auto_create_topics_enabled is '$auto_create', expected 'false'." >&2
  exit 1
fi
echo "OK: auto_create_topics_enabled=false"

# topic:partitions
TOPICS=(
  "ecommerce.events:3"
  "ecommerce.events.dlq:1"
)

for spec in "${TOPICS[@]}"; do
  topic="${spec%%:*}"
  want="${spec##*:}"

  rpk topic create "$topic" --partitions "$want" --replicas "$REPLICAS" \
    --if-not-exists -X brokers="$BROKERS" >/dev/null

  have="$(rpk topic list -X brokers="$BROKERS" | awk -v t="$topic" '$1 == t { print $2 }')"
  if [[ "$have" != "$want" ]]; then
    echo "ERROR: topic $topic has ${have:-no} partitions, expected $want." >&2
    echo "Reset local data with: docker compose -f infra/docker-compose.yml down -v" >&2
    exit 1
  fi
  echo "OK: $topic partitions=$have replicas=$REPLICAS"
done

echo "Topic setup complete."
