#!/usr/bin/env bash
set -euo pipefail

TOPIC="${TOPIC:-api-response-events}"
PARTITIONS="${PARTITIONS:-4}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"

echo "Creating topic '${TOPIC}' with ${PARTITIONS} partitions on ${BOOTSTRAP}..."

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe \
  --topic "${TOPIC}"

echo "Topic '${TOPIC}' is ready."
