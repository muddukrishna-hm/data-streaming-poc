#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Downloading Flink Kafka connector JARs"
bash "${ROOT}/scripts/download-jars.sh"

echo "==> Building Flink cluster image"
docker compose -f "${ROOT}/docker-compose.yml" build jobmanager

echo "==> Starting Docker stack (Kafka + Flink)"
docker compose -f "${ROOT}/docker-compose.yml" up -d

echo "==> Waiting for Kafka..."
until docker compose -f "${ROOT}/docker-compose.yml" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "==> Creating Kafka topic"
bash "${ROOT}/scripts/create-topic.sh"

echo ""
echo "Stack is ready."
echo "  Flink UI:  http://localhost:8081"
echo "  Kafka:     localhost:9094"
echo ""
echo "Next steps:"
echo "  1. bash scripts/run-flink-job.sh"
echo "  2. pip install -r producer/requirements.txt && python producer/event_producer.py"
