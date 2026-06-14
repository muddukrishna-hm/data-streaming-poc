#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Building Flink cluster image"
docker compose -f "${ROOT}/docker-compose.yml" build jobmanager

echo "==> Starting Flink cluster"
docker compose -f "${ROOT}/docker-compose.yml" up -d

echo ""
echo "Stack is ready."
echo "  Flink UI:     http://localhost:8081"
echo "  Events file:  events/events.jsonl"
echo ""
echo "Next steps:"
echo "  1. bash scripts/run-flink-job.sh"
echo "  2. bash scripts/send-event.sh auth 200"
echo "  3. docker compose logs -f taskmanager"
