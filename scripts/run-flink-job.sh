#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "${ROOT}"

echo "Submitting Flink job to cluster..."
docker compose exec -T jobmanager bash /opt/flink/job/submit-remote.sh

echo ""
echo "Job submitted to the Docker Flink cluster."
echo "  Flink UI:  http://localhost:8081  (Running Jobs)"
echo "  Output:    docker compose logs -f taskmanager"
