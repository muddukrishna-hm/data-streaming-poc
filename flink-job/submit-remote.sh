#!/usr/bin/env bash
set -euo pipefail

export EXECUTION_TARGET=cluster
export EVENTS_PATH="${EVENTS_PATH:-/opt/flink/events/events.jsonl}"
export THRESHOLDS_PATH="${THRESHOLDS_PATH:-/opt/flink/config-custom/thresholds.json}"

exec /opt/flink/bin/flink run -d \
  -py /opt/flink/job/error_rate_job.py \
  -pyfs /opt/flink/job/models.py,/opt/flink/job/alert_engine.py \
  -p 2 \
  -Dpython.executable=/usr/bin/python3 \
  -Dpython.execution-mode=process \
  -Dpipeline.name=api-error-rate-monitor
