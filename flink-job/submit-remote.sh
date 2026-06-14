#!/usr/bin/env bash
set -euo pipefail

export EXECUTION_TARGET=cluster
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
export THRESHOLDS_PATH="${THRESHOLDS_PATH:-/opt/flink/config-custom/thresholds.json}"

JAR_ARGS=()
for jar in /opt/flink/usrlib/*.jar; do
  [[ -f "${jar}" ]] || continue
  JAR_ARGS+=("-C" "file://${jar}")
done

if [[ ${#JAR_ARGS[@]} -eq 0 ]]; then
  echo "No connector JARs in /opt/flink/usrlib. Run: bash scripts/download-jars.sh" >&2
  exit 1
fi

exec /opt/flink/bin/flink run -d \
  -py /opt/flink/job/error_rate_job.py \
  -pyfs /opt/flink/job/models.py,/opt/flink/job/alert_engine.py \
  -p 2 \
  "${JAR_ARGS[@]}" \
  -Dpython.executable=/usr/bin/python3 \
  -Dpython.execution-mode=process \
  -Dpipeline.name=api-error-rate-monitor
