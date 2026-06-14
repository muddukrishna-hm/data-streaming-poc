#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE="${1:-auth}"
STATUS_CODE="${2:-200}"
LATENCY_MS="${3:-}"
EVENTS_FILE="${EVENTS_FILE:-${ROOT}/events/events.jsonl}"

VALID_SERVICES="auth order pay notif"
if [[ ! " ${VALID_SERVICES} " =~ " ${SERVICE} " ]]; then
  echo "Usage: $0 <service> [status_code] [latency_ms]" >&2
  echo "  service: auth | order | pay | notif" >&2
  echo "  status_code: default 200" >&2
  echo "  latency_ms: default random 20-500" >&2
  exit 1
fi

if [[ -z "${LATENCY_MS}" ]]; then
  LATENCY_MS=$((20 + RANDOM % 481))
fi

mkdir -p "$(dirname "${EVENTS_FILE}")"
touch "${EVENTS_FILE}"

TIMESTAMP="$(python3 -c 'import time; print(int(time.time() * 1000))')"
EVENT="$(cat <<EOF
{"service":"${SERVICE}","status_code":${STATUS_CODE},"latency_ms":${LATENCY_MS},"timestamp":${TIMESTAMP}}
EOF
)"

printf '%s\n' "${EVENT}" >> "${EVENTS_FILE}"
echo "Appended to ${EVENTS_FILE}:"
echo "${EVENT}"
