# Real-Time API Monitoring — Dev Branch

Minimal Flink-only setup for learning. **No Kafka, no producer** — push events manually via CLI.

```
CLI (append JSONL) → events/events.jsonl → PyFlink (60s windows) → [METRIC] / [ALERT] logs
```

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- `bash`, `curl`, `python3`

## Quick start

### 1. Start Flink cluster

```bash
bash scripts/setup.sh
```

Starts **JobManager** (UI on :8081) and **TaskManager**.

### 2. Submit the Flink job

```bash
bash scripts/run-flink-job.sh
```

Verify at http://localhost:8081 → **Running Jobs** → `api-error-rate-monitor`.

### 3. Push events manually

One event:

```bash
bash scripts/send-event.sh auth 200
bash scripts/send-event.sh pay 503
```

Append raw JSON yourself:

```bash
echo '{"service":"order","status_code":404,"latency_ms":88,"timestamp":'$(python3 -c 'import time; print(int(time.time()*1000))')'}' >> events/events.jsonl
```

Send many events quickly:

```bash
for i in $(seq 1 20); do bash scripts/send-event.sh auth 503; done
```

### 4. Watch output

```bash
docker compose logs -f taskmanager
```

After each 60-second event-time window:

```
[METRIC] service=order error_rate=0.0312 errors=15/481 window=[...] threshold=0.0500
[ALERT]  service=pay   error_rate=0.4123 errors=198/480 window=[...] threshold=0.0200
```

## Event format

Each line in `events/events.jsonl` must be JSON:

```json
{"service": "auth", "status_code": 200, "latency_ms": 142, "timestamp": 1718280000000}
```

| Field | Description |
|-------|-------------|
| `service` | `auth`, `order`, `pay`, or `notif` |
| `status_code` | `200` = OK; `400`–`599` = error |
| `latency_ms` | Response time (parsed, not used in alerts yet) |
| `timestamp` | Event time in milliseconds (drives 60s windows) |

Flink tails this file continuously (polls every 1s).

## Project layout

```
├── docker-compose.yml       # Flink JobManager + TaskManager only
├── events/events.jsonl      # Append events here (shared with Flink)
├── config/thresholds.json
├── flink-job/
│   ├── error_rate_job.py    # File source + windowed error rates
│   ├── submit-remote.sh
│   ├── alert_engine.py
│   └── models.py
└── scripts/
    ├── setup.sh
    ├── run-flink-job.sh
    └── send-event.sh        # Append one event via CLI
```

## Thresholds

Edit [`config/thresholds.json`](config/thresholds.json) and restart the job:

```bash
bash scripts/run-flink-job.sh
```

## Tear down

```bash
docker compose down
```

## Differences from `main`

| `main` branch | `dev` branch |
|---------------|--------------|
| Kafka + producer | JSONL file only |
| Automated event stream | Manual CLI (`send-event.sh` or `echo >>`) |
| Kafka connector JARs | Plain Flink, no external connectors |
