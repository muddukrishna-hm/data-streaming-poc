# Real-Time API Monitoring POC

A local learning project that streams simulated API response events through **Apache Kafka**, computes per-service error rates with **Apache Flink (PyFlink)**, and prints **metrics** or **alerts** to the console when thresholds are exceeded.

Architecture matches [`.cursor/design.png`](.cursor/design.png):

```
Services (simulated) → Kafka → PyFlink (60s windows) → [METRIC] / [ALERT] console
```

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Python 3.10+ (for the Kafka producer only)
- `curl` and `bash`

> **Note:** The PyFlink job is submitted to the Docker Flink cluster via `flink run -py` (see `flink-job/submit-remote.sh`). Job output appears in TaskManager logs. The producer runs on your host against `localhost:9094`.

## Quick start

### 1. Bootstrap infrastructure

```bash
bash scripts/setup.sh
```

This will:

1. Download Flink Kafka connector JARs into `flink-job/jars/`
2. Start Kafka (KRaft) + Flink JobManager/TaskManager via Docker Compose
3. Create the `api-response-events` topic (4 partitions)

Verify:

- Flink UI: http://localhost:8081
- Kafka broker: `localhost:9094` (host) / `kafka:9092` (inside Docker network)

### 2. Install producer dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r producer/requirements.txt
```

The Flink job dependencies are installed in the Flink cluster image (`Dockerfile.flink-python`).

### 3. Start the Flink job

In one terminal:

```bash
bash scripts/run-flink-job.sh
```

This submits the job to the Docker Flink cluster. You should see `api-error-rate-monitor` under **Running Jobs** at http://localhost:8081.

Watch metrics and alerts in TaskManager logs:

```bash
docker compose logs -f taskmanager
```

### 4. Start the event producer

In a second terminal (with venv active):

```bash
python producer/event_producer.py
```

This simulates **auth**, **order**, **pay**, and **notif** services at ~8 events/sec each. Events are JSON:

```json
{"service": "auth", "status_code": 200, "latency_ms": 142, "timestamp": 1718280000000}
```

### 5. Observe metrics

After each 60-second event-time window closes, expect lines like:

```
[METRIC] service=order error_rate=0.0312 errors=15/481 window=[...] threshold=0.0500
```

## Trigger an alert (demo)

With the Flink job and producer still running, restart the producer with burst mode:

```bash
python producer/event_producer.py --burst pay
```

This injects ~40% 5xx errors into the **pay** service for 2 minutes. The pay threshold is 2%, so within one window you should see:

```
[ALERT] service=pay error_rate=0.4123 errors=198/480 window=[...] threshold=0.0200
```

## Project layout

```
├── docker-compose.yml       # Kafka + Flink cluster
├── config/thresholds.json   # Per-service error-rate thresholds
├── producer/
│   └── event_producer.py    # Kafka event simulator
├── flink-job/
│   ├── error_rate_job.py    # PyFlink pipeline
│   ├── submit-remote.sh     # Submit job via flink run -py
│   ├── alert_engine.py      # Threshold evaluation
│   └── models.py            # ApiEvent, ErrorMetric
└── scripts/
    ├── setup.sh             # One-shot bootstrap
    ├── create-topic.sh      # Create api-response-events topic
    ├── download-jars.sh     # Fetch Flink Kafka connector JARs
    └── run-flink-job.sh     # Submit PyFlink job to cluster
```

## Flink pipeline (what each operator does)

| Step | Operator | Purpose |
|------|----------|---------|
| 1 | `FlinkKafkaConsumer` | Read JSON events from `api-response-events` |
| 2 | `assign_timestamps_and_watermarks` | Event-time + 5s bounded out-of-orderness |
| 3 | `keyBy(service)` | Per-service parallel streams |
| 4 | `TumblingEventTimeWindows(60s)` | Fixed 60s buckets |
| 5 | `AggregateFunction` | Count errors (4xx/5xx) and total per window |
| 6 | `ProcessWindowFunction` | Emit `ErrorMetric{service, rate, count}` |
| 7 | `ThresholdEvaluator` | Compare rate to `config/thresholds.json` |
| 8 | `print()` sink | Console `[METRIC]` or `[ALERT]` lines |

## Thresholds

Edit [`config/thresholds.json`](config/thresholds.json):

```json
{
  "default_threshold": 0.05,
  "services": {
    "auth": 0.03,
    "order": 0.05,
    "pay": 0.02,
    "notif": 0.10
  }
}
```

Restart the Flink job after changes:

```bash
bash scripts/run-flink-job.sh
```

## Useful commands

**Create topic manually:**

```bash
bash scripts/create-topic.sh
```

**Consume raw events (smoke test):**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic api-response-events \
  --from-beginning \
  --property print.key=true
```

**Producer options:**

```bash
python producer/event_producer.py --rate 10
python producer/event_producer.py --burst auth --burst-error-rate 0.5 --burst-duration 90
```

**Run Flink job locally (embedded mini-cluster, no Flink UI):**

```bash
EXECUTION_TARGET=local \
KAFKA_BOOTSTRAP=localhost:9094 \
THRESHOLDS_PATH=./config/thresholds.json \
python flink-job/error_rate_job.py
```

Output prints directly in the terminal. The default Docker setup uses `remote` mode instead.

**Tear down:**

```bash
docker compose down
```

## Learning checkpoints

| Concept | What to try |
|---------|-------------|
| Kafka partitioning | Consume with `--property print.partition=true` — same service key always hits the same partition |
| Event-time windows | Metrics appear on ~60s boundaries, not when you start the producer |
| Watermarks | Events up to 5s late are still counted in their window |
| Flink parallelism | Same service always processed by the same subtask (`keyBy`) |
| Backpressure | Lower `--rate 1` and compare Flink UI **Back Pressure** tab |

## Troubleshooting

**Platform mismatch (`does not provide the specified platform linux/amd64`)**

Only applies if running the optional host-side local mode with the `flink-job/Dockerfile` image. Rebuild with:

```bash
docker compose build jobmanager
docker compose up -d --force-recreate jobmanager taskmanager
```

Or rerun `bash scripts/setup.sh`, which builds the cluster image before starting.

**No JARs found**

```bash
bash scripts/download-jars.sh
```

**Topic does not exist**

```bash
bash scripts/create-topic.sh
```

**Flink job cannot reach Kafka**

The Flink job container uses `KAFKA_BOOTSTRAP=kafka:9092`. The producer on your host uses `localhost:9094`.

**No output in terminal**

The default job runs on the Flink cluster, not inside `flink-job` logs. Check:

```bash
docker compose logs -f taskmanager
```

Also confirm the job appears under **Running Jobs** at http://localhost:8081. Allow up to 60s for the first window to close.

## Out of scope (Phase 2)

- Real HTTP microservices
- Prometheus / InfluxDB metrics sinks
- Slack / PagerDuty alert sinks
- Exactly-once semantics and savepoints
