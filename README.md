# Real-Time API Monitoring — Dev Branch (Manual Steps)

A minimal learning project: **plain Apache Flink only**. No Kafka. No producer. No Docker Compose.

You append API events to a file by hand. Flink reads that file, computes per-service error rates over 60-second windows, and prints `[METRIC]` or `[ALERT]` to logs.

```
You (CLI) → events/events.jsonl → Flink job → TaskManager logs
```

---

## What you are building

| Piece | What it does |
|-------|----------------|
| **JobManager** | Cluster boss. Accepts jobs. Shows UI at http://localhost:8081 |
| **TaskManager** | Worker. Runs the PyFlink job and prints results |
| **events/events.jsonl** | Where you manually write fake API events (one JSON per line) |
| **error_rate_job.py** | Flink pipeline: read file → count errors → compare thresholds → print |

---

## Prerequisites

Install these on your machine before starting:

| Tool | Why you need it |
|------|-----------------|
| **Docker** | Runs Flink in containers |
| **bash** | Run the commands below |
| **curl** | Check JobManager is ready |
| **python3** | Generate `timestamp` when writing events |

You do **not** need Docker Compose for this guide.

---

## Project layout

```
real-time-api-monitoring/
├── config/thresholds.json      # Alert limits per service
├── events/events.jsonl        # YOU append events here
├── flink-job/
│   ├── Dockerfile.flink-python   # Builds Flink + Python image
│   ├── error_rate_job.py         # The Flink pipeline
│   ├── submit-remote.sh          # Submits job to cluster
│   ├── models.py
│   └── alert_engine.py
└── scripts/send-event.sh      # Optional helper (still manual CLI)
```

---

## Step 0 — Open a terminal in the project folder

Every command below assumes you are in the repo root:

```bash
cd /path/to/real-time-api-monitoring
```

Set a variable so paths stay short:

```bash
export REPO="$(pwd)"
```

---

## Step 1 — Create a Docker network

Flink JobManager and TaskManager must talk to each other by name.

```bash
docker network create flink-net
```

**What this does:** Creates a private network called `flink-net`. Containers on it can reach each other using container hostnames (e.g. `jobmanager`).

**If it already exists:** Docker prints an error — that is fine, continue.

---

## Step 2 — Build the Flink + Python image

```bash
docker build \
  -t real-time-api-monitoring-flink-python:1.18 \
  -f flink-job/Dockerfile.flink-python \
  flink-job
```

**What this does:**

- Starts from official `flink:1.18` image
- Installs Python 3 and PyFlink dependencies
- Tags the result as `real-time-api-monitoring-flink-python:1.18`

**Wait until:** You see `Successfully tagged real-time-api-monitoring-flink-python:1.18`.

This only needs to be repeated when `Dockerfile.flink-python` changes.

---

## Step 3 — Create the events file

Flink reads from this file. It must exist before you submit the job.

```bash
mkdir -p "${REPO}/events"
touch "${REPO}/events/events.jsonl"
```

**What this does:** Creates an empty `events/events.jsonl`. You will append JSON lines to it later.

---

## Step 4 — Start the JobManager container

```bash
docker run -d \
  --name jobmanager \
  --hostname jobmanager \
  --network flink-net \
  -p 8081:8081 \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager
parallelism.default: 2" \
  -v "${REPO}/flink-job:/opt/flink/job:ro" \
  -v "${REPO}/config:/opt/flink/config-custom:ro" \
  -v "${REPO}/events:/opt/flink/events" \
  real-time-api-monitoring-flink-python:1.18 \
  jobmanager
```

**Flag by flag:**

| Flag | Purpose |
|------|---------|
| `--name jobmanager` | Container name (used in later `docker exec` / `docker logs`) |
| `--hostname jobmanager` | RPC hostname TaskManager will use |
| `--network flink-net` | Same network as TaskManager |
| `-p 8081:8081` | Flink Web UI on http://localhost:8081 |
| `-v .../flink-job:...` | Mounts your Python job code into the container |
| `-v .../config:...` | Mounts `thresholds.json` |
| `-v .../events:...` | Mounts the events file (shared with your host) |
| `jobmanager` | Starts Flink in JobManager mode |

---

## Step 5 — Wait until JobManager is ready

```bash
until curl -sf http://localhost:8081/overview >/dev/null; do
  echo "Waiting for JobManager..."
  sleep 2
done
echo "JobManager is ready."
```

**What this does:** Polls the Flink UI until it responds.

**Or open in browser:** http://localhost:8081 — you should see the Flink dashboard.

---

## Step 6 — Start the TaskManager container

```bash
docker run -d \
  --name taskmanager \
  --hostname taskmanager \
  --network flink-net \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2" \
  -v "${REPO}/flink-job:/opt/flink/job:ro" \
  -v "${REPO}/config:/opt/flink/config-custom:ro" \
  -v "${REPO}/events:/opt/flink/events" \
  real-time-api-monitoring-flink-python:1.18 \
  taskmanager
```

**What this does:**

- Starts a worker that registers with JobManager
- Offers **2 task slots** (2 parallel workers)
- Mounts the **same** folders as JobManager so it can read job code, config, and events

**Check it registered:**

```bash
docker logs taskmanager 2>&1 | grep -i "successful registration"
```

You should see a line like `Successful registration at resource manager`.

---

## Step 7 — Submit the Flink job

Run the submit script **inside** the JobManager container:

```bash
docker exec jobmanager bash /opt/flink/job/submit-remote.sh
```

**What this does:**

1. Sets `EXECUTION_TARGET=cluster` (run on TaskManager, not embedded locally)
2. Runs `flink run -d -py error_rate_job.py` (detached / background job)
3. Ships `models.py` and `alert_engine.py` as dependencies

**Expected output:**

```
Job has been submitted with JobID <some-uuid>
```

**Verify in UI:**

1. Open http://localhost:8081
2. Click **Jobs** → **Running Jobs**
3. You should see **`api-error-rate-monitor`** with status **RUNNING**

---

## Step 8 — Push events manually (the data source)

Flink tails `events/events.jsonl`. Each line is one fake API response.

### Option A — Use the helper script

```bash
bash scripts/send-event.sh auth 200
bash scripts/send-event.sh pay 503
bash scripts/send-event.sh order 404
```

**What it does:** Appends one JSON line to `events/events.jsonl` with a current timestamp.

### Option B — Write JSON yourself (fully manual)

```bash
echo '{"service":"auth","status_code":503,"latency_ms":120,"timestamp":'$(python3 -c 'import time; print(int(time.time()*1000))')'}' >> events/events.jsonl
```

### Option C — Send many events in a loop

```bash
for i in $(seq 1 30); do
  bash scripts/send-event.sh auth 503
done
```

### Event format (required fields)

```json
{"service": "auth", "status_code": 200, "latency_ms": 142, "timestamp": 1718280000000}
```

| Field | Meaning |
|-------|---------|
| `service` | One of: `auth`, `order`, `pay`, `notif` |
| `status_code` | `200` = success. `400`–`599` = error |
| `latency_ms` | Fake response time (not used for alerts yet) |
| `timestamp` | Milliseconds since epoch — **drives the 60s windows** |

**Important:** Flink polls the file every ~1 second. New lines are picked up automatically. You do not restart the job after appending.

---

## Step 9 — Watch metrics and alerts

```bash
docker logs -f taskmanager
```

**What to expect:**

- Nothing immediately — Flink waits for a **60-second window** to close
- Then lines like:

```
[METRIC] service=order error_rate=0.0312 errors=15/481 window=[...] threshold=0.0500
[ALERT]  service=pay   error_rate=0.4123 errors=198/480 window=[...] threshold=0.0200
```

| Prefix | Meaning |
|--------|---------|
| `[METRIC]` | Error rate is **below** threshold — OK |
| `[ALERT]` | Error rate is **above** threshold — problem |

**Thresholds** live in `config/thresholds.json`:

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

Example: `pay` threshold is **2%**. If more than 2% of `pay` events in a minute are errors → `[ALERT]`.

---

## Step 10 — Confirm in Flink UI (optional)

Open http://localhost:8081 → click your running job.

You should see:

- **Status:** RUNNING
- **Graph:** Source (read file) → Sink (window + print)
- **Records sent/received:** increases as you append events

The UI shows **job health**, not the `[ALERT]` text. Alerts only appear in TaskManager logs (Step 9).

---

## Step 11 — Change thresholds and re-submit (optional)

1. Edit `config/thresholds.json` on your host
2. Cancel the old job in Flink UI (**Cancel Job**), or:

```bash
# List running jobs (note the JobID from UI or submit output)
curl -s http://localhost:8081/jobs/overview
```

3. Re-submit:

```bash
docker exec jobmanager bash /opt/flink/job/submit-remote.sh
```

---

## Step 12 — Tear down everything

Run these when you are done:

```bash
docker stop taskmanager jobmanager
docker rm taskmanager jobmanager
docker network rm flink-net
```

**What this does:** Stops and removes both Flink containers and the network. Your `events/events.jsonl` file on disk is kept.

---

## Full manual checklist (copy-paste order)

```bash
export REPO="$(pwd)"

docker network create flink-net

docker build -t real-time-api-monitoring-flink-python:1.18 \
  -f flink-job/Dockerfile.flink-python flink-job

mkdir -p "${REPO}/events" && touch "${REPO}/events/events.jsonl"

docker run -d --name jobmanager --hostname jobmanager --network flink-net \
  -p 8081:8081 \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager
parallelism.default: 2" \
  -v "${REPO}/flink-job:/opt/flink/job:ro" \
  -v "${REPO}/config:/opt/flink/config-custom:ro" \
  -v "${REPO}/events:/opt/flink/events" \
  real-time-api-monitoring-flink-python:1.18 jobmanager

until curl -sf http://localhost:8081/overview >/dev/null; do sleep 2; done

docker run -d --name taskmanager --hostname taskmanager --network flink-net \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2" \
  -v "${REPO}/flink-job:/opt/flink/job:ro" \
  -v "${REPO}/config:/opt/flink/config-custom:ro" \
  -v "${REPO}/events:/opt/flink/events" \
  real-time-api-monitoring-flink-python:1.18 taskmanager

docker exec jobmanager bash /opt/flink/job/submit-remote.sh

bash scripts/send-event.sh auth 503
bash scripts/send-event.sh pay 200

docker logs -f taskmanager
```

---

## Troubleshooting

### JobManager UI not loading

```bash
docker logs jobmanager
curl http://localhost:8081/overview
```

### TaskManager not registering

```bash
docker logs taskmanager | tail -30
```

Common cause: JobManager not ready before TaskManager started. Restart TaskManager:

```bash
docker restart taskmanager
```

### Job submission fails

```bash
docker exec jobmanager bash /opt/flink/job/submit-remote.sh
```

Check `events/events.jsonl` exists:

```bash
ls -la events/events.jsonl
```

### No output in logs

- Wait **~60 seconds** after sending events (window size)
- Send more events — a single line may not fill a window meaningfully
- Confirm job is RUNNING in UI

### Container name already in use

```bash
docker rm -f jobmanager taskmanager
```

Then re-run Step 4 and Step 6.

---

## How this branch differs from `main`

| `main` branch | `dev` branch (this guide) |
|---------------|---------------------------|
| Kafka + automated producer | Manual file append |
| Docker Compose | Plain `docker` commands |
| Kafka connector JARs | No external connectors |
