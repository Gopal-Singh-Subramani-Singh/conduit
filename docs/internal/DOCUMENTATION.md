# Conduit — In-Depth Documentation

## What Is Conduit?

Conduit is a lightweight, production-grade event-driven ML pipeline orchestrator. It solves the same problem as Apache Airflow — coordinating dependent tasks in a DAG — but does it with far less infrastructure overhead: no scheduler database cluster, no workers fleet, just Redis Streams + SQLite + asyncio.

The core idea: define your ML pipeline as Python functions decorated with `@conduit.task` and `@conduit.dag`, and Conduit handles dependency resolution, parallel dispatch, retries, dead-letter queuing, and observability.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│  Client (CLI / REST / SDK)                  │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  PipelineEngine  (Kahn's algorithm dispatch) │
└──────┬───────────────────────────┬──────────┘
       │                           │
┌──────▼──────┐           ┌────────▼────────┐
│ Redis Stream│           │  SQLite Store   │
│ (task queue)│           │  (run history)  │
└──────┬──────┘           └─────────────────┘
       │
┌──────▼──────────────────────────────────────┐
│  WorkerPool  (asyncio, configurable conc.)  │
│  - ResourceQuotaManager (psutil)            │
│  - RetryWithBackoff (exponential + jitter)  │
│  - DeadLetterQueue (exhausted tasks)        │
└─────────────────────────────────────────────┘
```

**Key components:**

| Component | File | Role |
|---|---|---|
| DAG | `conduit_core/dag.py` | Kahn's algorithm, cycle detection, parallel level computation |
| Engine | `conduit_core/engine.py` | Run lifecycle: dispatch, track, complete |
| Worker Pool | `conduit_core/worker.py` | asyncio concurrency, task execution |
| Task Queue | `conduit_core/queue.py` | Redis Streams XADD/XREADGROUP/XACK |
| Dead Letter Queue | `conduit_core/dlq.py` | Failed task forensics and replay |
| Retry | `conduit_core/retry.py` | Exponential backoff with jitter |
| Resource Manager | `conduit_core/resources.py` | psutil CPU/memory quota enforcement |
| Scheduler | `conduit_core/scheduler.py` | APScheduler cron triggers |
| Store | `conduit_core/store.py` | SQLite run history and task events |
| Webhooks | `conduit_core/webhook.py` | Success/failure/DLQ notifications |
| API | `conduit_api/main.py` | FastAPI REST endpoints |
| CLI | `conduit_api/cli.py` | Typer CLI |

---

## Project Structure

```
conduit/
├── conduit_core/
│   ├── decorators.py        ← @conduit.task / @conduit.dag DSL
│   ├── dag.py               ← DAG class, Kahn's algorithm, cycle detection
│   ├── engine.py            ← Pipeline engine — run lifecycle, dispatch
│   ├── worker.py            ← asyncio worker pool, task executor
│   ├── queue.py             ← Redis Streams queue (XADD/XREADGROUP/XACK)
│   ├── dlq.py               ← Dead letter queue + forensics
│   ├── retry.py             ← Exponential backoff with jitter
│   ├── resources.py         ← Resource quota manager (psutil)
│   ├── scheduler.py         ← APScheduler cron trigger manager
│   ├── store.py             ← SQLite execution store
│   ├── webhook.py           ← Webhook notification sender
│   ├── metrics.py           ← 10 Prometheus metrics
│   └── models.py            ← All Pydantic + dataclass models
├── conduit_api/
│   ├── main.py              ← FastAPI app, all routes
│   └── cli.py               ← Typer CLI
├── config/
│   ├── settings.py          ← Pydantic Settings
│   └── config.yaml          ← All tunable parameters
├── sdk/
│   └── client.py            ← Python SDK
├── tests/                   ← 30+ pytest tests
├── demo/
│   └── ml_training_pipeline.py
├── docker-compose.yml
├── prometheus.yml
├── requirements.txt
└── pyproject.toml
```

---

## How to Run

### Prerequisites

- Python 3.11+
- Docker (for Redis + Prometheus + Grafana)

### Step 1 — Install dependencies

```bash
cd "/Users/gopalsinghsubramanisingh/Documents/AI  Hive/conduit/conduit"
pip install -r requirements.txt
```

### Step 2 — Start infrastructure

```bash
docker compose up redis prometheus grafana -d
```

This starts:
- **Redis** on `localhost:6379` — task queue and DLQ
- **Prometheus** on `localhost:9090` — metrics scraping
- **Grafana** on `localhost:3000` — dashboards (admin / conduit)

Wait ~10 seconds for containers to be healthy:
```bash
docker compose ps
```

### Step 3 — Start Conduit

```bash
# From the conduit/ project directory
uvicorn conduit_api.main:app --port 8004 --reload
```

The server starts at `http://localhost:8004`. Interactive API docs at `http://localhost:8004/docs`.

### Step 4 — Run tests

```bash
pytest tests/ -v
```

Tests use `fakeredis` — no real Redis needed for the test suite.

### Step 5 — Run the ML training pipeline demo

```bash
python demo/ml_training_pipeline.py
```

---

## Full Docker Stack (Everything Containerized)

```bash
docker compose up --build
```

This builds the Conduit app container and runs everything together:
- Conduit API: `localhost:8004`
- Prometheus: `localhost:9090`
- Grafana: `localhost:3000` (admin / conduit or changeme)

---

## CLI Reference

```bash
# List all registered DAGs
conduit dags

# Trigger a DAG run manually
conduit run my_dag

# Check the status of a run
conduit status <run_id>

# List recent pipeline runs
conduit list

# Inspect the dead letter queue
conduit dlq
```

---

## REST API Reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/runs` | Trigger a DAG run |
| `GET` | `/runs/{id}` | Get run status and all task states |
| `GET` | `/runs` | List recent runs |
| `DELETE` | `/runs/{id}` | Cancel a running pipeline |
| `GET` | `/dags` | List all registered DAGs |
| `GET` | `/dlq` | List dead letter queue entries |
| `POST` | `/dlq/{task_run_id}/replay` | Replay a failed task from DLQ |
| `GET` | `/health` | Health check (Redis + SQLite status) |
| `GET` | `/metrics` | Prometheus metrics endpoint |

### Trigger a run

```bash
curl -X POST http://localhost:8004/runs \
  -H "Content-Type: application/json" \
  -d '{"dag_name": "ml_training", "input_data": {"dataset": "v2"}}'
```

### Check run status

```bash
curl http://localhost:8004/runs/<run_id>
```

### Replay a DLQ task

```bash
curl -X POST http://localhost:8004/dlq/<task_run_id>/replay
```

---

## SDK Usage

```python
import sdk as conduit

conduit.init("http://localhost:8004")

# Trigger a DAG
run = conduit.run("ml_training", input_data={"version": "v3"})
print(run.run_id)

# Check status
status = conduit.status(run.run_id)
print(status.state)
```

---

## Defining Pipelines (Decorator DSL)

```python
from conduit_core.decorators import task, dag

@task(retries=2, cpu_cores=1.0, memory_gb=2.0)
async def load_data(input_data):
    # Load dataset, return path or artifact
    return {"dataset_path": "/tmp/dataset.parquet"}

@task(depends_on=["load_data"], retries=3, cpu_cores=2.0, memory_gb=4.0)
async def train_model(input_data, load_data):
    # Train using output from load_data
    return {"model_path": "/tmp/model.pt"}

@task(depends_on=["train_model"])
async def evaluate_model(input_data, train_model):
    return {"accuracy": 0.94}

@dag(name="ml_training", schedule="0 2 * * *")  # 2am daily
def ml_training_pipeline():
    return [load_data, train_model, evaluate_model]
```

Conduit automatically resolves the dependency graph and dispatches `load_data` first, then `train_model` once `load_data` completes, then `evaluate_model`.

---

## How Kahn's Algorithm Works in Conduit

When a pipeline is triggered, the `DAG` class computes execution levels:

1. Assign in-degree = number of upstream dependencies for each task
2. All tasks with in-degree 0 form Level 0 (run immediately, in parallel)
3. Once a Level N task completes, decrement in-degree of its downstream tasks
4. Any task whose in-degree reaches 0 becomes ready to dispatch
5. If any tasks remain unprocessed after full traversal: cycle detected, pipeline rejected

This gives you automatic parallelism — tasks with no shared dependencies run simultaneously without any configuration.

---

## Retry Strategy

Conduit uses exponential backoff with jitter:

```
delay = min(base_delay × multiplier^attempt, max_delay)
With jitter: delay = delay × random.uniform(0.5, 1.5)
```

| Attempt | Base delay (no jitter) |
|---|---|
| 0 | 1s |
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |
| 5+ | capped at 300s |

Jitter prevents thundering herd — if 50 tasks fail simultaneously and retry at exactly the same time, the retry storm would recreate the original failure. Jitter spreads retries across a window.

After `max_retries` is exhausted the task goes to the Dead Letter Queue with full forensics preserved.

---

## Dead Letter Queue

Every task that exhausts all retries lands in the DLQ with:
- Full input payload
- Error message (truncated at 2000 chars)
- Attempt count
- Timestamps

Inspect and replay from CLI or REST API. Replaying resubmits the task payload to the main queue as a new attempt.

---

## Redis Streams Delivery Semantics

Conduit uses Redis Streams consumer groups for at-least-once delivery:

- **Producer** (`XADD`): writes task message to stream
- **Consumer** (`XREADGROUP`): reads message, worker executes task
- **Acknowledge** (`XACK`): only sent after successful execution
- **Reclaim** (`XAUTOCLAIM`): if a worker crashes before `XACK`, the message becomes "pending" and is reclaimed after `visibility_timeout_seconds` (default 300s)

This guarantees no task is silently lost — even on worker crash.

---

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `conduit_tasks_total` | Counter | Task executions by dag/task/status |
| `conduit_task_duration_seconds` | Histogram | Task wall-clock time |
| `conduit_task_retries_total` | Counter | Retry attempts by dag/task |
| `conduit_dlq_depth` | Gauge | Current DLQ size |
| `conduit_dlq_entries_total` | Counter | Total tasks ever sent to DLQ |
| `conduit_queue_depth` | Gauge | Pending tasks in main queue |
| `conduit_run_duration_seconds` | Histogram | Pipeline run total time |
| `conduit_runs_total` | Counter | Pipeline runs by dag/status |
| `conduit_active_runs` | Gauge | Currently executing pipelines |
| `conduit_resource_utilisation` | Gauge | CPU/memory utilisation fraction |

---

## Configuration Reference

Edit `config/config.yaml`:

```yaml
server:
  port: 8004               # API port

redis:
  url: "redis://localhost:6379"
  stream_key: "conduit:tasks"
  dlq_key: "conduit:dlq"
  consumer_group: "conduit:workers"
  batch_size: 10           # messages per dequeue call
  block_ms: 1000           # ms to block waiting for messages
  visibility_timeout_seconds: 300  # before stale reclaim

workers:
  concurrency: 4           # parallel task slots
  task_timeout_seconds: 3600

resources:
  cpu_limit_pct: 80.0      # block dispatch above this CPU%
  memory_limit_gb: 16.0    # block dispatch above this memory
  check_enabled: true

retry:
  base_delay_seconds: 1.0
  max_delay_seconds: 300.0
  jitter: true
  backoff_multiplier: 2.0

webhooks:
  enabled: false
  url: null                # POST to this URL on DAG success/failure/DLQ
  timeout_seconds: 10
```

---

## Task State Machine

```
PENDING → READY → RUNNING → SUCCESS
                          → FAILED → (retry) → RETRYING → RUNNING
                                   → (exhausted) → DLQ
         → CANCELLED
         → SKIPPED (upstream failed)
```

---

## Environment Variables

You can override config via environment variables using `CONDUIT_` prefix:

```bash
export CONDUIT_REDIS__URL="redis://my-redis-host:6379"
export CONDUIT_WORKERS__CONCURRENCY=8
export CONDUIT_SQLITE__DB_PATH="/data/conduit.db"
```

---

## Port Reference

| Service | Port |
|---|---|
| Conduit API | 8004 |
| Redis | 6379 |
| Prometheus | 9090 |
| Grafana | 3000 |

---

## Running Tests

The full test suite uses `fakeredis` — no real Redis instance required.

```bash
cd conduit/

# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=conduit_core --cov-report=term-missing

# Run a specific module
pytest tests/test_dag.py -v
pytest tests/test_engine.py -v
```

**Test modules:**

| File | Tests | What's covered |
|---|---|---|
| `test_dag.py` | 8 | Kahn's algorithm, cycle detection, execution levels, parallel dispatch |
| `test_retry.py` | 5 | Backoff computation, jitter bounds, max delay cap |
| `test_queue.py` | 7 | Redis Streams XADD/XREADGROUP/XACK, stale reclaim |
| `test_dlq.py` | 5 | DLQ send, list, depth, clear, truncated errors |
| `test_resources.py` | 4 | CPU/memory quota check, limits enforcement |
| `test_store.py` | 6 | SQLite CRUD, run history, task events |
| `test_integration.py` | 6 | FastAPI trigger/status/cancel/replay endpoints |

---

## Prometheus Queries

```promql
# Task failure rate across all DAGs
rate(conduit_tasks_total{status="failed"}[5m])

# DLQ depth (should stay near 0)
conduit_dlq_depth

# Active pipeline runs right now
conduit_active_runs

# P99 task duration per DAG
histogram_quantile(0.99, rate(conduit_task_duration_seconds_bucket[10m]))

# Queue depth (pending tasks)
conduit_queue_depth

# Resource utilisation (CPU and memory)
conduit_resource_utilisation
```

---

## Production Hardening

**Increase worker concurrency.** The default is 4 concurrent tasks. For CPU-bound workloads, set it to the number of available cores:

```yaml
workers:
  concurrency: 8
  task_timeout_seconds: 7200
```

**Persist the SQLite database.** Override via environment variable so the database survives container restarts:

```bash
export CONDUIT_SQLITE__DB_PATH=/data/conduit.db
```

**Enable webhooks.** Wire success/failure/DLQ events to your alerting system:

```yaml
webhooks:
  enabled: true
  url: "https://hooks.slack.com/services/your-webhook-url"
  timeout_seconds: 10
```

**Redis persistence.** For production, enable AOF on the Redis container to survive restarts:

```yaml
# docker-compose.yml
redis:
  command: redis-server --appendonly yes --appendfsync everysec
```

**Resource limits.** In resource-constrained environments, tighten the quota manager:

```yaml
resources:
  cpu_limit_pct: 70.0      # leave 30% headroom for system
  memory_limit_gb: 12.0
  check_enabled: true
```

**TLS.** Conduit speaks plain HTTP. Put it behind nginx or Caddy for HTTPS in production.

---

## Troubleshooting

### `conduit: command not found`

Install the package first:

```bash
pip install -r requirements.txt
```

The CLI is registered as a script entry point (`conduit = "conduit_api.cli:app"`) in `pyproject.toml`.

### `redis.exceptions.ConnectionError` on start

The Redis container isn't running or is still starting:

```bash
docker compose up redis -d
docker compose ps redis     # wait for "healthy"
```

### Tasks stuck in `RUNNING` state

The worker may have crashed before `XACK`. Conduit auto-reclaims stale messages after `visibility_timeout_seconds` (default 300s). To reclaim immediately, trigger via REST:

```bash
curl -X POST http://localhost:8004/internal/reclaim
```

Or restart Conduit — the reclaim loop runs on startup.

### DAG rejected with `CycleDetectedError`

Your `depends_on` lists form a cycle. Example: task A depends on B, and B depends on A. Fix the dependency graph — Conduit will tell you how many nodes it couldn't process.

### DLQ growing rapidly

Check `conduit_task_retries_total` in Prometheus to identify which task is failing. Inspect the full error:

```bash
curl http://localhost:8004/dlq
```

The DLQ entry contains the full error message, attempt count, and original input payload. Fix the underlying issue, then replay:

```bash
curl -X POST http://localhost:8004/dlq/<task_run_id>/replay
```
