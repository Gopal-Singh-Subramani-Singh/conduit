# Conduit — Demo Guide

## What this demo proves

- Python DAG DSL works end-to-end
- Topological task scheduling (Kahn's algorithm) dispatches tasks in dependency order
- Redis Streams queue delivers tasks to workers
- Retries with exponential backoff fire on task failure
- Dead letter queue captures exhausted tasks
- DLQ replay re-runs a failed task without re-running the full DAG
- Prometheus metrics populate on execution
- CLI provides run status and DLQ inspection

---

## Prerequisites

```bash
pip install -r requirements.txt
docker compose up redis prometheus grafana -d
```

---

## Demo Commands

### 1. Start Conduit

```bash
uvicorn conduit_api.main:app --port 8004 --reload
```

### 2. Verify health

```bash
curl http://localhost:8004/health
```

### 3. Run the ML training pipeline demo

```bash
python demo/ml_training_pipeline.py
```

Expected output:
```
[conduit] DAG 'ml_training_pipeline' registered
[conduit] Run triggered → run_id: abc123
[conduit] Task 'fetch_data' → RUNNING
[conduit] Task 'fetch_data' → SUCCESS (1.2s)
[conduit] Task 'preprocess' → RUNNING  (depends on fetch_data)
[conduit] Task 'preprocess' → SUCCESS (0.4s)
[conduit] Task 'train_model' → RUNNING  (depends on preprocess)
[conduit] Task 'train_model' → SUCCESS (3.1s)
[conduit] Pipeline 'ml_training_pipeline' COMPLETED in 4.7s
```

### 4. List DAGs via CLI

```bash
conduit dags
```

### 5. Trigger a run via CLI

```bash
conduit run ml_training_pipeline
```

### 6. Check run status

```bash
conduit status <run_id>
```

### 7. List runs

```bash
conduit list
```

### 8. Inspect the dead letter queue

```bash
conduit dlq
```

### 9. Replay a failed task

```bash
conduit dlq replay <task_run_id>
```

### 10. REST API — Trigger a run

```bash
curl -X POST http://localhost:8004/runs \
  -H "Content-Type: application/json" \
  -d '{"dag_id": "ml_training_pipeline"}'
```

### 11. View Prometheus metrics

```bash
curl http://localhost:8004/metrics | grep conduit_
```

### 12. Full Docker stack

```bash
docker compose up --build
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000 (admin / conduit)
```

---

## Expected Output Summary

| Check | Expected |
|-------|----------|
| Demo pipeline | All tasks succeed in dependency order |
| `conduit dags` | ml_training_pipeline listed |
| `conduit status` | Task states and durations shown |
| DLQ (if failure injected) | Failed task appears with error and stack trace |
| `/metrics` | conduit_tasks_completed_total, conduit_dlq_depth populated |
| Grafana | Task throughput, retry counts, DLQ depth dashboard |

---

## Known Limitations

- Redis required for task queue. Without Redis, Conduit cannot dispatch tasks.
- Workers are asyncio coroutines — single-machine concurrency only.
- At-least-once delivery: tasks may run more than once if a worker crashes mid-execution. Tasks must be idempotent.
- No DAG versioning — re-registering a DAG overwrites the previous definition.
