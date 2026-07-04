from __future__ import annotations
import time
from prometheus_client import Counter, Histogram, Gauge

TASKS_TOTAL = Counter(
    "conduit_tasks_total",
    "Total task executions",
    ["dag_name", "task_name", "status"],
)

TASK_DURATION = Histogram(
    "conduit_task_duration_seconds",
    "Task wall-clock execution time",
    ["dag_name", "task_name"],
    buckets=[1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600],
)

TASK_RETRIES = Counter(
    "conduit_task_retries_total",
    "Total task retry attempts",
    ["dag_name", "task_name"],
)

DLQ_DEPTH = Gauge(
    "conduit_dlq_depth",
    "Number of tasks in the dead letter queue",
)

DLQ_ENTRIES = Counter(
    "conduit_dlq_entries_total",
    "Total tasks sent to dead letter queue",
    ["dag_name", "task_name"],
)

QUEUE_DEPTH = Gauge(
    "conduit_queue_depth",
    "Number of tasks pending in the main queue",
)

RUN_DURATION = Histogram(
    "conduit_run_duration_seconds",
    "Total pipeline run wall-clock time",
    ["dag_name"],
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600],
)

RUNS_TOTAL = Counter(
    "conduit_runs_total",
    "Total pipeline runs",
    ["dag_name", "status"],
)

ACTIVE_RUNS = Gauge(
    "conduit_active_runs",
    "Currently executing pipeline runs",
)

RESOURCE_UTILISATION = Gauge(
    "conduit_resource_utilisation",
    "Current resource utilisation fraction",
    ["resource"],  # cpu | memory
)

UPTIME = Gauge("conduit_uptime_seconds", "Server uptime in seconds")
_START = time.time()


def update_uptime() -> float:
    elapsed = time.time() - _START
    UPTIME.set(elapsed)
    return elapsed
