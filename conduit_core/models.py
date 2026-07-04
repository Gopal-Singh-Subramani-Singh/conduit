from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskState(str, Enum):
    PENDING   = "pending"
    READY     = "ready"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"
    SKIPPED   = "skipped"
    RETRYING  = "retrying"
    DLQ       = "dlq"
    CANCELLED = "cancelled"


class RunState(str, Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"
    PARTIAL  = "partial"
    CANCELLED = "cancelled"


@dataclass
class TaskDefinition:
    name: str
    func: Callable
    depends_on: List[str] = field(default_factory=list)
    retries: int = 0
    timeout_seconds: int = 3600
    cpu_cores: float = 1.0
    memory_gb: float = 1.0
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DAGDefinition:
    name: str
    tasks: List[TaskDefinition]
    schedule: Optional[str] = None  # cron expression
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TaskRun:
    task_run_id: str
    run_id: str
    dag_name: str
    task_name: str
    state: TaskState = TaskState.PENDING
    attempt: int = 0
    max_retries: int = 0
    input_data: Dict[str, Any] = field(default_factory=dict)
    output_data: Optional[Any] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    queued_at: datetime = field(default_factory=_utcnow)
    next_retry_at: Optional[datetime] = None


@dataclass
class PipelineRun:
    run_id: str
    dag_name: str
    state: RunState = RunState.PENDING
    trigger: str = "manual"
    input_data: Dict[str, Any] = field(default_factory=dict)
    task_runs: Dict[str, TaskRun] = field(default_factory=dict)
    started_at: datetime = field(default_factory=_utcnow)
    finished_at: Optional[datetime] = None
    error: Optional[str] = None


# ── REST API models ────────────────────────────────────────────────────────────

class TriggerRequest(BaseModel):
    dag_name: str
    input_data: Dict[str, Any] = {}
    trigger: str = "manual"


class TriggerResponse(BaseModel):
    run_id: str
    dag_name: str
    status: str = "queued"


class TaskStatusResponse(BaseModel):
    task_run_id: str
    task_name: str
    state: str
    attempt: int
    error: Optional[str]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]


class RunStatusResponse(BaseModel):
    run_id: str
    dag_name: str
    state: str
    trigger: str
    started_at: datetime
    finished_at: Optional[datetime]
    task_runs: Dict[str, TaskStatusResponse]
    duration_seconds: Optional[float]


class DLQEntry(BaseModel):
    task_run_id: str
    run_id: str
    dag_name: str
    task_name: str
    attempt: int
    error: str
    input_data: Dict[str, Any]
    failed_at: datetime


class HealthResponse(BaseModel):
    status: str
    redis: str
    sqlite: str
    uptime_seconds: float
    registered_dags: int
    active_runs: int
