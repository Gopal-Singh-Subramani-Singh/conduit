from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import redis.asyncio as aioredis
import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from config.settings import get_config
from conduit_core import decorators
from conduit_core.dlq import DeadLetterQueue
from conduit_core.engine import PipelineEngine
from conduit_core.metrics import update_uptime
from conduit_core.models import (
    DLQEntry,
    HealthResponse,
    RunStatusResponse,
    TaskStatusResponse,
    TriggerRequest,
    TriggerResponse,
)
from conduit_core.queue import TaskQueue
from conduit_core.resources import ResourceQuotaManager
from conduit_core.scheduler import CronScheduler
from conduit_core.store import ExecutionStore
from conduit_core.webhook import WebhookSender
from conduit_core.worker import WorkerPool
import demo.ml_training_pipeline  # Import demo DAGs so they register

logger = structlog.get_logger(__name__)

# ── Version ────────────────────────────────────────────────────────────────────
try:
    from importlib.metadata import version as pkg_version
    _VERSION = pkg_version("conduit")
except Exception:
    _VERSION = "0.1.0"


# ── App state ──────────────────────────────────────────────────────────────────

@dataclass
class AppState:
    redis: Optional[aioredis.Redis] = None
    queue: Optional[TaskQueue] = None
    dlq: Optional[DeadLetterQueue] = None
    store: Optional[ExecutionStore] = None
    resources: Optional[ResourceQuotaManager] = None
    webhook: Optional[WebhookSender] = None
    worker_pool: Optional[WorkerPool] = None
    engine: Optional[PipelineEngine] = None
    scheduler: Optional[CronScheduler] = None
    start_time: float = field(default_factory=time.time)
    redis_ok: bool = False


app_state = AppState()


def _require_ready() -> AppState:
    """Dependency that raises 503 if the app hasn't fully started."""
    if app_state.engine is None or app_state.store is None:
        raise HTTPException(
            status_code=503, detail="Service not ready — startup in progress"
        )
    return app_state


# ── Authentication ─────────────────────────────────────────────────────────────

async def _verify_api_key(request: Request) -> None:
    """Optional API-key auth. Skipped if CONDUIT_API_KEY is not configured."""
    cfg = get_config()
    api_key = cfg.server.api_key
    if not api_key:
        return  # auth disabled
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided != api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_config()

    app_state.redis = aioredis.from_url(
        cfg.redis.url,
        decode_responses=True,
        max_connections=cfg.redis.max_connections,
    )
    try:
        await app_state.redis.ping()
        app_state.redis_ok = True
        logger.info("redis.connected", url=cfg.redis.url.split("@")[-1])
    except Exception as e:
        logger.warning("redis.unavailable", error=str(e))

    app_state.store = ExecutionStore(cfg.sqlite.db_path)
    app_state.queue = TaskQueue(app_state.redis)
    app_state.dlq = DeadLetterQueue(app_state.redis)
    app_state.resources = ResourceQuotaManager()
    app_state.webhook = WebhookSender()

    app_state.worker_pool = WorkerPool(
        queue=app_state.queue,
        dlq=app_state.dlq,
        store=app_state.store,
        resources=app_state.resources,
        webhook=app_state.webhook,
        concurrency=cfg.workers.concurrency,
    )
    app_state.engine = PipelineEngine(
        queue=app_state.queue,
        store=app_state.store,
        webhook=app_state.webhook,
        worker_pool=app_state.worker_pool,
    )
    app_state.scheduler = CronScheduler(app_state.engine)

    await app_state.worker_pool.start()
    await app_state.scheduler.start()

    for dag_def in decorators.list_dags():
        if dag_def.schedule:
            try:
                app_state.scheduler.register_dag(dag_def.name, dag_def.schedule)
            except Exception as exc:
                logger.warning(
                    "app.schedule_register_failed",
                    dag=dag_def.name,
                    error=str(exc),
                )

    logger.info("conduit.started", version=_VERSION, registered_dags=len(decorators.list_dags()))
    yield

    await app_state.scheduler.stop()
    await app_state.worker_pool.stop()
    if app_state.redis:
        await app_state.redis.aclose()
    logger.info("conduit.shutdown")


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Conduit — ML Pipeline Orchestrator",
    version=_VERSION,
    lifespan=lifespan,
    # Disable auto-generated docs in production if desired
    docs_url="/docs",
    redoc_url="/redoc",
)


# ── CORS ───────────────────────────────────────────────────────────────────────
# Evaluated at module import time; origins may be empty (no cross-origin access)
# or set via CONDUIT_CORS_ORIGINS env var / config.yaml.
try:
    _cors_origins = get_config().server.cors_origins or []
except Exception:
    _cors_origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["X-API-Key", "Content-Type", "Authorization"],
)


# ── Global error handler ───────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    request_id = request.state.request_id if hasattr(request.state, "request_id") else "unknown"
    logger.error(
        "unhandled_exception",
        request_id=request_id,
        path=request.url.path,
        error=str(exc),
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "request_id": request_id},
    )


# ── Request ID + access log middleware ────────────────────────────────────────

@app.middleware("http")
async def request_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:8]
    request.state.request_id = request_id

    # Bind request_id into the structlog context for this request
    with structlog.contextvars.bound_contextvars(request_id=request_id):
        t0 = time.monotonic()
        try:
            response = await call_next(request)
        except Exception:
            raise
        ms = round((time.monotonic() - t0) * 1000, 1)
        logger.info(
            "http.request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            ms=ms,
        )
        response.headers["X-Request-ID"] = request_id
        return response


# ── DAG management ─────────────────────────────────────────────────────────────

@app.get("/dags", dependencies=[Depends(_verify_api_key)])
async def list_dags():
    return [
        {
            "name": d.name,
            "schedule": d.schedule,
            "description": d.description,
            "task_count": len(d.tasks),
            "tags": d.tags,
        }
        for d in decorators.list_dags()
    ]


@app.get("/dags/{dag_name}", dependencies=[Depends(_verify_api_key)])
async def get_dag(dag_name: str):
    dag_def = decorators.get_dag(dag_name)
    if not dag_def:
        raise HTTPException(status_code=404, detail="DAG not found")
    return {
        "name": dag_def.name,
        "schedule": dag_def.schedule,
        "description": dag_def.description,
        "tasks": [
            {
                "name": t.name,
                "depends_on": t.depends_on,
                "retries": t.retries,
                "cpu_cores": t.cpu_cores,
                "memory_gb": t.memory_gb,
                "timeout_seconds": t.timeout_seconds,
                "tags": t.tags,
            }
            for t in dag_def.tasks
        ],
    }


# ── Run management ─────────────────────────────────────────────────────────────

@app.post(
    "/runs",
    response_model=TriggerResponse,
    status_code=202,
    dependencies=[Depends(_verify_api_key)],
)
async def trigger_run(req: TriggerRequest, state: AppState = Depends(_require_ready)):
    try:
        run_id = await state.engine.trigger(
            dag_name=req.dag_name,
            input_data=req.input_data,
            trigger=req.trigger,
        )
        logger.info("api.run_triggered", run_id=run_id, dag=req.dag_name)
        return TriggerResponse(run_id=run_id, dag_name=req.dag_name, status="queued")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get(
    "/runs/{run_id}",
    response_model=RunStatusResponse,
    dependencies=[Depends(_verify_api_key)],
)
async def get_run_status(run_id: str, state: AppState = Depends(_require_ready)):
    run = state.store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    task_rows = state.store.get_task_runs_for_run(run_id)
    task_statuses = {}
    for row in task_rows:
        task_statuses[row["task_name"]] = TaskStatusResponse(
            task_run_id=row["task_run_id"],
            task_name=row["task_name"],
            state=row["state"],
            attempt=row.get("attempt", 0),
            error=row.get("error"),
            started_at=(
                datetime.fromisoformat(row["started_at"])
                if row.get("started_at")
                else None
            ),
            finished_at=(
                datetime.fromisoformat(row["finished_at"])
                if row.get("finished_at")
                else None
            ),
        )

    started_at = datetime.fromisoformat(run["started_at"])
    finished_at = (
        datetime.fromisoformat(run["finished_at"]) if run.get("finished_at") else None
    )
    duration = (finished_at - started_at).total_seconds() if finished_at else None

    return RunStatusResponse(
        run_id=run_id,
        dag_name=run["dag_name"],
        state=run["state"],
        trigger=run.get("trigger", "manual"),
        started_at=started_at,
        finished_at=finished_at,
        task_runs=task_statuses,
        duration_seconds=duration,
    )


@app.get("/runs", dependencies=[Depends(_verify_api_key)])
async def list_runs(
    dag_name: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
    state: AppState = Depends(_require_ready),
):
    return state.store.list_runs(dag_name=dag_name, limit=limit)


@app.delete("/runs/{run_id}", dependencies=[Depends(_verify_api_key)])
async def cancel_run(run_id: str, state: AppState = Depends(_require_ready)):
    success = await state.engine.cancel(run_id)
    if not success:
        raise HTTPException(
            status_code=404, detail="Run not found or already complete"
        )
    logger.info("api.run_cancelled", run_id=run_id)
    return {"run_id": run_id, "status": "cancelled"}


# ── DLQ ───────────────────────────────────────────────────────────────────────

@app.get("/dlq", response_model=List[DLQEntry], dependencies=[Depends(_verify_api_key)])
async def list_dlq(
    limit: int = Query(default=50, ge=1, le=500),
    state: AppState = Depends(_require_ready),
):
    return await state.dlq.list_entries(limit=limit)


@app.delete("/dlq", dependencies=[Depends(_verify_api_key)])
async def clear_dlq(state: AppState = Depends(_require_ready)):
    await state.dlq.clear()
    logger.warning("api.dlq_cleared")
    return {"status": "cleared"}


@app.post("/dlq/{task_run_id}/replay", dependencies=[Depends(_verify_api_key)])
async def replay_dlq_task(task_run_id: str, state: AppState = Depends(_require_ready)):
    # Scan all entries (not just first 200) to find by task_run_id
    entries = await state.dlq.find_entry(task_run_id)
    if not entries:
        raise HTTPException(status_code=404, detail="DLQ entry not found")
    entry = entries[0]
    await state.queue.enqueue(
        task_run_id,
        {
            "task_run_id": task_run_id,
            "run_id": entry.run_id,
            "dag_name": entry.dag_name,
            "task_name": entry.task_name,
            **entry.input_data,
        },
    )
    logger.info(
        "api.dlq_replayed",
        task_run_id=task_run_id,
        dag=entry.dag_name,
        task=entry.task_name,
    )
    return {"status": "requeued", "task_run_id": task_run_id}


# ── Observability ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Live health check. Performs a real DB probe and reports current
    Redis connectivity (not just startup ping).
    """
    cfg = get_config()
    uptime = update_uptime()

    # Live Redis check
    redis_status = "unavailable"
    if app_state.redis:
        try:
            await app_state.redis.ping()
            redis_status = "ok"
            app_state.redis_ok = True
        except Exception:
            app_state.redis_ok = False

    # Live SQLite check
    sqlite_status = "ok"
    if app_state.store:
        sqlite_status = "ok" if app_state.store.health_check() else "error"

    overall = "ok" if redis_status == "ok" and sqlite_status == "ok" else "degraded"

    return HealthResponse(
        status=overall,
        redis=redis_status,
        sqlite=sqlite_status,
        uptime_seconds=round(uptime, 1),
        registered_dags=len(decorators.list_dags()),
        active_runs=len(app_state.engine.active_run_ids()) if app_state.engine else 0,
    )


@app.get("/metrics")
async def metrics():
    update_uptime()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root():
    return {
        "service": "Conduit",
        "version": _VERSION,
        "docs": "/docs",
        "registered_dags": [d.name for d in decorators.list_dags()],
    }
