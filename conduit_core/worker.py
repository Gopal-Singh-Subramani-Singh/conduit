from __future__ import annotations

import asyncio
import json
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional
import structlog

from conduit_core.models import TaskRun, TaskState
from conduit_core.queue import TaskQueue
from conduit_core.dlq import DeadLetterQueue
from conduit_core.store import ExecutionStore
from conduit_core.resources import ResourceQuotaManager
from conduit_core.webhook import WebhookSender
from conduit_core.metrics import TASKS_TOTAL, TASK_DURATION, TASK_RETRIES
from conduit_core.retry import compute_delay
from conduit_core import decorators
from config.settings import get_config

logger = structlog.get_logger(__name__)

# Maximum characters to store for error messages in SQLite / DLQ
_MAX_ERROR_LEN = 4000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerPool:
    """
    Asyncio-based worker pool that consumes tasks from Redis Streams.

    Each message is processed concurrently up to ``concurrency`` slots.
    Sync task functions are dispatched to a ThreadPoolExecutor so they
    don't block the event loop.

    Lifecycle:
        await pool.start()   # begins consuming
        await pool.stop()    # stops consuming and drains in-flight tasks
    """

    def __init__(
        self,
        queue: TaskQueue,
        dlq: DeadLetterQueue,
        store: ExecutionStore,
        resources: ResourceQuotaManager,
        webhook: WebhookSender,
        concurrency: int = 4,
    ) -> None:
        self._queue = queue
        self._dlq = dlq
        self._store = store
        self._resources = resources
        self._webhook = webhook
        self._concurrency = concurrency
        self._semaphore = asyncio.Semaphore(concurrency)
        self._executor = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="conduit-worker"
        )
        self._running = False
        self._tasks: set[asyncio.Task] = set()
        self._run_callbacks: Dict[str, Callable] = {}
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        self._running = True
        self._stop_event.clear()
        await self._queue.setup()
        logger.info("worker_pool.started", concurrency=self._concurrency)
        asyncio.create_task(self._consume_loop(), name="conduit-consumer")
        asyncio.create_task(self._reclaim_loop(), name="conduit-reclaim")

    async def stop(self) -> None:
        """
        Graceful shutdown: stop accepting new messages and wait for
        in-flight tasks to finish (up to 30 s).
        """
        self._running = False
        self._stop_event.set()

        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=30,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "worker_pool.shutdown_timeout",
                    remaining=len(self._tasks),
                )
                for t in self._tasks:
                    t.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)

        self._executor.shutdown(wait=False)
        logger.info("worker_pool.stopped")

    def register_callback(self, run_id: str, callback: Callable) -> None:
        self._run_callbacks[run_id] = callback

    # ── Internal loops ─────────────────────────────────────────────────────────

    async def _consume_loop(self) -> None:
        while self._running:
            try:
                messages = await self._queue.dequeue(count=self._concurrency)
                for msg_id, task_run_id, payload in messages:
                    task = asyncio.create_task(
                        self._process(msg_id, task_run_id, payload),
                        name=f"conduit-task-{task_run_id[:12]}",
                    )
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("worker_pool.consume_error", error=str(exc))
                await asyncio.sleep(1)

    async def _reclaim_loop(self) -> None:
        """Periodically reclaim messages from crashed/stalled workers."""
        cfg = get_config()
        while self._running:
            await asyncio.sleep(60)
            try:
                min_idle_ms = cfg.redis.visibility_timeout_seconds * 1000
                reclaimed = await self._queue.reclaim_stale(min_idle_ms)
                if reclaimed:
                    logger.info(
                        "worker_pool.reclaimed",
                        count=len(reclaimed),
                    )
            except Exception as exc:
                logger.warning("worker_pool.reclaim_error", error=str(exc))

    # ── Task processing ────────────────────────────────────────────────────────

    async def _process(
        self, msg_id: str, task_run_id: str, payload: Dict[str, Any]
    ) -> None:
        async with self._semaphore:
            task_run = self._load_task_run(task_run_id, payload)
            if task_run is None:
                logger.warning(
                    "worker.task_run_not_found",
                    task_run_id=task_run_id,
                )
                await self._queue.ack(msg_id)
                return

            task_def = decorators.get_task(task_run.task_name)
            if task_def is None:
                logger.error(
                    "worker.task_not_found",
                    task_name=task_run.task_name,
                    task_run_id=task_run_id,
                )
                await self._queue.ack(msg_id)
                return

            # ── Resource check (atomic: check + reserve under semaphore) ──────
            if not self._resources.can_dispatch(
                task_def.cpu_cores, task_def.memory_gb
            ):
                logger.debug(
                    "worker.resource_wait",
                    task=task_run.task_name,
                )
                # Don't ack — let visibility timeout expire and get redelivered
                await asyncio.sleep(5)
                return

            # Reserve before any await point to prevent TOCTOU race
            self._resources.reserve(task_def.cpu_cores, task_def.memory_gb)

            task_run.state = TaskState.RUNNING
            task_run.started_at = _utcnow()
            task_run.attempt += 1
            self._store.update_task_state(
                task_run_id, TaskState.RUNNING, attempt=task_run.attempt
            )
            self._store.log_event(
                task_run_id,
                task_run.run_id,
                "started",
                f"Attempt {task_run.attempt}",
            )

            t0 = time.monotonic()
            try:
                output = await asyncio.wait_for(
                    self._execute_task(task_def, task_run.input_data),
                    timeout=task_def.timeout_seconds,
                )
                duration = time.monotonic() - t0

                task_run.state = TaskState.SUCCESS
                task_run.output_data = output
                task_run.finished_at = _utcnow()

                self._store.update_task_state(
                    task_run_id, TaskState.SUCCESS, output_data=output
                )
                self._store.log_event(
                    task_run_id,
                    task_run.run_id,
                    "success",
                    f"Duration: {duration:.1f}s",
                )
                TASKS_TOTAL.labels(
                    dag_name=task_run.dag_name,
                    task_name=task_run.task_name,
                    status="success",
                ).inc()
                TASK_DURATION.labels(
                    dag_name=task_run.dag_name,
                    task_name=task_run.task_name,
                ).observe(duration)

                await self._queue.ack(msg_id)
                await self._notify_engine(task_run, success=True)

            except asyncio.TimeoutError:
                await self._handle_failure(
                    msg_id,
                    task_run,
                    task_def,
                    error=f"Task timed out after {task_def.timeout_seconds}s",
                )
            except asyncio.CancelledError:
                # Worker is shutting down — put message back by NOT acking
                logger.warning(
                    "worker.task_cancelled",
                    task_run_id=task_run_id,
                )
                raise  # propagate so the task set is cleaned up
            except Exception as exc:
                tb = traceback.format_exc()
                error_msg = f"{type(exc).__name__}: {exc}\n{tb}"
                await self._handle_failure(msg_id, task_run, task_def, error_msg)
            finally:
                self._resources.release(task_def.cpu_cores, task_def.memory_gb)

    async def _execute_task(self, task_def, input_data: Dict[str, Any]) -> Any:
        """
        Run sync tasks in the thread pool; async tasks directly in the event loop.
        """
        loop = asyncio.get_running_loop()
        if asyncio.iscoroutinefunction(task_def.func):
            return await task_def.func(**input_data)
        return await loop.run_in_executor(
            self._executor,
            lambda: task_def.func(**input_data),
        )

    async def _handle_failure(
        self,
        msg_id: str,
        task_run: TaskRun,
        task_def,
        error: str,
    ) -> None:
        # Truncate for storage
        error_stored = error[:_MAX_ERROR_LEN]
        if len(error) > _MAX_ERROR_LEN:
            error_stored += "\n[truncated]"

        TASKS_TOTAL.labels(
            dag_name=task_run.dag_name,
            task_name=task_run.task_name,
            status="failed",
        ).inc()

        remaining_retries = task_run.max_retries - task_run.attempt
        if remaining_retries > 0:
            delay = compute_delay(attempt=task_run.attempt)
            TASK_RETRIES.labels(
                dag_name=task_run.dag_name,
                task_name=task_run.task_name,
            ).inc()
            self._store.update_task_state(
                task_run.task_run_id,
                TaskState.RETRYING,
                error=error_stored,
                attempt=task_run.attempt,
            )
            self._store.log_event(
                task_run.task_run_id,
                task_run.run_id,
                "retry",
                f"Attempt {task_run.attempt} failed. "
                f"Retrying in {delay:.1f}s. "
                f"Remaining: {remaining_retries}",
            )
            await self._queue.ack(msg_id)
            await asyncio.sleep(delay)
            await self._queue.enqueue(
                task_run.task_run_id,
                {
                    "task_run_id": task_run.task_run_id,
                    "run_id": task_run.run_id,
                    "dag_name": task_run.dag_name,
                    "task_name": task_run.task_name,
                    "max_retries": task_run.max_retries,
                    "attempt": task_run.attempt,
                    **task_run.input_data,
                },
            )
        else:
            await self._dlq.send(
                task_run_id=task_run.task_run_id,
                run_id=task_run.run_id,
                dag_name=task_run.dag_name,
                task_name=task_run.task_name,
                attempt=task_run.attempt,
                error=error_stored,
                input_data=task_run.input_data,
            )
            self._store.update_task_state(
                task_run.task_run_id, TaskState.DLQ, error=error_stored
            )
            self._store.log_event(
                task_run.task_run_id,
                task_run.run_id,
                "dlq",
                f"Sent to DLQ after {task_run.attempt} attempts",
            )
            await self._queue.ack(msg_id)
            await self._webhook.send(
                "task_dlq",
                {
                    "dag_name": task_run.dag_name,
                    "task_name": task_run.task_name,
                    "run_id": task_run.run_id,
                    "error": error_stored[:500],
                },
            )
            await self._notify_engine(task_run, success=False)

    async def _notify_engine(self, task_run: TaskRun, success: bool) -> None:
        callback = self._run_callbacks.get(task_run.run_id)
        if callback:
            try:
                await callback(task_run, success)
            except Exception as exc:
                logger.error(
                    "worker.engine_callback_failed",
                    run_id=task_run.run_id,
                    task=task_run.task_name,
                    error=str(exc),
                )

    def _load_task_run(
        self, task_run_id: str, payload: Dict[str, Any]
    ) -> Optional[TaskRun]:
        """
        Load TaskRun from the DB.  Falls back to payload only if the DB
        row is genuinely missing (e.g. first delivery before the DB write
        committed).  A missing DB row is logged as a warning.
        """
        run_id = payload.get("run_id", "")
        if run_id:
            run_rows = self._store.get_task_runs_for_run(run_id)
            for row in run_rows:
                if row["task_run_id"] == task_run_id:
                    return TaskRun(
                        task_run_id=row["task_run_id"],
                        run_id=row["run_id"],
                        dag_name=row["dag_name"],
                        task_name=row["task_name"],
                        state=TaskState(row["state"]),
                        attempt=row.get("attempt", 0),
                        max_retries=row.get("max_retries", 0),
                        input_data=json.loads(row.get("input_data", "{}")),
                    )

        # DB row not found — build from payload with a warning
        logger.warning(
            "worker.task_run_missing_from_db",
            task_run_id=task_run_id,
            run_id=run_id,
        )
        known_keys = {
            "task_run_id",
            "run_id",
            "dag_name",
            "task_name",
            "attempt",
            "max_retries",
        }
        return TaskRun(
            task_run_id=task_run_id,
            run_id=payload.get("run_id", "unknown"),
            dag_name=payload.get("dag_name", "unknown"),
            task_name=payload.get("task_name", "unknown"),
            state=TaskState.PENDING,
            attempt=int(payload.get("attempt", 0)),
            max_retries=int(payload.get("max_retries", 0)),
            input_data={k: v for k, v in payload.items() if k not in known_keys},
        )
