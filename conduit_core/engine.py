from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import structlog

from conduit_core.dag import DAG
from conduit_core.models import (
    PipelineRun,
    TaskRun,
    TaskState,
    RunState,
)
from conduit_core.queue import TaskQueue
from conduit_core.store import ExecutionStore
from conduit_core.webhook import WebhookSender
from conduit_core.metrics import RUNS_TOTAL, RUN_DURATION, ACTIVE_RUNS
from conduit_core import decorators

logger = structlog.get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PipelineEngine:
    """
    Orchestrates pipeline runs using an event-driven Kahn's algorithm dispatch.

    After each task completes the engine re-evaluates which tasks are now
    ready and dispatches them to the Redis Streams queue. When all tasks
    are processed the run is finalised and metrics/webhooks are fired.

    Thread-safety note: all state mutations happen inside coroutines that
    are scheduled on a single asyncio event loop.  The ``_finalising``
    set provides an idempotency guard around ``_finalise_run`` to prevent
    duplicate finalisation if two tasks complete in the same event-loop
    iteration.
    """

    def __init__(
        self,
        queue: TaskQueue,
        store: ExecutionStore,
        webhook: WebhookSender,
        worker_pool,
    ) -> None:
        self._queue = queue
        self._store = store
        self._webhook = webhook
        self._worker_pool = worker_pool
        self._active_runs: Dict[str, PipelineRun] = {}
        self._active_dags: Dict[str, DAG] = {}
        self._completed: Dict[str, Set[str]] = {}
        self._failed: Dict[str, Set[str]] = {}
        # Guards against double-finalisation when two tasks complete
        # near-simultaneously in the same event loop.
        self._finalising: Set[str] = set()

    async def trigger(
        self,
        dag_name: str,
        input_data: Optional[Dict[str, Any]] = None,
        trigger: str = "manual",
    ) -> str:
        """
        Start a new pipeline run.

        Returns the run_id.  Raises ValueError if the DAG is not registered.
        Raises RuntimeError if the initial task dispatch fails (e.g. Redis down).
        """
        if input_data is None:
            input_data = {}

        dag_def = decorators.get_dag(dag_name)
        if dag_def is None:
            raise ValueError(f"DAG '{dag_name}' not registered")

        dag = DAG(dag_def)
        run_id = str(uuid.uuid4())  # full UUID — no birthday collision risk

        run = PipelineRun(
            run_id=run_id,
            dag_name=dag_name,
            state=RunState.RUNNING,
            trigger=trigger,
            input_data=input_data,
        )
        self._active_runs[run_id] = run
        self._active_dags[run_id] = dag
        self._completed[run_id] = set()
        self._failed[run_id] = set()
        self._store.save_run(run)
        ACTIVE_RUNS.inc()

        self._worker_pool.register_callback(
            run_id,
            lambda task_run, success: self._on_task_complete(
                run_id, dag, task_run, success
            ),
        )

        ready = dag.get_ready_tasks(
            completed=self._completed[run_id],
            failed=self._failed[run_id],
        )
        try:
            for task_def in ready:
                await self._dispatch_task(run_id, dag_name, task_def, input_data)
        except Exception as exc:
            # Dispatch failed (Redis down, etc.) — mark run failed immediately
            logger.error(
                "engine.dispatch_failed",
                run_id=run_id,
                dag=dag_name,
                error=str(exc),
            )
            self._active_runs.pop(run_id, None)
            self._active_dags.pop(run_id, None)
            self._completed.pop(run_id, None)
            self._failed.pop(run_id, None)
            self._store.update_run_state(run_id, RunState.FAILED, error=str(exc))
            ACTIVE_RUNS.dec()
            raise RuntimeError(f"Failed to dispatch initial tasks: {exc}") from exc

        logger.info(
            "engine.run_started",
            run_id=run_id,
            dag=dag_name,
            initial_tasks=len(ready),
        )
        return run_id

    async def _on_task_complete(
        self,
        run_id: str,
        dag: DAG,
        task_run: TaskRun,
        success: bool,
    ) -> None:
        run = self._active_runs.get(run_id)
        if run is None:
            logger.debug(
                "engine.stale_completion",
                run_id=run_id,
                task=task_run.task_name,
                success=success,
            )
            return

        if success:
            self._completed[run_id].add(task_run.task_name)
            logger.debug(
                "engine.task_succeeded",
                run_id=run_id,
                task=task_run.task_name,
                completed_count=len(self._completed[run_id]),
            )
        else:
            self._failed[run_id].add(task_run.task_name)
            logger.warning(
                "engine.task_failed",
                run_id=run_id,
                task=task_run.task_name,
            )
            # Mark all downstream tasks as SKIPPED in the store
            downstream = dag.get_downstream_tasks(task_run.task_name)
            for ds_name in downstream:
                self._failed[run_id].add(ds_name)
            task_rows = self._store.get_task_runs_for_run(run_id)
            for row in task_rows:
                if row["task_name"] in downstream and row["state"] in (
                    "pending",
                    "ready",
                ):
                    self._store.update_task_state(
                        row["task_run_id"], TaskState.SKIPPED
                    )
                    self._store.log_event(
                        row["task_run_id"],
                        run_id,
                        "skipped",
                        f"Skipped due to upstream failure: {task_run.task_name}",
                    )

        # Dispatch newly ready tasks
        ready = dag.get_ready_tasks(
            completed=self._completed[run_id],
            failed=self._failed[run_id],
        )
        for task_def in ready:
            if (
                task_def.name not in self._completed[run_id]
                and task_def.name not in self._failed[run_id]
            ):
                upstream_outputs = await self._collect_outputs(
                    run_id, task_def.depends_on
                )
                try:
                    await self._dispatch_task(
                        run_id, run.dag_name, task_def, upstream_outputs
                    )
                except Exception as exc:
                    logger.error(
                        "engine.dispatch_task_failed",
                        run_id=run_id,
                        task=task_def.name,
                        error=str(exc),
                    )
                    self._failed[run_id].add(task_def.name)

        # Check completion
        all_tasks = set(dag.task_names())
        processed = self._completed[run_id] | self._failed[run_id]
        if processed >= all_tasks:
            await self._finalise_run(run_id, dag)

    async def _dispatch_task(
        self,
        run_id: str,
        dag_name: str,
        task_def,
        input_data: Dict[str, Any],
    ) -> str:
        task_run_id = f"{run_id[:8]}-{task_def.name}"

        task_run = TaskRun(
            task_run_id=task_run_id,
            run_id=run_id,
            dag_name=dag_name,
            task_name=task_def.name,
            state=TaskState.READY,
            max_retries=task_def.retries,
            input_data=input_data,
        )
        self._store.save_task_run(task_run)
        self._store.log_event(task_run_id, run_id, "queued", "Dispatched to queue")

        payload: Dict[str, Any] = {
            "task_run_id": task_run_id,
            "run_id": run_id,
            "dag_name": dag_name,
            "task_name": task_def.name,
            "max_retries": task_def.retries,
            "attempt": 0,
            **input_data,
        }
        await self._queue.enqueue(task_run_id, payload)
        return task_run_id

    async def _collect_outputs(
        self, run_id: str, task_names: List[str]
    ) -> Dict[str, Any]:
        """Gather output_data from completed upstream tasks."""
        outputs: Dict[str, Any] = {}
        task_rows = self._store.get_task_runs_for_run(run_id)
        for row in task_rows:
            if row["task_name"] in task_names and row["output_data"]:
                output = json.loads(row["output_data"])
                outputs[f"{row['task_name']}_result"] = output
        return outputs

    async def _finalise_run(self, run_id: str, dag: DAG) -> None:
        # Idempotency guard — prevent double-finalisation
        if run_id in self._finalising:
            return
        self._finalising.add(run_id)

        run = self._active_runs.pop(run_id, None)
        if run is None:
            self._finalising.discard(run_id)
            return

        ACTIVE_RUNS.dec()
        failed = self._failed.pop(run_id, set())
        completed = self._completed.pop(run_id, set())
        self._active_dags.pop(run_id, None)
        self._finalising.discard(run_id)

        state = (
            RunState.SUCCESS
            if not failed
            else (RunState.PARTIAL if completed else RunState.FAILED)
        )
        run.state = state
        run.finished_at = _utcnow()
        self._store.update_run_state(run_id, state)

        duration = (run.finished_at - run.started_at).total_seconds()
        RUNS_TOTAL.labels(dag_name=run.dag_name, status=state.value).inc()
        RUN_DURATION.labels(dag_name=run.dag_name).observe(duration)

        await self._webhook.send(
            f"run_{state.value}",
            {
                "run_id": run_id,
                "dag_name": run.dag_name,
                "duration_seconds": duration,
                "failed_tasks": list(failed),
            },
        )
        logger.info(
            "engine.run_complete",
            run_id=run_id,
            dag=run.dag_name,
            state=state.value,
            duration_s=round(duration, 1),
            completed=len(completed),
            failed=len(failed),
        )

    async def cancel(self, run_id: str) -> bool:
        """
        Cancel an active run.  In-flight worker tasks will still complete
        (Redis messages are not retractable), but the engine will stop
        dispatching new tasks and mark the run CANCELLED.
        """
        run = self._active_runs.get(run_id)
        if not run:
            return False
        run.state = RunState.CANCELLED
        self._store.update_run_state(run_id, RunState.CANCELLED)
        self._active_runs.pop(run_id, None)
        self._active_dags.pop(run_id, None)
        self._completed.pop(run_id, None)
        self._failed.pop(run_id, None)
        self._finalising.discard(run_id)
        ACTIVE_RUNS.dec()
        logger.info("engine.run_cancelled", run_id=run_id)
        return True

    def active_run_ids(self) -> List[str]:
        return list(self._active_runs.keys())
