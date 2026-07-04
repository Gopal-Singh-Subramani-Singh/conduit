"""Tests for PipelineEngine — lifecycle, dispatch, finalisation, cancel."""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from conduit_core.engine import PipelineEngine
from conduit_core.models import TaskState, RunState
from tests.conftest import make_task_def, make_dag_def
from conduit_core import decorators


def _make_engine(tmp_store):
    queue = AsyncMock()
    queue.enqueue = AsyncMock(return_value="msg-1")
    webhook = AsyncMock()
    webhook.send = AsyncMock(return_value=True)
    worker_pool = MagicMock()
    worker_pool.register_callback = MagicMock()
    return PipelineEngine(
        queue=queue,
        store=tmp_store,
        webhook=webhook,
        worker_pool=worker_pool,
    ), queue, webhook, worker_pool


@pytest.mark.asyncio
async def test_trigger_unregistered_dag_raises(tmp_store):
    engine, _, _, _ = _make_engine(tmp_store)
    with pytest.raises(ValueError, match="not registered"):
        await engine.trigger("nonexistent_dag")


@pytest.mark.asyncio
async def test_trigger_creates_run_in_store(tmp_store):
    tasks = [make_task_def("step_a")]
    dag_def = make_dag_def("simple", tasks)
    decorators.register_dag(dag_def)
    # Also register the task so engine can dispatch it
    decorators._TASK_REGISTRY["step_a"] = tasks[0]

    engine, queue, _, _ = _make_engine(tmp_store)
    run_id = await engine.trigger("simple")

    assert run_id is not None
    assert len(run_id) == 36  # full UUID
    run = tmp_store.get_run(run_id)
    assert run is not None
    assert run["dag_name"] == "simple"
    assert run["state"] == "running"
    # Initial task should have been enqueued
    queue.enqueue.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_with_input_data(tmp_store):
    tasks = [make_task_def("step_a")]
    dag_def = make_dag_def("with_input", tasks)
    decorators.register_dag(dag_def)
    decorators._TASK_REGISTRY["step_a"] = tasks[0]

    engine, queue, _, _ = _make_engine(tmp_store)
    run_id = await engine.trigger("with_input", input_data={"key": "value"})

    run = tmp_store.get_run(run_id)
    assert run["input_data"] == '{"key": "value"}'


@pytest.mark.asyncio
async def test_cancel_active_run(tmp_store):
    tasks = [make_task_def("step_a")]
    dag_def = make_dag_def("cancel_me", tasks)
    decorators.register_dag(dag_def)
    decorators._TASK_REGISTRY["step_a"] = tasks[0]

    engine, _, _, _ = _make_engine(tmp_store)
    run_id = await engine.trigger("cancel_me")

    result = await engine.cancel(run_id)
    assert result is True

    run = tmp_store.get_run(run_id)
    assert run["state"] == "cancelled"
    assert run_id not in engine.active_run_ids()


@pytest.mark.asyncio
async def test_cancel_nonexistent_run(tmp_store):
    engine, _, _, _ = _make_engine(tmp_store)
    result = await engine.cancel("no-such-run")
    assert result is False


@pytest.mark.asyncio
async def test_active_run_ids_tracked(tmp_store):
    tasks = [make_task_def("step_a")]
    dag_def = make_dag_def("tracked", tasks)
    decorators.register_dag(dag_def)
    decorators._TASK_REGISTRY["step_a"] = tasks[0]

    engine, _, _, _ = _make_engine(tmp_store)
    assert engine.active_run_ids() == []

    run_id = await engine.trigger("tracked")
    assert run_id in engine.active_run_ids()


@pytest.mark.asyncio
async def test_on_task_complete_success_finalises(tmp_store):
    """Single-task DAG: task success should finalise the run as SUCCESS."""
    tasks = [make_task_def("only_task")]
    dag_def = make_dag_def("one_task", tasks)
    decorators.register_dag(dag_def)
    decorators._TASK_REGISTRY["only_task"] = tasks[0]

    engine, _, webhook, _ = _make_engine(tmp_store)
    run_id = await engine.trigger("one_task")

    from conduit_core.dag import DAG
    from conduit_core.models import TaskRun
    dag = DAG(dag_def)
    task_run = TaskRun(
        task_run_id=f"{run_id[:8]}-only_task",
        run_id=run_id,
        dag_name="one_task",
        task_name="only_task",
    )

    await engine._on_task_complete(run_id, dag, task_run, success=True)

    run = tmp_store.get_run(run_id)
    assert run["state"] == "success"
    webhook.send.assert_called_once()


@pytest.mark.asyncio
async def test_finalise_run_idempotent(tmp_store):
    """Calling _finalise_run twice must not decrement ACTIVE_RUNS twice."""
    tasks = [make_task_def("only_task")]
    dag_def = make_dag_def("idempotent", tasks)
    decorators.register_dag(dag_def)
    decorators._TASK_REGISTRY["only_task"] = tasks[0]

    engine, _, webhook, _ = _make_engine(tmp_store)
    run_id = await engine.trigger("idempotent")

    from conduit_core.dag import DAG
    from conduit_core.models import TaskRun
    dag = DAG(dag_def)
    task_run = TaskRun(
        task_run_id=f"{run_id[:8]}-only_task",
        run_id=run_id,
        dag_name="idempotent",
        task_name="only_task",
    )

    # Simulate two concurrent completions
    await engine._on_task_complete(run_id, dag, task_run, success=True)
    await engine._on_task_complete(run_id, dag, task_run, success=True)

    # Webhook should only fire once
    assert webhook.send.call_count == 1
