from __future__ import annotations
import pytest
from datetime import datetime
from conduit_core.models import (
    PipelineRun, TaskRun, TaskState, RunState
)


def make_run(run_id: str = "r-001", dag: str = "test_dag") -> PipelineRun:
    return PipelineRun(run_id=run_id, dag_name=dag, state=RunState.RUNNING)


def make_task_run(
    task_run_id: str = "tr-001",
    run_id: str = "r-001",
    task: str = "task_a",
) -> TaskRun:
    return TaskRun(
        task_run_id=task_run_id,
        run_id=run_id,
        dag_name="test_dag",
        task_name=task,
    )


def test_save_and_get_run(tmp_store):
    run = make_run()
    tmp_store.save_run(run)
    retrieved = tmp_store.get_run("r-001")
    assert retrieved is not None
    assert retrieved["run_id"] == "r-001"
    assert retrieved["dag_name"] == "test_dag"


def test_update_run_state(tmp_store):
    tmp_store.save_run(make_run())
    tmp_store.update_run_state("r-001", RunState.SUCCESS)
    run = tmp_store.get_run("r-001")
    assert run["state"] == "success"
    assert run["finished_at"] is not None


def test_list_runs_by_dag(tmp_store):
    tmp_store.save_run(make_run("r-1", "dag_a"))
    tmp_store.save_run(make_run("r-2", "dag_a"))
    tmp_store.save_run(make_run("r-3", "dag_b"))
    runs_a = tmp_store.list_runs(dag_name="dag_a")
    assert len(runs_a) == 2
    runs_b = tmp_store.list_runs(dag_name="dag_b")
    assert len(runs_b) == 1


def test_save_and_get_task_run(tmp_store):
    tmp_store.save_run(make_run())
    tr = make_task_run()
    tmp_store.save_task_run(tr)
    rows = tmp_store.get_task_runs_for_run("r-001")
    assert len(rows) == 1
    assert rows[0]["task_name"] == "task_a"


def test_update_task_state(tmp_store):
    tmp_store.save_run(make_run())
    tmp_store.save_task_run(make_task_run())
    tmp_store.update_task_state(
        "tr-001", TaskState.SUCCESS, output_data={"result": 42}
    )
    rows = tmp_store.get_task_runs_for_run("r-001")
    assert rows[0]["state"] == "success"
    assert rows[0]["finished_at"] is not None


def test_log_and_get_events(tmp_store):
    tmp_store.save_run(make_run())
    tmp_store.log_event("tr-001", "r-001", "started", "Beginning task")
    tmp_store.log_event("tr-001", "r-001", "success", "Task completed")
    events = tmp_store.get_events("r-001")
    assert len(events) == 2
    assert events[0]["event_type"] == "success"  # newest first
