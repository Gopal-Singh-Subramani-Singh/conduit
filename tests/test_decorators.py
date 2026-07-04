"""Tests for conduit_core.decorators — registration, eager loading, registry ops."""
from __future__ import annotations

import pytest
from conduit_core import decorators
from conduit_core.models import TaskDefinition, DAGDefinition


def test_task_decorator_registers():
    @decorators.task(name="my_task", retries=2)
    def do_work():
        pass

    td = decorators.get_task("my_task")
    assert td is not None
    assert td.name == "my_task"
    assert td.retries == 2


def test_task_decorator_uses_function_name():
    @decorators.task()
    def auto_named():
        pass

    td = decorators.get_task("auto_named")
    assert td is not None


def test_task_decorator_wrapper_still_callable():
    @decorators.task()
    def add(x: int, y: int) -> int:
        return x + y

    assert add(1, 2) == 3


def test_task_has_task_def_attribute():
    @decorators.task(cpu_cores=2.0)
    def heavy():
        pass

    assert hasattr(heavy, "_task_def")
    assert heavy._task_def.cpu_cores == 2.0


def test_dag_decorator_registers_eagerly():
    @decorators.task(name="dag_step_a")
    def step_a():
        return {}

    @decorators.dag(name="eager_dag", description="test dag")
    def my_pipeline():
        return [step_a]

    dag_def = decorators.get_dag("eager_dag")
    assert dag_def is not None
    assert dag_def.description == "test dag"
    assert len(dag_def.tasks) == 1
    assert dag_def.tasks[0].name == "dag_step_a"


def test_dag_schedule_stored():
    @decorators.task(name="sched_step")
    def sched_step():
        return {}

    @decorators.dag(name="scheduled_dag", schedule="0 2 * * *")
    def scheduled():
        return [sched_step]

    dag_def = decorators.get_dag("scheduled_dag")
    assert dag_def.schedule == "0 2 * * *"


def test_list_dags_returns_all():
    @decorators.task(name="list_task")
    def t():
        pass

    @decorators.dag(name="list_dag_1")
    def d1():
        return [t]

    @decorators.dag(name="list_dag_2")
    def d2():
        return [t]

    names = [d.name for d in decorators.list_dags()]
    assert "list_dag_1" in names
    assert "list_dag_2" in names


def test_clear_registry():
    @decorators.task(name="to_clear")
    def tc():
        pass

    @decorators.dag(name="clear_dag")
    def cd():
        return [tc]

    decorators.clear_registry()
    assert decorators.get_task("to_clear") is None
    assert decorators.get_dag("clear_dag") is None


def test_register_dag_directly():
    tasks = [
        TaskDefinition(
            name="manual_task",
            func=lambda: {},
        )
    ]
    dag_def = DAGDefinition(name="manual_dag", tasks=tasks)
    decorators.register_dag(dag_def)
    assert decorators.get_dag("manual_dag") is not None


def test_dag_with_non_task_raises():
    with pytest.raises(ValueError, match="non-task"):
        @decorators.dag(name="bad_dag")
        def bad():
            return ["not_a_task"]
