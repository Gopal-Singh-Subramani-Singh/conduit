from __future__ import annotations
import pytest
from conduit_core.dag import DAG, CycleDetectedError, DAGValidationError
from tests.conftest import make_task_def, make_dag_def


def test_simple_linear_dag():
    tasks = [
        make_task_def("a"),
        make_task_def("b", depends_on=["a"]),
        make_task_def("c", depends_on=["b"]),
    ]
    dag = DAG(make_dag_def("linear", tasks))
    levels = dag.execution_levels()
    assert len(levels) == 3
    assert levels[0][0].name == "a"
    assert levels[1][0].name == "b"
    assert levels[2][0].name == "c"


def test_parallel_tasks_same_level():
    tasks = [
        make_task_def("root"),
        make_task_def("branch_a", depends_on=["root"]),
        make_task_def("branch_b", depends_on=["root"]),
        make_task_def("merge", depends_on=["branch_a", "branch_b"]),
    ]
    dag = DAG(make_dag_def("parallel", tasks))
    levels = dag.execution_levels()
    assert len(levels) == 3
    level_1_names = {t.name for t in levels[1]}
    assert level_1_names == {"branch_a", "branch_b"}


def test_cycle_detection():
    tasks = [
        make_task_def("a", depends_on=["c"]),
        make_task_def("b", depends_on=["a"]),
        make_task_def("c", depends_on=["b"]),
    ]
    with pytest.raises(CycleDetectedError):
        DAG(make_dag_def("cycle", tasks))


def test_undefined_dependency_raises():
    tasks = [make_task_def("a", depends_on=["nonexistent"])]
    with pytest.raises(DAGValidationError):
        DAG(make_dag_def("bad", tasks))


def test_get_ready_tasks_after_completion():
    tasks = [
        make_task_def("a"),
        make_task_def("b", depends_on=["a"]),
        make_task_def("c", depends_on=["a"]),
    ]
    dag = DAG(make_dag_def("fan_out", tasks))
    ready = dag.get_ready_tasks(completed={"a"}, failed=set())
    ready_names = {t.name for t in ready}
    assert ready_names == {"b", "c"}


def test_get_downstream_tasks():
    tasks = [
        make_task_def("root"),
        make_task_def("mid", depends_on=["root"]),
        make_task_def("leaf", depends_on=["mid"]),
    ]
    dag = DAG(make_dag_def("chain", tasks))
    downstream = dag.get_downstream_tasks("root")
    assert set(downstream) == {"mid", "leaf"}


def test_single_task_dag():
    dag = DAG(make_dag_def("solo", [make_task_def("only")]))
    assert len(dag) == 1
    levels = dag.execution_levels()
    assert len(levels) == 1


def test_no_ready_tasks_when_all_complete():
    tasks = [make_task_def("a"), make_task_def("b", depends_on=["a"])]
    dag = DAG(make_dag_def("done", tasks))
    ready = dag.get_ready_tasks(completed={"a", "b"}, failed=set())
    assert ready == []
