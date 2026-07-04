from __future__ import annotations
import functools
from typing import Any, Callable, List, Optional
import structlog

from conduit_core.models import TaskDefinition, DAGDefinition

logger = structlog.get_logger(__name__)

# Global registries
_TASK_REGISTRY: dict[str, TaskDefinition] = {}
_DAG_REGISTRY: dict[str, DAGDefinition] = {}


def task(
    name: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    retries: int = 0,
    timeout_seconds: int = 3600,
    cpu_cores: float = 1.0,
    memory_gb: float = 1.0,
    tags: Optional[dict] = None,
):
    """
    Decorator to register a function as a Conduit task.

    Usage:
        @conduit.task(retries=3, cpu_cores=2.0, depends_on=["fetch_data"])
        def preprocess(fetch_data_result: dict) -> dict:
            ...
    """
    def decorator(func: Callable) -> Callable:
        task_name = name or func.__name__
        task_def = TaskDefinition(
            name=task_name,
            func=func,
            depends_on=depends_on or [],
            retries=retries,
            timeout_seconds=timeout_seconds,
            cpu_cores=cpu_cores,
            memory_gb=memory_gb,
            tags=tags or {},
        )
        _TASK_REGISTRY[task_name] = task_def
        logger.debug("decorators.task_registered", name=task_name)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper._task_def = task_def
        return wrapper

    return decorator


def dag(
    name: Optional[str] = None,
    schedule: Optional[str] = None,
    description: str = "",
    tags: Optional[dict] = None,
):
    """
    Decorator to register a function as a Conduit DAG.
    The decorated function must return a list of task functions.

    Usage:
        @conduit.dag(schedule="0 2 * * *")
        def training_pipeline():
            return [fetch_data, preprocess, train_model, evaluate]
    """
    def decorator(func: Callable) -> Callable:
        dag_name = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            task_funcs = func(*args, **kwargs)
            tasks = []
            for tf in task_funcs:
                if hasattr(tf, "_task_def"):
                    tasks.append(tf._task_def)
                elif isinstance(tf, TaskDefinition):
                    tasks.append(tf)
                else:
                    raise ValueError(
                        f"DAG '{dag_name}' returned non-task: {tf!r}"
                    )
            dag_def = DAGDefinition(
                name=dag_name,
                tasks=tasks,
                schedule=schedule,
                description=description,
                tags=tags or {},
            )
            _DAG_REGISTRY[dag_name] = dag_def
            return dag_def

        wrapper._dag_name = dag_name
        wrapper._schedule = schedule
        wrapper()  # eagerly register
        return wrapper

    return decorator


def get_dag(name: str) -> Optional[DAGDefinition]:
    return _DAG_REGISTRY.get(name)


def list_dags() -> list[DAGDefinition]:
    return list(_DAG_REGISTRY.values())


def get_task(name: str) -> Optional[TaskDefinition]:
    return _TASK_REGISTRY.get(name)


def register_dag(dag_def: DAGDefinition) -> None:
    _DAG_REGISTRY[dag_def.name] = dag_def


def clear_registry() -> None:
    """Used in tests."""
    _TASK_REGISTRY.clear()
    _DAG_REGISTRY.clear()
