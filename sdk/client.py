from __future__ import annotations
from typing import Any, Dict, List, Optional
from conduit_core import decorators
from conduit_core.models import DAGDefinition


def task(
    name: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    retries: int = 0,
    timeout_seconds: int = 3600,
    cpu_cores: float = 1.0,
    memory_gb: float = 1.0,
    tags: Optional[dict] = None,
):
    """Register a task. Alias for @conduit_core.decorators.task."""
    return decorators.task(
        name=name,
        depends_on=depends_on,
        retries=retries,
        timeout_seconds=timeout_seconds,
        cpu_cores=cpu_cores,
        memory_gb=memory_gb,
        tags=tags,
    )


def dag(
    name: Optional[str] = None,
    schedule: Optional[str] = None,
    description: str = "",
    tags: Optional[dict] = None,
):
    """Register a DAG. Alias for @conduit_core.decorators.dag."""
    return decorators.dag(
        name=name,
        schedule=schedule,
        description=description,
        tags=tags,
    )


def register_dag(dag_def: DAGDefinition) -> None:
    decorators.register_dag(dag_def)


async def run(
    dag_name: str,
    input_data: Optional[Dict[str, Any]] = None,
    engine=None,
) -> str:
    """
    Trigger a DAG run programmatically.

    Requires a live ``PipelineEngine`` instance to be passed via ``engine=``.
    Alternatively use the REST API: POST /runs
    """
    if engine is None:
        raise RuntimeError(
            "engine= is required. Pass a PipelineEngine instance "
            "or use the REST API: POST /runs"
        )
    return await engine.trigger(dag_name, input_data=input_data or {})
