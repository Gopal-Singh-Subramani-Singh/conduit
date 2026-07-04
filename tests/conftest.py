from __future__ import annotations
import pytest
import pytest_asyncio
from unittest.mock import MagicMock
from fakeredis import aioredis as fake_redis

from conduit_core import decorators
from conduit_core.models import (
    TaskDefinition, DAGDefinition, TaskRun, TaskState
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear decorator registries before and after each test."""
    decorators.clear_registry()
    yield
    decorators.clear_registry()


@pytest.fixture(autouse=True)
def reset_cfg():
    """Reset config singleton so env monkeypatches take effect."""
    from config.settings import reset_config
    reset_config()
    yield
    reset_config()


@pytest_asyncio.fixture
async def redis_client():
    r = fake_redis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


@pytest.fixture
def tmp_store(tmp_path):
    from conduit_core.store import ExecutionStore
    return ExecutionStore(db_path=str(tmp_path / "test.db"))


@pytest_asyncio.fixture
async def task_queue(redis_client):
    from conduit_core.queue import TaskQueue
    from unittest.mock import patch
    with patch("conduit_core.queue.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            redis=MagicMock(
                stream_key="test:tasks",
                dlq_key="test:dlq",
                consumer_group="test:workers",
                consumer_name="test-worker",
                batch_size=10,
                block_ms=100,
                visibility_timeout_seconds=30,
            )
        )
        q = TaskQueue(redis_client)
        await q.setup()
        yield q


@pytest_asyncio.fixture
async def dlq(redis_client):
    from conduit_core.dlq import DeadLetterQueue
    from unittest.mock import patch
    with patch("conduit_core.dlq.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            redis=MagicMock(dlq_key="test:dlq")
        )
        yield DeadLetterQueue(redis_client)


def make_task_def(
    name: str,
    depends_on: list = [],
    retries: int = 0,
    cpu_cores: float = 1.0,
    memory_gb: float = 1.0,
) -> TaskDefinition:
    return TaskDefinition(
        name=name,
        func=lambda **kwargs: {"result": f"{name}_output"},
        depends_on=list(depends_on),
        retries=retries,
        cpu_cores=cpu_cores,
        memory_gb=memory_gb,
    )


def make_dag_def(
    name: str,
    tasks: list = None,
) -> DAGDefinition:
    if tasks is None:
        tasks = [make_task_def("task_a"), make_task_def("task_b", ["task_a"])]
    return DAGDefinition(name=name, tasks=tasks)
