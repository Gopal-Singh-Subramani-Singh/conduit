from __future__ import annotations
import pytest
from datetime import datetime


@pytest.mark.asyncio
async def test_send_to_dlq(dlq):
    msg_id = await dlq.send(
        task_run_id="tr-fail",
        run_id="r-001",
        dag_name="test_dag",
        task_name="failing_task",
        attempt=3,
        error="ValueError: Something went wrong",
        input_data={"param": "value"},
    )
    assert msg_id is not None
    assert len(msg_id) > 0


@pytest.mark.asyncio
async def test_list_entries(dlq):
    await dlq.send(
        task_run_id="tr-1",
        run_id="r-1",
        dag_name="dag_a",
        task_name="task_x",
        attempt=2,
        error="Error msg",
        input_data={},
    )
    entries = await dlq.list_entries(limit=10)
    assert len(entries) == 1
    assert entries[0].task_run_id == "tr-1"
    assert entries[0].dag_name == "dag_a"


@pytest.mark.asyncio
async def test_dlq_depth(dlq):
    assert await dlq.depth() == 0
    await dlq.send("tr-2", "r-2", "dag", "task", 1, "err", {})
    assert await dlq.depth() == 1


@pytest.mark.asyncio
async def test_clear_dlq(dlq):
    await dlq.send("tr-3", "r-3", "dag", "task", 1, "err", {})
    await dlq.clear()
    assert await dlq.depth() == 0


@pytest.mark.asyncio
async def test_multiple_entries_ordered_newest_first(dlq):
    for i in range(3):
        await dlq.send(f"tr-{i}", "r-1", "dag", f"task_{i}", 1, "err", {})
    entries = await dlq.list_entries()
    assert len(entries) == 3
    # Most recent first
    names = [e.task_name for e in entries]
    assert names[0] == "task_2"
