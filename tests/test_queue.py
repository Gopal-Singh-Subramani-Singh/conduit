from __future__ import annotations
import pytest


@pytest.mark.asyncio
async def test_enqueue_and_dequeue(task_queue):
    await task_queue.enqueue("tr-001", {"key": "value", "run_id": "r1"})
    messages = await task_queue.dequeue(count=1)
    assert len(messages) == 1
    msg_id, task_run_id, payload = messages[0]
    assert task_run_id == "tr-001"
    assert payload["key"] == "value"


@pytest.mark.asyncio
async def test_ack_removes_from_pending(task_queue):
    await task_queue.enqueue("tr-002", {"run_id": "r1"})
    messages = await task_queue.dequeue(count=1)
    assert len(messages) == 1
    msg_id, _, _ = messages[0]
    await task_queue.ack(msg_id)
    pending = await task_queue.pending_count()
    assert pending == 0


@pytest.mark.asyncio
async def test_depth_increases_on_enqueue(task_queue):
    assert await task_queue.depth() == 0
    await task_queue.enqueue("tr-003", {"run_id": "r1"})
    await task_queue.enqueue("tr-004", {"run_id": "r1"})
    assert await task_queue.depth() == 2


@pytest.mark.asyncio
async def test_empty_queue_returns_empty(task_queue):
    messages = await task_queue.dequeue(count=1)
    assert messages == []


@pytest.mark.asyncio
async def test_multiple_enqueue_dequeue(task_queue):
    for i in range(5):
        await task_queue.enqueue(f"tr-{i:03d}", {"i": i, "run_id": "r1"})
    messages = await task_queue.dequeue(count=5)
    assert len(messages) == 5


@pytest.mark.asyncio
async def test_clear_empties_queue(task_queue):
    await task_queue.enqueue("tr-x", {"run_id": "r1"})
    await task_queue.clear()
    assert await task_queue.depth() == 0


@pytest.mark.asyncio
async def test_payload_preserved_through_queue(task_queue):
    payload = {
        "task_run_id": "tr-abc",
        "run_id": "run-1",
        "dag_name": "my_pipeline",
        "complex_data": {"nested": [1, 2, 3]},
    }
    await task_queue.enqueue("tr-abc", payload)
    messages = await task_queue.dequeue(count=1)
    assert len(messages) == 1
    _, _, dequeued_payload = messages[0]
    assert dequeued_payload["dag_name"] == "my_pipeline"
