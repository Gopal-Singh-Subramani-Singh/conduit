"""FastAPI integration tests — endpoints, error responses, health check."""
from __future__ import annotations

import os
import tempfile
import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, MagicMock


def _setup_app_state(tmp_db_path: str):
    """Provide a minimal app_state so endpoints can execute."""
    from conduit_api.main import app_state
    from conduit_core.store import ExecutionStore

    app_state.store = ExecutionStore(db_path=tmp_db_path)
    app_state.engine = MagicMock()
    app_state.engine.active_run_ids = MagicMock(return_value=[])
    app_state.engine.trigger = AsyncMock(return_value="run-abc123")
    app_state.engine.cancel = AsyncMock(return_value=False)
    app_state.dlq = AsyncMock()
    app_state.dlq.list_entries = AsyncMock(return_value=[])
    app_state.dlq.clear = AsyncMock()
    app_state.queue = AsyncMock()
    app_state.redis = AsyncMock()
    app_state.redis.ping = AsyncMock()
    app_state.redis_ok = True
    return app_state


@pytest.mark.asyncio
async def test_root_endpoint():
    from conduit_api.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "Conduit"


@pytest.mark.asyncio
async def test_health_endpoint():
    from conduit_api.main import app
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    state = _setup_app_state(db_path)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "redis" in data
        assert "uptime_seconds" in data
    finally:
        os.unlink(db_path)


@pytest.mark.asyncio
async def test_metrics_endpoint():
    from conduit_api.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert b"conduit_" in resp.content


@pytest.mark.asyncio
async def test_list_dags_empty():
    from conduit_api.main import app
    from conduit_core import decorators
    decorators.clear_registry()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/dags")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_trigger_unknown_dag():
    from conduit_api.main import app, app_state
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    state = _setup_app_state(db_path)
    state.engine.trigger = AsyncMock(
        side_effect=ValueError("DAG 'unknown' not registered")
    )
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/runs",
                json={"dag_name": "unknown", "input_data": {}},
            )
        assert resp.status_code == 422
    finally:
        os.unlink(db_path)


@pytest.mark.asyncio
async def test_get_nonexistent_run():
    from conduit_api.main import app
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    _setup_app_state(db_path)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/runs/nonexistent-run-id")
        assert resp.status_code == 404
    finally:
        os.unlink(db_path)
