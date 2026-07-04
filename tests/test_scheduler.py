"""Tests for CronScheduler — registration, removal, trigger."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_scheduler():
    with patch("conduit_core.scheduler.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            scheduler=MagicMock(timezone="UTC", misfire_grace_seconds=60)
        )
        from conduit_core.scheduler import CronScheduler
        engine = AsyncMock()
        engine.trigger = AsyncMock(return_value="run-abc")
        sched = CronScheduler(engine)
        return sched, engine


def test_register_dag_valid_cron():
    sched, _ = _make_scheduler()
    with patch.object(sched._scheduler, "start"), \
         patch.object(sched._scheduler, "add_job") as mock_add:
        mock_job = MagicMock()
        mock_job.id = "dag_test_dag"
        mock_add.return_value = mock_job

        job_id = sched.register_dag("test_dag", "0 2 * * *")
        assert job_id == "dag_test_dag"
        mock_add.assert_called_once()


def test_register_dag_invalid_cron_raises():
    sched, _ = _make_scheduler()
    with pytest.raises(ValueError, match="Invalid cron"):
        sched.register_dag("bad_dag", "not a valid cron")


def test_remove_existing_dag():
    sched, _ = _make_scheduler()
    sched._jobs["my_dag"] = "job_my_dag"
    with patch.object(sched._scheduler, "remove_job") as mock_remove:
        result = sched.remove_dag("my_dag")
        assert result is True
        mock_remove.assert_called_once_with("job_my_dag")
    assert "my_dag" not in sched._jobs


def test_remove_nonexistent_dag():
    sched, _ = _make_scheduler()
    result = sched.remove_dag("not_registered")
    assert result is False


@pytest.mark.asyncio
async def test_trigger_dag_calls_engine():
    sched, engine = _make_scheduler()
    await sched._trigger_dag("my_dag")
    engine.trigger.assert_awaited_once_with("my_dag", trigger="cron")


@pytest.mark.asyncio
async def test_trigger_dag_engine_failure_does_not_raise():
    sched, engine = _make_scheduler()
    engine.trigger.side_effect = RuntimeError("Redis down")
    # Should not propagate the exception
    await sched._trigger_dag("failing_dag")
