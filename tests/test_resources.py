from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def resources():
    from conduit_core.resources import ResourceQuotaManager
    with patch("conduit_core.resources.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            resources=MagicMock(
                cpu_limit_pct=80.0,
                memory_limit_gb=16.0,
                check_enabled=True,
            )
        )
        return ResourceQuotaManager()


def test_can_dispatch_when_disabled():
    from conduit_core.resources import ResourceQuotaManager
    with patch("conduit_core.resources.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            resources=MagicMock(check_enabled=False)
        )
        rm = ResourceQuotaManager()
        assert rm.can_dispatch(cpu_cores=100.0, memory_gb=1000.0) is True


def test_reserve_and_release(resources):
    resources.reserve(2.0, 4.0)
    assert resources._allocated_cpu == 2.0
    assert resources._allocated_mem == 4.0
    resources.release(2.0, 4.0)
    assert resources._allocated_cpu == 0.0
    assert resources._allocated_mem == 0.0


def test_release_does_not_go_negative(resources):
    resources.release(10.0, 10.0)
    assert resources._allocated_cpu == 0.0
    assert resources._allocated_mem == 0.0


def test_can_dispatch_returns_bool(resources):
    result = resources.can_dispatch(cpu_cores=0.1, memory_gb=0.1)
    assert isinstance(result, bool)
