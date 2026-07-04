from __future__ import annotations
import pytest
from conduit_core.retry import compute_delay, retry_with_backoff
from unittest.mock import patch, MagicMock


def test_delay_increases_with_attempts():
    delays = [
        compute_delay(attempt=i, jitter=False) for i in range(5)
    ]
    for i in range(1, len(delays)):
        assert delays[i] >= delays[i - 1]


def test_delay_capped_at_max():
    delay = compute_delay(attempt=100, max_delay=10.0, jitter=False)
    assert delay <= 10.0


def test_delay_with_jitter_in_range():
    for _ in range(20):
        delay = compute_delay(attempt=2, base_delay=4.0, jitter=True)
        # base=4, multiplier=2.0 (default), attempt=2 => 4 * 4 = 16
        # with jitter: 16 * [0.5, 1.5] => [8.0, 24.0]
        assert 2.0 <= delay <= 30.0


def test_delay_without_jitter_deterministic():
    d1 = compute_delay(attempt=3, jitter=False)
    d2 = compute_delay(attempt=3, jitter=False)
    assert d1 == d2


@pytest.mark.asyncio
async def test_retry_succeeds_on_first_attempt():
    call_count = 0

    async def succeed():
        nonlocal call_count
        call_count += 1
        return "ok"

    with patch("conduit_core.retry.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            retry=MagicMock(
                base_delay_seconds=0.01,
                max_delay_seconds=0.1,
                backoff_multiplier=2.0,
                jitter=False,
            )
        )
        result = await retry_with_backoff(succeed, max_retries=3)
    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_exhausted_raises():
    async def always_fail():
        raise ValueError("always fails")

    with patch("conduit_core.retry.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            retry=MagicMock(
                base_delay_seconds=0.001,
                max_delay_seconds=0.01,
                backoff_multiplier=2.0,
                jitter=False,
            )
        )
        with pytest.raises(ValueError, match="always fails"):
            await retry_with_backoff(always_fail, max_retries=2)
