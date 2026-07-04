from __future__ import annotations
import asyncio
import random
from typing import Optional
import structlog

from config.settings import get_config

logger = structlog.get_logger(__name__)


def compute_delay(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    multiplier: float = 2.0,
    jitter: bool = True,
) -> float:
    """
    Exponential backoff with optional jitter.

    delay = min(base * multiplier^attempt, max_delay)
    With jitter: delay = delay * random.uniform(0.5, 1.5)

    Jitter prevents thundering herd — if 100 workers all fail
    simultaneously and retry at the exact same time, they'll
    overwhelm the system again. Jitter spreads retries out.

    Attempt 0: ~1s
    Attempt 1: ~2s
    Attempt 2: ~4s
    Attempt 3: ~8s
    Attempt 4: ~16s
    Attempt 5: ~32s
    ...capped at max_delay
    """
    delay = min(base_delay * (multiplier ** attempt), max_delay)
    if jitter:
        delay = delay * random.uniform(0.5, 1.5)
    return delay


async def retry_with_backoff(
    coro_func,
    max_retries: int,
    task_name: str = "unknown",
    *args,
    **kwargs,
):
    """
    Execute an async coroutine with retry + exponential backoff.
    Returns the result on success.
    Raises the last exception after max_retries exhausted.
    """
    cfg = get_config().retry
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            result = await coro_func(*args, **kwargs)
            if attempt > 0:
                logger.info(
                    "retry.succeeded",
                    task=task_name,
                    attempt=attempt,
                )
            return result
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = compute_delay(
                    attempt=attempt,
                    base_delay=cfg.base_delay_seconds,
                    max_delay=cfg.max_delay_seconds,
                    multiplier=cfg.backoff_multiplier,
                    jitter=cfg.jitter,
                )
                logger.warning(
                    "retry.attempt_failed",
                    task=task_name,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    delay_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "retry.exhausted",
                    task=task_name,
                    attempts=max_retries + 1,
                    error=str(exc),
                )

    raise last_exc
