from __future__ import annotations

import asyncio
from typing import Tuple
import psutil
import structlog

from conduit_core.metrics import RESOURCE_UTILISATION
from config.settings import get_config

logger = structlog.get_logger(__name__)


def _sample_cpu_and_memory() -> Tuple[float, float]:
    """
    Synchronous psutil probe — must be called in a thread pool executor
    to avoid blocking the asyncio event loop (cpu_percent blocks ~100 ms).
    """
    cpu_pct = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory()
    mem_used_gb = mem.used / (1024**3)
    RESOURCE_UTILISATION.labels(resource="cpu").set(cpu_pct / 100.0)
    RESOURCE_UTILISATION.labels(resource="memory").set(
        mem_used_gb / max(mem.total / (1024**3), 1)
    )
    return cpu_pct, mem_used_gb


class ResourceQuotaManager:
    """
    Prevents OOM failures by checking available CPU and memory
    before dispatching each task.

    All public methods are synchronous because they are called from
    inside an asyncio coroutine *after* acquiring the worker semaphore.
    The expensive psutil CPU-sampling call is offloaded to the thread
    pool to avoid blocking the event loop.

    The check + reserve sequence is safe from TOCTOU races because it
    is always called while holding the WorkerPool semaphore, which
    limits concurrent entries.
    """

    def __init__(self) -> None:
        self._cfg = get_config().resources
        self._allocated_cpu: float = 0.0
        self._allocated_mem: float = 0.0

    async def current_usage_async(self) -> Tuple[float, float]:
        """Non-blocking variant — offloads psutil to thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _sample_cpu_and_memory)

    def current_usage(self) -> Tuple[float, float]:
        """Synchronous variant for use outside async context (e.g. tests)."""
        return _sample_cpu_and_memory()

    async def can_dispatch_async(self, cpu_cores: float, memory_gb: float) -> bool:
        """Async version — use this in production code paths."""
        if not self._cfg.check_enabled:
            return True
        cpu_pct, mem_used_gb = await self.current_usage_async()
        return self._check_capacity(cpu_pct, mem_used_gb, cpu_cores, memory_gb)

    def can_dispatch(self, cpu_cores: float, memory_gb: float) -> bool:
        """Sync version — used in tests and fallback contexts."""
        if not self._cfg.check_enabled:
            return True
        cpu_pct, mem_used_gb = _sample_cpu_and_memory()
        return self._check_capacity(cpu_pct, mem_used_gb, cpu_cores, memory_gb)

    def _check_capacity(
        self,
        cpu_pct: float,
        mem_used_gb: float,
        cpu_cores: float,
        memory_gb: float,
    ) -> bool:
        mem_total_gb = psutil.virtual_memory().total / (1024**3)
        cpu_count = psutil.cpu_count(logical=True) or 1

        cpu_headroom = self._cfg.cpu_limit_pct - cpu_pct
        cpu_needed_pct = (cpu_cores / cpu_count) * 100
        if cpu_needed_pct > cpu_headroom:
            logger.debug(
                "resources.cpu_constrained",
                needed_pct=round(cpu_needed_pct, 1),
                headroom=round(cpu_headroom, 1),
            )
            return False

        mem_limit_gb = min(self._cfg.memory_limit_gb, mem_total_gb)
        mem_headroom_gb = mem_limit_gb - mem_used_gb
        if memory_gb > mem_headroom_gb:
            logger.debug(
                "resources.memory_constrained",
                needed_gb=memory_gb,
                headroom_gb=round(mem_headroom_gb, 2),
            )
            return False

        return True

    def reserve(self, cpu_cores: float, memory_gb: float) -> None:
        """Track allocated resources (logical accounting)."""
        self._allocated_cpu += cpu_cores
        self._allocated_mem += memory_gb

    def release(self, cpu_cores: float, memory_gb: float) -> None:
        """Release allocated resources."""
        self._allocated_cpu = max(0.0, self._allocated_cpu - cpu_cores)
        self._allocated_mem = max(0.0, self._allocated_mem - memory_gb)
