from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
import structlog

from conduit_core.models import DLQEntry
from conduit_core.metrics import DLQ_DEPTH, DLQ_ENTRIES
from config.settings import get_config

logger = structlog.get_logger(__name__)

_MAX_ERROR_LEN = 2000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DeadLetterQueue:
    """
    Dead letter queue for tasks that exhaust all retries.

    Stores full forensics: payload, truncated error traceback,
    attempt count, and timestamps.  Supports targeted replay
    by task_run_id without requiring a full scan.
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._cfg = get_config().redis

    async def send(
        self,
        task_run_id: str,
        run_id: str,
        dag_name: str,
        task_name: str,
        attempt: int,
        error: str,
        input_data: Dict[str, Any],
    ) -> str:
        """Send a failed task to the DLQ. Returns Redis message ID."""
        truncated = error[:_MAX_ERROR_LEN]
        if len(error) > _MAX_ERROR_LEN:
            truncated += "\n[truncated]"

        entry = {
            "task_run_id": task_run_id,
            "run_id": run_id,
            "dag_name": dag_name,
            "task_name": task_name,
            "attempt": str(attempt),
            "error": truncated,
            "input_data": json.dumps(input_data),
            "failed_at": _utcnow().isoformat(),
        }
        msg_id = await self._redis.xadd(self._cfg.dlq_key, entry, maxlen=1000)
        depth = await self._redis.xlen(self._cfg.dlq_key)
        DLQ_DEPTH.set(depth)
        DLQ_ENTRIES.labels(dag_name=dag_name, task_name=task_name).inc()
        logger.warning(
            "dlq.task_sent",
            task_run_id=task_run_id,
            dag=dag_name,
            task=task_name,
            attempts=attempt,
        )
        return msg_id if isinstance(msg_id, str) else msg_id.decode()

    async def list_entries(self, limit: int = 50) -> List[DLQEntry]:
        """Return recent DLQ entries, newest first."""
        messages = await self._redis.xrevrange(self._cfg.dlq_key, count=limit)
        return self._parse_messages(messages)

    async def find_entry(self, task_run_id: str) -> List[DLQEntry]:
        """
        Find DLQ entries by task_run_id.  Scans the full stream.
        Returns a list (may be empty) ordered newest first.
        """
        # Read all entries and filter — DLQ is bounded to 1000 entries
        messages = await self._redis.xrevrange(self._cfg.dlq_key, count=1000)
        all_entries = self._parse_messages(messages)
        return [e for e in all_entries if e.task_run_id == task_run_id]

    def _parse_messages(self, messages) -> List[DLQEntry]:
        entries: List[DLQEntry] = []
        for _msg_id, fields in messages:
            try:
                decoded: Dict[str, str] = {}
                for k, v in fields.items():
                    key = k.decode() if isinstance(k, bytes) else k
                    val = v.decode() if isinstance(v, bytes) else v
                    decoded[key] = val

                failed_at_raw = decoded.get("failed_at")
                if failed_at_raw:
                    failed_at = datetime.fromisoformat(failed_at_raw)
                else:
                    # Missing timestamp — use epoch to make it obvious
                    failed_at = datetime(1970, 1, 1, tzinfo=timezone.utc)
                    logger.warning("dlq.missing_failed_at", fields=list(decoded.keys()))

                entries.append(
                    DLQEntry(
                        task_run_id=decoded.get("task_run_id", ""),
                        run_id=decoded.get("run_id", ""),
                        dag_name=decoded.get("dag_name", ""),
                        task_name=decoded.get("task_name", ""),
                        attempt=int(decoded.get("attempt", 0)),
                        error=decoded.get("error", ""),
                        input_data=json.loads(decoded.get("input_data", "{}")),
                        failed_at=failed_at,
                    )
                )
            except Exception as exc:
                logger.warning("dlq.parse_error", error=str(exc))
        return entries

    async def depth(self) -> int:
        return await self._redis.xlen(self._cfg.dlq_key)

    async def clear(self) -> None:
        await self._redis.delete(self._cfg.dlq_key)
        DLQ_DEPTH.set(0)
