from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Tuple
import redis.asyncio as aioredis
import structlog

from conduit_core.metrics import QUEUE_DEPTH
from config.settings import get_config

logger = structlog.get_logger(__name__)


class TaskQueue:
    """
    Redis Streams-backed task queue.

    Uses consumer groups for at-least-once delivery:
    - Producer: XADD message to stream
    - Consumer: XREADGROUP reads, processes, then XACK
    - If consumer crashes before XACK: message redelivered after timeout
    - XPENDING tracks unacked messages for monitoring

    Stream key:     conduit:tasks
    Consumer group: conduit:workers
    Consumer name:  worker-1 (configurable)
    """

    def __init__(self, redis_client: aioredis.Redis):
        self._redis = redis_client
        self._cfg = get_config().redis

    async def setup(self) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            await self._redis.xgroup_create(
                self._cfg.stream_key,
                self._cfg.consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "queue.consumer_group_created",
                group=self._cfg.consumer_group,
            )
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def enqueue(self, task_run_id: str, payload: Dict[str, Any]) -> str:
        """Add a task to the queue. Returns message ID."""
        msg = {
            "task_run_id": task_run_id,
            "payload": json.dumps(payload),
            "enqueued_at": time.time(),
        }
        msg_id = await self._redis.xadd(
            self._cfg.stream_key, msg, maxlen=10000, approximate=True
        )
        depth = await self._redis.xlen(self._cfg.stream_key)
        QUEUE_DEPTH.set(depth)
        logger.debug(
            "queue.enqueued",
            task_run_id=task_run_id,
            msg_id=msg_id,
        )
        return msg_id if isinstance(msg_id, str) else msg_id.decode()

    async def dequeue(
        self, count: int = 1
    ) -> List[Tuple[str, str, Dict]]:
        """
        Read messages from the consumer group.
        Returns list of (msg_id, task_run_id, payload).
        Blocks for block_ms if no messages available.
        """
        messages = await self._redis.xreadgroup(
            self._cfg.consumer_group,
            self._cfg.consumer_name,
            {self._cfg.stream_key: ">"},
            count=count,
            block=self._cfg.block_ms,
        )
        if not messages:
            return []

        result = []
        for _stream, msgs in messages:
            for msg_id, fields in msgs:
                msg_id_str = (
                    msg_id if isinstance(msg_id, str) else msg_id.decode()
                )
                task_run_id = fields.get("task_run_id", "")
                if isinstance(task_run_id, bytes):
                    task_run_id = task_run_id.decode()
                payload_raw = fields.get("payload", "{}")
                if isinstance(payload_raw, bytes):
                    payload_raw = payload_raw.decode()
                payload = json.loads(payload_raw)
                result.append((msg_id_str, task_run_id, payload))

        return result

    async def ack(self, msg_id: str) -> None:
        """Acknowledge successful processing. Removes from pending."""
        await self._redis.xack(
            self._cfg.stream_key,
            self._cfg.consumer_group,
            msg_id,
        )
        logger.debug("queue.acked", msg_id=msg_id)

    async def depth(self) -> int:
        return await self._redis.xlen(self._cfg.stream_key)

    async def pending_count(self) -> int:
        """Number of delivered but not yet acked messages."""
        info = await self._redis.xpending(
            self._cfg.stream_key, self._cfg.consumer_group
        )
        return info.get("pending", 0)

    async def reclaim_stale(self, min_idle_ms: int = 300000) -> List[str]:
        """
        Reclaim messages idle for > min_idle_ms (stuck/crashed workers).
        Returns list of reclaimed message IDs.
        """
        try:
            result = await self._redis.xautoclaim(
                self._cfg.stream_key,
                self._cfg.consumer_group,
                self._cfg.consumer_name,
                min_idle_ms,
                "0-0",
                count=10,
            )
            msgs = result[1] if result else []
            if msgs:
                logger.info(
                    "queue.reclaimed_stale",
                    count=len(msgs),
                )
            return [
                m[0] if isinstance(m[0], str) else m[0].decode()
                for m in msgs
            ]
        except Exception as exc:
            logger.warning("queue.reclaim_error", error=str(exc))
            return []

    async def clear(self) -> None:
        await self._redis.delete(self._cfg.stream_key)
