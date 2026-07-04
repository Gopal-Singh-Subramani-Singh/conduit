from __future__ import annotations

from typing import Dict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from prometheus_client import Counter
import structlog

from config.settings import get_config

logger = structlog.get_logger(__name__)

CRON_TRIGGERS_TOTAL = Counter(
    "conduit_cron_triggers_total",
    "Total scheduled DAG trigger attempts",
    ["dag_name", "status"],
)


class CronScheduler:
    """
    APScheduler-based cron trigger manager for DAGs.

    Timezone is read from ``config.scheduler.timezone`` (default: UTC).
    Misfire grace time prevents catch-up bursts after a server restart.
    """

    def __init__(self, engine) -> None:
        self._engine = engine
        cfg = get_config().scheduler
        self._scheduler = AsyncIOScheduler(timezone=cfg.timezone)
        self._misfire_grace = cfg.misfire_grace_seconds
        self._jobs: Dict[str, str] = {}  # dag_name -> job_id

    async def start(self) -> None:
        self._scheduler.start()
        cfg = get_config().scheduler
        logger.info("cron_scheduler.started", timezone=cfg.timezone)

    async def stop(self) -> None:
        self._scheduler.shutdown(wait=False)

    def register_dag(self, dag_name: str, cron_expression: str) -> str:
        """
        Register or replace a cron trigger for a DAG.

        ``cron_expression`` must be standard 5-part: min hour day month weekday.
        """
        if dag_name in self._jobs:
            try:
                self._scheduler.remove_job(self._jobs[dag_name])
            except Exception:
                pass

        parts = cron_expression.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: '{cron_expression}'. "
                "Expected: min hour day month weekday"
            )
        minute, hour, day, month, day_of_week = parts

        job = self._scheduler.add_job(
            self._trigger_dag,
            trigger="cron",
            args=[dag_name],
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            id=f"dag_{dag_name}",
            replace_existing=True,
            misfire_grace_time=self._misfire_grace,
            coalesce=True,   # don't fire multiple times for missed runs
            max_instances=1,  # don't overlap runs for the same DAG
        )
        self._jobs[dag_name] = job.id
        logger.info(
            "cron_scheduler.dag_registered",
            dag=dag_name,
            cron=cron_expression,
            misfire_grace_s=self._misfire_grace,
        )
        return job.id

    async def _trigger_dag(self, dag_name: str) -> None:
        try:
            run_id = await self._engine.trigger(dag_name, trigger="cron")
            CRON_TRIGGERS_TOTAL.labels(dag_name=dag_name, status="success").inc()
            logger.info("cron_scheduler.triggered", dag=dag_name, run_id=run_id)
        except Exception as exc:
            CRON_TRIGGERS_TOTAL.labels(dag_name=dag_name, status="failed").inc()
            logger.error(
                "cron_scheduler.trigger_failed",
                dag=dag_name,
                error=str(exc),
                exc_info=True,
            )

    def remove_dag(self, dag_name: str) -> bool:
        if dag_name not in self._jobs:
            return False
        try:
            self._scheduler.remove_job(self._jobs.pop(dag_name))
        except Exception:
            self._jobs.pop(dag_name, None)
        return True
