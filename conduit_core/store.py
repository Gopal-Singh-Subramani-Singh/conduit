from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
import structlog

from conduit_core.models import PipelineRun, TaskRun, TaskState, RunState

logger = structlog.get_logger(__name__)

# Allowed column names for dynamic UPDATE — prevents SQL injection if
# the set ever grows with externally derived keys.
_TASK_RUN_WRITABLE_COLS = frozenset(
    {
        "state",
        "error",
        "output_data",
        "attempt",
        "started_at",
        "finished_at",
    }
)

CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      TEXT PRIMARY KEY,
    dag_name    TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending',
    trigger     TEXT DEFAULT 'manual',
    input_data  TEXT DEFAULT '{}',
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    error       TEXT
)
"""

CREATE_TASK_RUNS = """
CREATE TABLE IF NOT EXISTS task_runs (
    task_run_id TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL,
    dag_name    TEXT NOT NULL,
    task_name   TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending',
    attempt     INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 0,
    input_data  TEXT DEFAULT '{}',
    output_data TEXT,
    error       TEXT,
    queued_at   TEXT,
    started_at  TEXT,
    finished_at TEXT
)
"""

CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_id TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    message     TEXT,
    timestamp   TEXT NOT NULL
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_task_runs_state   ON task_runs(state)",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag ON pipeline_runs(dag_name)",
    "CREATE INDEX IF NOT EXISTS idx_task_events_run   ON task_events(run_id)",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ExecutionStore:
    """
    SQLite-backed execution history store.

    Uses WAL journal mode for better concurrent read/write performance.
    All writes use parameterised queries. Column names in dynamic
    UPDATE clauses are validated against an explicit allowlist.
    """

    def __init__(self, db_path: str = "conduit.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(CREATE_RUNS)
            conn.execute(CREATE_TASK_RUNS)
            conn.execute(CREATE_EVENTS)
            for idx in CREATE_INDEXES:
                conn.execute(idx)

    # ── Pipeline runs ──────────────────────────────────────────────────────────

    def save_run(self, run: PipelineRun) -> None:
        try:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO pipeline_runs
                    (run_id, dag_name, state, trigger, input_data,
                     started_at, finished_at, error)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        run.run_id,
                        run.dag_name,
                        run.state.value,
                        run.trigger,
                        json.dumps(run.input_data),
                        run.started_at.isoformat(),
                        run.finished_at.isoformat() if run.finished_at else None,
                        run.error,
                    ),
                )
        except sqlite3.Error as exc:
            logger.error(
                "store.save_run_failed",
                run_id=run.run_id,
                error=str(exc),
            )
            raise

    def update_run_state(
        self,
        run_id: str,
        state: RunState,
        error: Optional[str] = None,
    ) -> None:
        finished = (
            _utcnow().isoformat()
            if state
            in (
                RunState.SUCCESS,
                RunState.FAILED,
                RunState.PARTIAL,
                RunState.CANCELLED,
            )
            else None
        )
        try:
            with self._conn() as conn:
                conn.execute(
                    """
                    UPDATE pipeline_runs
                    SET state=?, finished_at=?, error=?
                    WHERE run_id=?
                    """,
                    (state.value, finished, error, run_id),
                )
        except sqlite3.Error as exc:
            logger.error(
                "store.update_run_state_failed",
                run_id=run_id,
                state=state.value,
                error=str(exc),
            )
            raise

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_runs(
        self,
        dag_name: Optional[str] = None,
        limit: int = 50,
    ) -> List[dict]:
        limit = min(limit, 500)  # hard cap to prevent oversized responses
        if dag_name:
            query = (
                "SELECT * FROM pipeline_runs WHERE dag_name=? "
                "ORDER BY started_at DESC LIMIT ?"
            )
            params: list = [dag_name, limit]
        else:
            query = "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?"
            params = [limit]
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    # ── Task runs ──────────────────────────────────────────────────────────────

    def save_task_run(self, task_run: TaskRun) -> None:
        try:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO task_runs
                    (task_run_id, run_id, dag_name, task_name, state,
                     attempt, max_retries, input_data, output_data,
                     error, queued_at, started_at, finished_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        task_run.task_run_id,
                        task_run.run_id,
                        task_run.dag_name,
                        task_run.task_name,
                        task_run.state.value,
                        task_run.attempt,
                        task_run.max_retries,
                        json.dumps(task_run.input_data),
                        json.dumps(task_run.output_data),
                        task_run.error,
                        task_run.queued_at.isoformat() if task_run.queued_at else None,
                        task_run.started_at.isoformat() if task_run.started_at else None,
                        task_run.finished_at.isoformat() if task_run.finished_at else None,
                    ),
                )
        except sqlite3.Error as exc:
            logger.error(
                "store.save_task_run_failed",
                task_run_id=task_run.task_run_id,
                error=str(exc),
            )
            raise

    def update_task_state(
        self,
        task_run_id: str,
        state: TaskState,
        output_data=None,
        error: Optional[str] = None,
        attempt: Optional[int] = None,
    ) -> None:
        fields: dict = {"state": state.value, "error": error}
        if output_data is not None:
            fields["output_data"] = json.dumps(output_data)
        if attempt is not None:
            fields["attempt"] = attempt
        if state == TaskState.RUNNING:
            fields["started_at"] = _utcnow().isoformat()
        elif state in (
            TaskState.SUCCESS,
            TaskState.FAILED,
            TaskState.DLQ,
            TaskState.SKIPPED,
        ):
            fields["finished_at"] = _utcnow().isoformat()

        # Validate all column names against the allowlist
        invalid = set(fields.keys()) - _TASK_RUN_WRITABLE_COLS
        if invalid:
            raise ValueError(f"update_task_state: unknown columns {invalid}")

        set_clause = ", ".join(f"{k}=?" for k in fields)
        values = list(fields.values()) + [task_run_id]
        try:
            with self._conn() as conn:
                conn.execute(
                    f"UPDATE task_runs SET {set_clause} WHERE task_run_id=?",
                    values,
                )
        except sqlite3.Error as exc:
            logger.error(
                "store.update_task_state_failed",
                task_run_id=task_run_id,
                state=state.value,
                error=str(exc),
            )
            raise

    def get_task_runs_for_run(self, run_id: str) -> List[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? ORDER BY queued_at",
                (run_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def log_event(
        self,
        task_run_id: str,
        run_id: str,
        event_type: str,
        message: str = "",
    ) -> None:
        try:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO task_events
                    (task_run_id, run_id, event_type, message, timestamp)
                    VALUES (?,?,?,?,?)
                    """,
                    (
                        task_run_id,
                        run_id,
                        event_type,
                        message,
                        _utcnow().isoformat(),
                    ),
                )
        except sqlite3.Error as exc:
            # Events are best-effort — log and continue, don't fail the task
            logger.warning(
                "store.log_event_failed",
                task_run_id=task_run_id,
                event_type=event_type,
                error=str(exc),
            )

    def get_events(self, run_id: str, limit: int = 100) -> List[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM task_events WHERE run_id=?
                ORDER BY timestamp DESC LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def health_check(self) -> bool:
        """Return True if the DB is reachable and the schema is present."""
        try:
            with self._conn() as conn:
                conn.execute("SELECT 1 FROM pipeline_runs LIMIT 1")
            return True
        except sqlite3.Error:
            return False
