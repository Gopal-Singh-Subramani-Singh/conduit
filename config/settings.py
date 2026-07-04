from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


# ── Sub-models ─────────────────────────────────────────────────────────────────


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(default=8004, gt=0, lt=65536)
    log_level: str = "info"
    cors_origins: List[str] = Field(default_factory=list)
    api_key: Optional[str] = None

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"debug", "info", "warning", "error", "critical"}
        if v.lower() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.lower()

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        """Accept comma-separated string from env or list from YAML."""
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v or []


class RedisConfig(BaseModel):
    url: str = "redis://localhost:6379"
    stream_key: str = "conduit:tasks"
    dlq_key: str = "conduit:dlq"
    consumer_group: str = "conduit:workers"
    consumer_name: str = ""
    batch_size: int = Field(default=10, gt=0)
    block_ms: int = Field(default=1000, ge=0)
    visibility_timeout_seconds: int = Field(default=300, gt=0)
    max_connections: int = Field(default=20, gt=0)

    @model_validator(mode="after")
    def default_consumer_name(self) -> "RedisConfig":
        if not self.consumer_name:
            self.consumer_name = f"worker-{socket.gethostname()}"
        return self


class SQLiteConfig(BaseModel):
    db_path: str = "conduit.db"


class WorkerConfig(BaseModel):
    concurrency: int = Field(default=4, gt=0, le=64)
    task_timeout_seconds: int = Field(default=3600, gt=0)


class ResourceConfig(BaseModel):
    cpu_limit_pct: float = Field(default=80.0, gt=0, le=100)
    memory_limit_gb: float = Field(default=16.0, gt=0)
    check_enabled: bool = True

    @model_validator(mode="after")
    def auto_detect_memory(self) -> "ResourceConfig":
        """Scale memory limit down if host has less than the default."""
        if self.memory_limit_gb == 16.0:
            try:
                import psutil
                total_gb = psutil.virtual_memory().total / (1024**3)
                if total_gb < 16.0:
                    self.memory_limit_gb = round(total_gb * 0.9, 1)
            except Exception:
                pass
        return self


class RetryConfig(BaseModel):
    base_delay_seconds: float = Field(default=1.0, gt=0)
    max_delay_seconds: float = Field(default=300.0, gt=0)
    jitter: bool = True
    backoff_multiplier: float = Field(default=2.0, gt=1)


class WebhookConfig(BaseModel):
    enabled: bool = False
    url: Optional[str] = None
    timeout_seconds: int = Field(default=10, gt=0)
    secret: Optional[str] = None


class SchedulerConfig(BaseModel):
    timezone: str = "UTC"
    misfire_grace_seconds: int = Field(default=60, ge=0)


class ConduitConfig(BaseModel):
    model_config = {"extra": "ignore"}  # ignore unknown YAML keys (e.g. prometheus:)

    server: ServerConfig = Field(default_factory=ServerConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    sqlite: SQLiteConfig = Field(default_factory=SQLiteConfig)
    workers: WorkerConfig = Field(default_factory=WorkerConfig)
    resources: ResourceConfig = Field(default_factory=ResourceConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    webhooks: WebhookConfig = Field(default_factory=WebhookConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)


# ── Environment variable overlay ───────────────────────────────────────────────

def _apply_env_overrides(cfg: ConduitConfig) -> ConduitConfig:
    """
    Apply ``CONDUIT_<SECTION>__<KEY>`` environment variables on top of YAML.

    Example:
        CONDUIT_SERVER__PORT=9000 overrides cfg.server.port
        CONDUIT_REDIS__URL=redis://prod:6379 overrides cfg.redis.url
        CONDUIT_API_KEY=secret sets cfg.server.api_key
    """
    # Convenience: CONDUIT_API_KEY → server.api_key
    if val := os.environ.get("CONDUIT_API_KEY"):
        cfg.server.api_key = val

    # CONDUIT_CORS_ORIGINS → server.cors_origins
    if val := os.environ.get("CONDUIT_CORS_ORIGINS"):
        cfg.server.cors_origins = [
            o.strip() for o in val.split(",") if o.strip()
        ]

    section_map = {
        "SERVER": cfg.server,
        "REDIS": cfg.redis,
        "SQLITE": cfg.sqlite,
        "WORKERS": cfg.workers,
        "RESOURCES": cfg.resources,
        "RETRY": cfg.retry,
        "WEBHOOKS": cfg.webhooks,
        "SCHEDULER": cfg.scheduler,
    }
    for env_key, env_val in os.environ.items():
        if not env_key.startswith("CONDUIT_"):
            continue
        rest = env_key[len("CONDUIT_"):]
        if "__" not in rest:
            continue
        section, field = rest.split("__", 1)
        section = section.upper()
        field = field.lower()
        obj = section_map.get(section)
        if obj is None or not hasattr(obj, field):
            continue
        try:
            current = getattr(obj, field)
            if isinstance(current, bool):
                setattr(obj, field, env_val.lower() in ("1", "true", "yes"))
            elif isinstance(current, int):
                setattr(obj, field, int(env_val))
            elif isinstance(current, float):
                setattr(obj, field, float(env_val))
            elif isinstance(current, list):
                setattr(obj, field, [v.strip() for v in env_val.split(",") if v.strip()])
            else:
                setattr(obj, field, env_val)
        except (ValueError, TypeError):
            pass  # ignore malformed env values

    return cfg


# ── Loader ─────────────────────────────────────────────────────────────────────

_config: Optional[ConduitConfig] = None


def load_config(path: str = "config/config.yaml") -> ConduitConfig:
    """
    Load config with precedence: env vars > YAML file > defaults.
    """
    base: dict = {}
    p = Path(path)
    if p.exists():
        with open(p) as f:
            data = yaml.safe_load(f) or {}
        base = data

    cfg = ConduitConfig(**base)
    return _apply_env_overrides(cfg)


def get_config() -> ConduitConfig:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Force reload on next access. Used in tests."""
    global _config
    _config = None
