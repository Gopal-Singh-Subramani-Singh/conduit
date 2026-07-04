from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any, Dict
import httpx
import structlog

from config.settings import get_config

logger = structlog.get_logger(__name__)


def _sign_payload(payload_bytes: bytes, secret: str) -> str:
    """Compute HMAC-SHA256 signature for webhook payload verification."""
    return hmac.new(
        secret.encode(),
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()


class WebhookSender:
    """
    Sends event notifications to a configured HTTP endpoint.

    When ``webhooks.secret`` is set, each request includes an
    ``X-Conduit-Signature`` header containing the HMAC-SHA256
    digest of the request body, allowing receivers to verify authenticity.
    """

    async def send(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> bool:
        cfg = get_config().webhooks
        if not cfg.enabled or not cfg.url:
            return False

        body: Dict[str, Any] = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        body_bytes = json.dumps(body, default=str).encode()

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if cfg.secret:
            sig = _sign_payload(body_bytes, cfg.secret)
            headers["X-Conduit-Signature"] = f"sha256={sig}"

        # Use explicit per-phase timeouts to avoid hanging on slow reads
        timeout = httpx.Timeout(
            connect=5.0,
            read=float(cfg.timeout_seconds),
            write=5.0,
            pool=5.0,
        )

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    cfg.url, content=body_bytes, headers=headers
                )
                resp.raise_for_status()
            logger.info("webhook.sent", event_type=event_type, status=resp.status_code)
            return True
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "webhook.http_error",
                event_type=event_type,
                status=exc.response.status_code,
                error=str(exc),
            )
            return False
        except Exception as exc:
            logger.warning("webhook.failed", event_type=event_type, error=str(exc))
            return False
