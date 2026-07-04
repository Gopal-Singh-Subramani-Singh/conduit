"""Tests for WebhookSender — disabled, send, HMAC signature, error handling."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

from conduit_core.webhook import WebhookSender, _sign_payload


def _mock_cfg(enabled=True, url="http://example.com/hook", secret=None, timeout=5):
    return MagicMock(
        webhooks=MagicMock(
            enabled=enabled,
            url=url,
            secret=secret,
            timeout_seconds=timeout,
        )
    )


@pytest.mark.asyncio
async def test_webhook_disabled_returns_false():
    sender = WebhookSender()
    with patch("conduit_core.webhook.get_config") as mock_cfg:
        mock_cfg.return_value = _mock_cfg(enabled=False)
        result = await sender.send("run_success", {"run_id": "abc"})
    assert result is False


@pytest.mark.asyncio
async def test_webhook_no_url_returns_false():
    sender = WebhookSender()
    with patch("conduit_core.webhook.get_config") as mock_cfg:
        mock_cfg.return_value = _mock_cfg(enabled=True, url=None)
        result = await sender.send("run_success", {"run_id": "abc"})
    assert result is False


@pytest.mark.asyncio
async def test_webhook_sends_on_success():
    sender = WebhookSender()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("conduit_core.webhook.get_config") as mock_cfg, \
         patch("httpx.AsyncClient") as mock_client_cls:
        mock_cfg.return_value = _mock_cfg()
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        result = await sender.send("run_success", {"run_id": "abc"})

    assert result is True
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_webhook_http_error_returns_false():
    sender = WebhookSender()

    with patch("conduit_core.webhook.get_config") as mock_cfg, \
         patch("httpx.AsyncClient") as mock_client_cls:
        mock_cfg.return_value = _mock_cfg()
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "server error", request=MagicMock(), response=mock_response
            )
        )
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        result = await sender.send("run_failed", {"run_id": "abc"})

    assert result is False


def test_sign_payload_produces_hex_digest():
    sig = _sign_payload(b'{"event": "test"}', "mysecret")
    assert isinstance(sig, str)
    assert len(sig) == 64  # SHA-256 hex digest


def test_sign_payload_deterministic():
    payload = b'{"event": "test"}'
    assert _sign_payload(payload, "secret") == _sign_payload(payload, "secret")


def test_sign_payload_different_secret_different_sig():
    payload = b'{"event": "test"}'
    assert _sign_payload(payload, "secret1") != _sign_payload(payload, "secret2")
