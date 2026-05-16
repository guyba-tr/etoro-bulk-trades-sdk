"""Test the API-key default + user-key requirement behaviour."""

from __future__ import annotations

import pytest

from etoro_bulk_trades import AsyncBulkTradesClient, BulkTradesClient
from etoro_bulk_trades._auth import DEFAULT_API_KEY, ApiKeyAuth


def test_api_key_defaults_to_shared_partner_key() -> None:
    """``ApiKeyAuth(user_key=...)`` alone uses the bundled partner key."""
    auth = ApiKeyAuth(user_key="user-123")
    assert auth.api_key == DEFAULT_API_KEY
    headers = auth.headers()
    assert headers["x-user-key"] == "user-123"
    assert headers["x-api-key"] == DEFAULT_API_KEY


def test_api_key_override_takes_precedence() -> None:
    auth = ApiKeyAuth(user_key="user-123", api_key="my-partner-key")
    assert auth.api_key == "my-partner-key"
    assert auth.headers()["x-api-key"] == "my-partner-key"


def test_default_api_key_value_is_pinned() -> None:
    """Pin the shipped default so a refactor can't silently rotate it."""
    assert DEFAULT_API_KEY == "sdgdskldFPLGfjHn1421dgnlxdGTbngdflg6290bRjslfihsjhSDsdgGHH25hjf"


def test_async_from_api_key_accepts_user_key_only() -> None:
    """Async client constructor works with just ``user_key``."""
    client = AsyncBulkTradesClient.from_api_key("user-only")
    auth_ctx = client._auth.ctx
    assert isinstance(auth_ctx, ApiKeyAuth)
    assert auth_ctx.user_key == "user-only"
    assert auth_ctx.api_key == DEFAULT_API_KEY


def test_async_from_api_key_rejects_empty_user_key() -> None:
    with pytest.raises(ValueError, match="user_key is required"):
        AsyncBulkTradesClient.from_api_key("")


def test_async_from_api_key_uses_explicit_api_key() -> None:
    client = AsyncBulkTradesClient.from_api_key("u", api_key="partner-xyz")
    auth_ctx = client._auth.ctx
    assert isinstance(auth_ctx, ApiKeyAuth)
    assert auth_ctx.api_key == "partner-xyz"


def test_sync_from_api_key_accepts_user_key_only() -> None:
    """Sync facade mirrors the async signature."""
    client = BulkTradesClient.from_api_key("user-only")
    try:
        inner_ctx = client._inner._auth.ctx
        assert isinstance(inner_ctx, ApiKeyAuth)
        assert inner_ctx.api_key == DEFAULT_API_KEY
        assert inner_ctx.user_key == "user-only"
    finally:
        client.close()
