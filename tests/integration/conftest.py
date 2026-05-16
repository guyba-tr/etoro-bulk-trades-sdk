"""Integration-test fixtures gated by ``ETORO_DEMO_USER_KEY``.

These run against the **demo** environment and place small (~$25) crypto
trades that the suite then closes. Crypto is used because it trades 24/7;
equity tickers would make the verification step flaky on weekends.
Skipped automatically when the env var is unset. ``ETORO_DEMO_API_KEY``
is optional; if absent, the SDK's bundled default partner key is used.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from etoro_bulk_trades import AsyncBulkTradesClient


def _require_demo_credentials() -> tuple[str, str | None]:
    user_key = os.environ.get("ETORO_DEMO_USER_KEY")
    api_key = os.environ.get("ETORO_DEMO_API_KEY")
    if not user_key:
        pytest.skip("ETORO_DEMO_USER_KEY required")
    return user_key, api_key


@pytest.fixture
async def demo_client() -> AsyncIterator[AsyncBulkTradesClient]:
    user_key, api_key = _require_demo_credentials()
    async with AsyncBulkTradesClient.from_api_key(user_key, api_key=api_key) as client:
        await client.connect(env="demo")
        yield client
