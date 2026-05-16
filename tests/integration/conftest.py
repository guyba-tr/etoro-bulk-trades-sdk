"""Integration-test fixtures gated by ``ETORO_DEMO_API_KEY`` /
``ETORO_DEMO_USER_KEY`` environment variables.

These run against the **demo** environment and place small ($5) trades that
the suite then closes. Skipped automatically when the env vars are unset.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from etoro_bulk_trades import AsyncBulkTradesClient


def _require_demo_credentials() -> tuple[str, str]:
    api_key = os.environ.get("ETORO_DEMO_API_KEY")
    user_key = os.environ.get("ETORO_DEMO_USER_KEY")
    if not api_key or not user_key:
        pytest.skip("ETORO_DEMO_API_KEY and ETORO_DEMO_USER_KEY required")
    return api_key, user_key


@pytest.fixture
async def demo_client() -> AsyncIterator[AsyncBulkTradesClient]:
    api_key, user_key = _require_demo_credentials()
    async with AsyncBulkTradesClient.from_api_key(api_key, user_key) as client:
        await client.connect(env="demo")
        yield client
