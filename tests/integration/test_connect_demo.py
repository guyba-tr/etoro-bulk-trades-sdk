"""Integration: ``connect`` against the demo environment.

Verifies the credential probe + ``env`` enforcement matrix without placing
any trades. Skipped automatically when ``ETORO_DEMO_API_KEY`` is unset.
"""

from __future__ import annotations

import pytest

from etoro_bulk_trades import AsyncBulkTradesClient, EnvironmentMismatchError


@pytest.mark.integration
async def test_connect_without_env_returns_demo(demo_client: AsyncBulkTradesClient) -> None:
    info = demo_client.connection
    assert info.env == "demo"
    assert info.auth_mode == "api_key"


@pytest.mark.integration
async def test_connect_with_real_env_raises(demo_client: AsyncBulkTradesClient) -> None:
    """The client is already connected to demo; re-connecting as real raises."""
    with pytest.raises(EnvironmentMismatchError) as exc:
        await demo_client.connect(env="real")
    assert exc.value.requested == "real"
    assert exc.value.actual == "demo"


@pytest.mark.integration
async def test_get_account_returns_consistent_equity(demo_client: AsyncBulkTradesClient) -> None:
    snap = await demo_client.get_account()
    assert snap.env == "demo"
    # Equity formula = available_cash + total_invested + unrealized_pnl_total
    assert snap.equity == snap.available_cash + snap.total_invested + snap.unrealized_pnl_total
