"""Integration: single open + close of a $5 AAPL position."""

from __future__ import annotations

from decimal import Decimal

import pytest

from etoro_bulk_trades import AsyncBulkTradesClient, CloseIntent, OpenIntent, TradeResult


@pytest.mark.integration
async def test_open_then_close_single_aapl_position(
    demo_client: AsyncBulkTradesClient,
) -> None:
    open_result = await demo_client.open_trade(OpenIntent(instrument="AAPL", amount=Decimal("5")))
    assert open_result.status in ("ok", "filled"), f"open status: {open_result.status}"
    assert open_result.order_id is not None

    verified = await demo_client.verify_orders(open_result, mode="pnl", timeout_s=30.0)
    assert isinstance(verified, TradeResult)
    assert verified.status in ("filled", "pending_market_open"), (
        f"verified status: {verified.status}, error: {verified.error}"
    )

    if verified.status == "filled" and verified.position_id is not None:
        close_result = await demo_client.close_trade(CloseIntent(position_id=verified.position_id))
        assert close_result.status in ("ok", "filled"), f"close status: {close_result.status}"
