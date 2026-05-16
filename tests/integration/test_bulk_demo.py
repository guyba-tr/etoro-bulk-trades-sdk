"""Integration: bulk trade of 3 small positions and a no-pending rebalance dry-run."""

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from etoro_bulk_trades import (
    AsyncBulkTradesClient,
    BulkTradePlan,
    CloseIntent,
    EtoroSDKError,
    PendingOrdersExistError,
    RebalancePlan,
)


@pytest.mark.integration
async def test_bulk_three_small_positions(demo_client: AsyncBulkTradesClient) -> None:
    plan = BulkTradePlan(
        weights={
            "AAPL": Decimal("0.34"),
            "MSFT": Decimal("0.33"),
            "GOOG": Decimal("0.33"),
        },
        total_amount=Decimal("15"),  # ~$5 each
    )
    result = await demo_client.execute_bulk_trade(
        plan,
        auto_verify=True,
        verify_mode="pnl",
        verify_timeout_s=30.0,
    )
    assert len(result.trades) == 3
    statuses = [tr.status for tr in result.trades]
    assert all(
        s in ("filled", "pending_market_open", "ok", "ambiguous", "not_landed") for s in statuses
    )

    # Clean up: close any positions opened by this test
    snap = await demo_client.get_account()
    targets = {1001, 1002, 1003}  # AAPL, MSFT, GOOG approximate IDs
    for pos in snap.positions:
        if int(pos.instrument_id) in targets:
            with contextlib.suppress(EtoroSDKError):
                await demo_client.close_trade(CloseIntent(position_id=pos.position_id))


@pytest.mark.integration
async def test_rebalance_dry_run_with_no_pending(demo_client: AsyncBulkTradesClient) -> None:
    """A dry-run rebalance computes the diff without touching the account."""
    plan = RebalancePlan(
        target_weights={"AAPL": Decimal("1.0")},
        total_amount=Decimal("10"),
    )
    try:
        result = await demo_client.rebalance(plan, dry_run=True)
    except PendingOrdersExistError:
        pytest.skip("Account has pending orders; rebalance dry-run also refuses.")
    assert result.env == "demo"
    assert result.phase_1_closes == ()
    assert result.phase_2_opens == ()
