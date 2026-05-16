"""Integration: bulk trade of 3 small crypto positions and a no-pending rebalance dry-run.

Crypto symbols are used (BTC/ETH/ADA) because they trade 24/7 on eToro; using
equity tickers would make these tests flaky on weekends — the execute endpoint
returns ``200 OK`` but the order never materializes for verification.
"""

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
async def test_bulk_three_small_crypto_positions(demo_client: AsyncBulkTradesClient) -> None:
    symbols = ("BTC", "ETH", "ADA")
    plan = BulkTradePlan(
        weights={
            "BTC": Decimal("0.34"),
            "ETH": Decimal("0.33"),
            "ADA": Decimal("0.33"),
        },
        total_amount=Decimal("75"),  # ~$25 each — above eToro's crypto min
    )
    result = await demo_client.execute_bulk_trade(
        plan,
        auto_verify=True,
        verify_mode="pnl",
        verify_timeout_s=30.0,
    )
    assert len(result.trades) == 3
    statuses = [tr.status for tr in result.trades]
    # 24/7 crypto markets — every trade must reach a terminal-ish state, not
    # silently disappear. ``not_landed`` would indicate a real bug.
    assert all(s in ("filled", "pending_market_open", "ok", "ambiguous") for s in statuses), (
        f"unexpected statuses: {statuses}"
    )

    # Clean up: resolve the symbols we opened, then close any matching positions.
    refs = await demo_client.resolve(list(symbols))
    target_ids = {int(refs[s].instrument_id) for s in symbols}
    snap = await demo_client.get_account()
    for pos in snap.positions:
        if int(pos.instrument_id) in target_ids:
            with contextlib.suppress(EtoroSDKError):
                await demo_client.close_trade(
                    CloseIntent(
                        position_id=pos.position_id,
                        instrument_id=pos.instrument_id,
                    )
                )


@pytest.mark.integration
async def test_rebalance_dry_run_with_no_pending(demo_client: AsyncBulkTradesClient) -> None:
    """A dry-run rebalance computes the diff without touching the account."""
    plan = RebalancePlan(
        target_weights={"BTC": Decimal("1.0")},
        total_amount=Decimal("25"),
    )
    try:
        result = await demo_client.rebalance(plan, dry_run=True)
    except PendingOrdersExistError:
        pytest.skip("Account has pending orders; rebalance dry-run also refuses.")
    assert result.env == "demo"
    assert result.phase_1_closes == ()
    assert result.phase_2_opens == ()
