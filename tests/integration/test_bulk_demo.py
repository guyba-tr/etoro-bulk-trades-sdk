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

    # Snapshot which crypto positions the account already holds so the
    # cleanup loop only closes positions THIS test opened. Closing
    # pre-existing positions is the bug class fixed in commit history
    # (was: cleanup matched by instrument_id without a pre/post diff).
    refs = await demo_client.resolve(list(symbols))
    target_ids = {int(refs[s].instrument_id) for s in symbols}
    pre_snap = await demo_client.get_account()
    pre_pids_by_iid: dict[int, set[int]] = {iid: set() for iid in target_ids}
    for pos in pre_snap.positions:
        iid = int(pos.instrument_id)
        if iid in target_ids:
            pre_pids_by_iid[iid].add(int(pos.position_id))

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

    # Every filled trade must have an attributed position_id that is NOT in
    # the pre-trade set for its instrument.
    for tr in result.trades:
        if tr.status == "filled":
            assert tr.position_id is not None, (
                f"filled trade missing position_id (error: {tr.error})"
            )
            assert tr.instrument_id is not None
            iid = int(tr.instrument_id)
            assert int(tr.position_id) not in pre_pids_by_iid.get(iid, set()), (
                f"verifier returned a pre-existing position_id for instrument {iid}"
            )

    # Clean up: close ONLY positions that did not exist before the test.
    post_snap = await demo_client.get_account()
    for pos in post_snap.positions:
        iid = int(pos.instrument_id)
        if iid in target_ids and int(pos.position_id) not in pre_pids_by_iid.get(iid, set()):
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
