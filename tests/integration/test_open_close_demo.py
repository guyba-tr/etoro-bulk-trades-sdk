"""Integration: single open + close of a small BTC position.

BTC is used because it trades 24/7 on eToro — equity tickers (AAPL, MSFT, …)
return ``200 OK`` from the execute endpoint on weekends but the order does
**not** materialize as a position or as an entry in ``ordersForOpen[]``,
which would make verification flaky depending on the day of the week.

Safety discipline (added after a bug where the verifier mis-attributed a
pre-existing BTC position to a freshly-opened trade and the test then
closed the user's old position):

* Snapshot the account **before** opening.
* After verification, assert ``verified.position_id`` is not in the
  pre-trade BTC position-id set — i.e. the SDK correctly identified the
  position that the trade just opened.
* Only close that explicitly-new position.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from etoro_bulk_trades import AsyncBulkTradesClient, CloseIntent, OpenIntent, TradeResult


@pytest.mark.integration
async def test_open_then_close_single_btc_position(
    demo_client: AsyncBulkTradesClient,
) -> None:
    # Resolve BTC first so we know which instrument_id to filter on.
    refs = await demo_client.resolve(["BTC"])
    btc_id = int(refs["BTC"].instrument_id)

    pre_snap = await demo_client.get_account()
    pre_btc_pids = {
        int(p.position_id) for p in pre_snap.positions if int(p.instrument_id) == btc_id
    }

    open_result = await demo_client.open_trade(OpenIntent(instrument="BTC", amount=Decimal("25")))
    assert open_result.status in ("ok", "filled"), f"open status: {open_result.status}"
    assert open_result.order_id is not None

    verified = await demo_client.verify_orders(open_result, mode="pnl", timeout_s=30.0)
    assert isinstance(verified, TradeResult)
    assert verified.status in ("filled", "pending_market_open"), (
        f"verified status: {verified.status}, error: {verified.error}"
    )

    if verified.status == "filled":
        assert verified.position_id is not None, (
            "verifier returned 'filled' but did not attribute a position_id "
            f"(error: {verified.error})"
        )
        assert int(verified.position_id) not in pre_btc_pids, (
            "verifier returned a pre-existing position_id "
            f"({int(verified.position_id)} was already held before the trade)"
        )
        assert verified.instrument_id is not None
        close_result = await demo_client.close_trade(
            CloseIntent(
                position_id=verified.position_id,
                instrument_id=verified.instrument_id,
            )
        )
        assert close_result.status in ("ok", "filled"), f"close status: {close_result.status}"
