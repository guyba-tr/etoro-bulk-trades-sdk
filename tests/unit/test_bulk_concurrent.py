"""Bulk trade & rebalance must fire per-trade POSTs concurrently while
respecting the eToro at-most-once rule for trade-execution endpoints.

The at-most-once decision table (from the ``etoro-api-conventions`` rule,
§ "Trade-execution endpoints have NO idempotency key") forbids:

* re-firing a trade after an ambiguous outcome (timeout, parse error, …),
* cancelling an in-flight POST when a sibling task fails — cancellation
  mid-send may leave the trade *executed but unobserved*, which is the
  same ambiguous class. The SDK therefore never lets one trade's failure
  cancel its siblings; per-task failures are folded into a typed
  :class:`TradeResult.status` instead.

These tests assert:

1. Three concurrent opens actually overlap in flight (peak > 1).
2. A 401 on one open does NOT cancel sibling opens; siblings complete
   normally with their natural statuses, and the 401-victim is marked
   ``failed`` with an explanatory error.
3. The bulk path no longer enforces a sequential cumulative-spend check
   that would refuse plans the planner already validated.
"""

from __future__ import annotations

import asyncio as _asyncio_module
import json
from decimal import Decimal
from typing import Any

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._execute import execute_bulk_trade
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._instrument_resolution import InstrumentCache
from etoro_bulk_trades.types import (
    BulkTradePlan,
    InstrumentID,
    InstrumentRef,
)

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"

# Captured BEFORE the autouse ``_no_real_sleep`` fixture replaces
# ``asyncio.sleep`` so the in-flight overlap test can use a real yield.
_REAL_ASYNC_SLEEP = _asyncio_module.sleep


def _make_http() -> HttpClient:
    handle = AuthHandle(ApiKeyAuth(api_key="k", user_key="u"))
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    return http


def _seed(cache: InstrumentCache, symbol: str, iid: int) -> None:
    cache.put(
        InstrumentRef(
            instrument_id=InstrumentID(iid),
            symbol=symbol,
            display_name=symbol,
        )
    )


def _empty_portfolio() -> dict[str, object]:
    return {
        "clientPortfolio": {
            "credit": 100_000.0,
            "positions": [],
            "orders": [],
            "ordersForOpen": [],
            "mirrors": [],
        }
    }


@pytest.mark.asyncio
async def test_bulk_opens_run_concurrently() -> None:
    """All POSTs must overlap in flight — sequential execution would be
    a regression. The slow handler waits a real ~10ms tick to allow
    sibling tasks scheduled in the same gather() to interleave.
    """
    cache = InstrumentCache()
    _seed(cache, "BTC", 100000)
    _seed(cache, "ETH", 100001)
    _seed(cache, "ADA", 100017)
    http = _make_http()

    in_flight = [0]
    peak_in_flight = [0]
    order_counter = [1000]

    async def post_handler(request: Any) -> Response:
        in_flight[0] += 1
        peak_in_flight[0] = max(peak_in_flight[0], in_flight[0])
        await _REAL_ASYNC_SLEEP(0.01)
        order_counter[0] += 1
        in_flight[0] -= 1
        return Response(
            200,
            json={
                "orderForOpen": {
                    "orderID": order_counter[0],
                    "amount": 25.0,
                    "statusID": 1,
                }
            },
        )

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
            return_value=Response(200, json=_empty_portfolio())
        )
        router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
            side_effect=post_handler
        )

        plan = BulkTradePlan(
            weights={
                "BTC": Decimal("0.34"),
                "ETH": Decimal("0.33"),
                "ADA": Decimal("0.33"),
            },
            total_amount=Decimal("75"),
        )
        result = await execute_bulk_trade(http, env="demo", plan=plan, cache=cache)

    assert len(result.trades) == 3
    assert all(tr.status == "ok" for tr in result.trades)
    assert peak_in_flight[0] >= 2, (
        f"expected concurrent execution POSTs, got peak in-flight={peak_in_flight[0]}"
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_bulk_401_on_one_does_not_cancel_siblings() -> None:
    """At-most-once forbids cancelling in-flight POSTs (they may have
    already reached the server). When BTC's POST returns 401, ETH and
    ADA must still complete with their natural ``ok`` status; the
    401-victim is marked ``failed`` with a clear error.
    """
    cache = InstrumentCache()
    _seed(cache, "BTC", 100000)
    _seed(cache, "ETH", 100001)
    _seed(cache, "ADA", 100017)
    http = _make_http()

    counter = [2000]

    def handler(request: Any) -> Response:
        payload = json.loads(request.content)
        if payload["InstrumentID"] == 100000:
            return Response(401, json={"error": "auth failed"})
        counter[0] += 1
        return Response(
            200,
            json={
                "orderForOpen": {
                    "orderID": counter[0],
                    "amount": 25.0,
                    "statusID": 1,
                }
            },
        )

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
            return_value=Response(200, json=_empty_portfolio())
        )
        router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
            side_effect=handler
        )

        plan = BulkTradePlan(
            weights={
                "BTC": Decimal("0.34"),
                "ETH": Decimal("0.33"),
                "ADA": Decimal("0.33"),
            },
            total_amount=Decimal("75"),
        )
        result = await execute_bulk_trade(http, env="demo", plan=plan, cache=cache)

    by_symbol = {tr.intent.instrument: tr for tr in result.trades}  # type: ignore[union-attr]
    assert by_symbol["BTC"].status == "failed"
    assert by_symbol["BTC"].error is not None
    assert "401" in by_symbol["BTC"].error
    # Siblings completed unscathed — this is the at-most-once guarantee.
    assert by_symbol["ETH"].status == "ok"
    assert by_symbol["ADA"].status == "ok"
    await http.aclose()


@pytest.mark.asyncio
async def test_bulk_per_trade_classifier_branches_independently() -> None:
    """The at-most-once decision table maps each error class onto a
    distinct status. Run a single bulk that triggers all four
    non-success branches in parallel:

    * BTC → 4xx → ``failed``
    * ETH → 429 (exhausts retries) → ``rate_limited_giveup``
    * ADA → connection drop → ``ambiguous``
    * XRP → 2xx → ``ok``

    Every status must land exactly where the table predicts; no branch
    poisons the others.
    """
    from httpx import ConnectError

    cache = InstrumentCache()
    _seed(cache, "BTC", 100000)
    _seed(cache, "ETH", 100001)
    _seed(cache, "ADA", 100017)
    _seed(cache, "XRP", 100018)
    http = _make_http()

    counter = [3000]

    def handler(request: Any) -> Response:
        iid = json.loads(request.content)["InstrumentID"]
        if iid == 100000:
            return Response(400, json={"error": "bad request"})
        if iid == 100001:
            return Response(429, json={"error": "throttled"})
        if iid == 100017:
            raise ConnectError("simulated network drop")
        counter[0] += 1
        return Response(
            200,
            json={
                "orderForOpen": {
                    "orderID": counter[0],
                    "amount": 25.0,
                    "statusID": 1,
                }
            },
        )

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
            return_value=Response(200, json=_empty_portfolio())
        )
        router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
            side_effect=handler
        )

        plan = BulkTradePlan(
            weights={
                "BTC": Decimal("0.25"),
                "ETH": Decimal("0.25"),
                "ADA": Decimal("0.25"),
                "XRP": Decimal("0.25"),
            },
            total_amount=Decimal("100"),
        )
        result = await execute_bulk_trade(http, env="demo", plan=plan, cache=cache)

    by_symbol = {tr.intent.instrument: tr for tr in result.trades}  # type: ignore[union-attr]
    assert by_symbol["BTC"].status == "failed"
    assert by_symbol["ETH"].status == "rate_limited_giveup"
    assert by_symbol["ADA"].status == "ambiguous"
    assert by_symbol["XRP"].status == "ok"
    await http.aclose()
