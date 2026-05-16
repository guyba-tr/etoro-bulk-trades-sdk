"""Verifier must NEVER attribute a pre-existing position to a fresh trade.

This regression guard exists because of a real incident: a verifier built a
``{instrument_id: position_id}`` dict via comprehension that silently
overwrote duplicate keys, then handed the (arbitrary, often wrong)
``position_id`` back to the caller. The caller then closed the wrong BTC
position. The fix is that the verifier:

* receives ``pre_existing_position_ids`` per trade,
* subtracts them from the post-trade snapshot,
* assigns ``position_id`` **only** when exactly one new candidate exists,
* leaves ``position_id=None`` with an ``error`` note otherwise.
"""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._verify import verify_orders
from etoro_bulk_trades.types import (
    InstrumentID,
    OpenIntent,
    OrderID,
    PositionID,
    TradeResult,
)

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"
BTC_IID = 100000


def _make_http() -> tuple[HttpClient, AuthHandle]:
    handle = AuthHandle(ApiKeyAuth(api_key="k", user_key="u"))
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    return http, handle


def _pnl_with_btc_positions(*position_ids: int) -> dict[str, object]:
    return {
        "clientPortfolio": {
            "credit": 1000.0,
            "positions": [
                {
                    "positionID": pid,
                    "instrumentID": BTC_IID,
                    "units": 0.001,
                    "amount": 25.0,
                    "openRate": 25000.0,
                    "leverage": 1,
                    "isBuy": True,
                    "mirrorID": 0,
                    "unrealizedPnL": {"pnL": 0.0},
                }
                for pid in position_ids
            ],
            "orders": [],
            "ordersForOpen": [],
            "mirrors": [],
        }
    }


def _open_result(*, order_id: int, pre_pids: tuple[int, ...] = ()) -> TradeResult:
    intent = OpenIntent(instrument="BTC", amount=Decimal("25"))
    return TradeResult(
        intent=intent,
        instrument_id=cast(InstrumentID, BTC_IID),
        status="ok",
        order_id=cast(OrderID, order_id),
        requested_amount=Decimal("25"),
        filled_amount=Decimal("25"),
        pre_existing_position_ids=tuple(cast(PositionID, p) for p in pre_pids),
    )


@pytest.mark.asyncio
async def test_verifier_picks_only_the_new_position() -> None:
    """Account holds one BTC position before; one new one appears after.

    The verifier must attribute the **new** position_id, never the old one.
    """
    http, handle = _make_http()
    tr = _open_result(order_id=1001, pre_pids=(3500000001,))

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
            return_value=Response(
                200,
                json=_pnl_with_btc_positions(3500000001, 3500000999),
            )
        )

        verified = await verify_orders(
            http,
            handle,
            tr,
            env="demo",
            mode="pnl",
            timeout_s=5.0,
        )

    assert isinstance(verified, TradeResult)
    assert verified.status == "filled"
    assert verified.position_id is not None
    assert int(verified.position_id) == 3500000999, (
        f"verifier picked pre-existing position {verified.position_id} instead of the new one"
    )
    assert verified.error is None
    await http.aclose()


@pytest.mark.asyncio
async def test_verifier_refuses_when_multiple_new_candidates() -> None:
    """Two new BTC positions appear after the trade (e.g. account aggregation
    quirk or concurrent action). The verifier must refuse to pick one and
    leave ``position_id=None`` with an explanatory error.
    """
    http, handle = _make_http()
    tr = _open_result(order_id=1002, pre_pids=(3500000001,))

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
            return_value=Response(
                200,
                json=_pnl_with_btc_positions(3500000001, 3500000998, 3500000999),
            )
        )

        verified = await verify_orders(
            http,
            handle,
            tr,
            env="demo",
            mode="pnl",
            timeout_s=5.0,
        )

    assert isinstance(verified, TradeResult)
    assert verified.status == "filled"
    assert verified.position_id is None, (
        f"verifier guessed position_id={verified.position_id} when it should have refused"
    )
    assert verified.error is not None
    assert "refuses to assign position_id" in verified.error
    await http.aclose()


@pytest.mark.asyncio
async def test_verifier_no_new_positions_yields_error_note() -> None:
    """Pre-snapshot already contains every position in the post-snapshot
    (the new fill hasn't shown up). Status flips to ``filled`` because the
    instrument is present, but ``position_id`` stays unset with an
    explanatory error.
    """
    http, handle = _make_http()
    tr = _open_result(order_id=1003, pre_pids=(3500000001,))

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
            return_value=Response(
                200,
                json=_pnl_with_btc_positions(3500000001),
            )
        )

        verified = await verify_orders(
            http,
            handle,
            tr,
            env="demo",
            mode="pnl",
            timeout_s=5.0,
        )

    assert isinstance(verified, TradeResult)
    assert verified.status == "filled"
    assert verified.position_id is None
    assert verified.error is not None
    assert "could not identify the new position" in verified.error
    await http.aclose()
