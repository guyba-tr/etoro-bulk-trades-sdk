"""Test the at-most-once response classifier — every branch of the table."""

from __future__ import annotations

from decimal import Decimal

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._execute import open_trade
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._resolve import InstrumentCache
from etoro_bulk_trades.types import InstrumentID, InstrumentRef, OpenIntent

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"


def _make_http() -> HttpClient:
    handle = AuthHandle(ApiKeyAuth(api_key="k", user_key="u"))
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    return http


def _pre_seed_cache(cache: InstrumentCache, symbol: str, iid: int) -> None:
    cache.put(
        InstrumentRef(
            instrument_id=InstrumentID(iid),
            symbol=symbol,
            display_name=symbol,
        )
    )


@pytest.mark.asyncio
async def test_open_trade_ok_status() -> None:
    """2xx with ``orderForOpen.orderID`` → status ``ok``."""
    cache = InstrumentCache()
    _pre_seed_cache(cache, "AAPL", 1001)
    http = _make_http()

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/real/pnl").mock(
            return_value=Response(
                200,
                json={
                    "clientPortfolio": {
                        "credit": 1000.0,
                        "positions": [],
                        "orders": [],
                        "ordersForOpen": [],
                        "mirrors": [],
                    }
                },
            )
        )
        router.post(f"{PUBLIC_BASE}/trading/execution/real/market-open-orders/by-amount").mock(
            return_value=Response(
                200,
                json={"orderForOpen": {"orderID": 999, "amount": 100.0, "statusID": 1}},
            )
        )

        result = await open_trade(
            http,
            env="real",
            intent=OpenIntent(instrument="AAPL", amount=Decimal("100")),
            cache=cache,
        )

    assert result.status == "ok"
    assert int(result.order_id) == 999 if result.order_id is not None else False
    assert result.requested_amount == Decimal("100.00")
    await http.aclose()


@pytest.mark.asyncio
async def test_open_trade_failed_status_on_4xx() -> None:
    """4xx with body → status ``failed`` (no retry, never ambiguous)."""
    cache = InstrumentCache()
    _pre_seed_cache(cache, "AAPL", 1001)
    http = _make_http()

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/real/pnl").mock(
            return_value=Response(
                200,
                json={
                    "clientPortfolio": {
                        "credit": 1000.0,
                        "positions": [],
                        "orders": [],
                        "ordersForOpen": [],
                        "mirrors": [],
                    }
                },
            )
        )
        router.post(f"{PUBLIC_BASE}/trading/execution/real/market-open-orders/by-amount").mock(
            return_value=Response(
                400, json={"errorCode": "InvalidArgument", "message": "bad amount"}
            )
        )

        result = await open_trade(
            http,
            env="real",
            intent=OpenIntent(instrument="AAPL", amount=Decimal("100")),
            cache=cache,
        )

    assert result.status == "failed"
    assert result.error is not None
    assert "400" in result.error
    await http.aclose()


@pytest.mark.asyncio
async def test_open_trade_ambiguous_on_transport_error() -> None:
    """Network error → status ``ambiguous`` (NOT retried; verified later)."""
    cache = InstrumentCache()
    _pre_seed_cache(cache, "AAPL", 1001)
    http = _make_http()

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/real/pnl").mock(
            return_value=Response(
                200,
                json={
                    "clientPortfolio": {
                        "credit": 1000.0,
                        "positions": [],
                        "orders": [],
                        "ordersForOpen": [],
                        "mirrors": [],
                    }
                },
            )
        )
        # All 5xx attempts time out / fail before classify → TransportError
        # The HTTP layer retries 3 times, so emit 4 server errors.
        from httpx import ConnectError

        router.post(f"{PUBLIC_BASE}/trading/execution/real/market-open-orders/by-amount").mock(
            side_effect=ConnectError("simulated network drop")
        )

        result = await open_trade(
            http,
            env="real",
            intent=OpenIntent(instrument="AAPL", amount=Decimal("100")),
            cache=cache,
        )

    assert result.status == "ambiguous"
    assert result.error is not None
    await http.aclose()


@pytest.mark.asyncio
async def test_open_trade_rate_limit_giveup_after_retries() -> None:
    """429 retried 3 times → status ``rate_limited_giveup``."""
    cache = InstrumentCache()
    _pre_seed_cache(cache, "AAPL", 1001)
    http = _make_http()

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/real/pnl").mock(
            return_value=Response(
                200,
                json={
                    "clientPortfolio": {
                        "credit": 1000.0,
                        "positions": [],
                        "orders": [],
                        "ordersForOpen": [],
                        "mirrors": [],
                    }
                },
            )
        )
        router.post(f"{PUBLIC_BASE}/trading/execution/real/market-open-orders/by-amount").mock(
            return_value=Response(429, json={"error": "too many"})
        )

        result = await open_trade(
            http,
            env="real",
            intent=OpenIntent(instrument="AAPL", amount=Decimal("100")),
            cache=cache,
        )

    assert result.status == "rate_limited_giveup"
    assert result.error is not None
    await http.aclose()


@pytest.mark.asyncio
async def test_open_trade_rejected_on_insufficient_cash() -> None:
    """Pre-flight refuses if requested > available_cash."""
    cache = InstrumentCache()
    _pre_seed_cache(cache, "AAPL", 1001)
    http = _make_http()

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/trading/info/real/pnl").mock(
            return_value=Response(
                200,
                json={
                    "clientPortfolio": {
                        "credit": 50.0,  # only $50 available
                        "positions": [],
                        "orders": [],
                        "ordersForOpen": [],
                        "mirrors": [],
                    }
                },
            )
        )

        from etoro_bulk_trades.errors import InsufficientCashError

        with pytest.raises(InsufficientCashError) as excinfo:
            await open_trade(
                http,
                env="real",
                intent=OpenIntent(instrument="AAPL", amount=Decimal("100")),
                cache=cache,
            )
        assert excinfo.value.requested == Decimal("100")
        assert excinfo.value.available == Decimal("50.0")
    await http.aclose()
