"""Shared pytest fixtures and configuration."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest


@pytest.fixture
def fake_clock() -> tuple[list[float], list[float]]:
    """Return ``(clock, sleeps)`` lists that share monotonic state.

    Use ``time_func=lambda: clock[0]`` and ``sleep_func=async fn`` patterns
    in tests. See ``test_ratelimit.py`` for a concrete example.
    """
    clock: list[float] = [0.0]
    sleeps: list[float] = []
    return clock, sleeps


@pytest.fixture
def pnl_fixture() -> dict[str, Any]:
    """A hand-checked ``clientPortfolio`` fixture.

    Numbers reverse-engineered to assert exact values for every account
    formula:

    * Available Cash = 5000 - 200 - 100 = 4700
    * Total Invested = 1000 + 500 + (2000 - 50) + 200 + 100 + 1.5 = 3751.5
    * Unrealized P&L = 25.50 + 10.00 + 50.00 = 85.50
    * Equity = 4700 + 3751.5 + 85.5 = 8537
    """
    return {
        "credit": 5000.00,
        "positions": [
            {
                "positionID": 1,
                "instrumentID": 1001,
                "units": 5.6,
                "amount": 1000.00,
                "openRate": 178.50,
                "leverage": 1,
                "isBuy": True,
                "mirrorID": 0,
                "unrealizedPnL": {"pnL": 25.50},
                "openDateTime": "2025-01-01T10:00:00Z",
            },
        ],
        "mirrors": [
            {
                "mirrorID": 99,
                "userId": 12345,
                "availableAmount": 2000.00,
                "closedPositionsNetProfit": 50.00,
                "positions": [
                    {
                        "positionID": 2,
                        "instrumentID": 2002,
                        "amount": 500.00,
                        "units": 3.0,
                        "unrealizedPnL": {"pnL": 10.00},
                    },
                ],
            },
        ],
        "orders": [{"amount": 100.00}],
        "ordersForOpen": [
            {
                "orderID": 7,
                "instrumentID": 3003,
                "amount": 200.00,
                "mirrorID": 0,
                "totalExternalCosts": 1.50,
                "isBuy": True,
                "leverage": 1,
            },
            {
                "orderID": 8,
                "instrumentID": 4004,
                "amount": 150.00,
                "mirrorID": 99,
                "isBuy": True,
                "leverage": 1,
            },
        ],
    }


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make ``asyncio.sleep`` and ``time.sleep`` instantaneous in unit tests
    so we don't accidentally wait on real time."""
    import asyncio
    import time

    async def _instant_async_sleep(delay: float, result: Any = None) -> Any:
        return result

    def _instant_sync_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _instant_async_sleep)
    monkeypatch.setattr(time, "sleep", _instant_sync_sleep)
    yield
