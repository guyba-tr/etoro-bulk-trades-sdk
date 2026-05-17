"""Test the bulk-trade sizing math: ceilings, cents flooring, open buffer."""

from __future__ import annotations

from decimal import Decimal

import pytest

from etoro_bulk_trades._sizing import (
    OPEN_BUFFER_FACTOR,
    OPEN_BUFFER_THRESHOLD,
    ceil_cents,
    floor_cents,
    size_bulk_amounts,
)
from etoro_bulk_trades.types import BulkTradePlan


def test_floor_cents_truncates_never_rounds_up() -> None:
    assert floor_cents(Decimal("99.999")) == Decimal("99.99")
    assert floor_cents(Decimal("99.991")) == Decimal("99.99")
    assert floor_cents(Decimal("99.99")) == Decimal("99.99")
    assert floor_cents(Decimal("0.001")) == Decimal("0.00")


def test_ceil_cents_rounds_up() -> None:
    assert ceil_cents(Decimal("99.991")) == Decimal("100.00")
    assert ceil_cents(Decimal("99.99")) == Decimal("99.99")
    assert ceil_cents(Decimal("0.001")) == Decimal("0.01")


def test_bulk_sizing_no_buffer() -> None:
    plan = BulkTradePlan(
        weights={"AAPL": Decimal("0.5"), "MSFT": Decimal("0.3"), "GOOG": Decimal("0.2")},
        total_amount=Decimal("1000"),
    )
    amts, buffer = size_bulk_amounts(
        plan, equity_anchor=Decimal("2000"), cash_anchor=Decimal("2000")
    )
    assert amts == {
        "AAPL": Decimal("500.00"),
        "MSFT": Decimal("300.00"),
        "GOOG": Decimal("200.00"),
    }
    assert buffer is False


def test_bulk_sizing_open_buffer_fires_when_cash_drops_below_1pct() -> None:
    """Plan would leave \\$5 / \\$1000 = 0.5% < 1% → buffer kicks in."""
    plan = BulkTradePlan(
        weights={"AAPL": Decimal("0.5"), "MSFT": Decimal("0.3"), "GOOG": Decimal("0.2")},
        total_amount=Decimal("995"),
    )
    amts, buffer = size_bulk_amounts(
        plan, equity_anchor=Decimal("1000"), cash_anchor=Decimal("1000")
    )
    assert buffer is True
    # Original AAPL = floor(0.5 * 995) = 497.50; buffered = floor(497.50 * 0.99) = 492.52
    assert amts["AAPL"] == Decimal("492.52")
    assert amts["MSFT"] == Decimal("295.51")
    assert amts["GOOG"] == Decimal("197.01")


def test_bulk_sizing_buffer_constants_documented() -> None:
    """Pin the threshold and factor so a refactor doesn't silently shift them."""
    assert OPEN_BUFFER_THRESHOLD == Decimal("0.01")
    assert OPEN_BUFFER_FACTOR == Decimal("0.99")


def test_bulk_sizing_uneven_floor() -> None:
    plan = BulkTradePlan(weights={"AAPL": Decimal("0.333")}, total_amount=Decimal("100"))
    amts, _ = size_bulk_amounts(plan, equity_anchor=Decimal("1000"), cash_anchor=Decimal("1000"))
    assert amts["AAPL"] == Decimal("33.30")


@pytest.mark.parametrize(
    ("equity", "cash", "weight", "total", "expected_buffer"),
    [
        (Decimal("10000"), Decimal("10000"), Decimal("0.99"), Decimal("9000"), False),
        (Decimal("10000"), Decimal("100"), Decimal("0.5"), Decimal("100"), True),
        (Decimal("1000"), Decimal("100"), Decimal("0.99"), Decimal("99"), True),
    ],
)
def test_bulk_sizing_buffer_threshold_boundary(
    equity: Decimal,
    cash: Decimal,
    weight: Decimal,
    total: Decimal,
    expected_buffer: bool,
) -> None:
    plan = BulkTradePlan(weights={"X": weight}, total_amount=total)
    _, buffer = size_bulk_amounts(plan, equity_anchor=equity, cash_anchor=cash)
    assert buffer is expected_buffer
