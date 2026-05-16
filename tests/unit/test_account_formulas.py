"""Test the four eToro account-snapshot aggregation formulas."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from etoro_bulk_trades._account import (
    available_cash,
    build_snapshot,
    equity,
    total_invested,
    unrealized_pnl,
)


def test_available_cash_excludes_mirror_pending(pnl_fixture: dict[str, Any]) -> None:
    """Mirror-driven pending opens (``mirrorID > 0``) are NOT subtracted from
    cash. Only manual pending opens (``mirrorID == 0``) reduce Available Cash."""
    assert available_cash(pnl_fixture) == Decimal("4700.00")


def test_total_invested_includes_pending_open_costs(pnl_fixture: dict[str, Any]) -> None:
    """Manual pending opens count their full amount PLUS ``totalExternalCosts``."""
    assert total_invested(pnl_fixture) == Decimal("3751.50")


def test_unrealized_pnl_includes_mirror_closed_profit(pnl_fixture: dict[str, Any]) -> None:
    """``mirrors[].closedPositionsNetProfit`` is realized profit from closed
    copy positions; it shows up in the *unrealized* total too because of how
    the snapshot dedup works."""
    assert unrealized_pnl(pnl_fixture) == Decimal("85.50")


def test_equity_is_sum_of_three(pnl_fixture: dict[str, Any]) -> None:
    """The Equity formula is the direct sum of the other three."""
    assert equity(pnl_fixture) == Decimal("8537.00")
    assert equity(pnl_fixture) == (
        available_cash(pnl_fixture) + total_invested(pnl_fixture) + unrealized_pnl(pnl_fixture)
    )


def test_nested_unrealized_pnl_optional() -> None:
    """``unrealizedPnL`` is absent for closed positions; absence => 0, not error."""
    closed_cp = {
        "credit": 100.0,
        "positions": [
            {
                "positionID": 1,
                "instrumentID": 1001,
                "amount": 50.0,
                "units": 1,
                "openRate": 50.0,
                "leverage": 1,
                "isBuy": True,
                "mirrorID": 0,
                # no unrealizedPnL
            }
        ],
    }
    assert unrealized_pnl(closed_cp) == Decimal(0)


def test_build_snapshot_groups_pendings_correctly(pnl_fixture: dict[str, Any]) -> None:
    snap = build_snapshot(pnl_fixture, env="real")
    assert snap.env == "real"
    assert snap.available_cash == Decimal("4700.00")
    assert snap.equity == Decimal("8537.00")
    assert len(snap.positions) == 1
    # Both mirror and manual pending orders are exposed
    assert len(snap.pending_orders) == 2
    assert len(snap.mirrors) == 1
    assert snap.mirrors[0].mirror_id == 99


def test_mirror_user_id_reads_uppercase_cid() -> None:
    """Real wire shape uses ``CID`` (capital) for the copied investor's user
    ID — matches the rest of the snapshot's capital-suffix convention. The
    parser must accept it; missing key must raise a clear error."""
    cp = {
        "credit": 100.0,
        "mirrors": [
            {
                "mirrorID": 42,
                "CID": 22770558,
                "availableAmount": 7.24,
                "closedPositionsNetProfit": 0.0,
                "positions": [],
            }
        ],
    }
    snap = build_snapshot(cp, env="real")
    assert len(snap.mirrors) == 1
    assert int(snap.mirrors[0].user_id) == 22770558
    assert snap.mirrors[0].mirror_id == 42


def test_mirror_user_id_falls_back_to_userid_variant() -> None:
    """Forward-compat: accept ``userId`` / ``userID`` if eToro ever flips."""
    for variant in ("userId", "userID"):
        cp = {
            "credit": 100.0,
            "mirrors": [
                {
                    "mirrorID": 42,
                    variant: 999,
                    "availableAmount": 0.0,
                    "closedPositionsNetProfit": 0.0,
                    "positions": [],
                }
            ],
        }
        snap = build_snapshot(cp, env="real")
        assert int(snap.mirrors[0].user_id) == 999, f"failed for variant {variant!r}"


def test_mirror_without_any_cid_field_raises() -> None:
    cp = {
        "credit": 100.0,
        "mirrors": [
            {
                "mirrorID": 42,
                "availableAmount": 0.0,
                "closedPositionsNetProfit": 0.0,
                "positions": [],
            }
        ],
    }
    import pytest

    with pytest.raises(KeyError, match="missing CID"):
        build_snapshot(cp, env="real")


def test_money_coercion_preserves_decimal_precision() -> None:
    """The wire delivers floats (``5000.00``); coercion must not introduce
    ``0.1 + 0.2`` drift."""
    cp = {
        "credit": 0.1 + 0.2,  # float drift on entry
        "orders": [{"amount": 0.0}],
        "ordersForOpen": [],
    }
    cash = available_cash(cp)
    # The float-drift example expands as Decimal("0.30000000000000004"),
    # which is the honest representation of the float. The formula must
    # return that value rather than silently normalising it.
    assert cash == Decimal("0.30000000000000004")
