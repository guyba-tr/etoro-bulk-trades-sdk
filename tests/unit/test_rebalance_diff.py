"""Test the rebalance diff calculator and close-position selection."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from etoro_bulk_trades._execute import _build_diff, _select_positions_for_close
from etoro_bulk_trades.types import (
    AccountSnapshot,
    InstrumentID,
    InstrumentRef,
    Position,
    PositionID,
    RebalancePlan,
)


def _make_snapshot(positions: tuple[Position, ...]) -> AccountSnapshot:
    return AccountSnapshot(
        env="real",
        snapshot_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        credit=Decimal("1000"),
        available_cash=Decimal("400"),
        total_invested=Decimal("600"),
        unrealized_pnl_total=Decimal(0),
        equity=Decimal("1000"),
        positions=positions,
    )


def _position(
    pid: int, iid: int, amount: Decimal, units: Decimal, *, open_at: datetime | None = None
) -> Position:
    return Position(
        position_id=PositionID(pid),
        instrument_id=InstrumentID(iid),
        is_buy=True,
        leverage=1,
        units=units,
        amount=amount,
        open_rate=Decimal("200"),
        open_date_time=open_at or datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


def test_open_increase_reduce_close_actions() -> None:
    """Cover all four non-trivial RebalanceAction values in one diff."""
    snap = _make_snapshot(
        (
            _position(1, 1001, Decimal("400"), Decimal("2.0")),  # AAPL: hold 400
            _position(2, 1003, Decimal("200"), Decimal("1.0")),  # GOOG: hold 200
        )
    )
    plan = RebalancePlan(
        target_weights={
            "AAPL": Decimal("0.5"),  # increase 400 -> 500
            "MSFT": Decimal("0.5"),  # open 0 -> 500
        }
    )
    refs: dict[str | int, InstrumentRef] = {
        "AAPL": InstrumentRef(
            instrument_id=InstrumentID(1001), symbol="AAPL", display_name="Apple"
        ),
        "MSFT": InstrumentRef(
            instrument_id=InstrumentID(1002), symbol="MSFT", display_name="Microsoft"
        ),
    }
    diff = _build_diff(plan, snap, refs, total_amount=Decimal(1000))

    actions = {d.instrument.symbol: d.action for d in diff}
    # GOOG was current but not in target → close (synthetic ref with symbol "#1003")
    assert actions == {"AAPL": "increase", "MSFT": "open", "#1003": "close"}

    amounts = {d.instrument.symbol: (d.current_amount, d.target_amount) for d in diff}
    assert amounts["AAPL"] == (Decimal(400), Decimal("500.00"))
    assert amounts["MSFT"] == (Decimal(0), Decimal("500.00"))
    assert amounts["#1003"] == (Decimal(200), Decimal(0))


def test_close_excluded_false_leaves_unmentioned_positions() -> None:
    snap = _make_snapshot((_position(1, 1001, Decimal("400"), Decimal("2.0")),))
    plan = RebalancePlan(
        target_weights={"MSFT": Decimal("1.0")},
        close_excluded=False,
    )
    refs: dict[str | int, InstrumentRef] = {
        "MSFT": InstrumentRef(
            instrument_id=InstrumentID(1002), symbol="MSFT", display_name="Microsoft"
        ),
    }
    diff = _build_diff(plan, snap, refs, total_amount=Decimal(1000))
    syms = {d.instrument.symbol for d in diff}
    assert syms == {"MSFT"}, "AAPL must be left alone when close_excluded=False"


def test_noop_when_at_target() -> None:
    snap = _make_snapshot((_position(1, 1001, Decimal("500"), Decimal("2.5")),))
    plan = RebalancePlan(target_weights={"AAPL": Decimal("0.5")})
    refs: dict[str | int, InstrumentRef] = {
        "AAPL": InstrumentRef(
            instrument_id=InstrumentID(1001), symbol="AAPL", display_name="Apple"
        ),
    }
    diff = _build_diff(plan, snap, refs, total_amount=Decimal(1000))
    assert len(diff) == 1
    assert diff[0].action == "noop"
    assert diff[0].delta_amount == Decimal(0)


def test_close_selection_full_close_when_exact() -> None:
    snap = _make_snapshot((_position(7, 1003, Decimal("200"), Decimal("1.0")),))
    plans = _select_positions_for_close(
        snap,
        InstrumentID(1003),
        amount_to_free=Decimal("200"),
        close_buffer_pct=Decimal("0.0"),  # exact match, no buffer
    )
    assert plans == [(PositionID(7), None)]


def test_close_selection_partial_when_oversized() -> None:
    """Need \\$100 of a \\$500 position → partial close, units pro-rated UP."""
    snap = _make_snapshot((_position(7, 1003, Decimal("500"), Decimal("10.0")),))
    plans = _select_positions_for_close(
        snap,
        InstrumentID(1003),
        amount_to_free=Decimal("100"),
        close_buffer_pct=Decimal("0.01"),
    )
    assert len(plans) == 1
    pid, units = plans[0]
    assert pid == PositionID(7)
    # buffer means we actually free 101, fraction = 101/500 = 0.202,
    # 0.202 * 10.0 units = 2.02 → ceil_cents → 2.02
    assert units == Decimal("2.02")


def test_close_selection_newest_first() -> None:
    """When multiple positions on the same instrument exist, close newest first."""
    snap = _make_snapshot(
        (
            _position(
                1,
                1003,
                Decimal("100"),
                Decimal("1.0"),
                open_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            ),
            _position(
                2,
                1003,
                Decimal("100"),
                Decimal("1.0"),
                open_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
            ),
        )
    )
    plans = _select_positions_for_close(
        snap,
        InstrumentID(1003),
        amount_to_free=Decimal("50"),
        close_buffer_pct=Decimal("0.0"),
    )
    # Should pick position 2 (newer), partial close
    pid, _units = plans[0]
    assert pid == PositionID(2)
