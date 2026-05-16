"""Account-snapshot formulas — verbatim from the eToro Account Snapshot rule.

The Public-API endpoint ``/trading/info/{env}/pnl`` returns the entire
``clientPortfolio`` despite its URL implying P&L only. This module
contains:

1. The four official aggregation formulas (Available Cash, Total Invested,
   Profit/Loss, Equity).
2. A parser that turns the raw wire dict into an
   :class:`~etoro_bulk_trades.types.AccountSnapshot`.

Source guides (treat as authoritative if a number disagrees):

* https://api-portal.etoro.com/guides/calculate-available-cash
* https://api-portal.etoro.com/guides/calculate-total-invested
* https://api-portal.etoro.com/guides/calculate-profit-loss
* https://api-portal.etoro.com/guides/calculate-equity

Casing reminder: identifier fields inside ``positions[]`` / ``orders[]`` /
``ordersForOpen[]`` / ``mirrors[]`` use **capital-suffix** form
(``positionID``, ``instrumentID``, ``mirrorID``, ``orderID``, ``CID``,
``parentPositionID``). We type each wire object exactly as it arrives.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast

from etoro_bulk_trades.types import (
    CID,
    AccountSnapshot,
    Environment,
    InstrumentID,
    Mirror,
    MirrorPosition,
    OrderID,
    PendingOrder,
    Position,
    PositionID,
)


def _to_decimal(value: object) -> Decimal:
    """Coerce wire JSON numbers (which arrive as float / int / str / None)
    into a clean ``Decimal``. ``None`` becomes ``Decimal(0)``."""
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, int | float | str):
        return Decimal(str(value))
    raise TypeError(f"cannot coerce {type(value).__name__} to Decimal")


def _nested_pnl(obj: dict[str, Any]) -> Decimal:
    """Read the nested ``unrealizedPnL.pnL`` field (lower-n, capital-L).

    Per the account-snapshot rule §1, ``unrealizedPnL`` is missing for
    closed/historical positions; treat absent as zero.
    """
    nested = obj.get("unrealizedPnL")
    if not isinstance(nested, dict):
        return Decimal(0)
    return _to_decimal(nested.get("pnL"))


# ── account-level aggregation formulas ──────────────────────────────────────


def available_cash(client_portfolio: dict[str, Any]) -> Decimal:
    """``credit
       - Σ(ordersForOpen[i].amount where mirrorID == 0)
       - Σ(orders[i].amount)``

    The ``ordersForOpen`` filter excludes mirror-driven pending opens (which
    are accounted for inside ``mirrors[]``); ``orders`` is summed in full.
    """
    credit = _to_decimal(client_portfolio.get("credit"))
    pending_manual = sum(
        (
            _to_decimal(o.get("amount"))
            for o in client_portfolio.get("ordersForOpen", [])
            if int(o.get("mirrorID", 0)) == 0
        ),
        start=Decimal(0),
    )
    pending_close = sum(
        (_to_decimal(o.get("amount")) for o in client_portfolio.get("orders", [])),
        start=Decimal(0),
    )
    return credit - pending_manual - pending_close


def total_invested(client_portfolio: dict[str, Any]) -> Decimal:
    """``Σ(positions[i].amount)
       + Σ(mirrors[i].positions[j].amount)
       + Σ(mirrors[i].availableAmount - mirrors[i].closedPositionsNetProfit)
       + Σ(ordersForOpen[i].amount where mirrorID == 0)
       + Σ(orders[i].amount)
       + Σ(ordersForOpen[i].totalExternalCosts where mirrorID == 0)``

    The ``availableAmount - closedPositionsNetProfit`` term subtracts realized
    profit from closed copy positions so it isn't double-counted via the
    Profit/Loss formula. ``totalExternalCosts`` accounts for fees on manual
    pending orders.
    """
    direct_positions = sum(
        (_to_decimal(p.get("amount")) for p in client_portfolio.get("positions", [])),
        start=Decimal(0),
    )

    mirror_positions = Decimal(0)
    mirror_avail_minus_realized = Decimal(0)
    for mirror in client_portfolio.get("mirrors", []):
        mirror_positions += sum(
            (_to_decimal(mp.get("amount")) for mp in mirror.get("positions", [])),
            start=Decimal(0),
        )
        avail = _to_decimal(mirror.get("availableAmount"))
        closed = _to_decimal(mirror.get("closedPositionsNetProfit"))
        mirror_avail_minus_realized += avail - closed

    manual_pending_open = sum(
        (
            _to_decimal(o.get("amount"))
            for o in client_portfolio.get("ordersForOpen", [])
            if int(o.get("mirrorID", 0)) == 0
        ),
        start=Decimal(0),
    )
    pending_close = sum(
        (_to_decimal(o.get("amount")) for o in client_portfolio.get("orders", [])),
        start=Decimal(0),
    )
    manual_pending_open_costs = sum(
        (
            _to_decimal(o.get("totalExternalCosts"))
            for o in client_portfolio.get("ordersForOpen", [])
            if int(o.get("mirrorID", 0)) == 0
        ),
        start=Decimal(0),
    )

    return (
        direct_positions
        + mirror_positions
        + mirror_avail_minus_realized
        + manual_pending_open
        + pending_close
        + manual_pending_open_costs
    )


def unrealized_pnl(client_portfolio: dict[str, Any]) -> Decimal:
    """``Σ(positions[i].unrealizedPnL.pnL)
       + Σ(mirrors[i].positions[j].unrealizedPnL.pnL)
       + Σ(mirrors[i].closedPositionsNetProfit)``

    The third term is realized profit from closed copy positions (which is
    why Total Invested above subtracts it from ``availableAmount`` — keeping
    Equity from double-counting it).
    """
    direct = sum(
        (_nested_pnl(p) for p in client_portfolio.get("positions", [])),
        start=Decimal(0),
    )
    mirror_positions_pnl = Decimal(0)
    mirror_realized = Decimal(0)
    for mirror in client_portfolio.get("mirrors", []):
        mirror_positions_pnl += sum(
            (_nested_pnl(mp) for mp in mirror.get("positions", [])),
            start=Decimal(0),
        )
        mirror_realized += _to_decimal(mirror.get("closedPositionsNetProfit"))
    return direct + mirror_positions_pnl + mirror_realized


def equity(client_portfolio: dict[str, Any]) -> Decimal:
    """``available_cash + total_invested + unrealized_pnl``."""
    return (
        available_cash(client_portfolio)
        + total_invested(client_portfolio)
        + unrealized_pnl(client_portfolio)
    )


# ── snapshot builder ────────────────────────────────────────────────────────


def _parse_position(p: dict[str, Any]) -> Position:
    """Build a :class:`Position` from the wire shape. Mirror positions are
    flagged via ``mirrorID > 0``; both flat positions and the ones nested
    under ``mirrors[].positions[]`` come through here when explicitly built
    by the caller (see :func:`build_snapshot` for the dedup contract)."""
    return Position(
        position_id=cast(PositionID, int(p["positionID"])),
        instrument_id=cast(InstrumentID, int(p["instrumentID"])),
        is_buy=bool(p.get("isBuy", True)),
        leverage=int(p.get("leverage", 1)),
        units=_to_decimal(p.get("units")),
        amount=_to_decimal(p.get("amount")),
        open_rate=_to_decimal(p.get("openRate")),
        is_mirror=int(p.get("mirrorID", 0)) > 0,
        mirror_id=int(p.get("mirrorID", 0)),
        parent_position_id=(
            cast(PositionID, int(p["parentPositionID"]))
            if p.get("parentPositionID") is not None
            else None
        ),
        unrealized_pnl=_nested_pnl(p),
        open_date_time=_parse_datetime(p.get("openDateTime")),
    )


def _parse_pending_order(o: dict[str, Any]) -> PendingOrder:
    return PendingOrder(
        order_id=cast(OrderID, int(o["orderID"])),
        instrument_id=cast(InstrumentID, int(o["instrumentID"])),
        is_buy=bool(o.get("isBuy", True)),
        leverage=int(o.get("leverage", 1)),
        amount=_to_decimal(o.get("amount")),
        mirror_id=int(o.get("mirrorID", 0)),
        total_external_costs=_to_decimal(o.get("totalExternalCosts")),
    )


def _parse_mirror(m: dict[str, Any]) -> Mirror:
    """Decode one ``clientPortfolio.mirrors[]`` entry.

    The copied investor's user ID arrives as ``CID`` (capital — matching the
    rest of the snapshot's capital-suffix identifier convention); accept
    ``userId`` / ``userID`` as fallbacks for forward compatibility.
    """
    cid_raw = m.get("CID")
    if cid_raw is None:
        cid_raw = m.get("userID") or m.get("userId")
    if cid_raw is None:
        raise KeyError(
            f"mirror entry missing CID field (keys present: {sorted(m.keys())})"
        )
    return Mirror(
        mirror_id=int(m["mirrorID"]),
        user_id=cast(CID, int(cid_raw)),
        available_amount=_to_decimal(m.get("availableAmount")),
        closed_positions_net_profit=_to_decimal(m.get("closedPositionsNetProfit")),
        positions=tuple(
            MirrorPosition(
                position_id=cast(PositionID, int(mp["positionID"])),
                instrument_id=cast(InstrumentID, int(mp["instrumentID"])),
                amount=_to_decimal(mp.get("amount")),
                units=_to_decimal(mp.get("units")),
                unrealized_pnl=_nested_pnl(mp),
            )
            for mp in m.get("positions", [])
        ),
    )


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # eToro returns ISO-8601, sometimes with trailing 'Z'.
        s = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None
    return None


def build_snapshot(
    client_portfolio: dict[str, Any],
    *,
    env: Environment,
    snapshot_at: datetime | None = None,
) -> AccountSnapshot:
    """Convert a raw ``clientPortfolio`` dict into a typed
    :class:`AccountSnapshot`.

    Mirror dedup contract: ``positions`` includes ``clientPortfolio.positions[]``
    (the flat list, including mirror positions identified by ``mirrorID > 0``).
    The grouped-by-trader projection lives under ``mirrors[].positions[]`` and
    is exposed separately for UI convenience. **Don't sum across both** — the
    aggregation formulas above already account for the dedup.
    """
    when = snapshot_at or datetime.now(tz=timezone.utc)
    return AccountSnapshot(
        env=env,
        snapshot_at=when,
        credit=_to_decimal(client_portfolio.get("credit")),
        available_cash=available_cash(client_portfolio),
        total_invested=total_invested(client_portfolio),
        unrealized_pnl_total=unrealized_pnl(client_portfolio),
        equity=equity(client_portfolio),
        positions=tuple(_parse_position(p) for p in client_portfolio.get("positions", [])),
        pending_orders=tuple(
            _parse_pending_order(o) for o in client_portfolio.get("ordersForOpen", [])
        ),
        mirrors=tuple(_parse_mirror(m) for m in client_portfolio.get("mirrors", [])),
    )
