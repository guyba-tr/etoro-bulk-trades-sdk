"""Order verification — WebSocket-first with a robust PnL fallback.

After execution returns :class:`TradeResult` / :class:`BulkTradeResult` /
:class:`RebalanceResult` with statuses like ``ok`` / ``ambiguous``, the
verifier upgrades each trade to one of:

* ``filled`` — observed in ``positions[]`` (or the WS confirmed it).
* ``pending_market_open`` — present in ``ordersForOpen[]`` (open scheduled
  for next market open).
* ``not_landed`` — neither WS confirmed nor present in ``/pnl``.
* ``failed`` — preserved from execution (the verifier never downgrades).

Three modes:

* ``ws`` (default) — open WS, subscribe to ``private``, await events whose
  ``OrderID`` matches one of the expected order IDs. On WS error or
  timeout, fall back to ``pnl``.
* ``pnl`` — sleep 10s (PnL cache window), read ``/pnl`` once, match by
  ``instrumentID``.
* ``auto`` — try ``ws`` with a short 5s window, then fall back to ``pnl``.

The fallback is what makes ``ws`` safe to default to even though only
``Trading.OrderForCloseMultiple.Update`` is documented — we never depend on
WS to *prove a trade failed*, only to *upgrade an ambiguous status faster*.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from etoro_bulk_trades._account import build_snapshot
from etoro_bulk_trades._execute import _summarize_bulk, _summarize_rebalance
from etoro_bulk_trades.errors import EtoroSDKError
from etoro_bulk_trades.types import (
    BulkTradeResult,
    Environment,
    PositionID,
    RebalanceResult,
    TradeResult,
    TradeStatus,
    VerifyMode,
)

if TYPE_CHECKING:
    from etoro_bulk_trades._auth import AuthHandle
    from etoro_bulk_trades._http import HttpClient


SleepFn = Callable[[float], Awaitable[None]]

logger = logging.getLogger(__name__)

PNL_CACHE_WINDOW_S: float = 10.0


def _expected_order_ids(trades: tuple[TradeResult, ...]) -> set[int]:
    return {int(tr.order_id) for tr in trades if tr.order_id is not None}


def _upgrade_status(current: TradeStatus, new: TradeStatus) -> TradeStatus:
    """Apply the status-upgrade precedence rule.

    Execution statuses (``ok``, ``ambiguous``, ``rate_limited_giveup``) are
    *upgraded* by verification statuses (``filled``, ``pending_market_open``,
    ``not_landed``). The verifier never overwrites a hard ``failed``
    (an explicit 4xx — we trust the server to know what it didn't accept).
    """
    if current == "failed":
        return current
    return new


def _replace_status(tr: TradeResult, new: TradeStatus) -> TradeResult:
    """Return a copy of ``tr`` with the upgraded status. (Models are frozen.)"""
    return tr.model_copy(update={"status": _upgrade_status(tr.status, new)})


async def _ws_collect(
    auth: AuthHandle,
    expected_orders: set[int],
    *,
    timeout_s: float,
) -> dict[int, TradeStatus]:
    """Subscribe to the private topic and collect statuses for expected orders.

    Returns ``{order_id: status}`` for every order observed in time. Missing
    orders are *not* in the dict — the caller falls back to PnL for those.
    """
    # Local imports to avoid the http/ws import cycle visible in __init__.
    from etoro_bulk_trades._ws import (
        connect_and_authenticate,
        stream_private_events,
        subscribe,
    )

    observed: dict[int, TradeStatus] = {}
    try:
        async with connect_and_authenticate(auth.ctx) as conn:
            await subscribe(conn, topics=["private"])
            async for event in stream_private_events(conn, timeout_s=timeout_s):
                if event.order_id is None or event.order_id not in expected_orders:
                    continue
                # Heuristic mapping. status_id semantics vary by entity; the
                # documented Trading.OrderForCloseMultiple.Update lacks an
                # enum. We treat any update for an expected order as a
                # confirmation; pending stays pending only when /pnl says so.
                observed[event.order_id] = "filled"
                if len(observed) >= len(expected_orders):
                    return observed
    except (OSError, EtoroSDKError) as exc:
        logger.info("WS verify dropped (%s); falling back to PnL.", exc)
        # Return what we have; the caller fills the rest from /pnl.
    return observed


async def _pnl_classify(
    http: HttpClient,
    *,
    env: Environment,
) -> tuple[set[int], set[int], dict[int, list[int]]]:
    """Read ``/pnl`` and return ``(filled_instrument_ids, pending_order_ids,
    instrument_to_position_ids)``.

    The third element maps each ``instrument_id`` to the **list of all
    non-mirror ``position_id`` values** the account currently holds for
    that instrument. Callers that need to attribute a freshly-opened
    position must subtract their pre-trade snapshot from the list to find
    the new entry. Returning a list rather than a single id avoids the
    silent-overwrite trap of a ``{iid: pid}`` dict comprehension when the
    account holds multiple positions on the same instrument.

    The 10s PnL cache wait is the responsibility of the caller (it's only
    needed once for an entire verify run, not per trade).
    """
    body = await http.request("GET", f"/trading/info/{env}/pnl", category="general")
    if not isinstance(body, dict) or "clientPortfolio" not in body:
        return set(), set(), {}
    snap = build_snapshot(body["clientPortfolio"], env=env)
    filled_iids: set[int] = {int(p.instrument_id) for p in snap.positions if not p.is_mirror}
    pending_oids: set[int] = {int(o.order_id) for o in snap.pending_orders}
    iid_to_pids: dict[int, list[int]] = {}
    for p in snap.positions:
        if p.is_mirror:
            continue
        iid_to_pids.setdefault(int(p.instrument_id), []).append(int(p.position_id))
    return filled_iids, pending_oids, iid_to_pids


async def _verify_trades(
    http: HttpClient,
    auth: AuthHandle,
    *,
    env: Environment,
    trades: tuple[TradeResult, ...],
    mode: VerifyMode,
    timeout_s: float,
    sleep_func: SleepFn | None = None,
) -> tuple[TradeResult, ...]:
    """Apply the verification dispatch to a flat list of trades.

    The flow:

    1. If mode in ``ws`` / ``auto``, try the WS path first (5s for ``auto``,
       full ``timeout_s`` for ``ws``).
    2. Sleep 10s (PnL cache window) if we have any not-yet-confirmed trades.
    3. Read ``/pnl`` and classify each remaining trade as ``filled`` /
       ``pending_market_open`` / ``not_landed``.

    Trades that were ``failed`` at execution time are preserved as-is.
    """
    sleeper = sleep_func or asyncio.sleep
    if not trades:
        return ()

    expected_orders = _expected_order_ids(trades)

    ws_observed: dict[int, TradeStatus] = {}
    if mode in ("ws", "auto") and expected_orders:
        ws_budget = 5.0 if mode == "auto" else timeout_s
        ws_observed = await _ws_collect(auth, expected_orders, timeout_s=ws_budget)

    # If WS confirmed everything we were looking for, skip the PnL read.
    pending_after_ws = expected_orders - set(ws_observed.keys())
    if mode in ("ws", "auto") and expected_orders and not pending_after_ws:
        ws_upgraded: list[TradeResult] = []
        for tr in trades:
            if tr.order_id is not None and int(tr.order_id) in ws_observed:
                ws_upgraded.append(_replace_status(tr, ws_observed[int(tr.order_id)]))
            else:
                ws_upgraded.append(tr)
        return tuple(ws_upgraded)

    # Always do the PnL pass for whatever WS didn't observe (or for ``pnl``
    # mode). One sleep + one read covers the whole batch.
    await sleeper(PNL_CACHE_WINDOW_S)
    filled_iids, pending_oids, iid_to_pids = await _pnl_classify(http, env=env)

    upgraded: list[TradeResult] = []
    for tr in trades:
        if tr.status == "failed":
            upgraded.append(tr)
            continue
        if tr.order_id is not None and int(tr.order_id) in ws_observed:
            upgraded.append(_replace_status(tr, ws_observed[int(tr.order_id)]))
            continue

        # Match by instrument_id (the typical case for open verification —
        # the OrderID lives in the response, but /pnl doesn't expose it
        # again once the order has filled into a position).
        #
        # **Safety rule**: only attribute a ``position_id`` to this trade if
        # there is exactly **one** position for this instrument that did
        # NOT exist before the trade was placed (see
        # :attr:`TradeResult.pre_existing_position_ids`). If pre-trade
        # capture is missing, fall back to "all current positions for the
        # instrument" — but if that set has more than one entry, leave
        # ``position_id=None`` rather than guess. Closing a guessed
        # position is catastrophic and unrecoverable.
        if tr.instrument_id is not None and int(tr.instrument_id) in filled_iids:
            current_pids = iid_to_pids.get(int(tr.instrument_id), [])
            pre_set = {int(p) for p in tr.pre_existing_position_ids}
            new_pids = [pid for pid in current_pids if pid not in pre_set]
            updates: dict[str, object] = {"status": _upgrade_status(tr.status, "filled")}
            error_note: str | None = None
            if len(new_pids) == 1:
                updates["position_id"] = cast(PositionID, new_pids[0])
            elif len(new_pids) == 0:
                # Position already existed (rare: caller forgot to pass a
                # pre-snapshot AND only one position is present, so we
                # cannot prove novelty), or the open fell into a fill we
                # can't yet see. Leave position_id unset.
                error_note = (
                    "verifier could not identify the new position: no "
                    "position with a fresh positionID was found for "
                    f"instrument_id={int(tr.instrument_id)} after the "
                    "PnL cache wait."
                )
            else:
                # Multiple new-looking candidates — refuse to pick. This
                # is the safe failure mode that prevents closing the
                # wrong position.
                error_note = (
                    "verifier refuses to assign position_id: "
                    f"{len(new_pids)} positions for "
                    f"instrument_id={int(tr.instrument_id)} are not in "
                    "pre_existing_position_ids. Caller must disambiguate "
                    "(see TradeResult.pre_existing_position_ids and "
                    "AccountSnapshot.positions)."
                )
            if error_note is not None and tr.error is None:
                updates["error"] = error_note
            upgraded.append(tr.model_copy(update=updates))
            continue

        if tr.order_id is not None and int(tr.order_id) in pending_oids:
            upgraded.append(_replace_status(tr, "pending_market_open"))
            continue

        # If the trade was ``ok`` at execution but isn't in either pool, it
        # never landed (rejected silently / lost in transit / etc.).
        if tr.status in ("ok", "ambiguous"):
            upgraded.append(_replace_status(tr, "not_landed"))
        else:
            upgraded.append(tr)

    return tuple(upgraded)


async def verify_orders(
    http: HttpClient,
    auth: AuthHandle,
    result: TradeResult | BulkTradeResult | RebalanceResult,
    *,
    env: Environment,
    mode: VerifyMode = "ws",
    timeout_s: float = 30.0,
    sleep_func: SleepFn | None = None,
) -> TradeResult | BulkTradeResult | RebalanceResult:
    """Upgrade an execution result's statuses to verified statuses.

    The dispatch lives here because all three result shapes share the same
    underlying ``trades`` semantics — we flatten, verify, and rebuild.
    """
    if isinstance(result, TradeResult):
        verified_single = await _verify_trades(
            http,
            auth,
            env=env,
            trades=(result,),
            mode=mode,
            timeout_s=timeout_s,
            sleep_func=sleep_func,
        )
        return verified_single[0]

    if isinstance(result, BulkTradeResult):
        verified = await _verify_trades(
            http,
            auth,
            env=result.env,
            trades=result.trades,
            mode=mode,
            timeout_s=timeout_s,
            sleep_func=sleep_func,
        )
        total_planned = sum(
            (tr.requested_amount or Decimal(0) for tr in result.trades),
            start=Decimal(0),
        )
        return result.model_copy(
            update={
                "trades": verified,
                "summary": _summarize_bulk(verified, total_planned=total_planned),
            }
        )

    if isinstance(result, RebalanceResult):
        verified_opens = await _verify_trades(
            http,
            auth,
            env=result.env,
            trades=result.phase_2_opens,
            mode=mode,
            timeout_s=timeout_s,
            sleep_func=sleep_func,
        )
        verified_closes = await _verify_trades(
            http,
            auth,
            env=result.env,
            trades=result.phase_1_closes,
            mode=mode,
            timeout_s=timeout_s,
            sleep_func=sleep_func,
        )
        return result.model_copy(
            update={
                "phase_1_closes": verified_closes,
                "phase_2_opens": verified_opens,
                "summary": _summarize_rebalance(result.diff, verified_closes, verified_opens),
            }
        )

    raise TypeError(f"unsupported result type: {type(result).__name__}")


__all__ = ["PNL_CACHE_WINDOW_S", "verify_orders"]
