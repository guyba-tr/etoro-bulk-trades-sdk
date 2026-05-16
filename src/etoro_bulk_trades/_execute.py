"""Trade execution — single, bulk, and rebalance.

This module is the orchestration layer: it composes the HTTP client, the
account-snapshot formulas, the resolver, and the at-most-once response
classifier into the four user-visible workflows.

* :func:`open_trade` / :func:`close_trade` — single market open / close.
* :func:`execute_bulk_trade` — multi-position open from one cash pool.
* :func:`rebalance` — close-then-wait-then-open against a target allocation.

All four share the same execution disciplines from the
``etoro-trading-assistant`` agent skill:

* **Anchor freeze** — read ``/pnl`` once at workflow start; freeze
  ``EQUITY_ANCHOR`` / ``CASH_ANCHOR``; never recompute mid-flow.
* **Ceilings, never targets** — ``amount = floor(weight * total * 100) / 100``;
  the SDK never rounds up.
* **Open buffer** — when the plan would leave cash below 1% of equity,
  shrink each open by 1% so per-trade fees can't push displayed cash
  negative.
* **At-most-once** — every trade-execution POST follows the status table
  in :func:`_classify_response`; ambiguous outcomes are reconciled by
  reading state in ``verify_orders``, never by re-firing.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import TYPE_CHECKING, Any, cast

from etoro_bulk_trades._account import build_snapshot
from etoro_bulk_trades._resolve import InstrumentCache, resolve
from etoro_bulk_trades.errors import (
    AuthError,
    EtoroSDKError,
    HttpStatusError,
    InsufficientCashError,
    PendingOrdersExistError,
    RateLimitError,
    RebalanceCashShortfallError,
    TransportError,
)
from etoro_bulk_trades.types import (
    AccountSnapshot,
    BulkTradePlan,
    BulkTradeResult,
    BulkTradeSummary,
    CloseIntent,
    Environment,
    InstrumentID,
    InstrumentRef,
    OpenIntent,
    OrderID,
    PositionID,
    RebalanceAction,
    RebalanceDelta,
    RebalancePlan,
    RebalanceResult,
    RebalanceSummary,
    TradeResult,
    TradeStatus,
)

if TYPE_CHECKING:
    from etoro_bulk_trades._http import HttpClient

CENT = Decimal("0.01")
"""All money outputs are floored to cents — eToro's ``Amount`` field is
USD-with-cents; sending more precision is silently truncated."""

OPEN_BUFFER_THRESHOLD = Decimal("0.01")
"""Apply the 1% open buffer when planned post-workflow cash divided by
EQUITY_ANCHOR is below this value."""

OPEN_BUFFER_FACTOR = Decimal("0.99")


def floor_cents(value: Decimal) -> Decimal:
    """Floor to two decimal places (cents). Never rounds up."""
    return value.quantize(CENT, rounding=ROUND_DOWN)


def ceil_cents(value: Decimal) -> Decimal:
    """Round up to cents. Used for close-side amounts that need to over-free
    cash to absorb fees in the rebalance flow."""
    return value.quantize(CENT, rounding=ROUND_UP)


# ── snapshot helpers ────────────────────────────────────────────────────────


async def fetch_snapshot(http: HttpClient, env: Environment) -> AccountSnapshot:
    """Fetch ``/trading/info/{env}/pnl`` and decode into an
    :class:`AccountSnapshot`."""
    body = await http.request("GET", f"/trading/info/{env}/pnl", category="general")
    if not isinstance(body, dict) or "clientPortfolio" not in body:
        raise TransportError(message="Unexpected /pnl response shape")
    return build_snapshot(body["clientPortfolio"], env=env)


# ── at-most-once classifier ────────────────────────────────────────────────


def _classify_open_response(
    intent: OpenIntent,
    instrument_id: InstrumentID,
    requested_amount: Decimal | None,
    body: Any,
) -> TradeResult:
    """Map a 2xx ``market-open-orders/by-amount|by-units`` response into a
    typed :class:`TradeResult` with status ``ok``.

    The wire shape of the success response (per the OpenAPI for
    ``/api/v1/trading/execution/demo/market-open-orders/by-amount``):

    .. code-block:: json

        {
          "orderForOpen": {
            "instrumentID": 100000,
            "amount": 150,
            "isBuy": true,
            "leverage": 1,
            "orderID": 13902598,
            "statusID": 1,
            ...
          },
          "token": "066faaee-..."
        }
    """
    payload = body.get("orderForOpen", {}) if isinstance(body, dict) else {}
    order_id_raw = payload.get("orderID")
    return TradeResult(
        intent=intent,
        instrument_id=instrument_id,
        status="ok",
        order_id=cast(OrderID, int(order_id_raw)) if order_id_raw is not None else None,
        requested_amount=requested_amount,
        filled_amount=(Decimal(str(payload["amount"])) if "amount" in payload else None),
        filled_units=None,  # market-open responses don't include units
    )


def _failed_result(
    intent: OpenIntent | CloseIntent,
    instrument_id: InstrumentID | None,
    requested_amount: Decimal | None,
    *,
    status: TradeStatus,
    error: str,
) -> TradeResult:
    return TradeResult(
        intent=intent,
        instrument_id=instrument_id,
        status=status,
        order_id=None,
        position_id=None,
        requested_amount=requested_amount,
        filled_amount=None,
        filled_units=None,
        error=error,
    )


# ── instrument resolution helper for single trade ──────────────────────────


async def _resolve_one(
    http: HttpClient,
    cache: InstrumentCache,
    instrument: str | int,
) -> InstrumentRef:
    """Resolve a single symbol-or-ID to an :class:`InstrumentRef`."""
    refs = await resolve(http, [instrument], cache=cache)
    return refs[instrument]


# ── single trade ───────────────────────────────────────────────────────────


async def open_trade(
    http: HttpClient,
    *,
    env: Environment,
    intent: OpenIntent,
    cache: InstrumentCache,
    snapshot: AccountSnapshot | None = None,
) -> TradeResult:
    """Execute a single market-open trade.

    Routes to ``/market-open-orders/by-amount`` when ``intent.amount`` is
    set, or ``/market-open-orders/by-units`` when ``intent.units`` is set
    (the validator on :class:`OpenIntent` guarantees exactly one).

    Pre-flight pulls a fresh ``/pnl`` (unless ``snapshot`` is provided) and
    rejects with :class:`InsufficientCashError` if the requested amount
    exceeds Available Cash. The single-trade open-buffer rule shrinks the
    request by 1% when post-trade cash would drop below 1% of equity.
    """
    ref = await _resolve_one(http, cache, intent.instrument)

    requested_amount = intent.amount
    if intent.amount is not None:
        snap = snapshot or await fetch_snapshot(http, env)
        if intent.amount > snap.available_cash:
            raise InsufficientCashError(
                requested=intent.amount,
                available=snap.available_cash,
            )
        # Open buffer (single-trade variant).
        post_cash = snap.available_cash - intent.amount
        if snap.equity > 0 and (post_cash / snap.equity) < OPEN_BUFFER_THRESHOLD:
            requested_amount = floor_cents(intent.amount * OPEN_BUFFER_FACTOR)
        else:
            requested_amount = floor_cents(intent.amount)

    body: dict[str, Any]
    path: str
    if intent.amount is not None:
        body = {
            "InstrumentID": int(ref.instrument_id),
            "IsBuy": intent.is_buy,
            "Leverage": intent.leverage,
            "Amount": float(requested_amount) if requested_amount is not None else None,
        }
        path = f"/trading/execution/{env}/market-open-orders/by-amount"
    else:
        assert intent.units is not None  # validator guarantees
        body = {
            "InstrumentID": int(ref.instrument_id),
            "IsBuy": intent.is_buy,
            "Leverage": intent.leverage,
            "Units": float(intent.units),
        }
        path = f"/trading/execution/{env}/market-open-orders/by-units"

    if intent.stop_loss_rate is not None:
        body["StopLossRate"] = float(intent.stop_loss_rate)
    if intent.take_profit_rate is not None:
        body["TakeProfitRate"] = float(intent.take_profit_rate)
    if intent.trailing_stop_loss:
        body["IsTslEnabled"] = True

    try:
        response = await http.request("POST", path, json=body)
    except HttpStatusError as exc:
        return _failed_result(
            intent,
            ref.instrument_id,
            requested_amount,
            status="failed",
            error=f"HTTP {exc.status_code}: {exc.body}",
        )
    except RateLimitError as exc:
        return _failed_result(
            intent,
            ref.instrument_id,
            requested_amount,
            status="rate_limited_giveup",
            error=str(exc),
        )
    except TransportError as exc:
        # Per at-most-once: timeouts / connection drops are AMBIGUOUS, never
        # retried. Verification reconciles via /pnl.
        return _failed_result(
            intent,
            ref.instrument_id,
            requested_amount,
            status="ambiguous",
            error=str(exc),
        )
    except EtoroSDKError as exc:
        # 401 (InvalidCredentials, SessionExpired) propagates — the caller
        # decides whether to surface "no trade was placed" or stop a batch.
        raise exc

    return _classify_open_response(intent, ref.instrument_id, requested_amount, response)


async def close_trade(
    http: HttpClient,
    *,
    env: Environment,
    intent: CloseIntent,
) -> TradeResult:
    """Execute a single full or partial close by ``position_id``.

    ``UnitsToDeduct: null`` performs a full close; a positive value performs
    a partial close. The position lookup is the caller's responsibility —
    typically read from :class:`AccountSnapshot.positions`.
    """
    body = {"UnitsToDeduct": float(intent.units_to_deduct) if intent.units_to_deduct else None}
    path = f"/trading/execution/{env}/market-close-orders/positions/{int(intent.position_id)}"

    try:
        response = await http.request("POST", path, json=body)
    except HttpStatusError as exc:
        return _failed_result(
            intent,
            None,
            intent.units_to_deduct,
            status="failed",
            error=f"HTTP {exc.status_code}: {exc.body}",
        )
    except RateLimitError as exc:
        return _failed_result(
            intent,
            None,
            intent.units_to_deduct,
            status="rate_limited_giveup",
            error=str(exc),
        )
    except TransportError as exc:
        return _failed_result(
            intent,
            None,
            intent.units_to_deduct,
            status="ambiguous",
            error=str(exc),
        )

    payload = response.get("orderForCloseMultiple", response) if isinstance(response, dict) else {}
    order_id_raw = payload.get("orderID") if isinstance(payload, dict) else None
    return TradeResult(
        intent=intent,
        instrument_id=None,
        status="ok",
        order_id=cast(OrderID, int(order_id_raw)) if order_id_raw is not None else None,
        position_id=cast(PositionID, int(intent.position_id)),
        requested_amount=None,
        filled_amount=None,
        filled_units=intent.units_to_deduct,
    )


# ── bulk trade ─────────────────────────────────────────────────────────────


def _size_bulk_amounts(
    plan: BulkTradePlan,
    *,
    equity_anchor: Decimal,
    cash_anchor: Decimal,
) -> tuple[dict[str | int, Decimal], bool]:
    """Compute per-position USD amounts for a bulk plan.

    Implements the **ceilings** (floor to cents, never round up) and the
    **open buffer** (shrink each amount by 1% if planned post-workflow cash
    drops below 1% of equity).

    Returns ``(amounts, open_buffer_applied)``.
    """
    base_amounts: dict[str | int, Decimal] = {}
    for key, weight in plan.weights.items():
        base_amounts[key] = floor_cents(weight * plan.total_amount)

    total_planned = sum(base_amounts.values(), start=Decimal(0))
    post_cash = cash_anchor - total_planned
    apply_buffer = equity_anchor > 0 and (post_cash / equity_anchor) < OPEN_BUFFER_THRESHOLD
    if apply_buffer:
        return (
            {k: floor_cents(v * OPEN_BUFFER_FACTOR) for k, v in base_amounts.items()},
            True,
        )
    return base_amounts, False


def _summarize_bulk(
    trades: tuple[TradeResult, ...],
    *,
    total_planned: Decimal,
) -> BulkTradeSummary:
    counts: dict[TradeStatus, int] = {}
    filled = Decimal(0)
    pending = Decimal(0)
    failed = Decimal(0)
    for tr in trades:
        counts[tr.status] = counts.get(tr.status, 0) + 1
        amt = tr.filled_amount or tr.requested_amount or Decimal(0)
        if tr.status in ("ok", "filled"):
            filled += amt
        elif tr.status == "pending_market_open":
            pending += amt
        elif tr.status in ("failed", "rate_limited_giveup", "ambiguous", "not_landed"):
            failed += amt
    return BulkTradeSummary(
        total_planned_amount=total_planned,
        total_filled_amount=filled,
        total_pending_amount=pending,
        total_failed_amount=failed,
        counts=counts,
    )


async def _execute_one_open(
    http: HttpClient,
    *,
    env: Environment,
    intent: OpenIntent,
    instrument_id: InstrumentID,
    requested_amount: Decimal,
    is_buy: bool,
    leverage: int,
) -> TradeResult:
    """Single open-by-amount POST inside a bulk loop.

    Distinct from :func:`open_trade` because the bulk loop has already done
    the pre-flight cash + open-buffer math; we just send the POST and
    classify.
    """
    body = {
        "InstrumentID": int(instrument_id),
        "IsBuy": is_buy,
        "Leverage": leverage,
        "Amount": float(requested_amount),
    }
    path = f"/trading/execution/{env}/market-open-orders/by-amount"
    try:
        response = await http.request("POST", path, json=body)
    except AuthError:
        # 401 → bubble up so the bulk loop can stop the entire batch and
        # surface a partial-state report (per execution-invariants §4).
        raise
    except HttpStatusError as exc:
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="failed",
            error=f"HTTP {exc.status_code}: {exc.body}",
        )
    except RateLimitError as exc:
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="rate_limited_giveup",
            error=str(exc),
        )
    except TransportError as exc:
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="ambiguous",
            error=str(exc),
        )
    return _classify_open_response(intent, instrument_id, requested_amount, response)


async def execute_bulk_trade(
    http: HttpClient,
    *,
    env: Environment,
    plan: BulkTradePlan,
    cache: InstrumentCache,
    dry_run: bool = False,
) -> BulkTradeResult:
    """Execute a multi-position open from a single cash pool.

    Workflow:

    1. Resolve every symbol/ID up front; reject the whole plan if any miss.
    2. **Anchor freeze** — read ``/pnl`` once; capture EQUITY_ANCHOR and
       CASH_ANCHOR.
    3. Sufficiency check: ``plan.total_amount <= CASH_ANCHOR``; raise
       :class:`InsufficientCashError` if not.
    4. Size each position via :func:`_size_bulk_amounts` (ceilings + open
       buffer).
    5. Cumulative ``spent_so_far`` discipline before each POST.
    6. Per-trade at-most-once classification via :func:`_classify_open_response`
       and :func:`_failed_result`.
    7. On 401 mid-batch: stop immediately; remaining trades are recorded as
       ``failed`` with a clear "not attempted" error so the result still
       enumerates the entire plan.

    Verification (filled / pending / failed) is performed by
    :func:`etoro_bulk_trades._verify.verify_orders` — call it on the result
    of this function (the ``AsyncBulkTradesClient`` does so by default via
    ``auto_verify=True``).
    """
    refs = await resolve(http, plan.weights.keys(), cache=cache)

    snap = await fetch_snapshot(http, env)
    equity_anchor = snap.equity
    cash_anchor = snap.available_cash

    if plan.total_amount > cash_anchor:
        raise InsufficientCashError(
            requested=plan.total_amount,
            available=cash_anchor,
        )

    amounts, buffer_applied = _size_bulk_amounts(
        plan,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
    )
    total_planned = sum(amounts.values(), start=Decimal(0))

    if dry_run:
        # Build a synthetic result describing what WOULD be sent.
        synthetic_trades: list[TradeResult] = []
        for key, ref in refs.items():
            amt = amounts[key]
            intent = OpenIntent(
                instrument=key,
                amount=amt,
                is_buy=plan.is_buy,
                leverage=plan.leverage,
            )
            synthetic_trades.append(
                TradeResult(
                    intent=intent,
                    instrument_id=ref.instrument_id,
                    status="ok",
                    requested_amount=amt,
                )
            )
        return BulkTradeResult(
            plan=plan,
            env=env,
            equity_anchor=equity_anchor,
            cash_anchor=cash_anchor,
            open_buffer_applied=buffer_applied,
            trades=tuple(synthetic_trades),
            summary=_summarize_bulk(tuple(synthetic_trades), total_planned=total_planned),
        )

    spent_so_far = Decimal(0)
    trades: list[TradeResult] = []
    auth_error: AuthError | None = None

    keys = list(plan.weights.keys())
    for idx, key in enumerate(keys):
        ref = refs[key]
        amt = amounts[key]

        if auth_error is not None:
            # 401 already hit; mark every remaining trade as not-attempted.
            intent = OpenIntent(
                instrument=key,
                amount=amt,
                is_buy=plan.is_buy,
                leverage=plan.leverage,
            )
            trades.append(
                _failed_result(
                    intent,
                    ref.instrument_id,
                    amt,
                    status="failed",
                    error="not attempted (workflow stopped after upstream 401)",
                )
            )
            continue

        # Cumulative ceiling check — should never fire for a valid plan, but
        # surface as failed if it does so the user sees the bug.
        if spent_so_far + amt > total_planned + CENT or spent_so_far + amt > cash_anchor:
            intent = OpenIntent(
                instrument=key,
                amount=amt,
                is_buy=plan.is_buy,
                leverage=plan.leverage,
            )
            trades.append(
                _failed_result(
                    intent,
                    ref.instrument_id,
                    amt,
                    status="failed",
                    error=(
                        f"cumulative ceiling violated at index {idx}: "
                        f"spent={spent_so_far} + next={amt} > planned={total_planned}"
                    ),
                )
            )
            continue

        intent = OpenIntent(
            instrument=key,
            amount=amt,
            is_buy=plan.is_buy,
            leverage=plan.leverage,
        )
        try:
            tr = await _execute_one_open(
                http,
                env=env,
                intent=intent,
                instrument_id=ref.instrument_id,
                requested_amount=amt,
                is_buy=plan.is_buy,
                leverage=plan.leverage,
            )
        except AuthError as exc:
            # Stop the batch.
            auth_error = exc
            trades.append(
                _failed_result(
                    intent,
                    ref.instrument_id,
                    amt,
                    status="failed",
                    error=f"401 stopped the batch: {exc}",
                )
            )
            continue

        trades.append(tr)
        if tr.status in ("ok", "filled"):
            spent_so_far += amt

    result = BulkTradeResult(
        plan=plan,
        env=env,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
        open_buffer_applied=buffer_applied,
        trades=tuple(trades),
        summary=_summarize_bulk(tuple(trades), total_planned=total_planned),
    )

    if auth_error is not None:
        # Bubble the AuthError but attach the partial result so the SDK
        # surface can decide how to present it. We do NOT raise here so the
        # caller can inspect the result; instead the orchestration layer in
        # client.py is responsible for raising if it sees auth-related
        # failures in the summary.
        pass

    return result


# ── rebalance ──────────────────────────────────────────────────────────────


REBALANCE_SETTLEMENT_WAIT_S: float = 10.0
"""Time to wait after Phase-1 closes before re-reading ``/pnl``. The eToro
PnL endpoint caches for 10 seconds per user-per-environment, so a shorter
wait would re-fetch the pre-close numbers and break the Phase-2 sizing."""


def _build_diff(
    plan: RebalancePlan,
    snapshot: AccountSnapshot,
    refs: dict[str | int, InstrumentRef],
    *,
    total_amount: Decimal,
) -> tuple[RebalanceDelta, ...]:
    """Compute the per-instrument diff between current and target allocations.

    The diff covers two domains:

    * **Targets in the plan** — get a ``delta`` of
      ``target_amount - current_amount`` and one of ``open`` / ``increase`` /
      ``reduce`` / ``close`` / ``noop`` based on sign + presence in the
      portfolio.
    * **Excluded current positions** (when ``plan.close_excluded=True``) —
      added with ``target_amount=0`` and action ``close``.
    """
    # Aggregate current positions by instrument_id (a user can hold multiple
    # positions on the same instrument; rebalance treats them as a pool).
    current_by_id: dict[int, Decimal] = {}
    for pos in snapshot.positions:
        if pos.is_mirror:
            # Copy-trading positions are out of scope; never touched.
            continue
        current_by_id[int(pos.instrument_id)] = (
            current_by_id.get(int(pos.instrument_id), Decimal(0)) + pos.amount
        )

    target_by_id: dict[int, tuple[str | int, Decimal]] = {}
    for key, weight in plan.target_weights.items():
        ref = refs[key]
        target = floor_cents(weight * total_amount)
        target_by_id[int(ref.instrument_id)] = (key, target)

    deltas: list[RebalanceDelta] = []

    for iid, (key, target) in target_by_id.items():
        current = current_by_id.get(iid, Decimal(0))
        delta = target - current
        action: RebalanceAction
        if current == 0:
            action = "open" if delta > 0 else "noop"
        elif target == 0:
            action = "close"
        elif delta > 0:
            action = "increase"
        elif delta < 0:
            action = "reduce"
        else:
            action = "noop"
        deltas.append(
            RebalanceDelta(
                instrument=refs[key],
                current_amount=current,
                target_amount=target,
                delta_amount=delta,
                action=action,
            )
        )

    # Excluded positions → close-to-zero.
    if plan.close_excluded:
        target_ids = set(target_by_id.keys())
        held_excluded = [iid for iid in current_by_id if iid not in target_ids]
        # We may not have an InstrumentRef for these (they weren't in the
        # user's plan inputs). Build a synthetic ref so the delta typing
        # holds; the close phase uses position_id, not the ref.
        for iid in held_excluded:
            deltas.append(
                RebalanceDelta(
                    instrument=InstrumentRef(
                        instrument_id=cast(InstrumentID, iid),
                        symbol=f"#{iid}",
                        display_name=f"#{iid}",
                    ),
                    current_amount=current_by_id[iid],
                    target_amount=Decimal(0),
                    delta_amount=-current_by_id[iid],
                    action="close",
                )
            )

    return tuple(deltas)


def _select_positions_for_close(
    snapshot: AccountSnapshot,
    instrument_id: InstrumentID,
    *,
    amount_to_free: Decimal,
    close_buffer_pct: Decimal,
) -> list[tuple[PositionID, Decimal | None]]:
    """Pick which position(s) to close to free ``amount_to_free`` of cash on a
    specific instrument.

    Returns a list of ``(position_id, units_to_deduct)`` tuples:

    * ``units_to_deduct=None`` → full close of that position.
    * ``units_to_deduct=<Decimal>`` → partial close sized to the cleared
      amount plus the close buffer.

    The selection strategy picks positions newest-first (eToro's first-in
    last-out close discipline) until enough cash is freed.
    """
    candidates = [
        p for p in snapshot.positions if p.instrument_id == instrument_id and not p.is_mirror
    ]
    candidates.sort(
        key=lambda p: p.open_date_time or datetime.min.replace(tzinfo=timezone.utc), reverse=True
    )

    out: list[tuple[PositionID, Decimal | None]] = []
    target = ceil_cents(amount_to_free * (Decimal(1) + close_buffer_pct))
    freed = Decimal(0)

    for pos in candidates:
        if freed >= target:
            break
        if pos.amount <= target - freed:
            out.append((pos.position_id, None))
            freed += pos.amount
        else:
            # Partial close — convert the cash target into a units fraction.
            fraction = (target - freed) / pos.amount if pos.amount > 0 else Decimal(0)
            units = ceil_cents(pos.units * fraction) if pos.units > 0 else None
            out.append((pos.position_id, units))
            freed = target
    return out


async def rebalance(
    http: HttpClient,
    *,
    env: Environment,
    plan: RebalancePlan,
    cache: InstrumentCache,
    dry_run: bool = False,
    sleep: Any = None,
) -> RebalanceResult:
    """Rebalance the portfolio to a target allocation.

    Three-phase workflow:

    1. **Pre-check** — read ``/pnl``; if any pending market-open orders
       exist, raise :class:`PendingOrdersExistError` (rebalance refuses to
       race the queue).
    2. **Phase 1 — closes / reduces.** Compute the diff against the snapshot;
       send all ``close`` and ``reduce`` actions first; close-side amounts
       are rounded UP by ``plan.close_buffer_pct`` so per-trade fees can't
       leave a few-dollar shortfall.
    3. **Wait + re-check.** Sleep ``REBALANCE_SETTLEMENT_WAIT_S`` (PnL cache
       window), re-read ``/pnl``, and confirm cash has caught up. If still
       short, raise :class:`RebalanceCashShortfallError` with the partial
       result attached.
    4. **Phase 2 — opens / increases.** Same execution path as
       :func:`execute_bulk_trade`, sized against the re-read cash.

    ``sleep`` is injectable for tests; production code leaves it at
    ``asyncio.sleep``.
    """
    sleep_func = sleep or asyncio.sleep

    pre_snap = await fetch_snapshot(http, env)
    manual_pending = sum(1 for o in pre_snap.pending_orders if o.mirror_id == 0)
    if manual_pending > 0:
        raise PendingOrdersExistError(pending_count=manual_pending)

    refs = await resolve(http, plan.target_weights.keys(), cache=cache)
    total_amount = plan.total_amount if plan.total_amount is not None else pre_snap.equity
    diff = _build_diff(plan, pre_snap, refs, total_amount=total_amount)

    equity_anchor = pre_snap.equity
    cash_anchor = pre_snap.available_cash

    if dry_run:
        return RebalanceResult(
            plan=plan,
            env=env,
            equity_anchor=equity_anchor,
            cash_anchor=cash_anchor,
            diff=diff,
            phase_1_closes=(),
            phase_2_opens=(),
            summary=_summarize_rebalance(diff, (), ()),
        )

    # Phase 1 — closes / reduces.
    phase_1: list[TradeResult] = []
    for delta in diff:
        if delta.action not in ("close", "reduce"):
            continue
        amount_to_free = -delta.delta_amount  # positive
        plans = _select_positions_for_close(
            pre_snap,
            delta.instrument.instrument_id,
            amount_to_free=amount_to_free,
            close_buffer_pct=plan.close_buffer_pct,
        )
        for position_id, units in plans:
            close_intent = CloseIntent(position_id=position_id, units_to_deduct=units)
            try:
                tr = await close_trade(http, env=env, intent=close_intent)
            except AuthError:
                raise
            phase_1.append(tr)

    # Wait for PnL cache + re-read.
    if phase_1:
        await sleep_func(REBALANCE_SETTLEMENT_WAIT_S)
    settled = await fetch_snapshot(http, env)

    # Compute the opens budget. We need enough cash to cover every positive
    # delta in the diff.
    opens_budget = sum(
        (d.delta_amount for d in diff if d.action in ("open", "increase")),
        start=Decimal(0),
    )
    if opens_budget > settled.available_cash:
        # Shortfall after the wait. Surface a partial result so the caller
        # can decide whether to give up the cash they freed.
        partial = RebalanceResult(
            plan=plan,
            env=env,
            equity_anchor=equity_anchor,
            cash_anchor=cash_anchor,
            diff=diff,
            phase_1_closes=tuple(phase_1),
            phase_2_opens=(),
            summary=_summarize_rebalance(diff, tuple(phase_1), ()),
        )
        err = RebalanceCashShortfallError(
            requested=opens_budget,
            available=settled.available_cash,
            message=(
                f"Phase-1 closes settled but available cash {settled.available_cash} "
                f"is below the opens budget {opens_budget}. "
                f"Partial result attached as `partial`."
            ),
        )
        err.partial = partial  # type: ignore[attr-defined]
        raise err

    # Phase 2 — opens / increases.
    phase_2: list[TradeResult] = []
    spent_so_far = Decimal(0)
    for delta in diff:
        if delta.action not in ("open", "increase"):
            continue
        amt = floor_cents(delta.delta_amount)
        if spent_so_far + amt > settled.available_cash:
            phase_2.append(
                _failed_result(
                    OpenIntent(
                        instrument=delta.instrument.symbol or int(delta.instrument.instrument_id),
                        amount=amt,
                        is_buy=plan.is_buy,
                        leverage=plan.leverage,
                    ),
                    delta.instrument.instrument_id,
                    amt,
                    status="failed",
                    error=(
                        "cumulative cash check failed in Phase 2: "
                        f"spent={spent_so_far} + next={amt} > available={settled.available_cash}"
                    ),
                )
            )
            continue
        intent = OpenIntent(
            instrument=delta.instrument.symbol or int(delta.instrument.instrument_id),
            amount=amt,
            is_buy=plan.is_buy,
            leverage=plan.leverage,
        )
        try:
            tr = await _execute_one_open(
                http,
                env=env,
                intent=intent,
                instrument_id=delta.instrument.instrument_id,
                requested_amount=amt,
                is_buy=plan.is_buy,
                leverage=plan.leverage,
            )
        except AuthError:
            phase_2.append(
                _failed_result(
                    intent,
                    delta.instrument.instrument_id,
                    amt,
                    status="failed",
                    error="401 stopped Phase 2",
                )
            )
            break
        phase_2.append(tr)
        if tr.status in ("ok", "filled"):
            spent_so_far += amt

    return RebalanceResult(
        plan=plan,
        env=env,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
        diff=diff,
        phase_1_closes=tuple(phase_1),
        phase_2_opens=tuple(phase_2),
        summary=_summarize_rebalance(diff, tuple(phase_1), tuple(phase_2)),
    )


def _summarize_rebalance(
    diff: tuple[RebalanceDelta, ...],
    phase_1: tuple[TradeResult, ...],
    phase_2: tuple[TradeResult, ...],
) -> RebalanceSummary:
    counts_by_action: dict[RebalanceAction, int] = {}
    for d in diff:
        counts_by_action[d.action] = counts_by_action.get(d.action, 0) + 1

    counts_by_status: dict[TradeStatus, int] = {}
    for tr in (*phase_1, *phase_2):
        counts_by_status[tr.status] = counts_by_status.get(tr.status, 0) + 1

    total_closed = sum(
        (tr.filled_units or tr.requested_amount or Decimal(0) for tr in phase_1),
        start=Decimal(0),
    )
    total_opened = sum(
        (tr.requested_amount or Decimal(0) for tr in phase_2),
        start=Decimal(0),
    )
    return RebalanceSummary(
        counts_by_action=counts_by_action,
        total_closed_amount=total_closed,
        total_opened_amount=total_opened,
        counts_by_status=counts_by_status,
    )


__all__ = [
    "build_snapshot",
    "ceil_cents",
    "close_trade",
    "execute_bulk_trade",
    "fetch_snapshot",
    "floor_cents",
    "open_trade",
    "rebalance",
]
