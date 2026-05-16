"""Trade execution — single, bulk, and rebalance.

This module is the orchestration layer: it composes the HTTP client, the
account-snapshot formulas, the resolver, and the at-most-once response
classifier into the four user-visible workflows.

* :func:`open_trade` / :func:`close_trade` — single market open / close.
* :func:`execute_bulk_trade` — multi-position open from one cash pool.
* :func:`rebalance` — close-then-wait-then-open against a target allocation.

All four share the same execution disciplines:

* **Anchor freeze** — read ``/pnl`` once at workflow start; freeze
  ``EQUITY_ANCHOR`` / ``CASH_ANCHOR``; never recompute mid-flow.
* **Ceilings, never targets** — ``amount = floor(weight * total * 100) / 100``;
  the SDK never rounds up.
* **Open buffer** — when the plan would leave cash below 1% of equity,
  shrink each open by 1% so per-trade fees can't push displayed cash
  negative.
* **At-most-once** — every trade-execution POST follows the decision
  table from the ``etoro-api-conventions`` rule (§ "Trade-execution
  endpoints have NO idempotency key"):

  ===========================================  ==============================
  Outcome                                      Status / action
  ===========================================  ==============================
  2xx with ``orderId``                         ``ok`` — done, don't resend
  4xx (other than 429)                         ``failed`` — surface, don't retry
  401                                          ``failed`` (in concurrent gather:
                                               folded per-trade so siblings
                                               aren't cancelled mid-send)
  429 after backoff retries                    ``rate_limited_giveup``
  5xx with body                                ``failed`` (per HTTP layer)
  Timeout / connection reset / parse error    ``ambiguous`` — verifier
                                               reconciles via ``/pnl``
  ===========================================  ==============================

* **Concurrent gather, never cancellation** — :func:`execute_bulk_trade`
  and both :func:`rebalance` phases fire all per-trade POSTs in one
  ``asyncio.gather``. The 20 rpm execution rate-limiter serializes
  underlying sends; the per-task error handlers (in
  :func:`_execute_one_open` and :func:`_execute_one_close`) map every
  failure class onto a typed :class:`TradeResult` status. Cancelling a
  sibling task mid-POST would create at-most-once-violating ambiguity
  (the trade may have reached the server and executed), so no batch
  function ever lets one trade's failure bring down siblings.
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


def _collect_pre_existing_pids(
    snapshot: AccountSnapshot,
    instrument_id: InstrumentID,
) -> tuple[PositionID, ...]:
    """Position IDs the account already holds for ``instrument_id``.

    Mirror positions are excluded (rebalance/open never touches copy
    positions, and the close endpoint we use isn't valid for them
    anyway). The result is stored on :class:`TradeResult` so the verifier
    can later identify which position in ``/pnl`` is the new one.
    """
    return tuple(
        p.position_id
        for p in snapshot.positions
        if int(p.instrument_id) == int(instrument_id) and not p.is_mirror
    )


def _classify_open_response(
    intent: OpenIntent,
    instrument_id: InstrumentID,
    requested_amount: Decimal | None,
    body: Any,
    *,
    pre_existing_position_ids: tuple[PositionID, ...] = (),
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
        pre_existing_position_ids=pre_existing_position_ids,
    )


def _failed_result(
    intent: OpenIntent | CloseIntent,
    instrument_id: InstrumentID | None,
    requested_amount: Decimal | None,
    *,
    status: TradeStatus,
    error: str,
    pre_existing_position_ids: tuple[PositionID, ...] = (),
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
        pre_existing_position_ids=pre_existing_position_ids,
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

    # Always read a snapshot (even on the by-units path) so we can capture
    # which positions for this instrument the account already holds. The
    # verifier needs this set to safely identify the just-opened position
    # without confusing it with pre-existing ones on the same instrument.
    snap = snapshot or await fetch_snapshot(http, env)
    pre_pids = _collect_pre_existing_pids(snap, ref.instrument_id)

    requested_amount = intent.amount
    if intent.amount is not None:
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
            pre_existing_position_ids=pre_pids,
        )
    except RateLimitError as exc:
        return _failed_result(
            intent,
            ref.instrument_id,
            requested_amount,
            status="rate_limited_giveup",
            error=str(exc),
            pre_existing_position_ids=pre_pids,
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
            pre_existing_position_ids=pre_pids,
        )
    except EtoroSDKError as exc:
        # 401 (InvalidCredentials, SessionExpired) propagates — the caller
        # decides whether to surface "no trade was placed" or stop a batch.
        raise exc

    return _classify_open_response(
        intent,
        ref.instrument_id,
        requested_amount,
        response,
        pre_existing_position_ids=pre_pids,
    )


async def _execute_one_close(
    http: HttpClient,
    *,
    env: Environment,
    intent: CloseIntent,
    raise_auth: bool,
) -> TradeResult:
    """Single close POST. Shared between :func:`close_trade` (single,
    ``raise_auth=True``) and the parallel rebalance-Phase-1 close gather
    (``raise_auth=False``, same reasoning as :func:`_execute_one_open`).

    Per the at-most-once decision table, every error class maps to a
    distinct :class:`TradeResult.status`:

    * 4xx → ``failed``
    * 401 → re-raised when ``raise_auth=True``; folded into ``failed``
      otherwise (sibling closes are already in flight; cancellation
      mid-send creates at-most-once-violating ambiguity).
    * 429 (after retries) → ``rate_limited_giveup``
    * Timeout / connection reset → ``ambiguous`` (verifier reconciles
      via ``/pnl``).
    """
    body = {
        "InstrumentID": int(intent.instrument_id),
        "UnitsToDeduct": (float(intent.units_to_deduct) if intent.units_to_deduct else None),
    }
    path = f"/trading/execution/{env}/market-close-orders/positions/{int(intent.position_id)}"

    try:
        response = await http.request("POST", path, json=body)
    except AuthError as exc:
        if raise_auth:
            raise
        return _failed_result(
            intent,
            intent.instrument_id,
            intent.units_to_deduct,
            status="failed",
            error=f"401 (auth rejected this POST): {exc}",
        )
    except HttpStatusError as exc:
        return _failed_result(
            intent,
            intent.instrument_id,
            intent.units_to_deduct,
            status="failed",
            error=f"HTTP {exc.status_code}: {exc.body}",
        )
    except RateLimitError as exc:
        return _failed_result(
            intent,
            intent.instrument_id,
            intent.units_to_deduct,
            status="rate_limited_giveup",
            error=str(exc),
        )
    except TransportError as exc:
        return _failed_result(
            intent,
            intent.instrument_id,
            intent.units_to_deduct,
            status="ambiguous",
            error=str(exc),
        )

    payload = response.get("orderForCloseMultiple", response) if isinstance(response, dict) else {}
    order_id_raw = payload.get("orderID") if isinstance(payload, dict) else None
    return TradeResult(
        intent=intent,
        instrument_id=intent.instrument_id,
        status="ok",
        order_id=cast(OrderID, int(order_id_raw)) if order_id_raw is not None else None,
        position_id=cast(PositionID, int(intent.position_id)),
        requested_amount=None,
        filled_amount=None,
        filled_units=intent.units_to_deduct,
    )


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

    The eToro close endpoint requires both the position ID in the URL **and**
    the matching ``InstrumentID`` in the body as a server-side cross-check;
    omitting it returns ``HTTP 400 -- InstrumentId: The instrument id does
    not exist``. :class:`CloseIntent` enforces both fields.

    For single-trade callers, an ``AuthError`` propagates so the caller can
    decide whether to surface a re-auth flow; the parallel close path used
    by :func:`rebalance` Phase 1 calls :func:`_execute_one_close` directly
    with ``raise_auth=False`` so the gather() doesn't cancel siblings.
    """
    return await _execute_one_close(http, env=env, intent=intent, raise_auth=True)


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
    pre_existing_position_ids: tuple[PositionID, ...] = (),
) -> TradeResult:
    """Single open-by-amount POST for use inside a concurrent bulk gather().

    Distinct from :func:`open_trade` because the bulk caller has already
    done the pre-flight cash + open-buffer math; we just send the POST and
    classify per the eToro at-most-once decision table (see the
    ``etoro-api-conventions`` rule § "Trade-execution endpoints have NO
    idempotency key"):

    * 2xx → ``ok``
    * HTTP 4xx (other than 429/401) → ``failed``
    * 401 → ``failed`` with explicit note. NOT re-raised — we run inside
      ``asyncio.gather(return_exceptions=False)`` and re-raising would
      cancel sibling open-POSTs that are already in flight. Cancellation
      mid-send is *ambiguous* (the trade may have reached the server and
      executed before the cancel landed); at-most-once forbids creating
      ambiguity we don't have to. The caller can detect a 401 outcome by
      filtering ``trades`` for ``error`` strings starting with ``"401"``.
    * 429 after retries → ``rate_limited_giveup``
    * Timeout / connection reset → ``ambiguous`` (verifier reconciles
      via ``/pnl``).

    ``pre_existing_position_ids`` is captured by the caller from the
    anchor snapshot and threaded through so verification can identify
    the new position unambiguously.
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
    except AuthError as exc:
        # See docstring: do NOT re-raise inside a gather() — would cancel
        # in-flight siblings and create at-most-once-violating ambiguity.
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="failed",
            error=f"401 (auth rejected this POST): {exc}",
            pre_existing_position_ids=pre_existing_position_ids,
        )
    except HttpStatusError as exc:
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="failed",
            error=f"HTTP {exc.status_code}: {exc.body}",
            pre_existing_position_ids=pre_existing_position_ids,
        )
    except RateLimitError as exc:
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="rate_limited_giveup",
            error=str(exc),
            pre_existing_position_ids=pre_existing_position_ids,
        )
    except TransportError as exc:
        return _failed_result(
            intent,
            instrument_id,
            requested_amount,
            status="ambiguous",
            error=str(exc),
            pre_existing_position_ids=pre_existing_position_ids,
        )
    return _classify_open_response(
        intent,
        instrument_id,
        requested_amount,
        response,
        pre_existing_position_ids=pre_existing_position_ids,
    )


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

    # Capture which positions the account already holds for each instrument
    # in the plan. Threaded through to every TradeResult so verify_orders
    # can tell a newly-opened position apart from a pre-existing one.
    pre_pids_by_key: dict[str | int, tuple[PositionID, ...]] = {
        key: _collect_pre_existing_pids(snap, refs[key].instrument_id) for key in plan.weights
    }

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
                    pre_existing_position_ids=pre_pids_by_key[key],
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

    # Build the per-key open intents up front so we can fire them all
    # concurrently. No sequential cumulative ``spent_so_far`` check: the
    # plan's ``total_amount`` is already validated against ``cash_anchor``
    # above, and per-trade sizing is deterministic from the anchor — every
    # POST is independent. If something *between* the snapshot and the
    # POSTs eats cash (another process), eToro will return per-trade 400s,
    # which the at-most-once classifier records as ``failed`` without
    # poisoning the rest of the batch.
    keys = list(plan.weights.keys())
    intents: dict[str | int, OpenIntent] = {
        key: OpenIntent(
            instrument=key,
            amount=amounts[key],
            is_buy=plan.is_buy,
            leverage=plan.leverage,
        )
        for key in keys
    }

    # asyncio.gather fans out all POSTs onto the shared rate-limiter
    # (20 rpm execution); concurrent acquires serialize cleanly inside
    # the limiter. We deliberately do NOT use ``return_exceptions=True``
    # AND deliberately fold 401 into a per-trade ``failed`` inside
    # _execute_one_open — both of those exist to preserve at-most-once:
    # cancelling a sibling POST mid-send creates ambiguity we can't
    # reconcile (the trade may have reached the server and executed).
    trades = await asyncio.gather(
        *(
            _execute_one_open(
                http,
                env=env,
                intent=intents[key],
                instrument_id=refs[key].instrument_id,
                requested_amount=amounts[key],
                is_buy=plan.is_buy,
                leverage=plan.leverage,
                pre_existing_position_ids=pre_pids_by_key[key],
            )
            for key in keys
        )
    )

    return BulkTradeResult(
        plan=plan,
        env=env,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
        open_buffer_applied=buffer_applied,
        trades=tuple(trades),
        summary=_summarize_bulk(tuple(trades), total_planned=total_planned),
    )


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

    # Phase 1 — closes / reduces, fired concurrently.
    #
    # All closes from the diff are independent (each targets a distinct
    # ``positionID``); we fire them as one ``asyncio.gather`` to compress
    # wall-clock from O(N) round-trips to O(1) round-trips + whatever the
    # 20 rpm execution rate-limiter forces. ``_execute_one_close`` with
    # ``raise_auth=False`` folds 401 into a per-trade ``failed`` so a
    # single bad credential can't cancel sibling closes (which would
    # leave the SDK in a half-closed state that violates at-most-once on
    # the trades already in flight).
    close_intents: list[CloseIntent] = []
    for delta in diff:
        if delta.action not in ("close", "reduce"):
            continue
        amount_to_free = -delta.delta_amount  # positive
        for position_id, units in _select_positions_for_close(
            pre_snap,
            delta.instrument.instrument_id,
            amount_to_free=amount_to_free,
            close_buffer_pct=plan.close_buffer_pct,
        ):
            close_intents.append(
                CloseIntent(
                    position_id=position_id,
                    instrument_id=delta.instrument.instrument_id,
                    units_to_deduct=units,
                )
            )

    phase_1: tuple[TradeResult, ...] = ()
    if close_intents:
        phase_1 = tuple(
            await asyncio.gather(
                *(
                    _execute_one_close(http, env=env, intent=ci, raise_auth=False)
                    for ci in close_intents
                )
            )
        )

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
            phase_1_closes=phase_1,
            phase_2_opens=(),
            summary=_summarize_rebalance(diff, phase_1, ()),
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

    # Phase 2 — opens / increases, fired concurrently for the same
    # reasons as Phase 1 (see comment above) and as
    # :func:`execute_bulk_trade`. Pre-existing PIDs come from the
    # post-close ``settled`` snapshot so the verifier doesn't
    # mis-attribute a brand-new Phase-2 fill to a position that
    # survived Phase 1. The ``opens_budget`` total has already been
    # validated against ``settled.available_cash`` above; per-trade
    # cumulative checks would be redundant in the concurrent model.
    open_args: list[tuple[OpenIntent, InstrumentID, Decimal, tuple[PositionID, ...]]] = []
    for delta in diff:
        if delta.action not in ("open", "increase"):
            continue
        amt = floor_cents(delta.delta_amount)
        pre_pids = _collect_pre_existing_pids(settled, delta.instrument.instrument_id)
        intent = OpenIntent(
            instrument=delta.instrument.symbol or int(delta.instrument.instrument_id),
            amount=amt,
            is_buy=plan.is_buy,
            leverage=plan.leverage,
        )
        open_args.append((intent, delta.instrument.instrument_id, amt, pre_pids))

    phase_2: tuple[TradeResult, ...] = ()
    if open_args:
        phase_2 = tuple(
            await asyncio.gather(
                *(
                    _execute_one_open(
                        http,
                        env=env,
                        intent=intent,
                        instrument_id=iid,
                        requested_amount=amt,
                        is_buy=plan.is_buy,
                        leverage=plan.leverage,
                        pre_existing_position_ids=pre_pids,
                    )
                    for intent, iid, amt, pre_pids in open_args
                )
            )
        )

    return RebalanceResult(
        plan=plan,
        env=env,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
        diff=diff,
        phase_1_closes=phase_1,
        phase_2_opens=phase_2,
        summary=_summarize_rebalance(diff, phase_1, phase_2),
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
