"""Trade execution — single, bulk, and rebalance.

This module is the **orchestration layer**: it composes the HTTP client,
the at-most-once classifier (:mod:`_at_most_once`), the sizing math
(:mod:`_sizing`), the rebalance planner (:mod:`_rebalance_planning`),
the PnL reader (:mod:`_pnl`), the instrument resolver
(:mod:`_instrument_resolution`), the summariser (:mod:`_summary`), and
the optional client-side idempotency layer (:mod:`_idempotency`) into
the four user-visible workflows:

* :func:`open_trade` / :func:`close_trade` — single market open / close.
* :func:`execute_bulk_trade` — multi-position open from one cash pool.
* :func:`rebalance` — close-then-wait-then-open against a target allocation.

All four share the same execution disciplines:

* **Anchor freeze** — read ``/pnl`` once at the workflow's first I/O step
  and freeze ``EQUITY_ANCHOR`` / ``CASH_ANCHOR``. Sizing math never
  re-reads. ``rebalance`` reads twice on purpose (once before Phase 1,
  once after the cache wait) — each phase has its own anchor.
* **Ceilings, never targets** — see :mod:`_sizing`.
* **Open buffer** — see :mod:`_sizing`.
* **At-most-once** — every trade-execution POST goes through
  :func:`_at_most_once.classify_open` or
  :func:`_at_most_once.classify_close`. That module owns the entire
  decision table; this module only knows "send POST → hand outcome to
  classifier → convert :class:`Outcome` to :class:`TradeResult`".
* **Concurrent gather, never cancellation** — :func:`execute_bulk_trade`
  and both :func:`rebalance` phases fire all per-trade POSTs in one
  ``asyncio.gather``. The 20 rpm execution rate-limiter serialises
  underlying sends. Cancelling a sibling task mid-POST would create
  at-most-once-violating ambiguity (the trade may have reached the
  server and executed), so no batch function ever lets one trade's
  failure bring down siblings. ``raise_auth=True`` on the single-trade
  close path is the one exception, kept so single-trade callers still
  see a typed :class:`AuthError`.
* **Optional client-side idempotency** — every trade method accepts an
  optional ``idempotency_store`` (defaults to a no-op) and
  ``idempotency_key``. If both are set, the layer checks the store before
  POSTing; on a hit returns the cached :class:`TradeResult` and skips
  the POST (and any pre-flight reads). After a POST with a terminal
  status it writes the result back. For bulk / rebalance the caller
  supplies a *batch* key and the layer derives stable per-trade keys.
  See :mod:`_idempotency` for the full discipline.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from etoro_bulk_trades._at_most_once import (
    Outcome,
    classify_close,
    classify_open,
)
from etoro_bulk_trades._idempotency import (
    IdempotencyStore,
    derive_close_key,
    derive_open_key,
    is_cacheable,
)
from etoro_bulk_trades._instrument_resolution import (
    InstrumentCache,
    resolve_instruments,
)
from etoro_bulk_trades._pnl import read_snapshot
from etoro_bulk_trades._rebalance_planning import (
    build_diff,
    select_positions_for_close,
)
from etoro_bulk_trades._sizing import (
    apply_open_buffer_single,
    floor_cents,
    size_bulk_amounts,
)
from etoro_bulk_trades._summary import summarize_bulk, summarize_rebalance
from etoro_bulk_trades.errors import (
    AuthError,
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
    CloseIntent,
    Environment,
    InstrumentID,
    InstrumentRef,
    OpenIntent,
    PositionID,
    RebalancePlan,
    RebalanceResult,
    TradeResult,
)

if TYPE_CHECKING:
    from etoro_bulk_trades._http import HttpClient


# Exceptions that the at-most-once classifier knows how to map. The
# execution wrappers all use this exact tuple in their ``except`` clauses
# so a future error class lands in one place: add it to
# ``_at_most_once._classify_exception`` *and* this tuple.
_CLASSIFIED_EXCEPTIONS: tuple[type[Exception], ...] = (
    AuthError,
    HttpStatusError,
    RateLimitError,
    TransportError,
)

REBALANCE_SETTLEMENT_WAIT_S: float = 10.0
"""Time to wait after Phase-1 closes before re-reading ``/pnl``. The
eToro PnL endpoint caches for 10 seconds per user-per-environment, so a
shorter wait would re-fetch the pre-close numbers and break Phase-2
sizing."""


# ── snapshot helper (thin facade preserved for external callers) ───────────


async def fetch_snapshot(http: HttpClient, env: Environment) -> AccountSnapshot:
    """Fetch ``/trading/info/{env}/pnl`` and decode into an
    :class:`AccountSnapshot`.

    Kept as a one-line passthrough so callers outside this package who
    were importing :func:`fetch_snapshot` directly don't break. New code
    should call :func:`_pnl.read_snapshot` directly.
    """
    return await read_snapshot(http, env)


# ── identity helpers ───────────────────────────────────────────────────────


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


def _trade_result_from_outcome(
    *,
    intent: OpenIntent | CloseIntent,
    instrument_id: InstrumentID | None,
    outcome: Outcome,
    requested_amount: Decimal | None,
    pre_existing_position_ids: tuple[PositionID, ...] = (),
) -> TradeResult:
    """Convert an :class:`Outcome` (from :mod:`_at_most_once`) into a
    :class:`TradeResult` for an open path.

    Centralised so any future field on :class:`TradeResult` only has to
    learn the conversion once.
    """
    return TradeResult(
        intent=intent,
        instrument_id=instrument_id,
        status=outcome.status,
        order_id=outcome.order_id,
        requested_amount=requested_amount,
        filled_amount=outcome.filled_amount,
        filled_units=None,
        error=outcome.error,
        pre_existing_position_ids=pre_existing_position_ids,
    )


def _close_result_from_outcome(
    *,
    intent: CloseIntent,
    outcome: Outcome,
) -> TradeResult:
    """Convert an :class:`Outcome` from a close POST into a
    :class:`TradeResult`. On ``ok`` the ``position_id`` is the same one
    that was closed; on failure it stays ``None``.
    """
    return TradeResult(
        intent=intent,
        instrument_id=intent.instrument_id,
        status=outcome.status,
        order_id=outcome.order_id,
        position_id=(cast(PositionID, int(intent.position_id)) if outcome.status == "ok" else None),
        requested_amount=None,
        filled_amount=None,
        filled_units=intent.units_to_deduct if outcome.status == "ok" else None,
        error=outcome.error,
    )


# ── idempotency helpers ────────────────────────────────────────────────────


async def _idempotency_lookup(
    store: IdempotencyStore | None,
    key: str | None,
) -> TradeResult | None:
    """Return the cached :class:`TradeResult` for ``key`` if both store
    and key are present. Used at the top of every per-trade path so the
    callsite stays a one-liner."""
    if store is None or key is None:
        return None
    return await store.get(key)


async def _idempotency_store_if_cacheable(
    store: IdempotencyStore | None,
    key: str | None,
    result: TradeResult,
) -> None:
    """Persist ``result`` under ``key`` iff both store and key are present
    and the status is in :data:`_idempotency.CACHEABLE_STATUSES`. Ambiguous
    / rate-limited / not-landed outcomes are intentionally **not** stored
    — they must be retryable on re-run."""
    if store is None or key is None:
        return
    if is_cacheable(result.status):
        await store.put(key, result)


# ── instrument resolution helper for single trade ──────────────────────────


async def _resolve_one_instrument(
    http: HttpClient,
    cache: InstrumentCache,
    instrument: str | int,
) -> InstrumentRef:
    """Resolve a single symbol-or-ID to an :class:`InstrumentRef`."""
    refs = await resolve_instruments(http, [instrument], cache=cache)
    return refs[instrument]


# ── single trade ───────────────────────────────────────────────────────────


async def open_trade(
    http: HttpClient,
    *,
    env: Environment,
    intent: OpenIntent,
    cache: InstrumentCache,
    snapshot: AccountSnapshot | None = None,
    idempotency_store: IdempotencyStore | None = None,
    idempotency_key: str | None = None,
) -> TradeResult:
    """Execute a single market-open trade.

    Routes to ``/market-open-orders/by-amount`` when ``intent.amount`` is
    set, or ``/market-open-orders/by-units`` when ``intent.units`` is set
    (the validator on :class:`OpenIntent` guarantees exactly one).

    Pre-flight pulls a fresh ``/pnl`` (unless ``snapshot`` is provided)
    and rejects with :class:`InsufficientCashError` if the requested
    amount exceeds Available Cash. The single-trade open buffer
    (:func:`_sizing.apply_open_buffer_single`) shrinks the request by 1%
    when post-trade cash would drop below 1% of equity.

    Unlike the bulk path, ``open_trade`` re-raises :class:`AuthError`
    (and other classified exceptions) wrapped as a :class:`TradeResult`
    via the classifier — single-trade callers get a ``next_action`` hint
    on the result so they can branch on ``reauth_required`` vs
    ``reconcile_via_pnl`` without parsing strings.

    If both ``idempotency_store`` and ``idempotency_key`` are provided
    and the store already has a result for that key, the cached
    :class:`TradeResult` is returned immediately — no resolve, no
    ``/pnl``, no POST. After a successful POST the result is stored back
    if its status is in :data:`_idempotency.CACHEABLE_STATUSES`.
    """
    # Cache check is the very first I/O — a hit skips resolve + /pnl + POST.
    cached = await _idempotency_lookup(idempotency_store, idempotency_key)
    if cached is not None:
        return cached

    ref = await _resolve_one_instrument(http, cache, intent.instrument)

    # Always read a snapshot so we can capture pre-existing position IDs
    # for the verifier even on the by-units path.
    snap = snapshot or await read_snapshot(http, env)
    pre_pids = _collect_pre_existing_pids(snap, ref.instrument_id)

    requested_amount = intent.amount
    if intent.amount is not None:
        if intent.amount > snap.available_cash:
            raise InsufficientCashError(
                requested=intent.amount,
                available=snap.available_cash,
            )
        requested_amount, _ = apply_open_buffer_single(
            intent.amount,
            equity_anchor=snap.equity,
            cash_anchor=snap.available_cash,
        )

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
        outcome = classify_open(response=response, exception=None)
    except _CLASSIFIED_EXCEPTIONS as exc:
        outcome = classify_open(response=None, exception=exc)

    result = _trade_result_from_outcome(
        intent=intent,
        instrument_id=ref.instrument_id,
        outcome=outcome,
        requested_amount=requested_amount,
        pre_existing_position_ids=pre_pids,
    )
    await _idempotency_store_if_cacheable(idempotency_store, idempotency_key, result)
    return result


async def _execute_one_close(
    http: HttpClient,
    *,
    env: Environment,
    intent: CloseIntent,
    raise_auth: bool,
    idempotency_store: IdempotencyStore | None = None,
    idempotency_key: str | None = None,
) -> TradeResult:
    """Single close POST. Shared between :func:`close_trade`
    (``raise_auth=True``) and the parallel rebalance-Phase-1 close
    gather (``raise_auth=False``).

    Every error class maps to a distinct :class:`TradeResult.status` via
    :func:`_at_most_once.classify_close`. The ``raise_auth`` toggle
    exists for one reason: single-trade callers want a typed
    :class:`AuthError`, but inside a ``gather`` re-raising would cancel
    sibling closes that may already have reached the server, breaking
    at-most-once.

    Caches the outcome under ``idempotency_key`` when both store and key
    are set and the status is cacheable (see :mod:`_idempotency`).
    """
    cached = await _idempotency_lookup(idempotency_store, idempotency_key)
    if cached is not None:
        return cached

    body = {
        "InstrumentID": int(intent.instrument_id),
        "UnitsToDeduct": (float(intent.units_to_deduct) if intent.units_to_deduct else None),
    }
    path = f"/trading/execution/{env}/market-close-orders/positions/{int(intent.position_id)}"

    try:
        response = await http.request("POST", path, json=body)
        outcome = classify_close(response=response, exception=None)
    except _CLASSIFIED_EXCEPTIONS as exc:
        if raise_auth and isinstance(exc, AuthError):
            raise
        outcome = classify_close(response=None, exception=exc)

    result = _close_result_from_outcome(intent=intent, outcome=outcome)
    await _idempotency_store_if_cacheable(idempotency_store, idempotency_key, result)
    return result


async def close_trade(
    http: HttpClient,
    *,
    env: Environment,
    intent: CloseIntent,
    idempotency_store: IdempotencyStore | None = None,
    idempotency_key: str | None = None,
) -> TradeResult:
    """Execute a single full or partial close by ``position_id``.

    ``UnitsToDeduct: null`` performs a full close; a positive value
    performs a partial close. The position lookup is the caller's
    responsibility — typically read from
    :attr:`AccountSnapshot.positions`.

    The eToro close endpoint requires both the position ID in the URL
    **and** the matching ``InstrumentID`` in the body as a server-side
    cross-check; omitting it returns ``HTTP 400 -- InstrumentId: The
    instrument id does not exist``. :class:`CloseIntent` enforces both
    fields.

    For single-trade callers, :class:`AuthError` propagates so the caller
    can decide whether to surface a re-auth flow; the parallel close
    path used by :func:`rebalance` Phase 1 calls
    :func:`_execute_one_close` directly with ``raise_auth=False`` so the
    ``gather`` doesn't cancel siblings.

    Supplying both ``idempotency_store`` and ``idempotency_key`` makes
    re-runs with the same key return the cached result instead of
    POSTing a duplicate close.
    """
    return await _execute_one_close(
        http,
        env=env,
        intent=intent,
        raise_auth=True,
        idempotency_store=idempotency_store,
        idempotency_key=idempotency_key,
    )


# ── bulk trade ─────────────────────────────────────────────────────────────


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
    idempotency_store: IdempotencyStore | None = None,
    idempotency_key: str | None = None,
) -> TradeResult:
    """Single open-by-amount POST for use inside a concurrent bulk gather.

    Distinct from :func:`open_trade` because the bulk caller has already
    done the pre-flight cash + open-buffer math; we just send the POST
    and hand the outcome to :func:`_at_most_once.classify_open`.

    Never re-raises — every error class folds into a
    :class:`TradeResult` so a single failure can't cancel sibling POSTs
    that are already in flight (cancellation mid-send is ambiguous; the
    trade may have reached the server and executed).

    Caches the outcome under ``idempotency_key`` when both store and key
    are set and the status is cacheable.
    """
    cached = await _idempotency_lookup(idempotency_store, idempotency_key)
    if cached is not None:
        return cached

    body = {
        "InstrumentID": int(instrument_id),
        "IsBuy": is_buy,
        "Leverage": leverage,
        "Amount": float(requested_amount),
    }
    path = f"/trading/execution/{env}/market-open-orders/by-amount"

    try:
        response = await http.request("POST", path, json=body)
        outcome = classify_open(response=response, exception=None)
    except _CLASSIFIED_EXCEPTIONS as exc:
        outcome = classify_open(response=None, exception=exc)

    result = _trade_result_from_outcome(
        intent=intent,
        instrument_id=instrument_id,
        outcome=outcome,
        requested_amount=requested_amount,
        pre_existing_position_ids=pre_existing_position_ids,
    )
    await _idempotency_store_if_cacheable(idempotency_store, idempotency_key, result)
    return result


async def execute_bulk_trade(
    http: HttpClient,
    *,
    env: Environment,
    plan: BulkTradePlan,
    cache: InstrumentCache,
    dry_run: bool = False,
    idempotency_store: IdempotencyStore | None = None,
    idempotency_key: str | None = None,
) -> BulkTradeResult:
    """Execute a multi-position open from a single cash pool.

    Workflow:

    1. Resolve every symbol/ID up front; reject the whole plan if any
       miss.
    2. **Anchor freeze** — read ``/pnl`` once; capture EQUITY_ANCHOR and
       CASH_ANCHOR.
    3. Size each position via :func:`_sizing.size_bulk_amounts`
       (ceilings + open buffer).
    4. **Idempotency split** — if both ``idempotency_store`` and a batch
       ``idempotency_key`` are provided, derive per-trade keys
       (``derive_open_key(batch_key, instrument_id)``) and partition the
       plan into already-cached vs to-execute. Cached trades return
       their stored :class:`TradeResult` directly.
    5. Sufficiency check: ``sum(amounts[to_execute]) <= CASH_ANCHOR``.
       The check ignores cached amounts so a partially-executed batch
       can be safely re-run after the cash for that batch has already
       been spent.
    6. Fire every uncached open POST concurrently via ``asyncio.gather``;
       each per-trade outcome is classified independently by the
       at-most-once module and (on cacheable statuses) written back to
       the store under its derived key.
    7. Re-assemble in the original ``plan.weights`` order so callers see
       a stable trades tuple regardless of which were cached.

    Verification (filled / pending / failed) is performed by
    :func:`_verify.verify_orders` — call it on the result (the
    ``AsyncBulkTradesClient`` does so by default via
    ``auto_verify=True``).
    """
    refs = await resolve_instruments(http, plan.weights.keys(), cache=cache)

    snap = await read_snapshot(http, env)
    equity_anchor = snap.equity
    cash_anchor = snap.available_cash

    pre_pids_by_key: dict[str | int, tuple[PositionID, ...]] = {
        key: _collect_pre_existing_pids(snap, refs[key].instrument_id) for key in plan.weights
    }

    amounts, buffer_applied = size_bulk_amounts(
        plan,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
    )
    total_planned = sum(amounts.values(), start=Decimal(0))

    keys = list(plan.weights.keys())

    # Derive a stable per-trade key from the batch key (if any). The
    # instrument_id (not the symbol) anchors the derivation so users
    # who pass "AAPL" in one run and the numeric ID in another still
    # hit the same cache entry.
    per_trade_keys: dict[str | int, str | None] = {
        key: derive_open_key(idempotency_key, refs[key].instrument_id) for key in keys
    }

    # Cache check — collect hits up front so the sufficiency check
    # below knows what's left to spend.
    cached_results: dict[str | int, TradeResult] = {}
    if idempotency_store is not None and idempotency_key is not None and not dry_run:
        for key in keys:
            tk = per_trade_keys[key]
            if tk is None:
                continue
            hit = await idempotency_store.get(tk)
            if hit is not None:
                cached_results[key] = hit

    keys_to_execute = [k for k in keys if k not in cached_results]
    remaining_amount = sum((amounts[k] for k in keys_to_execute), start=Decimal(0))

    # Sufficiency check runs on the *remaining* amount only. A user
    # re-running a partial bulk has already spent the cached trades'
    # cash — counting them again would spuriously trip
    # InsufficientCashError.
    if remaining_amount > cash_anchor:
        raise InsufficientCashError(
            requested=remaining_amount,
            available=cash_anchor,
        )

    if dry_run:
        synthetic_trades: list[TradeResult] = []
        for key in keys:
            ref = refs[key]
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
            summary=summarize_bulk(tuple(synthetic_trades), total_planned=total_planned),
        )

    intents: dict[str | int, OpenIntent] = {
        key: OpenIntent(
            instrument=key,
            amount=amounts[key],
            is_buy=plan.is_buy,
            leverage=plan.leverage,
        )
        for key in keys_to_execute
    }

    new_trades_list = await asyncio.gather(
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
                idempotency_store=idempotency_store,
                idempotency_key=per_trade_keys[key],
            )
            for key in keys_to_execute
        )
    )
    new_by_key: dict[str | int, TradeResult] = dict(
        zip(keys_to_execute, new_trades_list, strict=True)
    )

    # Re-assemble in original order so the trades tuple is deterministic
    # and matches plan.weights iteration order regardless of caching.
    trades: tuple[TradeResult, ...] = tuple(
        cached_results[k] if k in cached_results else new_by_key[k] for k in keys
    )

    return BulkTradeResult(
        plan=plan,
        env=env,
        equity_anchor=equity_anchor,
        cash_anchor=cash_anchor,
        open_buffer_applied=buffer_applied,
        trades=trades,
        summary=summarize_bulk(trades, total_planned=total_planned),
    )


# ── rebalance ──────────────────────────────────────────────────────────────


async def rebalance(
    http: HttpClient,
    *,
    env: Environment,
    plan: RebalancePlan,
    cache: InstrumentCache,
    dry_run: bool = False,
    sleep: Any = None,
    idempotency_store: IdempotencyStore | None = None,
    idempotency_key: str | None = None,
) -> RebalanceResult:
    """Rebalance the portfolio to a target allocation.

    Three-phase workflow:

    1. **Pre-check** — read ``/pnl``; if any pending market-open orders
       exist, raise :class:`PendingOrdersExistError` (rebalance refuses
       to race the queue).
    2. **Phase 1 — closes / reduces.** Compute the diff against the
       snapshot via :func:`_rebalance_planning.build_diff`; send every
       ``close`` and ``reduce`` action concurrently. Close-side amounts
       are rounded UP by ``plan.close_buffer_pct`` so per-trade fees
       can't leave a few-dollar shortfall.
    3. **Wait + re-check.** Sleep ``REBALANCE_SETTLEMENT_WAIT_S`` (PnL
       cache window), re-read ``/pnl``, confirm cash has caught up. If
       still short, raise :class:`RebalanceCashShortfallError` with the
       partial result attached.
    4. **Phase 2 — opens / increases.** Same execution path as
       :func:`execute_bulk_trade`, sized against the re-read cash.

    Optional ``idempotency_store`` + ``idempotency_key`` provide
    Phase-aware dedup: Phase-1 closes derive a key from
    ``(batch_key, "close", position_id)`` and Phase-2 opens derive from
    ``(batch_key, "open", instrument_id)``. Re-running with the same
    batch key skips every trade whose key is already cached and POSTs
    only the rest — the natural shape for retrying a partial rebalance.

    ``sleep`` is injectable for tests; production code leaves it at
    ``asyncio.sleep``.
    """
    sleep_func = sleep or asyncio.sleep

    pre_snap = await read_snapshot(http, env)
    manual_pending = sum(1 for o in pre_snap.pending_orders if o.mirror_id == 0)
    if manual_pending > 0:
        raise PendingOrdersExistError(pending_count=manual_pending)

    refs = await resolve_instruments(http, plan.target_weights.keys(), cache=cache)
    total_amount = plan.total_amount if plan.total_amount is not None else pre_snap.equity
    diff = build_diff(plan, pre_snap, refs, total_amount=total_amount)

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
            summary=summarize_rebalance(diff, (), ()),
        )

    # Phase 1 — closes / reduces, fired concurrently. Each close gets a
    # unique idempotency key (derived from the position_id) so a
    # re-run after a partial close is safe.
    close_intents: list[CloseIntent] = []
    for delta in diff:
        if delta.action not in ("close", "reduce"):
            continue
        amount_to_free = -delta.delta_amount  # positive
        for position_id, units in select_positions_for_close(
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
                    _execute_one_close(
                        http,
                        env=env,
                        intent=ci,
                        raise_auth=False,
                        idempotency_store=idempotency_store,
                        idempotency_key=derive_close_key(idempotency_key, ci.position_id),
                    )
                    for ci in close_intents
                )
            )
        )

    if phase_1:
        await sleep_func(REBALANCE_SETTLEMENT_WAIT_S)
    settled = await read_snapshot(http, env)

    opens_budget = sum(
        (d.delta_amount for d in diff if d.action in ("open", "increase")),
        start=Decimal(0),
    )
    if opens_budget > settled.available_cash:
        partial = RebalanceResult(
            plan=plan,
            env=env,
            equity_anchor=equity_anchor,
            cash_anchor=cash_anchor,
            diff=diff,
            phase_1_closes=phase_1,
            phase_2_opens=(),
            summary=summarize_rebalance(diff, phase_1, ()),
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

    # Phase 2 — opens / increases, fired concurrently. Per-trade keys
    # are derived from instrument_id so the cache hits the same entry
    # regardless of whether the caller used a symbol or numeric ID.
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
                        idempotency_store=idempotency_store,
                        idempotency_key=derive_open_key(idempotency_key, iid),
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
        summary=summarize_rebalance(diff, phase_1, phase_2),
    )


__all__ = [
    "close_trade",
    "execute_bulk_trade",
    "fetch_snapshot",
    "open_trade",
    "rebalance",
]
