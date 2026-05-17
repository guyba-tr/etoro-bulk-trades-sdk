"""The eToro at-most-once decision table, as a single Module.

The Public API offers **no idempotency key** for trade-execution POSTs.
Re-sending the same trade after a timeout / 5xx / connection reset can
(and does) create duplicate positions. The
``etoro-api-conventions`` rule (§ "Trade-execution endpoints have NO
idempotency key") therefore prescribes a per-outcome action table:

============================================  =================================
Outcome of one trade-execution POST           Resulting ``TradeStatus``
============================================  =================================
2xx with ``orderId``                          ``ok``
4xx other than 429 (400, 403, 404, …)         ``failed``
401 — credentials invalid                     ``failed`` (next: ``none``)
401 — bearer session expired, refresh failed  ``failed`` (next: ``reauth_required``)
429 after backoff retries                     ``rate_limited_giveup``
5xx with body                                 ``failed``
Timeout / connection reset / parse error      ``ambiguous`` (next: ``reconcile_via_pnl``)
============================================  =================================

Before this module existed the table was enforced in four different
places (the HTTP layer plus three execution wrappers), each with a
slightly different ``try/except`` ladder. Most notably,
:class:`SessionExpiredError` was mislabeled as "auth rejected this POST"
in the close path because the close ladder didn't distinguish it from
:class:`InvalidCredentialsError`. Concentrating the policy here means
adding a new outcome class is one edit + one parameterised test, and
every call site agrees on what each class means.

This module is **execution-scoped**: it only knows about the four trade
POST endpoints. Reads (``/pnl``, ``/market-data/*``, ``/me``) keep
their own simpler error handling — the at-most-once rule doesn't apply
to them because duplicate reads aren't a risk.

Public surface (intentionally tiny):

* :class:`Outcome` — a small value type carrying status + optional
  recovery action + parsed identifiers.
* :func:`classify_open` — outcome for the
  ``/market-open-orders/by-amount`` and ``by-units`` POSTs.
* :func:`classify_close` — outcome for the
  ``/market-close-orders/positions/{positionId}`` POSTs.

Both functions take *one* of ``response`` or ``exception`` (never
both); the caller's only job is to forward the result of its ``try``
block plus the result of its ``except`` blocks. The execution wrappers
in :mod:`_execute` shrink to one shape: "send the POST, hand the
outcome to the classifier, convert ``Outcome → TradeResult``".
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal, cast

from etoro_bulk_trades.errors import (
    AuthError,
    HttpStatusError,
    InvalidCredentialsError,
    RateLimitError,
    SessionExpiredError,
    TransportError,
)
from etoro_bulk_trades.types import OrderID, StrictModel, TradeStatus

NextAction = Literal["none", "reconcile_via_pnl", "reauth_required"]
"""What the caller should consider doing next based on the outcome.

* ``none`` — terminal; the trade either succeeded or was definitively
  rejected. Don't retry.
* ``reconcile_via_pnl`` — outcome was ambiguous. Verifier MUST read
  ``/pnl`` to determine whether the trade landed; never re-fire.
* ``reauth_required`` — the bearer session is dead. Surface
  "Reconnect to eToro"; refreshing a dead session loops indefinitely.
"""


class Outcome(StrictModel):
    """The classifier's verdict on one trade-execution POST.

    Frozen + ``extra='forbid'`` per the project's :class:`StrictModel`
    convention; callers either use the typed fields or fail loudly.
    """

    status: TradeStatus
    error: str | None
    next_action: NextAction
    order_id: OrderID | None
    filled_amount: Decimal | None


def _classify_exception(exc: Exception) -> Outcome:
    """Failure branches of the at-most-once table.

    Shared between :func:`classify_open` and :func:`classify_close` —
    both endpoints have the same failure semantics. The success-parsing
    half differs (``orderForOpen`` vs ``orderForCloseMultiple``), which
    is why the public seam has two ``classify_*`` functions and one
    private helper.
    """
    # Order matters: more specific AuthError subclasses first.
    if isinstance(exc, SessionExpiredError):
        return Outcome(
            status="failed",
            error=f"session expired (refresh failed): {exc}",
            next_action="reauth_required",
            order_id=None,
            filled_amount=None,
        )
    if isinstance(exc, InvalidCredentialsError):
        return Outcome(
            status="failed",
            error=f"401 credentials invalid: {exc}",
            next_action="none",
            order_id=None,
            filled_amount=None,
        )
    if isinstance(exc, AuthError):
        # Unknown auth subclass — be conservative; treat as creds-invalid.
        return Outcome(
            status="failed",
            error=f"401 auth rejected this POST: {exc}",
            next_action="none",
            order_id=None,
            filled_amount=None,
        )
    if isinstance(exc, RateLimitError):
        return Outcome(
            status="rate_limited_giveup",
            error=str(exc),
            next_action="none",
            order_id=None,
            filled_amount=None,
        )
    if isinstance(exc, TransportError):
        # Per at-most-once: timeouts / connection drops are AMBIGUOUS,
        # never retried. The verifier reconciles via /pnl.
        return Outcome(
            status="ambiguous",
            error=str(exc),
            next_action="reconcile_via_pnl",
            order_id=None,
            filled_amount=None,
        )
    if isinstance(exc, HttpStatusError):
        return Outcome(
            status="failed",
            error=f"HTTP {exc.status_code}: {exc.body}",
            next_action="none",
            order_id=None,
            filled_amount=None,
        )
    raise TypeError(
        f"_classify_exception got an unexpected exception type "
        f"{type(exc).__name__}; the at-most-once table covers only the "
        "execution-layer error hierarchy. If you've added a new error "
        "class, add a branch here and a row to the module docstring."
    )


def _ok_open(payload: dict[str, Any]) -> Outcome:
    """Map a 2xx ``market-open-orders/...`` response into an
    :class:`Outcome` with ``status='ok'``.

    Wire shape (per the OpenAPI):

    .. code-block:: json

        {
          "orderForOpen": {
            "instrumentID": 100000,
            "amount": 150,
            "orderID": 13902598,
            "statusID": 1,
            ...
          }
        }
    """
    order_id_raw = payload.get("orderID")
    amount_raw = payload.get("amount")
    return Outcome(
        status="ok",
        error=None,
        next_action="none",
        order_id=(cast(OrderID, int(order_id_raw)) if order_id_raw is not None else None),
        filled_amount=(Decimal(str(amount_raw)) if amount_raw is not None else None),
    )


def _ok_close(payload: dict[str, Any]) -> Outcome:
    """Map a 2xx ``market-close-orders/...`` response into an
    :class:`Outcome` with ``status='ok'``.

    Close responses carry the ``orderID`` under ``orderForCloseMultiple``
    on the documented shape; the implementation also accepts the bare
    ``orderID`` at the top level as a defensive fallback for
    undocumented response variants.
    """
    order_id_raw = payload.get("orderID")
    return Outcome(
        status="ok",
        error=None,
        next_action="none",
        order_id=(cast(OrderID, int(order_id_raw)) if order_id_raw is not None else None),
        filled_amount=None,
    )


def classify_open(
    *,
    response: object | None,
    exception: Exception | None,
) -> Outcome:
    """Classify the outcome of one
    ``/trading/execution/{env}/market-open-orders/by-amount|by-units``
    POST.

    Exactly one of ``response`` (the parsed JSON body returned by
    ``HttpClient.request``) and ``exception`` (whatever ``except``
    branch caught) must be non-``None``.
    """
    if (response is None) == (exception is None):
        raise ValueError("classify_open requires exactly one of {response, exception}")
    if exception is not None:
        return _classify_exception(exception)
    payload = response.get("orderForOpen", {}) if isinstance(response, dict) else {}
    return _ok_open(payload if isinstance(payload, dict) else {})


def classify_close(
    *,
    response: object | None,
    exception: Exception | None,
) -> Outcome:
    """Classify the outcome of one
    ``/trading/execution/{env}/market-close-orders/positions/{positionId}``
    POST. Mirror of :func:`classify_open` for the close-side wire shape.
    """
    if (response is None) == (exception is None):
        raise ValueError("classify_close requires exactly one of {response, exception}")
    if exception is not None:
        return _classify_exception(exception)
    payload: dict[str, Any] = {}
    if isinstance(response, dict):
        nested = response.get("orderForCloseMultiple", response)
        if isinstance(nested, dict):
            payload = nested
    return _ok_close(payload)


__all__ = [
    "NextAction",
    "Outcome",
    "classify_close",
    "classify_open",
]
