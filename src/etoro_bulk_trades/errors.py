"""Typed exception hierarchy for the SDK.

Every error the SDK raises is a subclass of :class:`EtoroSDKError`, so callers
can broadly ``except EtoroSDKError`` and then narrow as needed. Subclasses
expose structured attributes (`requested`, `actual`, `unresolved`, ...) instead
of stuffing data into the message — so error-handling logic can branch on
fields rather than parse strings.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etoro_bulk_trades.types import Environment, InstrumentRef


class EtoroSDKError(Exception):
    """Root of every exception raised by this SDK."""


# ── auth ────────────────────────────────────────────────────────────────────


class AuthError(EtoroSDKError):
    """Generic authentication or session failure."""


class InvalidCredentialsError(AuthError):
    """The credential was rejected (HTTP 401) and no refresh is possible.

    Raised for API-key auth on any 401, and for Bearer auth when no
    ``refresh_token`` was supplied at construction.
    """


class SessionExpiredError(AuthError):
    """Bearer refresh failed (typically ``400 invalid_grant``).

    The user must re-authorize — surface a ``Reconnect to eToro`` flow rather
    than retrying.
    """


class EnvironmentMismatchError(AuthError):
    """``connect(env=...)`` was called but the credential is bound to the other
    environment.

    eToro user-keys are scoped to one of ``real`` / ``demo`` at creation; you
    cannot reuse a key on the other side.
    """

    def __init__(
        self,
        *,
        requested: Environment,
        actual: Environment,
        message: str | None = None,
    ) -> None:
        self.requested = requested
        self.actual = actual
        super().__init__(
            message
            or (
                f"Credential is bound to {actual!r} environment but {requested!r} "
                f"was requested. Create a new key in eToro Settings > Trading."
            )
        )


# ── transport ───────────────────────────────────────────────────────────────


class RateLimitError(EtoroSDKError):
    """HTTP 429 — rate limit exceeded after the SDK exhausted its retry budget."""

    def __init__(self, *, retry_after_s: float | None = None, message: str | None = None) -> None:
        self.retry_after_s = retry_after_s
        super().__init__(message or "Rate limit exceeded after retries")


class TransportError(EtoroSDKError):
    """5xx response, network error, or timeout that survived the retry layer."""

    def __init__(self, *, status_code: int | None = None, message: str | None = None) -> None:
        self.status_code = status_code
        super().__init__(message or f"Transport error (status={status_code})")


class PayloadTooLargeError(EtoroSDKError):
    """HTTP 413 / 414 — the caller is responsible for shrinking and retrying.

    Used internally by the resolver to drive the adaptive 50 → 25 batch ladder
    on ``/market-data/instruments``.
    """


class HttpStatusError(EtoroSDKError):
    """A 4xx response that doesn't have a more specific subclass."""

    def __init__(
        self,
        *,
        status_code: int,
        body: object | None = None,
        message: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(message or f"HTTP {status_code}: {body!r}")


# ── planning ────────────────────────────────────────────────────────────────


class ResolutionError(EtoroSDKError):
    """One or more symbols / instrument IDs could not be resolved."""

    def __init__(
        self,
        *,
        unresolved: tuple[str | int, ...],
        message: str | None = None,
    ) -> None:
        self.unresolved = unresolved
        super().__init__(message or f"Unresolved instruments: {list(unresolved)}")


class InsufficientCashError(EtoroSDKError):
    """The requested allocation exceeds Available Cash (computed per the
    eToro Available-Cash formula)."""

    def __init__(
        self,
        *,
        requested: Decimal,
        available: Decimal,
        message: str | None = None,
    ) -> None:
        self.requested = requested
        self.available = available
        super().__init__(
            message
            or f"Requested {requested} exceeds available cash {available} "
            "(per eToro Available-Cash formula)"
        )


class PendingOrdersExistError(EtoroSDKError):
    """``rebalance()`` refuses to run while pending market-open orders exist."""

    def __init__(self, *, pending_count: int, message: str | None = None) -> None:
        self.pending_count = pending_count
        super().__init__(
            message
            or f"{pending_count} pending market-open orders exist; "
            "settle or cancel before rebalancing."
        )


class RebalanceCashShortfallError(InsufficientCashError):
    """Phase-1 closes did not free enough cash for Phase-2 opens.

    Typically caused by close-side fees larger than the close_buffer or by an
    unsettled close still in ``ordersForOpen[]`` when Phase 2 begins.
    """


# ── execution ───────────────────────────────────────────────────────────────


class CeilingViolationError(EtoroSDKError):
    """Post-fill verification: an ``actual_amount`` exceeds the planned ceiling.

    Indicates an SDK-side sizing bug (the API returned a fill larger than the
    request). Surface and offer a corrective partial close.
    """

    def __init__(
        self,
        *,
        instrument: InstrumentRef,
        expected: Decimal,
        actual: Decimal,
        message: str | None = None,
    ) -> None:
        self.instrument = instrument
        self.expected = expected
        self.actual = actual
        super().__init__(
            message
            or f"Ceiling violation on {instrument.symbol}: actual {actual} > expected {expected}"
        )


class AmbiguousTradeError(EtoroSDKError):
    """Raised only in opt-in strict mode to surface an unreconciled
    ambiguous trade outcome.

    By default the SDK reconciles ambiguous trades silently via ``/pnl``; this
    error is for callers that want to fail hard rather than see ``not_landed``.
    """

    def __init__(
        self,
        *,
        instrument_id: int,
        order_id: int | None,
        last_response: str | None = None,
    ) -> None:
        self.instrument_id = instrument_id
        self.order_id = order_id
        self.last_response = last_response
        super().__init__(
            f"Ambiguous trade outcome on instrument_id={instrument_id} (order_id={order_id})"
        )
