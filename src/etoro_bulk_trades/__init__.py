"""etoro-bulk-trades-sdk — async-first Python SDK for the eToro Public API.

Public surface
--------------

* :class:`AsyncBulkTradesClient` — async core; preferred in new code.
* :class:`BulkTradesClient` — sync facade; for scripts and notebooks.

Both expose the same nine methods:

``connect``, ``close``, ``get_account``, ``resolve_instruments``,
``open_trade``, ``close_trade``, ``execute_bulk_trade``, ``rebalance``,
``verify_orders``.

All public input / output objects (``OpenIntent``, ``CloseIntent``,
``BulkTradePlan``, ``RebalancePlan``, ``AccountSnapshot``,
``TradeResult``, ``BulkTradeResult``, ``RebalanceResult``,
``ConnectionInfo``, ``TokenPair``, ``InstrumentRef``) are pydantic v2 models
with ``extra='forbid'`` and ``frozen=True``.

Every exception the SDK raises inherits from :class:`EtoroSDKError`.

Optional client-side idempotency: pass an :class:`IdempotencyStore` to
the constructor and an ``idempotency_key`` to any trade method to dedup
re-runs. :class:`InMemoryIdempotencyStore` is the simplest opt-in;
implement the protocol yourself for Redis / SQL / file-backed stores.
The default :class:`NullIdempotencyStore` is a no-op, so existing
callers see zero behaviour change.
"""

from __future__ import annotations

from etoro_bulk_trades._idempotency import (
    IdempotencyStore,
    InMemoryIdempotencyStore,
    NullIdempotencyStore,
)
from etoro_bulk_trades._sync import BulkTradesClient
from etoro_bulk_trades._version import __version__
from etoro_bulk_trades.client import AsyncBulkTradesClient
from etoro_bulk_trades.errors import (
    AmbiguousTradeError,
    AuthError,
    CeilingViolationError,
    EnvironmentMismatchError,
    EtoroSDKError,
    HttpStatusError,
    InstrumentResolutionError,
    InsufficientCashError,
    InvalidCredentialsError,
    PayloadTooLargeError,
    PendingOrdersExistError,
    RateLimitError,
    RebalanceCashShortfallError,
    SessionExpiredError,
    TransportError,
)
from etoro_bulk_trades.types import (
    AccountSnapshot,
    AuthMode,
    BulkTradePlan,
    BulkTradeResult,
    BulkTradeSummary,
    CloseIntent,
    ConnectionInfo,
    Environment,
    InstrumentID,
    InstrumentRef,
    Mirror,
    MirrorPosition,
    OpenIntent,
    OrderID,
    PendingOrder,
    Position,
    PositionID,
    ProgressEvent,
    RebalanceAction,
    RebalanceDelta,
    RebalancePlan,
    RebalanceResult,
    RebalanceSummary,
    TokenPair,
    TradeResult,
    TradeStatus,
    VerifyMode,
)

__all__ = [
    "AccountSnapshot",
    "AmbiguousTradeError",
    "AsyncBulkTradesClient",
    "AuthError",
    "AuthMode",
    "BulkTradePlan",
    "BulkTradeResult",
    "BulkTradeSummary",
    "BulkTradesClient",
    "CeilingViolationError",
    "CloseIntent",
    "ConnectionInfo",
    "Environment",
    "EnvironmentMismatchError",
    "EtoroSDKError",
    "HttpStatusError",
    "IdempotencyStore",
    "InMemoryIdempotencyStore",
    "InstrumentID",
    "InstrumentRef",
    "InstrumentResolutionError",
    "InsufficientCashError",
    "InvalidCredentialsError",
    "Mirror",
    "MirrorPosition",
    "NullIdempotencyStore",
    "OpenIntent",
    "OrderID",
    "PayloadTooLargeError",
    "PendingOrder",
    "PendingOrdersExistError",
    "Position",
    "PositionID",
    "ProgressEvent",
    "RateLimitError",
    "RebalanceAction",
    "RebalanceCashShortfallError",
    "RebalanceDelta",
    "RebalancePlan",
    "RebalanceResult",
    "RebalanceSummary",
    "SessionExpiredError",
    "TokenPair",
    "TradeResult",
    "TradeStatus",
    "TransportError",
    "VerifyMode",
    "__version__",
]
