"""etoro-bulk-trades-sdk — async-first Python SDK for the eToro Public API.

Public surface
--------------

* :class:`AsyncBulkTradesClient` — async core; preferred in new code.
* :class:`BulkTradesClient` — sync facade; for scripts and notebooks.

Both expose the same nine methods:

``connect``, ``close``, ``get_account``, ``resolve``, ``open_trade``,
``close_trade``, ``execute_bulk_trade``, ``rebalance``, ``verify_orders``.

All public input / output objects (``OpenIntent``, ``CloseIntent``,
``BulkTradePlan``, ``RebalancePlan``, ``AccountSnapshot``,
``TradeResult``, ``BulkTradeResult``, ``RebalanceResult``,
``ConnectionInfo``, ``TokenPair``, ``InstrumentRef``) are pydantic v2 models
with ``extra='forbid'`` and ``frozen=True``.

Every exception the SDK raises inherits from :class:`EtoroSDKError`.
"""

from __future__ import annotations

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
    InsufficientCashError,
    InvalidCredentialsError,
    PayloadTooLargeError,
    PendingOrdersExistError,
    RateLimitError,
    RebalanceCashShortfallError,
    ResolutionError,
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
    "InstrumentID",
    "InstrumentRef",
    "InsufficientCashError",
    "InvalidCredentialsError",
    "Mirror",
    "MirrorPosition",
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
    "ResolutionError",
    "SessionExpiredError",
    "TokenPair",
    "TradeResult",
    "TradeStatus",
    "TransportError",
    "VerifyMode",
    "__version__",
]
