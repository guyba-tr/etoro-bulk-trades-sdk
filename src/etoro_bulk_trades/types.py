"""Public types for the SDK — pydantic v2 models, branded IDs, and enums.

Three principles drive the shape of this module:

* **Branded IDs** — :data:`InstrumentID`, :data:`PositionID`, :data:`OrderID`,
  :data:`CID`, :data:`GCID` are all ``int`` at runtime but distinct to the type
  checker. Stops the most embarrassing class of bug in this domain — passing
  an instrument ID where a position ID was expected (and vice-versa).
* **Pydantic v2 strict models** — every public input/output type derives from
  :class:`StrictModel` with ``extra="forbid"`` (catches typos at construction)
  and ``frozen=True`` (results can't drift between execution and verification).
* **`Decimal` for all money** — the boundary validators coerce ``int`` /
  ``float`` / ``str`` inputs through ``Decimal(str(v))`` so users don't have
  to type ``Decimal("1000.50")`` everywhere, but float-drift bugs are
  prevented internally.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal, NewType

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# ── branded numeric IDs ─────────────────────────────────────────────────────

InstrumentID = NewType("InstrumentID", int)
PositionID = NewType("PositionID", int)
OrderID = NewType("OrderID", int)
CID = NewType("CID", int)
GCID = NewType("GCID", int)


# ── enums (Literal aliases — both mypy and pydantic enforce) ────────────────

Environment = Literal["real", "demo"]
AuthMode = Literal["api_key", "bearer"]
VerifyMode = Literal["auto", "ws", "pnl"]
"""``ws`` is the default — the SDK opens a private WebSocket subscription and
matches by ``OrderID``. On any drop or timeout the dispatch falls back to
``pnl`` (read ``/pnl`` after the 10s cache window)."""

TradeStatus = Literal[
    # produced by execution; not yet verified
    "ok",
    "failed",
    "ambiguous",
    "rate_limited_giveup",
    # produced by verification
    "filled",
    "pending_market_open",
    "not_landed",
]

RebalanceAction = Literal["open", "increase", "reduce", "close", "noop"]


# ── money coercion helper ───────────────────────────────────────────────────


def _coerce_money(v: object) -> Decimal:
    """Accept ``Decimal | int | float | str`` and return a clean ``Decimal``.

    Floats are routed through ``str()`` to dodge ``0.1 + 0.2`` drift.
    """
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        raise TypeError("bool is not a valid money value")
    if isinstance(v, int | float | str):
        return Decimal(str(v))
    raise TypeError(f"cannot coerce {type(v).__name__} to Decimal")


Money = Annotated[Decimal, BeforeValidator(_coerce_money)]
"""Public money type. Use this on any field that represents a USD amount."""


# ── base model ──────────────────────────────────────────────────────────────


class StrictModel(BaseModel):
    """Base for all public models.

    * ``extra="forbid"`` — typos in keyword args fail at construction.
    * ``frozen=True``   — results are immutable; callers can't accidentally
      mutate a ``BulkTradeResult`` between execution and verification.
    * ``strict=True``   — disables pydantic's loose coercions (e.g. ``"5"`` for
      ``int``); each field opts back in via the appropriate annotation
      (e.g. :data:`Money`).
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        arbitrary_types_allowed=False,
    )


# ── auth & connection ──────────────────────────────────────────────────────


class TokenPair(StrictModel):
    """Bearer access + refresh token pair, surfaced to the
    ``on_token_refresh`` callback after a successful refresh."""

    access_token: str
    refresh_token: str
    expires_at: datetime
    token_type: Literal["Bearer"] = "Bearer"


class ConnectionInfo(StrictModel):
    """Result of :meth:`AsyncBulkTradesClient.connect`."""

    env: Environment
    auth_mode: AuthMode
    gcid: GCID | None = None
    """Resolved from ``/api/v1/me`` when available; ``None`` if the call
    isn't supported on this auth mode (verified by the A5 probe)."""
    real_cid: CID | None = None


# ── instrument metadata ─────────────────────────────────────────────────────


class InstrumentImage(StrictModel):
    uri: str
    format: str
    width: int | None = None
    height: int | None = None
    background_color: str | None = None
    text_color: str | None = None


class InstrumentRef(StrictModel):
    instrument_id: InstrumentID
    symbol: str
    """``symbolFull`` from the eToro API."""
    display_name: str
    instrument_type: str | None = None
    exchange_id: int | None = None
    images: tuple[InstrumentImage, ...] = ()


# ── account snapshot ────────────────────────────────────────────────────────


class UnrealizedPnL(StrictModel):
    pnl: Money
    """``unrealizedPnL.pnL`` (lower-n, capital-L) from the PnL endpoint."""


class Position(StrictModel):
    position_id: PositionID
    instrument_id: InstrumentID
    is_buy: bool
    leverage: int = Field(ge=1)
    units: Money
    amount: Money
    """USD margin committed (NOT notional). For unleveraged positions this
    coincides with notional; for leveraged it does not."""
    open_rate: Money
    """In the instrument's NATIVE currency for non-USD instruments — see the
    eToro ``account-snapshot`` rule §2."""
    is_mirror: bool = False
    mirror_id: int = 0
    parent_position_id: PositionID | None = None
    unrealized_pnl: Money = Decimal(0)
    """Flattened from the API's nested ``unrealizedPnL.pnL`` for ergonomics."""
    open_date_time: datetime | None = None


class PendingOrder(StrictModel):
    order_id: OrderID
    instrument_id: InstrumentID
    is_buy: bool
    leverage: int = Field(ge=1)
    amount: Money
    mirror_id: int = 0
    total_external_costs: Money = Decimal(0)


class MirrorPosition(StrictModel):
    position_id: PositionID
    instrument_id: InstrumentID
    amount: Money
    units: Money
    unrealized_pnl: Money = Decimal(0)


class Mirror(StrictModel):
    mirror_id: int
    user_id: CID
    available_amount: Money
    closed_positions_net_profit: Money = Decimal(0)
    positions: tuple[MirrorPosition, ...] = ()


class AccountSnapshot(StrictModel):
    """Computed via the eToro account-snapshot formulas — Available Cash,
    Total Invested, Profit/Loss and Equity. See the
    ``etoro-account-snapshot`` rule for the source-of-truth derivation."""

    env: Environment
    snapshot_at: datetime
    credit: Money
    available_cash: Money
    total_invested: Money
    unrealized_pnl_total: Money
    equity: Money
    positions: tuple[Position, ...] = ()
    pending_orders: tuple[PendingOrder, ...] = ()
    mirrors: tuple[Mirror, ...] = ()


# ── intents (single-trade inputs) ───────────────────────────────────────────


class OpenIntent(StrictModel):
    """A single market-open intent.

    Exactly one of :attr:`amount` (cash) and :attr:`units` (volume) must be
    set. ``leverage`` defaults to ``1`` and is sent explicitly on every POST
    so accidental leverage from API defaults is impossible.
    """

    instrument: str | int
    """A symbol (``"AAPL"``) or an :data:`InstrumentID`. Symbols are
    resolved via ``/market-data/search``; IDs are passed straight through."""

    amount: Money | None = None
    units: Money | None = None
    is_buy: bool = True
    leverage: int = Field(default=1, ge=1, le=400)
    stop_loss_rate: Money | None = None
    take_profit_rate: Money | None = None
    trailing_stop_loss: bool = False

    @model_validator(mode="after")
    def _exactly_one_of_amount_or_units(self) -> OpenIntent:
        if (self.amount is None) == (self.units is None):
            raise ValueError("OpenIntent requires exactly one of `amount` or `units`")
        return self

    @model_validator(mode="after")
    def _amount_or_units_positive(self) -> OpenIntent:
        if self.amount is not None and self.amount <= 0:
            raise ValueError("`amount` must be > 0")
        if self.units is not None and self.units <= 0:
            raise ValueError("`units` must be > 0")
        return self

    @model_validator(mode="after")
    def _sl_tp_positive_if_set(self) -> OpenIntent:
        for name, val in (
            ("stop_loss_rate", self.stop_loss_rate),
            ("take_profit_rate", self.take_profit_rate),
        ):
            if val is not None and val <= 0:
                raise ValueError(f"`{name}` must be > 0; use None to omit")
        return self


class CloseIntent(StrictModel):
    """A single market-close intent.

    ``units_to_deduct=None`` means "close the entire position" (matches
    eToro's ``UnitsToDeduct: null`` payload). A positive value performs a
    partial close.
    """

    position_id: PositionID
    units_to_deduct: Money | None = None

    @field_validator("units_to_deduct")
    @classmethod
    def _positive_if_set(cls, v: Decimal | None) -> Decimal | None:
        if v is not None and v <= 0:
            raise ValueError("`units_to_deduct` must be > 0; use None for full close")
        return v


# ── multi-trade inputs ─────────────────────────────────────────────────────


class BulkTradePlan(StrictModel):
    """A multi-position open plan.

    :attr:`weights` maps each instrument (symbol or ID) to a weight in
    ``(0, 1]``. The sum must be ``≤ 1.0001`` (small slack for float coercion);
    any unallocated weight is treated as cash buffer.

    :attr:`total_amount` is the USD pool to allocate. Each per-instrument
    amount is computed as ``floor(weight * total_amount * 100) / 100`` (cents
    flooring); the SDK never rounds up.
    """

    weights: dict[str | int, Money] = Field(min_length=1)
    total_amount: Money
    is_buy: bool = True
    """Applied to every position in the plan; per-position direction is out
    of scope for v1."""
    leverage: int = Field(default=1, ge=1, le=400)

    @model_validator(mode="after")
    def _weights_valid(self) -> BulkTradePlan:
        if any(w <= 0 for w in self.weights.values()):
            raise ValueError("all weights must be > 0")
        total = sum(self.weights.values(), start=Decimal(0))
        if total > Decimal("1.0001"):
            raise ValueError(f"weights sum to {total}, must be ≤ 1.0")
        return self

    @model_validator(mode="after")
    def _total_amount_positive(self) -> BulkTradePlan:
        if self.total_amount <= 0:
            raise ValueError("`total_amount` must be > 0")
        return self


class RebalancePlan(StrictModel):
    """A target-allocation rebalance plan.

    Internally the SDK computes the diff between the user's current portfolio
    and the target, then runs Phase 1 (closes / reduces) and Phase 2 (opens /
    increases) with a 10s wait in between for the PnL cache to refresh.

    If :attr:`total_amount` is ``None`` (default), the SDK uses the current
    Equity as the rebalance pool.
    """

    target_weights: dict[str | int, Money] = Field(min_length=1)
    total_amount: Money | None = None
    close_excluded: bool = True
    """If True (default), positions not in :attr:`target_weights` are fully
    closed. If False, they remain untouched."""
    close_buffer_pct: Money = Decimal("0.01")
    """Phase-1 closes round UP by this fraction when freeing cash for Phase 2,
    so per-trade fees and unit rounding can't leave a few-dollar shortfall."""
    is_buy: bool = True
    leverage: int = Field(default=1, ge=1, le=400)

    @model_validator(mode="after")
    def _weights_valid(self) -> RebalancePlan:
        if any(w <= 0 for w in self.target_weights.values()):
            raise ValueError("all target weights must be > 0")
        total = sum(self.target_weights.values(), start=Decimal(0))
        if total > Decimal("1.0001"):
            raise ValueError(f"target weights sum to {total}, must be ≤ 1.0")
        return self

    @model_validator(mode="after")
    def _close_buffer_in_range(self) -> RebalancePlan:
        if not (Decimal(0) <= self.close_buffer_pct <= Decimal("0.10")):
            raise ValueError("`close_buffer_pct` must be between 0 and 0.10")
        return self


# ── outputs ────────────────────────────────────────────────────────────────


class TradeResult(StrictModel):
    """Outcome of a single :meth:`open_trade` / :meth:`close_trade` call.

    Statuses progress through verification:

    * ``ok`` — server returned 2xx with an ``orderId``; not yet confirmed
      filled.
    * ``failed`` — server returned an explicit error response.
    * ``ambiguous`` — timeout / connection drop / no response. Reconciled by
      reading ``/pnl`` at verification time; never re-fired.
    * ``rate_limited_giveup`` — ``429`` retried 3 times and still failed.
    * ``filled`` / ``pending_market_open`` / ``not_landed`` — assigned by
      :meth:`verify_orders`.
    """

    intent: OpenIntent | CloseIntent
    instrument_id: InstrumentID | None = None
    """Resolved at execution time; ``None`` only for failures before
    resolution completed."""
    status: TradeStatus
    order_id: OrderID | None = None
    position_id: PositionID | None = None
    requested_amount: Money | None = None
    filled_amount: Money | None = None
    filled_units: Money | None = None
    error: str | None = None


class BulkTradeSummary(StrictModel):
    total_planned_amount: Money
    total_filled_amount: Money
    total_pending_amount: Money
    total_failed_amount: Money
    counts: dict[TradeStatus, int]


class BulkTradeResult(StrictModel):
    plan: BulkTradePlan
    env: Environment
    equity_anchor: Money
    cash_anchor: Money
    open_buffer_applied: bool = False
    trades: tuple[TradeResult, ...]
    summary: BulkTradeSummary


class RebalanceDelta(StrictModel):
    instrument: InstrumentRef
    current_amount: Money
    target_amount: Money
    delta_amount: Money
    """Positive => Phase 2 open/increase; negative => Phase 1 close/reduce."""
    action: RebalanceAction


class RebalanceSummary(StrictModel):
    counts_by_action: dict[RebalanceAction, int]
    total_closed_amount: Money
    total_opened_amount: Money
    counts_by_status: dict[TradeStatus, int]


class RebalanceResult(StrictModel):
    plan: RebalancePlan
    env: Environment
    equity_anchor: Money
    cash_anchor: Money
    diff: tuple[RebalanceDelta, ...]
    phase_1_closes: tuple[TradeResult, ...]
    phase_2_opens: tuple[TradeResult, ...]
    summary: RebalanceSummary


class ProgressEvent(StrictModel):
    """Best-effort progress events for callers that pass a callback into
    multi-trade methods. The SDK does not depend on these for correctness."""

    workflow: Literal["bulk", "rebalance"]
    phase: Literal["plan", "phase_1", "phase_2", "verify"]
    instrument: InstrumentRef | None = None
    status: TradeStatus | None = None
    message: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)
