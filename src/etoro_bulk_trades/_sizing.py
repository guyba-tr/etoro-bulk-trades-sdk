"""Pure sizing math for trade-execution amounts.

Everything in this module is a pure function of its inputs — no I/O,
no global state. Lifting these helpers out of ``_execute.py`` makes them
the test surface for sizing rules: the existing tests
(``tests/unit/test_bulk_ceilings.py``) cross a *real* interface instead
of reaching into ``_execute``'s private helpers.

Sizing covers three rules that the eToro Public API and the
``etoro-account-snapshot`` rule require:

* **Ceilings, never targets.** ``Amount`` is USD-with-cents; the API
  silently truncates extra precision. Always ``floor`` to cents before
  sending; never ``round`` (which can round up).
* **Close-side ceil.** Rebalance close-amounts are rounded UP by a
  small buffer so per-trade fees can't leave the workflow a few cents
  short of its opens budget.
* **Open buffer (1%).** If a workflow would leave planned post-trade
  cash below 1% of equity, every open amount shrinks by 1% so per-trade
  fees can't push displayed cash negative.

The public surface is intentionally small:

* :func:`floor_cents`, :func:`ceil_cents` — money quantization.
* :data:`CENT`, :data:`OPEN_BUFFER_THRESHOLD`, :data:`OPEN_BUFFER_FACTOR`
  — pinned constants documented by the at-most-once / account-snapshot
  rules.
* :func:`size_bulk_amounts` — applies ceilings + the (vectorized) open
  buffer across a :class:`BulkTradePlan`.
* :func:`apply_open_buffer_single` — the single-trade variant of the
  buffer (one amount in / one amount out) used by ``open_trade``.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Final

from etoro_bulk_trades.types import BulkTradePlan

CENT: Final[Decimal] = Decimal("0.01")
"""All money outputs are floored to cents — eToro's ``Amount`` field is
USD-with-cents; sending more precision is silently truncated."""

OPEN_BUFFER_THRESHOLD: Final[Decimal] = Decimal("0.01")
"""Apply the 1% open buffer when planned post-workflow cash divided by
EQUITY_ANCHOR is below this value."""

OPEN_BUFFER_FACTOR: Final[Decimal] = Decimal("0.99")
"""The factor each open amount is multiplied by when the open buffer
fires (i.e. shrink by 1%)."""


def floor_cents(value: Decimal) -> Decimal:
    """Floor to two decimal places (cents). Never rounds up."""
    return value.quantize(CENT, rounding=ROUND_DOWN)


def ceil_cents(value: Decimal) -> Decimal:
    """Round up to cents. Used for close-side amounts that need to over-free
    cash to absorb fees in the rebalance flow."""
    return value.quantize(CENT, rounding=ROUND_UP)


def apply_open_buffer_single(
    amount: Decimal,
    *,
    equity_anchor: Decimal,
    cash_anchor: Decimal,
) -> tuple[Decimal, bool]:
    """Apply the single-trade open buffer.

    Returns ``(sized_amount, buffer_applied)``. The buffer fires when
    planned post-trade cash drops below 1% of equity; the amount shrinks
    by ``OPEN_BUFFER_FACTOR`` in that case. Either way the result is
    floored to cents.
    """
    post_cash = cash_anchor - amount
    apply_buffer = equity_anchor > 0 and (post_cash / equity_anchor) < OPEN_BUFFER_THRESHOLD
    if apply_buffer:
        return floor_cents(amount * OPEN_BUFFER_FACTOR), True
    return floor_cents(amount), False


def size_bulk_amounts(
    plan: BulkTradePlan,
    *,
    equity_anchor: Decimal,
    cash_anchor: Decimal,
) -> tuple[dict[str | int, Decimal], bool]:
    """Compute per-position USD amounts for a bulk plan.

    Implements **ceilings** (floor to cents, never round up) and the
    **open buffer** (shrink each amount by 1% if planned post-workflow
    cash drops below 1% of equity).

    Returns ``(amounts, open_buffer_applied)``.
    """
    base_amounts: dict[str | int, Decimal] = {
        key: floor_cents(weight * plan.total_amount) for key, weight in plan.weights.items()
    }

    total_planned = sum(base_amounts.values(), start=Decimal(0))
    post_cash = cash_anchor - total_planned
    apply_buffer = equity_anchor > 0 and (post_cash / equity_anchor) < OPEN_BUFFER_THRESHOLD
    if apply_buffer:
        return (
            {k: floor_cents(v * OPEN_BUFFER_FACTOR) for k, v in base_amounts.items()},
            True,
        )
    return base_amounts, False


__all__ = [
    "CENT",
    "OPEN_BUFFER_FACTOR",
    "OPEN_BUFFER_THRESHOLD",
    "apply_open_buffer_single",
    "ceil_cents",
    "floor_cents",
    "size_bulk_amounts",
]
