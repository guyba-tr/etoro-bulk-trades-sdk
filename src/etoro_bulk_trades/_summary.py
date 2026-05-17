"""Roll-up summarisers for bulk and rebalance results.

Both :mod:`_execute` (for the initial pre-verification summary) and
:mod:`_verify` (for the rebuild after status upgrades) need the same
roll-up logic. Before this module existed, ``_verify`` reached *into*
``_execute`` and imported private ``_summarize_bulk`` /
``_summarize_rebalance`` helpers — a back-reference that made the two
modules circular in spirit and forced anyone changing "what counts as
filled" to know both files.

Lifting the summarisers here turns that into a real seam: ``_execute``
and ``_verify`` are both *callers* of this module's small public
interface, and neither knows about the other.
"""

from __future__ import annotations

from decimal import Decimal

from etoro_bulk_trades.types import (
    BulkTradeSummary,
    RebalanceAction,
    RebalanceDelta,
    RebalanceSummary,
    TradeResult,
    TradeStatus,
)


def summarize_bulk(
    trades: tuple[TradeResult, ...],
    *,
    total_planned: Decimal,
) -> BulkTradeSummary:
    """Aggregate per-trade results into a :class:`BulkTradeSummary`.

    ``filled`` and ``ok`` both count as filled; ``pending_market_open``
    counts as pending; every failure-like status (``failed``,
    ``rate_limited_giveup``, ``ambiguous``, ``not_landed``) counts as
    failed.
    """
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


def summarize_rebalance(
    diff: tuple[RebalanceDelta, ...],
    phase_1: tuple[TradeResult, ...],
    phase_2: tuple[TradeResult, ...],
) -> RebalanceSummary:
    """Aggregate a rebalance into action counts (from the diff) and status
    counts (from the executed trades)."""
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


__all__ = ["summarize_bulk", "summarize_rebalance"]
