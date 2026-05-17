"""Pure planning for rebalance — diffing and close-position selection.

Like :mod:`_sizing`, this module is pure: snapshot + plan → planned
deltas + close intents. No I/O, no global state, no exceptions raised
for transport reasons. Lifting these helpers out of ``_execute.py``
gives them their own test surface (``tests/unit/test_rebalance_diff.py``)
without that test reaching past the execution module's public interface.

Two responsibilities:

* :func:`build_diff` — compute per-instrument :class:`RebalanceDelta`
  records for every target in the plan, plus excluded current positions
  when ``plan.close_excluded`` is set.
* :func:`select_positions_for_close` — pick which position(s) of an
  instrument to (partially or fully) close to free a requested USD
  amount. Newest-first ordering follows eToro's first-in-last-out
  close discipline; the close-side ``ceil`` buffer prevents per-trade
  fees from leaving the workflow a few cents short.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

from etoro_bulk_trades._sizing import ceil_cents, floor_cents
from etoro_bulk_trades.types import (
    AccountSnapshot,
    InstrumentID,
    InstrumentRef,
    PositionID,
    RebalanceAction,
    RebalanceDelta,
    RebalancePlan,
)


def build_diff(
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
    current_by_id: dict[int, Decimal] = {}
    for pos in snapshot.positions:
        if pos.is_mirror:
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

    if plan.close_excluded:
        target_ids = set(target_by_id.keys())
        held_excluded = [iid for iid in current_by_id if iid not in target_ids]
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


def select_positions_for_close(
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

    Newest-first ordering follows eToro's first-in last-out close
    discipline.
    """
    candidates = [
        p for p in snapshot.positions if p.instrument_id == instrument_id and not p.is_mirror
    ]
    candidates.sort(
        key=lambda p: p.open_date_time or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
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
            fraction = (target - freed) / pos.amount if pos.amount > 0 else Decimal(0)
            units = ceil_cents(pos.units * fraction) if pos.units > 0 else None
            out.append((pos.position_id, units))
            freed = target
    return out


__all__ = ["build_diff", "select_positions_for_close"]
