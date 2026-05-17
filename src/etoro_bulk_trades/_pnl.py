"""Single seam for everything ``/trading/info/{env}/pnl``-related.

This module is the only place in the SDK that knows the wire shape of
the PnL endpoint, the 10-second per-user cache window, and the
environment-segmented path. Both :mod:`_execute` (for anchor reads,
single-trade pre-flight, and post-Phase-1 settlement) and :mod:`_verify`
(for the post-execution reconciliation) go through it.

Public surface:

* :data:`PNL_CACHE_WINDOW_S` — the documented cache TTL.
* :func:`read_snapshot` — GET ``/pnl`` + decode into
  :class:`AccountSnapshot`. Raises :class:`TransportError` if the
  response shape is wrong, so every caller gets the same typed failure.
* :func:`classify_positions` — extract the three sets/maps the verifier
  needs from a snapshot: filled instrument IDs, pending order IDs, and
  instrument → list-of-position-IDs.
* :func:`wait_for_cache` — small helper around ``sleeper(PNL_CACHE_WINDOW_S)``
  so callers don't have to import the constant separately.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Final, NamedTuple

from etoro_bulk_trades._account import build_snapshot
from etoro_bulk_trades.errors import TransportError
from etoro_bulk_trades.types import AccountSnapshot, Environment

if TYPE_CHECKING:
    from etoro_bulk_trades._http import HttpClient


SleepFn = Callable[[float], Awaitable[None]]

PNL_CACHE_WINDOW_S: Final[float] = 10.0
"""The eToro PnL endpoint caches per-user-per-environment for ~10
seconds. Reads inside that window after a write return the *pre-write*
snapshot; reconciliation has to wait this long before the second read."""


class ClassifiedPositions(NamedTuple):
    """The verifier-friendly projection of an :class:`AccountSnapshot`.

    * ``filled_instrument_ids`` — every non-mirror ``instrument_id`` the
      account currently holds. Used to flip ``ok``/``ambiguous`` trades
      to ``filled``.
    * ``pending_order_ids`` — every ``order_id`` in ``ordersForOpen[]``.
      Used to flip ``ok``/``ambiguous`` to ``pending_market_open`` when
      the market is closed and the open is scheduled.
    * ``instrument_to_position_ids`` — full list of non-mirror
      ``position_id`` values per instrument. Used by the position-novelty
      check that prevents the verifier from claiming a pre-existing
      position as the result of a new trade.
    """

    filled_instrument_ids: set[int]
    pending_order_ids: set[int]
    instrument_to_position_ids: dict[int, list[int]]


async def read_snapshot(http: HttpClient, env: Environment) -> AccountSnapshot:
    """GET ``/trading/info/{env}/pnl`` and decode into an
    :class:`AccountSnapshot`. Raises :class:`TransportError` if the
    response is missing ``clientPortfolio``."""
    body = await http.request("GET", f"/trading/info/{env}/pnl", category="general")
    if not isinstance(body, dict) or "clientPortfolio" not in body:
        raise TransportError(message="Unexpected /pnl response shape")
    return build_snapshot(body["clientPortfolio"], env=env)


def classify_positions(snapshot: AccountSnapshot) -> ClassifiedPositions:
    """Extract the projection :func:`_verify._verify_trades` consumes.

    Mirror positions are excluded — the verifier never assigns a
    just-opened trade to a copy-trading position.
    """
    filled_iids: set[int] = {int(p.instrument_id) for p in snapshot.positions if not p.is_mirror}
    pending_oids: set[int] = {int(o.order_id) for o in snapshot.pending_orders}
    iid_to_pids: dict[int, list[int]] = {}
    for p in snapshot.positions:
        if p.is_mirror:
            continue
        iid_to_pids.setdefault(int(p.instrument_id), []).append(int(p.position_id))
    return ClassifiedPositions(filled_iids, pending_oids, iid_to_pids)


async def wait_for_cache(sleeper: SleepFn | None = None) -> None:
    """Sleep ``PNL_CACHE_WINDOW_S``. Centralised so callers can't drift on
    the constant or forget to wait between a write and a reconciling
    read."""
    await (sleeper or asyncio.sleep)(PNL_CACHE_WINDOW_S)


__all__ = [
    "PNL_CACHE_WINDOW_S",
    "ClassifiedPositions",
    "SleepFn",
    "classify_positions",
    "read_snapshot",
    "wait_for_cache",
]
