"""Optional client-side idempotency for trade-execution POSTs.

The eToro Public API offers **no idempotency key** on its
trade-execution endpoints (see the ``etoro-api-conventions`` rule §
"Trade-execution endpoints have NO idempotency key"). This module is a
**client-side** dedup layer for caller-supplied keys. It is strictly
additive — every entry point is optional with a no-op default, so
existing callers see zero behaviour change.

What it solves
--------------
Re-running the same workflow with the same ``idempotency_key`` returns
the prior :class:`TradeResult` from the store instead of placing a
fresh trade. This protects against the most common real-world cause of
duplicate trades — accidental double-click in a UI, accidental re-run
of a script, retries from a workflow engine.

What it does NOT solve
----------------------
* It does not dedupe ambiguous outcomes. Those are reconciled by the
  verifier reading ``/pnl``; never by re-firing. Only terminal,
  server-confirmed outcomes are cached (see :data:`CACHEABLE_STATUSES`).
* It does not dedupe across two concurrent in-flight POSTs that race
  to the same key. The store is intentionally not locked; if two
  workers fire the same key simultaneously, both POST. Cross-process
  atomicity is the responsibility of the store implementation
  (e.g. Redis ``SETNX`` + TTL). Within a single bulk / rebalance call,
  derived per-trade keys are distinct, so concurrent ``asyncio.gather``
  execution remains safe.

Bulk and rebalance
------------------
Pass a **batch** key on the public method; the module derives a
per-trade key from ``(batch_key, instrument_id)`` for opens and
``(batch_key, "close", position_id)`` for closes. Re-running with the
same batch key skips any trade whose key is already in the store and
POSTs only the missing ones — the natural shape for retrying a partial
bulk.

Public surface
--------------
* :class:`IdempotencyStore` — protocol; bring your own (Redis, SQL,
  Postgres, file-backed, …).
* :class:`NullIdempotencyStore` — default; always misses, never persists.
* :class:`InMemoryIdempotencyStore` — single-process default for opt-in
  callers.
"""

from __future__ import annotations

from typing import Final, Protocol, runtime_checkable

from etoro_bulk_trades.types import (
    InstrumentID,
    PositionID,
    TradeResult,
    TradeStatus,
)

CACHEABLE_STATUSES: Final[frozenset[TradeStatus]] = frozenset(
    {
        # Execution-time success — server confirmed the order.
        "ok",
        # Verifier-confirmed success states.
        "filled",
        "pending_market_open",
        # Hard rejection — server told us "no". Re-firing the same payload
        # would just get the same 4xx, so caching is safe and saves a
        # round-trip.
        "failed",
    }
)
"""Statuses safe to cache. Conspicuously absent:

* ``ambiguous`` — the trade's fate is unknown; the verifier reconciles.
* ``rate_limited_giveup`` — caller may want to retry after the rate-
  limit window resets.
* ``not_landed`` — verifier said it didn't land; caller may want to
  place it deliberately.
"""


def is_cacheable(status: TradeStatus) -> bool:
    """Whether a result with this status should be persisted to the store."""
    return status in CACHEABLE_STATUSES


@runtime_checkable
class IdempotencyStore(Protocol):
    """Where confirmed trade outcomes live for caller-controlled dedup.

    Both methods are ``async`` so HTTP- or Redis-backed implementations
    fit naturally. In-process implementations can simply ``return``
    without ``await``-ing anything.

    Implementations MUST be safe for concurrent use within one
    asyncio loop. They are NOT required to be cross-process atomic;
    callers that need cross-process dedup pick a backing store that
    provides it (e.g. Redis ``SETNX``).
    """

    async def get(self, key: str) -> TradeResult | None:
        """Return the cached result for ``key``, or ``None`` on a miss."""
        ...

    async def put(self, key: str, result: TradeResult) -> None:
        """Persist ``result`` under ``key``. Caller has already checked
        :func:`is_cacheable` on ``result.status``."""
        ...


class NullIdempotencyStore:
    """Default store: always misses, never persists.

    Wiring this as the default means existing callers see no behaviour
    change from the idempotency layer — every ``get`` returns ``None``,
    every ``put`` is a no-op, every trade method behaves exactly as
    before.
    """

    async def get(self, key: str) -> TradeResult | None:
        return None

    async def put(self, key: str, result: TradeResult) -> None:
        return None


class InMemoryIdempotencyStore:
    """In-process dedup store. Lives as long as the instance is held.

    Use for single-process flows (UI clicks, scripts, notebooks); not
    durable across restarts. For crash-safe or multi-process dedup,
    implement :class:`IdempotencyStore` on top of Redis / SQL / a file.
    """

    def __init__(self) -> None:
        self._cache: dict[str, TradeResult] = {}

    async def get(self, key: str) -> TradeResult | None:
        return self._cache.get(key)

    async def put(self, key: str, result: TradeResult) -> None:
        self._cache[key] = result

    def clear(self) -> None:
        """Drop every cached entry. Useful in tests."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


# ── key derivation ────────────────────────────────────────────────────────


_OPEN_PREFIX: Final[str] = "open"
_CLOSE_PREFIX: Final[str] = "close"


def derive_open_key(
    batch_key: str | None,
    instrument_id: InstrumentID | int,
) -> str | None:
    """Per-trade key for a bulk open, derived from a batch-level key.

    Returns ``None`` when ``batch_key`` is ``None`` so a missing key
    cleanly short-circuits the cache lookup at the call site without
    any further branching.
    """
    if batch_key is None:
        return None
    return f"{batch_key}:{_OPEN_PREFIX}:{int(instrument_id)}"


def derive_close_key(
    batch_key: str | None,
    position_id: PositionID | int,
) -> str | None:
    """Per-trade key for a close, derived from a batch-level key.

    Closes target a specific ``position_id`` rather than an instrument —
    a single instrument can have multiple positions, each closed
    independently — so we key on ``position_id``.
    """
    if batch_key is None:
        return None
    return f"{batch_key}:{_CLOSE_PREFIX}:{int(position_id)}"


__all__ = [
    "CACHEABLE_STATUSES",
    "IdempotencyStore",
    "InMemoryIdempotencyStore",
    "NullIdempotencyStore",
    "derive_close_key",
    "derive_open_key",
    "is_cacheable",
]
