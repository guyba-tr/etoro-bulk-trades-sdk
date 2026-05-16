"""Sliding-window rate limiters for the eToro Public API.

The portal documents two limit classes (per
https://api-portal.etoro.com/getting-started/rate-limits):

* **General** — 60 requests / 60s rolling window for read endpoints
  (market-data, ``/pnl``, watchlists, feeds reads, ...).
* **Execution** — 20 requests / 60s for trade-execution and other write
  endpoints (open / close / cancel / limit, watchlist mutations, ...).

The two share no budget; both are tracked per ``x-user-key``.

Implementation
--------------
:class:`SlidingWindowLimiter` keeps a ``deque`` of monotonic send-timestamps
and pops expired entries on each acquire. ``acquire`` records its timestamp
**before** the send completes, so we cap the **send rate** rather than the
completion rate (which is what the server enforces).

The clock is injected via ``time_func`` / ``sleep_func`` so unit tests can
drive deterministic scenarios without real wall-clock waits.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

RateCategory = Literal["general", "execution"]

# Defaults from https://api-portal.etoro.com/getting-started/rate-limits
DEFAULT_GENERAL_CAPACITY: int = 60
DEFAULT_EXECUTION_CAPACITY: int = 20
DEFAULT_WINDOW_S: float = 60.0


@dataclass
class SlidingWindowLimiter:
    """Async-safe sliding-window rate limiter for one category.

    On :meth:`acquire`, the limiter:

    1. Pops timestamps older than ``window_s`` from the head of the deque.
    2. If at capacity, computes the wait until the oldest entry expires and
       sleeps that long.
    3. Records the current timestamp at the tail.

    A single :class:`asyncio.Lock` serializes acquires within one event loop;
    the SDK is documented as **single client per process per key**, so we
    don't try to coordinate across processes here.
    """

    capacity: int
    window_s: float = DEFAULT_WINDOW_S
    _time_func: Callable[[], float] = field(default=time.monotonic)
    _sleep_func: Callable[[float], Awaitable[None]] = field(default=asyncio.sleep)
    _log: deque[float] = field(default_factory=deque)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self) -> None:
        """Block (cooperatively) until a slot is available, then claim it."""
        async with self._lock:
            now = self._time_func()
            self._evict(now)
            if len(self._log) >= self.capacity:
                wait = self._log[0] + self.window_s - now
                if wait > 0:
                    await self._sleep_func(wait)
                now = self._time_func()
                self._evict(now)
            self._log.append(now)

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_s
        while self._log and self._log[0] <= cutoff:
            self._log.popleft()

    def snapshot(self) -> int:
        """How many slots are currently in use (synchronous, for tests)."""
        return len(self._log)


@dataclass
class RateLimiter:
    """Bundle of the two eToro categories on one client instance."""

    general: SlidingWindowLimiter
    execution: SlidingWindowLimiter

    @classmethod
    def default(
        cls,
        *,
        time_func: Callable[[], float] | None = None,
        sleep_func: Callable[[float], Awaitable[None]] | None = None,
    ) -> RateLimiter:
        """Build a limiter with the documented capacities.

        ``time_func`` / ``sleep_func`` are exposed for tests; production code
        should leave them at the defaults (``time.monotonic`` /
        ``asyncio.sleep``).
        """
        kwargs: dict[str, object] = {}
        if time_func is not None:
            kwargs["_time_func"] = time_func
        if sleep_func is not None:
            kwargs["_sleep_func"] = sleep_func
        return cls(
            general=SlidingWindowLimiter(capacity=DEFAULT_GENERAL_CAPACITY, **kwargs),  # type: ignore[arg-type]
            execution=SlidingWindowLimiter(capacity=DEFAULT_EXECUTION_CAPACITY, **kwargs),  # type: ignore[arg-type]
        )

    async def acquire(self, category: RateCategory) -> None:
        if category == "execution":
            await self.execution.acquire()
        else:
            await self.general.acquire()
