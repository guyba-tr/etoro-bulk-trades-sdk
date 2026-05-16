"""Test the sliding-window rate limiter under a fake clock."""

from __future__ import annotations

from typing import Any

import pytest

from etoro_bulk_trades._ratelimit import RateLimiter, SlidingWindowLimiter


def _make_fake_clock() -> tuple[list[float], list[float], Any, Any]:
    clock: list[float] = [0.0]
    sleeps: list[float] = []

    def now() -> float:
        return clock[0]

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock[0] += s

    return clock, sleeps, now, fake_sleep


@pytest.mark.asyncio
async def test_capacity_allows_burst() -> None:
    _, sleeps, now, fake_sleep = _make_fake_clock()
    limiter = SlidingWindowLimiter(
        capacity=3, window_s=10.0, _time_func=now, _sleep_func=fake_sleep
    )
    for _ in range(3):
        await limiter.acquire()
    assert sleeps == [], "burst within capacity should not sleep"
    assert limiter.snapshot() == 3


@pytest.mark.asyncio
async def test_4th_request_waits_for_window() -> None:
    clock, sleeps, now, fake_sleep = _make_fake_clock()
    limiter = SlidingWindowLimiter(
        capacity=3, window_s=10.0, _time_func=now, _sleep_func=fake_sleep
    )
    for _ in range(3):
        await limiter.acquire()
    await limiter.acquire()
    assert sleeps == [10.0], "4th request should wait exactly window_s after the 1st"
    assert clock[0] == 10.0
    assert limiter.snapshot() == 1


@pytest.mark.asyncio
async def test_eviction_frees_slots() -> None:
    clock, _, now, fake_sleep = _make_fake_clock()
    limiter = SlidingWindowLimiter(
        capacity=3, window_s=10.0, _time_func=now, _sleep_func=fake_sleep
    )
    for _ in range(3):
        await limiter.acquire()
    clock[0] = 15.0  # window elapsed
    await limiter.acquire()
    assert limiter.snapshot() == 1, "all three originals should have aged out"


@pytest.mark.asyncio
async def test_categories_are_independent() -> None:
    _, sleeps, now, fake_sleep = _make_fake_clock()
    limiter = RateLimiter.default(time_func=now, sleep_func=fake_sleep)
    # Fill execution (20/60s) but general should be untouched.
    for _ in range(20):
        await limiter.acquire("execution")
    for _ in range(60):
        await limiter.acquire("general")
    assert sleeps == [], "neither limiter should sleep at their published capacities"
