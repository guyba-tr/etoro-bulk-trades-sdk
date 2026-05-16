"""Unit-test fixtures.

``_no_real_sleep`` is intentionally scoped to ``tests/unit/`` (not the
top-level ``tests/conftest.py``) so it cannot leak into integration tests,
which need real ``asyncio.sleep`` to wait through eToro's PnL cache window
between an order's execution and verification.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make ``asyncio.sleep`` and ``time.sleep`` instantaneous in unit tests
    so we don't accidentally wait on real time."""
    import asyncio
    import time

    async def _instant_async_sleep(delay: float, result: Any = None) -> Any:
        return result

    def _instant_sync_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _instant_async_sleep)
    monkeypatch.setattr(time, "sleep", _instant_sync_sleep)
    yield
