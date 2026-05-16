"""Synchronous facade — :class:`BulkTradesClient`.

Mirrors the public surface of :class:`AsyncBulkTradesClient` with blocking
methods. Implementation strategy: a private :class:`asyncio.EventLoop` runs
on a daemon worker thread for the client's lifetime; each call dispatches
to the loop with :func:`asyncio.run_coroutine_threadsafe` and waits on the
future.

We deliberately avoid ``asyncio.run`` per call because:

* It creates a fresh loop for every call (the WebSocket would never be
  reused; the in-memory resolver cache would be wiped).
* It's incompatible with applications that already run an asyncio loop on
  the main thread (Jupyter notebooks, FastAPI middleware, ...).

The worker thread shuts down cleanly on :meth:`close` (or the sync context
manager).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine, Iterable
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import Self

from etoro_bulk_trades.client import AsyncBulkTradesClient
from etoro_bulk_trades.types import (
    AccountSnapshot,
    BulkTradePlan,
    BulkTradeResult,
    CloseIntent,
    ConnectionInfo,
    Environment,
    InstrumentRef,
    OpenIntent,
    RebalancePlan,
    RebalanceResult,
    TokenPair,
    TradeResult,
    VerifyMode,
)

if TYPE_CHECKING:
    from types import TracebackType

T = TypeVar("T")


class BulkTradesClient:
    """Synchronous wrapper around :class:`AsyncBulkTradesClient`.

    Construction uses the same :meth:`from_api_key` / :meth:`from_bearer`
    factories as the async client; under the hood we boot a private event
    loop and proxy each call to it.
    """

    def __init__(self, inner: AsyncBulkTradesClient) -> None:
        self._inner = inner
        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, name="etoro-sdk-loop", daemon=True)
        self._thread.start()
        self._loop_ready.wait()

    # ── loop plumbing ─────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        try:
            self._loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            finally:
                self._loop.close()

    def _submit(self, coro: Coroutine[Any, Any, T]) -> T:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    # ── constructors ──────────────────────────────────────────────────────

    @classmethod
    def from_api_key(
        cls,
        user_key: str,
        *,
        api_key: str | None = None,
    ) -> BulkTradesClient:
        return cls(AsyncBulkTradesClient.from_api_key(user_key, api_key=api_key))

    @classmethod
    def from_bearer(
        cls,
        access_token: str,
        *,
        refresh_token: str | None = None,
        client_id: str | None = None,
        on_token_refresh: Callable[[TokenPair], None] | None = None,
    ) -> BulkTradesClient:
        return cls(
            AsyncBulkTradesClient.from_bearer(
                access_token,
                refresh_token=refresh_token,
                client_id=client_id,
                on_token_refresh=on_token_refresh,
            )
        )

    # ── connection ────────────────────────────────────────────────────────

    def connect(self, env: Environment | None = None) -> ConnectionInfo:
        return self._submit(self._inner.connect(env))

    def close(self) -> None:
        try:
            self._submit(self._inner.close())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5.0)

    # ── reads ─────────────────────────────────────────────────────────────

    def get_account(self) -> AccountSnapshot:
        return self._submit(self._inner.get_account())

    def resolve(
        self,
        symbols_or_ids: Iterable[str | int],
        *,
        force_exact: bool = False,
    ) -> dict[str | int, InstrumentRef]:
        return self._submit(self._inner.resolve(symbols_or_ids, force_exact=force_exact))

    # ── single trade ──────────────────────────────────────────────────────

    def open_trade(self, intent: OpenIntent) -> TradeResult:
        return self._submit(self._inner.open_trade(intent))

    def close_trade(self, intent: CloseIntent) -> TradeResult:
        return self._submit(self._inner.close_trade(intent))

    # ── multi-trade ───────────────────────────────────────────────────────

    def execute_bulk_trade(
        self,
        plan: BulkTradePlan,
        *,
        dry_run: bool = False,
        auto_verify: bool = True,
        verify_mode: VerifyMode = "ws",
        verify_timeout_s: float = 30.0,
    ) -> BulkTradeResult:
        return self._submit(
            self._inner.execute_bulk_trade(
                plan,
                dry_run=dry_run,
                auto_verify=auto_verify,
                verify_mode=verify_mode,
                verify_timeout_s=verify_timeout_s,
            )
        )

    def rebalance(
        self,
        plan: RebalancePlan,
        *,
        dry_run: bool = False,
        auto_verify: bool = True,
        verify_mode: VerifyMode = "ws",
        verify_timeout_s: float = 30.0,
    ) -> RebalanceResult:
        return self._submit(
            self._inner.rebalance(
                plan,
                dry_run=dry_run,
                auto_verify=auto_verify,
                verify_mode=verify_mode,
                verify_timeout_s=verify_timeout_s,
            )
        )

    def verify_orders(
        self,
        result: TradeResult | BulkTradeResult | RebalanceResult,
        *,
        mode: VerifyMode = "ws",
        timeout_s: float = 30.0,
    ) -> TradeResult | BulkTradeResult | RebalanceResult:
        return self._submit(self._inner.verify_orders(result, mode=mode, timeout_s=timeout_s))

    # ── context manager ───────────────────────────────────────────────────

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
