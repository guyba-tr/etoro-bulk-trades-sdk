"""Public async client — :class:`AsyncBulkTradesClient`.

This is the user-facing surface; everything below it is private. Build with
one of the two constructors and call :meth:`connect` before issuing any
account / trade calls:

.. code-block:: python

    async with AsyncBulkTradesClient.from_api_key(user_key) as client:
        info = await client.connect(env="real")
        snap = await client.get_account()
        ...

The client is **bound to one environment** for its lifetime — calling
``connect`` again with a different ``env`` will raise. Create a second
client (typically with a different credential) if you need to operate on
both.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from typing_extensions import Self

from etoro_bulk_trades._auth import (
    ApiKeyAuth,
    AuthHandle,
    BearerAuth,
    enforce_environment,
)
from etoro_bulk_trades._execute import (
    close_trade as _close_trade_fn,
)
from etoro_bulk_trades._execute import (
    execute_bulk_trade as _execute_bulk_trade_fn,
)
from etoro_bulk_trades._execute import (
    fetch_snapshot,
)
from etoro_bulk_trades._execute import (
    open_trade as _open_trade_fn,
)
from etoro_bulk_trades._execute import (
    rebalance as _rebalance_fn,
)
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._idempotency import (
    IdempotencyStore,
    NullIdempotencyStore,
)
from etoro_bulk_trades._instrument_resolution import (
    InstrumentCache,
)
from etoro_bulk_trades._instrument_resolution import (
    resolve_instruments as _resolve_instruments_fn,
)
from etoro_bulk_trades._verify import verify_orders as _verify_orders_fn
from etoro_bulk_trades.errors import EtoroSDKError
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


class AsyncBulkTradesClient:
    """Async client for the eToro Public API.

    Use :meth:`from_api_key` or :meth:`from_bearer` to construct; :meth:`connect`
    to validate the credential and bind the environment; :meth:`close` (or
    the async context manager) to release resources.
    """

    def __init__(
        self,
        auth: AuthHandle,
        *,
        idempotency_store: IdempotencyStore | None = None,
    ) -> None:
        self._auth = auth
        self._http = HttpClient(auth_provider=auth)
        self._auth.bind(self._http)
        self._cache = InstrumentCache()
        self._env: Environment | None = None
        self._connection: ConnectionInfo | None = None
        # Default to a no-op store so existing callers see zero
        # behavioural change. Callers opt in by passing an
        # :class:`InMemoryIdempotencyStore` (or their own implementation)
        # at construction, or by assigning to the property at any time.
        self._idempotency_store: IdempotencyStore = idempotency_store or NullIdempotencyStore()

    # ── constructors ──────────────────────────────────────────────────────

    @classmethod
    def from_api_key(
        cls,
        user_key: str,
        *,
        api_key: str | None = None,
        idempotency_store: IdempotencyStore | None = None,
    ) -> AsyncBulkTradesClient:
        """Build a client using a per-user ``x-user-key`` (required) and a
        partner ``x-api-key`` (optional; defaults to
        :data:`etoro_bulk_trades._auth.DEFAULT_API_KEY`).

        Pass ``api_key=`` explicitly if your integration uses a dedicated
        partner key.

        ``idempotency_store`` is optional: when ``None`` (the default)
        the client uses :class:`NullIdempotencyStore` and trade methods
        behave exactly as before. Supply any :class:`IdempotencyStore`
        implementation (the SDK ships :class:`InMemoryIdempotencyStore`)
        to enable client-side dedup via the optional ``idempotency_key``
        on trade methods.
        """
        if not user_key:
            raise ValueError("user_key is required")
        auth = (
            ApiKeyAuth(user_key=user_key, api_key=api_key)
            if api_key is not None
            else ApiKeyAuth(user_key=user_key)
        )
        return cls(AuthHandle(auth), idempotency_store=idempotency_store)

    @classmethod
    def from_bearer(
        cls,
        access_token: str,
        *,
        refresh_token: str | None = None,
        client_id: str | None = None,
        on_token_refresh: Callable[[TokenPair], None] | None = None,
        idempotency_store: IdempotencyStore | None = None,
    ) -> AsyncBulkTradesClient:
        """Build a client using OAuth Bearer auth.

        Supply ``refresh_token`` + ``client_id`` to enable in-process refresh
        on 401. ``on_token_refresh`` (if provided) receives the freshly
        rotated :class:`TokenPair` so the application can persist it.

        ``idempotency_store`` mirrors :meth:`from_api_key` — opt-in only,
        with a no-op default.
        """
        if not access_token:
            raise ValueError("access_token is required")
        ctx = BearerAuth(
            access_token=access_token,
            refresh_token=refresh_token,
            client_id=client_id,
            on_token_refresh=on_token_refresh,
        )
        return cls(AuthHandle(ctx), idempotency_store=idempotency_store)

    # ── connection lifecycle ──────────────────────────────────────────────

    async def connect(self, env: Environment | None = None) -> ConnectionInfo:
        """Probe the credential's environment and (optionally) enforce it.

        If ``env`` is supplied and doesn't match what the credential is bound
        to, raises :class:`EnvironmentMismatchError`. Calling :meth:`connect`
        a second time with a *different* ``env`` also raises — clients are
        single-env for their lifetime.

        Also tries to enrich the result with ``gcid`` / ``realCid`` by
        calling ``/api/v1/me`` when available; failures there are non-fatal.
        """
        actual = await enforce_environment(self._http, env)

        if self._env is not None and self._env != actual:
            from etoro_bulk_trades.errors import EnvironmentMismatchError

            raise EnvironmentMismatchError(requested=actual, actual=self._env)

        self._env = actual

        auth_mode = "api_key" if isinstance(self._auth.ctx, ApiKeyAuth) else "bearer"
        gcid = real_cid = None
        try:
            me = await self._http.request("GET", "/me", category="general")
            if isinstance(me, dict):
                gcid_raw = me.get("gcid")
                rcid_raw = me.get("realCid") or me.get("realCID")
                if gcid_raw is not None:
                    gcid = int(gcid_raw)
                if rcid_raw is not None:
                    real_cid = int(rcid_raw)
        except EtoroSDKError:
            # /me coverage is gated by the A5 probe; ignore here.
            pass

        info = ConnectionInfo(
            env=actual,
            auth_mode=auth_mode,  # type: ignore[arg-type]
            gcid=gcid,  # type: ignore[arg-type]
            real_cid=real_cid,  # type: ignore[arg-type]
        )
        self._connection = info
        return info

    async def close(self) -> None:
        """Release HTTP resources. Safe to call multiple times."""
        await self._http.aclose()

    @property
    def env(self) -> Environment:
        """The environment this client is bound to. Raises if not connected."""
        if self._env is None:
            raise RuntimeError("client is not connected; call connect() first")
        return self._env

    @property
    def connection(self) -> ConnectionInfo:
        if self._connection is None:
            raise RuntimeError("client is not connected; call connect() first")
        return self._connection

    @property
    def idempotency_store(self) -> IdempotencyStore:
        """The store backing the optional ``idempotency_key`` parameter on
        trade methods. Defaults to :class:`NullIdempotencyStore` (no-op);
        reassign or supply via the constructor to opt in to client-side
        dedup. See :mod:`etoro_bulk_trades._idempotency` for the full
        contract."""
        return self._idempotency_store

    @idempotency_store.setter
    def idempotency_store(self, store: IdempotencyStore) -> None:
        self._idempotency_store = store

    # ── read calls ────────────────────────────────────────────────────────

    async def get_account(self) -> AccountSnapshot:
        """Fetch a fresh :class:`AccountSnapshot` from the ``/pnl`` endpoint."""
        return await fetch_snapshot(self._http, self.env)

    async def resolve_instruments(
        self,
        symbols_or_ids: Iterable[str | int],
        *,
        force_exact: bool = False,
    ) -> dict[str | int, InstrumentRef]:
        """Resolve a mixed iterable of symbols (uppercased by default) and
        instrument IDs to :class:`InstrumentRef` records. Raises
        :class:`InstrumentResolutionError` if any input misses."""
        _ = self.env  # ensure connected
        return await _resolve_instruments_fn(
            self._http, symbols_or_ids, cache=self._cache, force_exact=force_exact
        )

    # ── single trade ──────────────────────────────────────────────────────

    async def open_trade(
        self,
        intent: OpenIntent,
        *,
        idempotency_key: str | None = None,
    ) -> TradeResult:
        """Execute a single market-open trade and return the typed result.

        Pass ``idempotency_key`` to dedup re-runs through the configured
        :attr:`idempotency_store` (default: no-op). A cache hit returns
        the prior :class:`TradeResult` without resolving, reading
        ``/pnl``, or POSTing.
        """
        return await _open_trade_fn(
            self._http,
            env=self.env,
            intent=intent,
            cache=self._cache,
            idempotency_store=self._idempotency_store,
            idempotency_key=idempotency_key,
        )

    async def close_trade(
        self,
        intent: CloseIntent,
        *,
        idempotency_key: str | None = None,
    ) -> TradeResult:
        """Execute a single full or partial position close.

        ``idempotency_key`` works as on :meth:`open_trade`.
        """
        return await _close_trade_fn(
            self._http,
            env=self.env,
            intent=intent,
            idempotency_store=self._idempotency_store,
            idempotency_key=idempotency_key,
        )

    # ── multi-trade workflows ─────────────────────────────────────────────

    async def execute_bulk_trade(
        self,
        plan: BulkTradePlan,
        *,
        dry_run: bool = False,
        auto_verify: bool = True,
        verify_mode: VerifyMode = "ws",
        verify_timeout_s: float = 30.0,
        idempotency_key: str | None = None,
    ) -> BulkTradeResult:
        """Execute a multi-position open from a single cash pool.

        When ``auto_verify=True`` (default), the result is fed through
        :meth:`verify_orders` to upgrade each per-trade status to ``filled``
        / ``pending_market_open`` / ``not_landed``.

        ``idempotency_key`` here is a **batch** key. The SDK derives a
        stable per-trade key from ``(batch_key, instrument_id)`` for
        each position, so re-running the bulk with the same batch key
        skips trades already in the store and POSTs only the rest.
        """
        result = await _execute_bulk_trade_fn(
            self._http,
            env=self.env,
            plan=plan,
            cache=self._cache,
            dry_run=dry_run,
            idempotency_store=self._idempotency_store,
            idempotency_key=idempotency_key,
        )
        if auto_verify and not dry_run:
            verified = await self.verify_orders(
                result, mode=verify_mode, timeout_s=verify_timeout_s
            )
            assert isinstance(verified, BulkTradeResult)
            return verified
        return result

    async def rebalance(
        self,
        plan: RebalancePlan,
        *,
        dry_run: bool = False,
        auto_verify: bool = True,
        verify_mode: VerifyMode = "ws",
        verify_timeout_s: float = 30.0,
        idempotency_key: str | None = None,
    ) -> RebalanceResult:
        """Two-phase close-then-open to a target allocation. See
        :func:`etoro_bulk_trades._execute.rebalance` for the workflow.

        ``idempotency_key`` is a batch key; Phase-1 closes derive keys
        from ``(batch_key, "close", position_id)`` and Phase-2 opens
        derive from ``(batch_key, "open", instrument_id)``.
        """
        result = await _rebalance_fn(
            self._http,
            env=self.env,
            plan=plan,
            cache=self._cache,
            dry_run=dry_run,
            idempotency_store=self._idempotency_store,
            idempotency_key=idempotency_key,
        )
        if auto_verify and not dry_run:
            verified = await self.verify_orders(
                result, mode=verify_mode, timeout_s=verify_timeout_s
            )
            assert isinstance(verified, RebalanceResult)
            return verified
        return result

    async def verify_orders(
        self,
        result: TradeResult | BulkTradeResult | RebalanceResult,
        *,
        mode: VerifyMode = "ws",
        timeout_s: float = 30.0,
    ) -> TradeResult | BulkTradeResult | RebalanceResult:
        """Upgrade an execution result's per-trade statuses to verified
        statuses (``filled`` / ``pending_market_open`` / ``not_landed``)."""
        return await _verify_orders_fn(
            self._http,
            self._auth,
            result,
            env=self.env,
            mode=mode,
            timeout_s=timeout_s,
        )

    # ── context manager ───────────────────────────────────────────────────

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
