"""Tests for the optional client-side idempotency layer.

The layer is **opt-in** and **strictly additive**: every existing call
without an ``idempotency_key`` must behave exactly as before. With a
key, the caller gets:

* Single trade — cache hit returns the prior :class:`TradeResult`
  without resolving, reading ``/pnl``, or POSTing.
* Bulk / rebalance — a *batch* key splits into cached vs uncached;
  only uncached trades POST; cached trades are merged back in order.
* Terminal-status gating — ``ambiguous`` / ``rate_limited_giveup`` /
  ``not_landed`` are intentionally **not** cached so callers can
  retry them safely.

Each test pins one of those invariants.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._execute import (
    close_trade,
    execute_bulk_trade,
    open_trade,
)
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._idempotency import (
    CACHEABLE_STATUSES,
    InMemoryIdempotencyStore,
    NullIdempotencyStore,
    derive_close_key,
    derive_open_key,
    is_cacheable,
)
from etoro_bulk_trades._instrument_resolution import InstrumentCache
from etoro_bulk_trades.types import (
    BulkTradePlan,
    CloseIntent,
    InstrumentID,
    InstrumentRef,
    OpenIntent,
    OrderID,
    PositionID,
    TradeResult,
    TradeStatus,
)

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"


# ── helpers ───────────────────────────────────────────────────────────────


def _make_http() -> HttpClient:
    handle = AuthHandle(ApiKeyAuth(api_key="k", user_key="u"))
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    return http


def _seed(cache: InstrumentCache, symbol: str, iid: int) -> None:
    cache.put(
        InstrumentRef(
            instrument_id=InstrumentID(iid),
            symbol=symbol,
            display_name=symbol,
        )
    )


def _empty_portfolio() -> dict[str, object]:
    return {
        "clientPortfolio": {
            "credit": 100_000.0,
            "positions": [],
            "orders": [],
            "ordersForOpen": [],
            "mirrors": [],
        }
    }


# ── pure-unit tests for the module itself ─────────────────────────────────


class TestCacheableStatuses:
    """``CACHEABLE_STATUSES`` is the explicit allow-list; everything else
    must miss so it can be retried."""

    @pytest.mark.parametrize(
        "status",
        ["ok", "filled", "pending_market_open", "failed"],
    )
    def test_terminal_statuses_cache(self, status: TradeStatus) -> None:
        assert is_cacheable(status)
        assert status in CACHEABLE_STATUSES

    @pytest.mark.parametrize(
        "status",
        ["ambiguous", "rate_limited_giveup", "not_landed"],
    )
    def test_recoverable_statuses_do_not_cache(self, status: TradeStatus) -> None:
        # If any of these slipped into the allow-list, a re-run with the
        # same idempotency_key would silently skip a trade that the
        # caller intended to retry — a serious correctness bug.
        assert not is_cacheable(status)
        assert status not in CACHEABLE_STATUSES


class TestKeyDerivation:
    """Per-trade keys must be deterministic and uniformly derived from
    the (batch_key, identifier) pair regardless of whether the caller
    passed a symbol or a numeric ID upstream."""

    def test_open_key_uses_instrument_id(self) -> None:
        assert derive_open_key("batch-1", InstrumentID(100000)) == "batch-1:open:100000"

    def test_close_key_uses_position_id(self) -> None:
        assert derive_close_key("batch-1", PositionID(42)) == "batch-1:close:42"

    def test_open_and_close_namespaces_do_not_collide(self) -> None:
        # If a single batch happens to have an opening trade for an
        # instrument whose numeric ID equals a closing position_id,
        # both must still map to distinct cache slots.
        assert derive_open_key("b", InstrumentID(7)) != derive_close_key("b", PositionID(7))

    def test_none_batch_key_short_circuits(self) -> None:
        # ``None`` batch key disables derivation entirely so the callsite
        # can use the same ``if key is None`` branch as for an absent
        # store, with no extra special casing.
        assert derive_open_key(None, InstrumentID(100000)) is None
        assert derive_close_key(None, PositionID(42)) is None


class TestInMemoryStore:
    """The default opt-in implementation is straightforward; pin its
    semantics so future maintainers can't accidentally introduce
    cross-process drift."""

    @pytest.mark.asyncio
    async def test_get_miss_returns_none(self) -> None:
        store = InMemoryIdempotencyStore()
        assert await store.get("absent") is None

    @pytest.mark.asyncio
    async def test_put_then_get_round_trips(self) -> None:
        store = InMemoryIdempotencyStore()
        intent = OpenIntent(instrument="AAPL", amount=Decimal("100"))
        result = TradeResult(intent=intent, status="ok", order_id=OrderID(123))
        await store.put("k", result)
        got = await store.get("k")
        assert got is not None
        assert got.order_id == OrderID(123)

    @pytest.mark.asyncio
    async def test_clear_empties_the_store(self) -> None:
        store = InMemoryIdempotencyStore()
        intent = OpenIntent(instrument="AAPL", amount=Decimal("100"))
        await store.put("k", TradeResult(intent=intent, status="ok"))
        assert len(store) == 1
        store.clear()
        assert len(store) == 0
        assert await store.get("k") is None


class TestNullStore:
    """The default-default; nothing must ever land in it, no matter how
    many ``put``s the executor issues."""

    @pytest.mark.asyncio
    async def test_null_store_never_caches(self) -> None:
        store = NullIdempotencyStore()
        intent = OpenIntent(instrument="AAPL", amount=Decimal("100"))
        await store.put("k", TradeResult(intent=intent, status="ok"))
        assert await store.get("k") is None


# ── integration with the execution layer ──────────────────────────────────


class TestOpenTradeIdempotency:
    """End-to-end coverage of single-trade open with idempotency."""

    @pytest.mark.asyncio
    async def test_no_key_means_no_caching(self) -> None:
        """Existing callers — who pass no key — must see zero cached
        behaviour. Two open calls without a key must both POST."""
        cache = InstrumentCache()
        _seed(cache, "AAPL", 100000)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        post_count = [0]

        def handler(request: Any) -> Response:
            post_count[0] += 1
            return Response(
                200,
                json={"orderForOpen": {"orderID": 999 + post_count[0], "amount": 50.0}},
            )

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            intent = OpenIntent(instrument="AAPL", amount=Decimal("50"))
            r1 = await open_trade(
                http, env="demo", intent=intent, cache=cache, idempotency_store=store
            )
            r2 = await open_trade(
                http, env="demo", intent=intent, cache=cache, idempotency_store=store
            )

        assert r1.status == "ok"
        assert r2.status == "ok"
        assert post_count[0] == 2, "Without a key, every call must POST."
        assert len(store) == 0, "Without a key, nothing lands in the store."
        await http.aclose()

    @pytest.mark.asyncio
    async def test_second_call_with_same_key_returns_cached(self) -> None:
        """The core variant-B contract: re-run with the same key, get
        the original result back without any further HTTP traffic."""
        cache = InstrumentCache()
        _seed(cache, "AAPL", 100000)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        post_count = [0]

        def handler(request: Any) -> Response:
            post_count[0] += 1
            return Response(
                200,
                json={"orderForOpen": {"orderID": 12345, "amount": 50.0}},
            )

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            intent = OpenIntent(instrument="AAPL", amount=Decimal("50"))
            r1 = await open_trade(
                http,
                env="demo",
                intent=intent,
                cache=cache,
                idempotency_store=store,
                idempotency_key="my-trade-1",
            )
            r2 = await open_trade(
                http,
                env="demo",
                intent=intent,
                cache=cache,
                idempotency_store=store,
                idempotency_key="my-trade-1",
            )

        assert r1.order_id == OrderID(12345)
        assert r2.order_id == OrderID(12345)
        # The second call must NOT have POSTed — that's the whole point.
        assert post_count[0] == 1
        await http.aclose()

    @pytest.mark.asyncio
    async def test_cache_hit_skips_pnl_read_entirely(self) -> None:
        """A cache hit must short-circuit *all* I/O — not just the POST.
        Resolving and reading ``/pnl`` are themselves rate-limited reads
        we'd rather skip on a known-cached trade.
        """
        cache = InstrumentCache()
        _seed(cache, "AAPL", 100000)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        # Pre-populate the store; respx with assert_all_called=False
        # and no matching route would error on any HTTP call.
        intent = OpenIntent(instrument="AAPL", amount=Decimal("50"))
        cached = TradeResult(intent=intent, status="ok", order_id=OrderID(777))
        await store.put("hit-key", cached)

        with respx.mock(assert_all_called=False) as router:
            # Both routes are registered but neither should be hit.
            pnl_route = router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            post_route = router.post(
                f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount"
            ).mock(return_value=Response(200, json={"orderForOpen": {"orderID": 1}}))

            result = await open_trade(
                http,
                env="demo",
                intent=intent,
                cache=cache,
                idempotency_store=store,
                idempotency_key="hit-key",
            )

        assert result.order_id == OrderID(777)
        assert not pnl_route.called, "Cache hit must NOT read /pnl."
        assert not post_route.called, "Cache hit must NOT POST."
        await http.aclose()

    @pytest.mark.asyncio
    async def test_ambiguous_outcome_is_not_cached_so_retry_is_possible(self) -> None:
        """``ambiguous`` is the at-most-once "I don't know" status —
        caching it would lock the caller out of any retry. The verifier
        reconciles via ``/pnl`` instead; the executor must not write."""
        from httpx import ConnectError

        cache = InstrumentCache()
        _seed(cache, "AAPL", 100000)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        def handler(_request: Any) -> Response:
            raise ConnectError("simulated network drop")

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            intent = OpenIntent(instrument="AAPL", amount=Decimal("50"))
            r1 = await open_trade(
                http,
                env="demo",
                intent=intent,
                cache=cache,
                idempotency_store=store,
                idempotency_key="ambig-key",
            )

        assert r1.status == "ambiguous"
        # The store must remain empty so the caller can retry safely.
        assert len(store) == 0
        await http.aclose()

    @pytest.mark.asyncio
    async def test_failed_outcome_is_cached_to_save_round_trips(self) -> None:
        """A server-confirmed 4xx is terminal — re-firing the same body
        would just get the same rejection. Caching saves the round-trip
        and matches the rest of the at-most-once philosophy."""
        cache = InstrumentCache()
        _seed(cache, "AAPL", 100000)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        post_count = [0]

        def handler(_request: Any) -> Response:
            post_count[0] += 1
            return Response(400, json={"error": "bad request"})

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            intent = OpenIntent(instrument="AAPL", amount=Decimal("50"))
            r1 = await open_trade(
                http,
                env="demo",
                intent=intent,
                cache=cache,
                idempotency_store=store,
                idempotency_key="fail-key",
            )
            r2 = await open_trade(
                http,
                env="demo",
                intent=intent,
                cache=cache,
                idempotency_store=store,
                idempotency_key="fail-key",
            )

        assert r1.status == "failed"
        assert r2.status == "failed"
        assert post_count[0] == 1, "Failed outcome must cache to skip the second POST."
        await http.aclose()


class TestCloseTradeIdempotency:
    """Mirror of the open-trade integration tests."""

    @pytest.mark.asyncio
    async def test_close_cache_hit_skips_post(self) -> None:
        http = _make_http()
        store = InMemoryIdempotencyStore()

        intent = CloseIntent(
            position_id=PositionID(42),
            instrument_id=InstrumentID(100000),
        )
        cached = TradeResult(
            intent=intent,
            instrument_id=InstrumentID(100000),
            status="ok",
            position_id=PositionID(42),
        )
        await store.put("close-key", cached)

        with respx.mock(assert_all_called=False) as router:
            post_route = router.post(
                f"{PUBLIC_BASE}/trading/execution/demo/market-close-orders/positions/42"
            ).mock(return_value=Response(200, json={}))

            r = await close_trade(
                http,
                env="demo",
                intent=intent,
                idempotency_store=store,
                idempotency_key="close-key",
            )

        assert r.position_id == PositionID(42)
        assert r.status == "ok"
        assert not post_route.called
        await http.aclose()


class TestBulkIdempotency:
    """Batch-key derivation drives per-trade dedup inside a bulk."""

    @pytest.mark.asyncio
    async def test_partial_bulk_resumes_only_uncached_trades(self) -> None:
        """Pre-populate the store with a BTC result, then issue a bulk
        of {BTC, ETH, ADA}. Only ETH and ADA must POST; the final
        trades tuple must still contain all three in original order.
        """
        cache = InstrumentCache()
        _seed(cache, "BTC", 100000)
        _seed(cache, "ETH", 100001)
        _seed(cache, "ADA", 100017)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        # Pre-seed BTC under the derived key the bulk will use.
        btc_intent = OpenIntent(instrument="BTC", amount=Decimal("25"))
        btc_cached = TradeResult(
            intent=btc_intent,
            instrument_id=InstrumentID(100000),
            status="ok",
            order_id=OrderID(9999),
            requested_amount=Decimal("25"),
        )
        btc_key = derive_open_key("batch-resume", InstrumentID(100000))
        assert btc_key is not None
        await store.put(btc_key, btc_cached)

        instrument_ids_posted: list[int] = []

        def handler(request: Any) -> Response:
            iid = json.loads(request.content)["InstrumentID"]
            instrument_ids_posted.append(iid)
            return Response(
                200,
                json={"orderForOpen": {"orderID": 5000 + iid, "amount": 25.0}},
            )

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            plan = BulkTradePlan(
                weights={
                    "BTC": Decimal("0.34"),
                    "ETH": Decimal("0.33"),
                    "ADA": Decimal("0.33"),
                },
                total_amount=Decimal("75"),
            )
            result = await execute_bulk_trade(
                http,
                env="demo",
                plan=plan,
                cache=cache,
                idempotency_store=store,
                idempotency_key="batch-resume",
            )

        # BTC didn't POST (it was cached).
        assert 100000 not in instrument_ids_posted
        # ETH and ADA both posted exactly once.
        assert sorted(instrument_ids_posted) == [100001, 100017]
        # Final trades tuple preserves plan.weights order.
        assert [tr.intent.instrument for tr in result.trades] == [  # type: ignore[union-attr]
            "BTC",
            "ETH",
            "ADA",
        ]
        # BTC came from the store (same order_id).
        btc_result = next(t for t in result.trades if t.instrument_id == InstrumentID(100000))
        assert btc_result.order_id == OrderID(9999)
        await http.aclose()

    @pytest.mark.asyncio
    async def test_resume_after_partial_does_not_trip_insufficient_cash(self) -> None:
        """If the cached portion already consumed the cash, the
        sufficiency check must compare the *remaining* amount against
        available cash — not the original total."""
        cache = InstrumentCache()
        _seed(cache, "BTC", 100000)
        _seed(cache, "ETH", 100001)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        # The portfolio shows only $30 available cash — the *full*
        # plan ($60) would not fit, but the remaining ETH leg ($30)
        # exactly does.
        portfolio_with_low_cash = {
            "clientPortfolio": {
                "credit": 30.0,
                "positions": [],
                "orders": [],
                "ordersForOpen": [],
                "mirrors": [],
            }
        }

        btc_intent = OpenIntent(instrument="BTC", amount=Decimal("30"))
        btc_cached = TradeResult(
            intent=btc_intent,
            instrument_id=InstrumentID(100000),
            status="ok",
            order_id=OrderID(8888),
            requested_amount=Decimal("30"),
        )
        btc_key = derive_open_key("batch-low-cash", InstrumentID(100000))
        assert btc_key is not None
        await store.put(btc_key, btc_cached)

        def handler(_request: Any) -> Response:
            return Response(
                200,
                json={"orderForOpen": {"orderID": 5555, "amount": 30.0}},
            )

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=portfolio_with_low_cash)
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            plan = BulkTradePlan(
                weights={
                    "BTC": Decimal("0.5"),
                    "ETH": Decimal("0.5"),
                },
                total_amount=Decimal("60"),
            )
            result = await execute_bulk_trade(
                http,
                env="demo",
                plan=plan,
                cache=cache,
                idempotency_store=store,
                idempotency_key="batch-low-cash",
            )

        # No InsufficientCashError raised — and both trades present.
        assert len(result.trades) == 2
        assert {t.instrument_id for t in result.trades} == {
            InstrumentID(100000),
            InstrumentID(100001),
        }
        await http.aclose()

    @pytest.mark.asyncio
    async def test_first_run_then_full_resume_makes_zero_posts(self) -> None:
        """Run a bulk once, then re-run with the same batch key and the
        same store — the second run must make zero POSTs to
        ``market-open-orders``. ``/pnl`` is still read for the anchor."""
        cache = InstrumentCache()
        _seed(cache, "BTC", 100000)
        _seed(cache, "ETH", 100001)
        http = _make_http()
        store = InMemoryIdempotencyStore()

        post_count = [0]

        def handler(request: Any) -> Response:
            post_count[0] += 1
            iid = json.loads(request.content)["InstrumentID"]
            return Response(
                200,
                json={"orderForOpen": {"orderID": 9000 + iid, "amount": 25.0}},
            )

        with respx.mock(assert_all_called=False) as router:
            router.get(f"{PUBLIC_BASE}/trading/info/demo/pnl").mock(
                return_value=Response(200, json=_empty_portfolio())
            )
            router.post(f"{PUBLIC_BASE}/trading/execution/demo/market-open-orders/by-amount").mock(
                side_effect=handler
            )

            plan = BulkTradePlan(
                weights={"BTC": Decimal("0.5"), "ETH": Decimal("0.5")},
                total_amount=Decimal("50"),
            )
            first = await execute_bulk_trade(
                http,
                env="demo",
                plan=plan,
                cache=cache,
                idempotency_store=store,
                idempotency_key="batch-twice",
            )
            posts_after_first = post_count[0]
            second = await execute_bulk_trade(
                http,
                env="demo",
                plan=plan,
                cache=cache,
                idempotency_store=store,
                idempotency_key="batch-twice",
            )

        assert posts_after_first == 2, "First run posts both."
        assert post_count[0] == 2, "Second run posts zero."
        assert [t.order_id for t in first.trades] == [t.order_id for t in second.trades]
        await http.aclose()
