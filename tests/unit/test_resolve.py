"""Test bidirectional resolver: search casing, batching, cache, miss handling."""

from __future__ import annotations

import asyncio as _asyncio_module

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._resolve import InstrumentCache, resolve
from etoro_bulk_trades.errors import ResolutionError

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"

# Captured BEFORE the autouse ``_no_real_sleep`` fixture replaces
# ``asyncio.sleep`` with a no-op. Used in concurrency tests where we need
# the handler to actually yield control to the event loop so sibling
# gather()'d tasks can interleave.
_REAL_ASYNC_SLEEP = _asyncio_module.sleep


def _make_http() -> HttpClient:
    handle = AuthHandle(ApiKeyAuth(api_key="k", user_key="u"))
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    return http


@pytest.mark.asyncio
async def test_symbol_uppercased_by_default() -> None:
    http = _make_http()
    cache = InstrumentCache()
    seen_queries: list[str] = []

    with respx.mock(assert_all_called=True) as router:

        def capture(request: object) -> Response:
            qstr = request.url.query.decode()  # type: ignore[attr-defined]
            seen_queries.append(qstr)
            return Response(
                200,
                json={
                    "items": [
                        {
                            "instrumentId": 1001,
                            "internalSymbolFull": "AAPL",
                            "instrumentDisplayName": "Apple Inc",
                        }
                    ]
                },
            )

        router.get(f"{PUBLIC_BASE}/market-data/search").mock(side_effect=capture)

        out = await resolve(http, ["aapl"], cache=cache)

    assert "internalSymbolFull=AAPL" in seen_queries[0]
    assert int(out["aapl"].instrument_id) == 1001
    await http.aclose()


@pytest.mark.asyncio
async def test_force_exact_preserves_input_case() -> None:
    http = _make_http()
    cache = InstrumentCache()
    seen: list[str] = []

    with respx.mock(assert_all_called=True) as router:

        def capture(request: object) -> Response:
            seen.append(request.url.query.decode())  # type: ignore[attr-defined]
            return Response(
                200,
                json={
                    "items": [
                        {
                            "instrumentId": 1,
                            "internalSymbolFull": "lowercase",
                            "instrumentDisplayName": "x",
                        }
                    ]
                },
            )

        router.get(f"{PUBLIC_BASE}/market-data/search").mock(side_effect=capture)

        await resolve(http, ["lowercase"], cache=cache, force_exact=True)

    assert "internalSymbolFull=lowercase" in seen[0]
    await http.aclose()


@pytest.mark.asyncio
async def test_ids_use_literal_comma_url() -> None:
    http = _make_http()
    cache = InstrumentCache()
    seen_urls: list[str] = []

    with respx.mock(assert_all_called=True) as router:

        def capture(request: object) -> Response:
            seen_urls.append(str(request.url))  # type: ignore[attr-defined]
            return Response(
                200,
                json={
                    "instrumentDisplayDatas": [
                        {"instrumentID": 1, "symbolFull": "A", "instrumentDisplayName": "A"},
                        {"instrumentID": 2, "symbolFull": "B", "instrumentDisplayName": "B"},
                    ]
                },
            )

        router.get(host="public-api.etoro.com").mock(side_effect=capture)

        await resolve(http, [1, 2], cache=cache)

    assert any("instrumentIds=1,2" in u for u in seen_urls), seen_urls
    assert not any("instrumentIds=1%2C2" in u for u in seen_urls), seen_urls
    await http.aclose()


@pytest.mark.asyncio
async def test_resolution_error_lists_unresolved() -> None:
    http = _make_http()
    cache = InstrumentCache()

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/market-data/search").mock(
            return_value=Response(200, json={"items": []})
        )

        with pytest.raises(ResolutionError) as exc:
            await resolve(http, ["NOPE"], cache=cache)
        assert "NOPE" in exc.value.unresolved
    await http.aclose()


@pytest.mark.asyncio
async def test_symbol_searches_run_concurrently() -> None:
    """Multiple symbol resolves must fly in parallel — historically the
    loop was sequential which made a 3-symbol bulk-trade resolve take ~3
    round-trips serially. The fix is ``asyncio.gather`` over the per-symbol
    calls. We assert concurrency by watching how many requests are
    in-flight at the same moment.
    """
    http = _make_http()
    cache = InstrumentCache()
    in_flight = [0]
    peak_in_flight = [0]

    async def slow_handler(request: object) -> Response:
        in_flight[0] += 1
        peak_in_flight[0] = max(peak_in_flight[0], in_flight[0])
        # Real yield (the autouse fixture has patched asyncio.sleep to a
        # no-op, so we use the captured original).
        await _REAL_ASYNC_SLEEP(0.01)
        symbol = request.url.params.get("internalSymbolFull")  # type: ignore[attr-defined]
        in_flight[0] -= 1
        return Response(
            200,
            json={
                "items": [
                    {
                        "instrumentId": hash(symbol) & 0xFFFFFF,
                        "internalSymbolFull": symbol,
                        "instrumentDisplayName": symbol,
                    }
                ]
            },
        )

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/market-data/search").mock(side_effect=slow_handler)
        out = await resolve(http, ["BTC", "ETH", "XRP"], cache=cache)

    assert len(out) == 3
    assert peak_in_flight[0] >= 2, (
        f"expected concurrent search requests, got peak in-flight={peak_in_flight[0]} "
        "(symbol resolves are running sequentially)"
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_id_batches_run_concurrently() -> None:
    """When the caller passes more IDs than fit in one batch, the chunks
    must dispatch in parallel; sequential behaviour would compound
    latency linearly with ID count.
    """
    http = _make_http()
    cache = InstrumentCache()
    in_flight = [0]
    peak_in_flight = [0]

    async def slow_handler(request: object) -> Response:
        in_flight[0] += 1
        peak_in_flight[0] = max(peak_in_flight[0], in_flight[0])
        await _REAL_ASYNC_SLEEP(0.01)
        ids_str = request.url.params.get("instrumentIds", "")  # type: ignore[attr-defined]
        ids = [int(x) for x in ids_str.split(",") if x]
        in_flight[0] -= 1
        return Response(
            200,
            json={
                "instrumentDisplayDatas": [
                    {
                        "instrumentID": iid,
                        "symbolFull": f"S{iid}",
                        "instrumentDisplayName": f"S{iid}",
                    }
                    for iid in ids
                ]
            },
        )

    with respx.mock(assert_all_called=True) as router:
        router.get(host="public-api.etoro.com").mock(side_effect=slow_handler)
        # 120 IDs → 3 batches of 50 / 50 / 20 at the starting ladder size.
        ids: list[str | int] = list(range(1, 121))
        out = await resolve(http, ids, cache=cache)

    assert len(out) == 120
    assert peak_in_flight[0] >= 2, (
        f"expected concurrent ID batches, got peak in-flight={peak_in_flight[0]}"
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_rate_limit_429_is_retried_then_succeeds() -> None:
    """When eToro returns 429 for one of several concurrent symbol
    searches, the HTTP layer's typed backoff retries and the resolver
    still completes (no permanent failure on a transient burst). This
    is the "fallback" half of the rate-limit story; the limiter
    prevents most 429s, the retry handles the rest.
    """
    http = _make_http()
    cache = InstrumentCache()
    call_counts: dict[str, int] = {}

    def handler(request: object) -> Response:
        symbol = request.url.params.get("internalSymbolFull")  # type: ignore[attr-defined]
        call_counts[symbol] = call_counts.get(symbol, 0) + 1
        # ETH gets 429 on first attempt, success on retry.
        if symbol == "ETH" and call_counts[symbol] == 1:
            return Response(429, headers={"Retry-After": "0"}, json={"error": "throttled"})
        return Response(
            200,
            json={
                "items": [
                    {
                        "instrumentId": hash(symbol) & 0xFFFFFF,
                        "internalSymbolFull": symbol,
                        "instrumentDisplayName": symbol,
                    }
                ]
            },
        )

    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/market-data/search").mock(side_effect=handler)
        out = await resolve(http, ["BTC", "ETH", "XRP"], cache=cache)

    assert set(out.keys()) == {"BTC", "ETH", "XRP"}
    assert call_counts["ETH"] == 2, (
        f"expected the 429 retry to fire once, got {call_counts['ETH']} attempts"
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_cache_hits_avoid_network() -> None:
    http = _make_http()
    cache = InstrumentCache()
    calls = 0

    with respx.mock(assert_all_called=True) as router:

        def capture(_: object) -> Response:
            nonlocal calls
            calls += 1
            return Response(
                200,
                json={
                    "items": [
                        {
                            "instrumentId": 1001,
                            "internalSymbolFull": "AAPL",
                            "instrumentDisplayName": "Apple",
                        }
                    ]
                },
            )

        router.get(f"{PUBLIC_BASE}/market-data/search").mock(side_effect=capture)
        await resolve(http, ["AAPL"], cache=cache)
        await resolve(http, ["AAPL"], cache=cache)  # cache hit, no HTTP

    assert calls == 1
    await http.aclose()
