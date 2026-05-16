"""Test bidirectional resolver: search casing, batching, cache, miss handling."""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._resolve import InstrumentCache, resolve
from etoro_bulk_trades.errors import ResolutionError

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"


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
