"""Test the HTTP layer's retry-by-class strategy."""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from etoro_bulk_trades._auth import ApiKeyAuth, AuthHandle
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades.errors import (
    HttpStatusError,
    InvalidCredentialsError,
    PayloadTooLargeError,
    RateLimitError,
    TransportError,
)

PUBLIC_BASE = "https://public-api.etoro.com/api/v1"


def _make_http() -> HttpClient:
    handle = AuthHandle(ApiKeyAuth(api_key="k", user_key="u"))
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    return http


@pytest.mark.asyncio
async def test_401_without_refresh_raises_invalid_credentials() -> None:
    http = _make_http()
    with respx.mock(assert_all_called=True) as router:
        router.get(f"{PUBLIC_BASE}/me").mock(return_value=Response(401, json={"error": "bad"}))
        with pytest.raises(InvalidCredentialsError):
            await http.request("GET", "/me")
    await http.aclose()


@pytest.mark.asyncio
async def test_429_retries_3_times_then_giveup() -> None:
    http = _make_http()
    with respx.mock(assert_all_called=True) as router:
        route = router.get(f"{PUBLIC_BASE}/me").mock(
            return_value=Response(429, json={"error": "too many"})
        )
        with pytest.raises(RateLimitError):
            await http.request("GET", "/me")
        # Initial + 3 retries = 4 calls
        assert route.call_count == 4
    await http.aclose()


@pytest.mark.asyncio
async def test_5xx_retries_then_transport_error() -> None:
    http = _make_http()
    with respx.mock(assert_all_called=True) as router:
        route = router.get(f"{PUBLIC_BASE}/me").mock(return_value=Response(500))
        with pytest.raises(TransportError):
            await http.request("GET", "/me")
        assert route.call_count == 4
    await http.aclose()


@pytest.mark.asyncio
async def test_413_raises_payload_too_large_no_retry() -> None:
    http = _make_http()
    with respx.mock(assert_all_called=True) as router:
        route = router.get(f"{PUBLIC_BASE}/market-data/instruments?instrumentIds=1,2,3").mock(
            return_value=Response(413)
        )
        with pytest.raises(PayloadTooLargeError):
            await http.request(
                "GET",
                "",
                absolute_url=f"{PUBLIC_BASE}/market-data/instruments?instrumentIds=1,2,3",
            )
        assert route.call_count == 1
    await http.aclose()


@pytest.mark.asyncio
async def test_other_4xx_raises_http_status_error_no_retry() -> None:
    http = _make_http()
    with respx.mock(assert_all_called=True) as router:
        route = router.get(f"{PUBLIC_BASE}/me").mock(
            return_value=Response(400, json={"error": "bad request"})
        )
        with pytest.raises(HttpStatusError) as exc:
            await http.request("GET", "/me")
        assert exc.value.status_code == 400
        assert route.call_count == 1
    await http.aclose()


@pytest.mark.asyncio
async def test_request_id_injected_per_call() -> None:
    """Every request gets a fresh ``x-request-id`` header."""
    http = _make_http()
    seen_ids: list[str] = []
    with respx.mock(assert_all_called=True) as router:

        def capture(request: object) -> Response:
            rid = request.headers.get("x-request-id")  # type: ignore[attr-defined]
            if rid is not None:
                seen_ids.append(rid)
            return Response(200, json={})

        router.get(f"{PUBLIC_BASE}/me").mock(side_effect=capture)
        await http.request("GET", "/me")
        await http.request("GET", "/me")

    assert len(seen_ids) == 2
    assert seen_ids[0] != seen_ids[1], "x-request-id must be unique per call"
    await http.aclose()
