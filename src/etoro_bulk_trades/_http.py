"""Async HTTP transport for the eToro Public API.

Single shared :class:`httpx.AsyncClient` per :class:`AsyncBulkTradesClient`
instance; auth headers and ``x-request-id`` are injected per request; every
response is classified through :func:`_classify_response` and routed into the
typed retry strategy.

Two host families are deliberately kept separate by a sibling helper
(:func:`sso_form_post`) — the SSO host uses ``application/x-www-form-urlencoded``
and an OAuth-style error envelope, so sharing a wrapper would lose typing.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

import httpx

from etoro_bulk_trades._ratelimit import RateCategory, RateLimiter
from etoro_bulk_trades.errors import (
    HttpStatusError,
    InvalidCredentialsError,
    PayloadTooLargeError,
    RateLimitError,
    SessionExpiredError,
    TransportError,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

PUBLIC_API_BASE: Final[str] = "https://public-api.etoro.com/api/v1"
SSO_BASE: Final[str] = "https://www.etoro.com"

# Trade-execution paths share the 20 req/min budget.
TRADE_EXECUTION_PREFIXES: Final[tuple[str, ...]] = ("/trading/execution/",)

# Retry cadences from the etoro-api-conventions rule.
TRADE_429_BACKOFF_S: Final[tuple[float, ...]] = (15.0, 30.0, 60.0)
GENERAL_429_BACKOFF_S: Final[tuple[float, ...]] = (1.0, 5.0, 30.0)
SERVER_5XX_BACKOFF_S: Final[tuple[float, ...]] = (0.2, 0.6, 1.5)

DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)


@runtime_checkable
class AuthContextProvider(Protocol):
    """The HTTP layer's view of the auth context.

    Implemented by :mod:`etoro_bulk_trades._auth`; lets the transport stay
    blissfully ignorant of API-key vs Bearer details, while still being able
    to (optionally) refresh on 401.
    """

    def headers(self) -> dict[str, str]:
        """Return the auth headers to attach to a Public-API request."""

    def can_refresh(self) -> bool:
        """True iff a 401 should trigger an in-flight refresh attempt."""

    async def refresh(self) -> None:
        """Perform the refresh; raise :class:`SessionExpiredError` on
        ``invalid_grant``. After return, :meth:`headers` reflects the new
        access token."""


def is_trade_execution_path(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in TRADE_EXECUTION_PREFIXES)


def instruments_url(ids: list[int] | tuple[int, ...]) -> str:
    """Build the ``/market-data/instruments`` URL with a literal ``,``.

    eToro rejects percent-encoded ``%2C`` separators on this endpoint, but
    every standard URL-builder (``URLSearchParams``, ``httpx.QueryParams``,
    ``urllib.parse.urlencode``) encodes commas by default. This helper is the
    only sanctioned way to build that URL inside the SDK.
    """
    if not ids:
        raise ValueError("instruments_url requires at least one ID")
    joined = ",".join(str(int(i)) for i in ids)
    return f"{PUBLIC_API_BASE}/market-data/instruments?instrumentIds={joined}"


class HttpClient:
    """Thin layer over :class:`httpx.AsyncClient`."""

    def __init__(
        self,
        *,
        auth_provider: AuthContextProvider,
        rate_limiter: RateLimiter | None = None,
        timeout: httpx.Timeout | None = None,
        request_id_factory: Callable[[], str] | None = None,
        sleep_func: Callable[[float], Awaitable[None]] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._auth = auth_provider
        self._rate = rate_limiter or RateLimiter.default()
        self._request_id = request_id_factory or (lambda: str(uuid.uuid4()))
        self._sleep: Callable[[float], Awaitable[None]] = sleep_func or asyncio.sleep
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout or DEFAULT_TIMEOUT)

    @property
    def underlying(self) -> httpx.AsyncClient:
        """Escape hatch for the SSO form-post helper, which needs a fresh
        client without auth headers."""
        return self._client

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ── public API ────────────────────────────────────────────────────────

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: object | None = None,
        params: dict[str, str | int | float | bool] | None = None,
        absolute_url: str | None = None,
        category: RateCategory | None = None,
    ) -> Any:
        """Issue a Public-API request and return the parsed JSON body.

        Parameters
        ----------
        method
            HTTP verb (``GET`` / ``POST`` / ``DELETE`` / ...).
        path
            Path under ``/api/v1`` (must start with ``/``). Ignored when
            ``absolute_url`` is set.
        json
            JSON request body. Mutually exclusive with form-encoded bodies
            (which the SSO helper handles separately).
        params
            Query parameters. **Do not** use this for endpoints that need
            literal commas (``/market-data/instruments``); use
            :func:`instruments_url` and pass ``absolute_url`` instead.
        absolute_url
            For pre-built URLs (such as :func:`instruments_url`).
        category
            Override the default rate-limit category. If ``None``, trade
            execution paths route to ``execution``; everything else to
            ``general``.
        """
        url = absolute_url if absolute_url is not None else f"{PUBLIC_API_BASE}{path}"
        cat: RateCategory = category or (
            "execution" if is_trade_execution_path(path) else "general"
        )

        attempt = 0
        refreshed = False

        while True:
            await self._rate.acquire(cat)
            headers = self._build_headers()

            try:
                response = await self._client.request(
                    method,
                    url,
                    headers=headers,
                    json=json,
                    params=params,
                )
            except httpx.RequestError as exc:
                # Network-level failure (timeout, connection reset, no
                # response). Per the at-most-once rule, this is AMBIGUOUS for
                # trade-execution and the caller decides — surface it as
                # TransportError so they can mark the trade ambiguous.
                raise TransportError(
                    status_code=None,
                    message=f"network error: {exc.__class__.__name__}: {exc}",
                ) from exc

            status = response.status_code

            # 2xx — happy path
            if 200 <= status < 300:
                return self._parse_json(response)

            # 401 — credential failure; refresh once if allowed, else raise.
            if status == 401:
                if self._auth.can_refresh() and not refreshed:
                    try:
                        await self._auth.refresh()
                    except SessionExpiredError:
                        raise
                    refreshed = True
                    continue
                raise InvalidCredentialsError(
                    "API rejected credentials (HTTP 401). For Bearer auth, the "
                    "refresh token may also be expired or absent. For API-key "
                    "auth, the user may have revoked the key in eToro Settings."
                )

            # 413 / 414 — payload too large; caller (resolver) handles.
            if status in (413, 414):
                raise PayloadTooLargeError(f"HTTP {status}: payload too large; halve and retry")

            # 429 — rate limit; backoff and retry up to 3 times.
            if status == 429:
                schedule = TRADE_429_BACKOFF_S if cat == "execution" else GENERAL_429_BACKOFF_S
                if attempt < len(schedule):
                    wait = self._retry_after(response, default=schedule[attempt])
                    logger.warning(
                        "HTTP 429 on %s %s; sleeping %.1fs (attempt %d/%d)",
                        method,
                        url,
                        wait,
                        attempt + 1,
                        len(schedule),
                    )
                    await self._sleep(wait)
                    attempt += 1
                    continue
                raise RateLimitError(
                    retry_after_s=self._retry_after(response, default=None),
                    message=f"Rate limit exceeded after {len(schedule)} retries",
                )

            # 5xx — transient server error.
            if 500 <= status < 600:
                if attempt < len(SERVER_5XX_BACKOFF_S):
                    wait = SERVER_5XX_BACKOFF_S[attempt]
                    logger.warning(
                        "HTTP %d on %s %s; sleeping %.2fs (attempt %d/%d)",
                        status,
                        method,
                        url,
                        wait,
                        attempt + 1,
                        len(SERVER_5XX_BACKOFF_S),
                    )
                    await self._sleep(wait)
                    attempt += 1
                    continue
                raise TransportError(
                    status_code=status,
                    message=f"Transport error after {len(SERVER_5XX_BACKOFF_S)} retries: "
                    f"HTTP {status}",
                )

            # Other 4xx — explicit error, do not retry.
            raise HttpStatusError(
                status_code=status,
                body=self._parse_json(response, allow_empty=True),
                message=f"HTTP {status} on {method} {url}",
            )

    # ── helpers ──────────────────────────────────────────────────────────

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-request-id": self._request_id(),
        }
        headers.update(self._auth.headers())
        return headers

    @staticmethod
    def _parse_json(response: httpx.Response, *, allow_empty: bool = False) -> Any:
        if not response.content:
            return None if allow_empty else {}
        try:
            return response.json()
        except ValueError:
            if allow_empty:
                return response.text
            raise

    @staticmethod
    def _retry_after(response: httpx.Response, *, default: float | None) -> float:
        raw = response.headers.get("Retry-After")
        if raw is None:
            return default or 0.0
        try:
            return float(raw)
        except ValueError:
            return default or 0.0


async def sso_form_post(
    client: httpx.AsyncClient,
    path: str,
    body: dict[str, str],
) -> Any:
    """POST to the SSO host with form-encoded body.

    Used by :mod:`_auth` for the OAuth token exchange and refresh. Kept
    separate from :class:`HttpClient` because the SSO host has a different
    content type and error envelope and we don't want to share retry policy.
    """
    url = f"{SSO_BASE}{path}"
    response = await client.post(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if response.status_code >= 400:
        try:
            err: dict[str, Any] = response.json()
        except ValueError:
            err = {}
        oauth_code = err.get("error")
        if response.status_code == 400 and oauth_code == "invalid_grant":
            raise SessionExpiredError(
                err.get("error_description")
                or "Refresh token rejected (invalid_grant); user must re-authorize."
            )
        raise HttpStatusError(
            status_code=response.status_code,
            body=err or response.text,
            message=f"SSO {response.status_code}: {oauth_code or 'error'}",
        )
    return response.json()
