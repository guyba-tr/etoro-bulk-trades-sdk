"""Auth contexts, environment probing, and Bearer token refresh.

Two auth modes are first-class citizens (see
https://api-portal.etoro.com/getting-started/authentication):

* :class:`ApiKeyAuth` — partner ``x-api-key`` + per-user ``x-user-key``.
* :class:`BearerAuth` — ``Authorization: Bearer <access_token>``, optionally
  paired with a refresh token + client_id + an ``on_token_refresh`` callback
  for atomic rotation.

Both modes use the same Public-API endpoints; the SDK never sends both
header families simultaneously (the API rejects requests that try).

Environment binding
-------------------
Each user-key is bound to one environment (``real`` or ``demo``) at
creation. :func:`probe_environment` issues a single ``GET /trading/info/real/pnl``
to detect which side the credential belongs to. Cache the result for the
client's lifetime (the env can't change without a new credential).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from etoro_bulk_trades.errors import (
    EnvironmentMismatchError,
    HttpStatusError,
    InvalidCredentialsError,
)
from etoro_bulk_trades.types import Environment, TokenPair

if TYPE_CHECKING:
    from collections.abc import Callable

    from etoro_bulk_trades._http import HttpClient


# ── credential containers ───────────────────────────────────────────────────


@dataclass
class ApiKeyAuth:
    """Partner-key auth.

    Both fields are required; the API rejects requests that send only one.
    The SDK never falls back to API-key headers when Bearer is in use, and
    vice-versa.
    """

    api_key: str
    user_key: str

    def headers(self) -> dict[str, str]:
        return {"x-api-key": self.api_key, "x-user-key": self.user_key}

    def can_refresh(self) -> bool:
        return False

    async def refresh(self, _client: HttpClient) -> None:
        """No-op for API-key auth (raised as :class:`InvalidCredentialsError`
        by the HTTP layer instead)."""
        raise InvalidCredentialsError(
            "API-key auth has no refresh path; create a fresh key in eToro Settings > Trading."
        )


@dataclass
class BearerAuth:
    """OAuth bearer-token auth, optionally with refresh-token rotation.

    Refresh policy:

    * If ``refresh_token`` and ``client_id`` are both set, a single 401 from
      the Public API triggers one refresh attempt.
    * On success, the new tokens replace the in-memory state and the
      ``on_token_refresh`` callback (if any) is invoked so the application can
      persist the rotated pair.
    * On ``400 invalid_grant``, the SDK raises :class:`SessionExpiredError` —
      the caller must surface a "Reconnect to eToro" flow.
    """

    access_token: str
    refresh_token: str | None = None
    client_id: str | None = None
    expires_at: datetime | None = None
    on_token_refresh: Callable[[TokenPair], None] | None = field(default=None, repr=False)

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def can_refresh(self) -> bool:
        return self.refresh_token is not None and self.client_id is not None

    async def refresh(self, http: HttpClient) -> None:
        """Refresh the access token and rotate the refresh token.

        Posts ``grant_type=refresh_token`` to ``https://www.etoro.com/sso/oidc/token``
        and persists the new access + refresh + expiry tuple in-place. Calls
        ``on_token_refresh`` with a fresh :class:`TokenPair` if registered.
        """
        if not self.can_refresh():
            raise InvalidCredentialsError(
                "Bearer auth has no refresh_token / client_id configured; cannot refresh."
            )

        # Local import to avoid the auth → http → auth import cycle.
        from etoro_bulk_trades._http import sso_form_post

        # Cast assertion: can_refresh() validated both fields are non-None.
        assert self.refresh_token is not None
        assert self.client_id is not None

        body: dict[str, str] = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
        }
        result: Any = await sso_form_post(http.underlying, "/sso/oidc/token", body)

        access = result["access_token"]
        new_refresh = result.get("refresh_token", self.refresh_token)
        expires_in = int(result.get("expires_in", 0))
        expires_at = (
            datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in) if expires_in else None
        )

        self.access_token = access
        self.refresh_token = new_refresh
        self.expires_at = expires_at

        if self.on_token_refresh is not None and expires_at is not None:
            self.on_token_refresh(
                TokenPair(
                    access_token=access,
                    refresh_token=new_refresh,
                    expires_at=expires_at,
                )
            )


AuthContext = ApiKeyAuth | BearerAuth


# ── HTTP-layer adapter ──────────────────────────────────────────────────────


class AuthHandle:
    """Wraps an :class:`AuthContext` for the HTTP layer's
    :class:`AuthContextProvider` protocol.

    Holds a back-reference to the :class:`HttpClient` so :meth:`refresh` can
    POST to the SSO host without the call site knowing how.
    """

    def __init__(self, ctx: AuthContext) -> None:
        self._ctx = ctx
        self._http: HttpClient | None = None

    @property
    def ctx(self) -> AuthContext:
        return self._ctx

    def bind(self, http: HttpClient) -> None:
        self._http = http

    def headers(self) -> dict[str, str]:
        return self._ctx.headers()

    def can_refresh(self) -> bool:
        return self._ctx.can_refresh()

    async def refresh(self) -> None:
        if self._http is None:
            raise RuntimeError("AuthHandle.refresh called before bind(http)")
        await self._ctx.refresh(self._http)


# ── environment probe ───────────────────────────────────────────────────────


_PROBE_PATH = "/trading/info/real/pnl"


async def probe_environment(http: HttpClient) -> Environment:
    """Determine whether the active credential is bound to ``real`` or ``demo``.

    Issues a single ``GET /trading/info/real/pnl``:

    * ``200`` → real credential (the ``/real/`` path was accepted).
    * ``403 InsufficientPermissions`` → demo credential (the ``/real/`` path
      was rejected; the matching ``/demo/`` path will work).
    * Anything else (most commonly ``401``) → the credential is invalid.
    """
    try:
        await http.request("GET", _PROBE_PATH, category="general")
    except HttpStatusError as exc:
        body_text = _stringify_body(exc.body)
        if exc.status_code == 403 and "InsufficientPermissions" in body_text:
            return "demo"
        raise
    return "real"


async def enforce_environment(http: HttpClient, requested: Environment | None) -> Environment:
    """Probe and (optionally) enforce the requested environment.

    If ``requested`` is ``None``, returns whatever the probe detected. If
    ``requested`` is set and doesn't match, raises
    :class:`EnvironmentMismatchError`.
    """
    actual = await probe_environment(http)
    if requested is not None and actual != requested:
        raise EnvironmentMismatchError(requested=requested, actual=actual)
    return actual


def _stringify_body(body: object) -> str:
    if body is None:
        return ""
    if isinstance(body, str):
        return body
    if isinstance(body, dict | list):
        try:
            import json

            return json.dumps(body)
        except TypeError:
            return repr(body)
    return repr(body)
