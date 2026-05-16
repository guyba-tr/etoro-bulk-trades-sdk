"""Async WebSocket client for the eToro real-time streaming API.

Endpoint: ``wss://ws.etoro.com/ws``. Two operations matter for this SDK:

* ``Authenticate`` — sent immediately after connect; payload mirrors the
  Public-API auth headers.
* ``Subscribe`` — subscribes to one or more *topics*. The only topic the
  verifier uses is ``private`` (per-user account events including trade
  fills). Documented events of interest:

  - ``Trading.OrderForCloseMultiple.Update`` — close-side order updates.

  Open-side updates are not documented as a specific event type; we accept
  anything matching ``Trading.*.Update`` and try to match by ``OrderID``.
  Schema drift is tolerated — unknown fields are ignored, missing fields
  return as ``None`` rather than raising.

Auto-reconnect: out of scope for v1. On any disconnect during a
``verify_orders`` call, the verifier falls back to ``mode='pnl'``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

import websockets
from websockets.asyncio.client import ClientConnection

from etoro_bulk_trades._auth import ApiKeyAuth, AuthContext, BearerAuth

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

WS_URL: Final[str] = "wss://ws.etoro.com/ws"

DEFAULT_RECV_TIMEOUT_S: Final[float] = 60.0


@dataclass
class PrivateEvent:
    """Normalized private-topic event.

    The wire schema for events under the ``private`` topic isn't fully
    documented; we keep the parser tolerant by exposing the raw envelope
    alongside the extracted fields.
    """

    operation: str
    """``Update`` for most events; carries the change type after splitting
    the event name (``Trading.<EntityName>.Update``)."""

    entity: str
    """The entity that changed (e.g. ``OrderForCloseMultiple``,
    ``Position``, ``Order``)."""

    order_id: int | None
    position_id: int | None
    instrument_id: int | None
    status_id: int | None
    raw: dict[str, Any]


def _build_auth_payload(ctx: AuthContext) -> dict[str, Any]:
    """Build the ``Authenticate`` operation payload.

    The portal example uses API-key style fields (``userKey``, ``apiKey``);
    Bearer authentication on the WebSocket is supported via the same
    ``Authorization`` field name used by HTTP. If neither is present, this
    raises immediately rather than waiting for the server to reject.
    """
    if isinstance(ctx, ApiKeyAuth):
        return {"userKey": ctx.user_key, "apiKey": ctx.api_key}
    if isinstance(ctx, BearerAuth):
        return {"authorization": f"Bearer {ctx.access_token}"}
    raise TypeError(f"unsupported AuthContext: {type(ctx).__name__}")


@asynccontextmanager
async def connect_and_authenticate(
    ctx: AuthContext,
    *,
    url: str = WS_URL,
    request_id_factory: Any = None,
) -> AsyncIterator[ClientConnection]:
    """Open a WebSocket, send ``Authenticate``, yield the live connection.

    Used as ``async with connect_and_authenticate(ctx) as ws:``.
    """
    rid = request_id_factory or (lambda: str(uuid.uuid4()))
    async with websockets.connect(url) as conn:
        await conn.send(
            json.dumps(
                {
                    "id": rid(),
                    "operation": "Authenticate",
                    "data": _build_auth_payload(ctx),
                }
            )
        )
        # The server replies with an Authenticate-Ack; we don't enforce the
        # response shape (some deployments respond differently), but we DO
        # drain one frame so a subsequent ``recv`` isn't fed the ack.
        try:
            await asyncio.wait_for(conn.recv(), timeout=10.0)
        except (TimeoutError, asyncio.TimeoutError):
            logger.debug("No Authenticate ack within 10s; proceeding optimistically.")
        yield conn


async def subscribe(
    conn: ClientConnection,
    topics: list[str],
    *,
    snapshot: bool = False,
    request_id_factory: Any = None,
) -> None:
    """Send a single ``Subscribe`` frame for the supplied topics."""
    rid = request_id_factory or (lambda: str(uuid.uuid4()))
    await conn.send(
        json.dumps(
            {
                "id": rid(),
                "operation": "Subscribe",
                "data": {
                    "topics": topics,
                    "snapshot": snapshot,
                },
            }
        )
    )


def _parse_event(raw_text: str) -> PrivateEvent | None:
    """Parse a single WebSocket text frame into a :class:`PrivateEvent`.

    Returns ``None`` for any frame the SDK isn't interested in
    (acks, snapshots, market-data frames, ping/pong, ...).
    """
    try:
        msg = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(msg, dict):
        return None

    operation = msg.get("operation") or msg.get("event") or ""
    if not isinstance(operation, str):
        return None

    # Filter to ``Trading.<Entity>.Update`` events.
    parts = operation.split(".")
    if len(parts) != 3 or parts[0] != "Trading" or parts[2] != "Update":
        return None

    entity = parts[1]
    data = msg.get("data") if isinstance(msg.get("data"), dict) else msg

    def _maybe_int(key: str) -> int | None:
        val = data.get(key) if isinstance(data, dict) else None
        if val is None:
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    return PrivateEvent(
        operation="Update",
        entity=entity,
        order_id=_maybe_int("OrderID") or _maybe_int("orderID") or _maybe_int("orderId"),
        position_id=_maybe_int("PositionID")
        or _maybe_int("positionID")
        or _maybe_int("positionId"),
        instrument_id=_maybe_int("InstrumentID")
        or _maybe_int("instrumentID")
        or _maybe_int("instrumentId"),
        status_id=_maybe_int("StatusID") or _maybe_int("statusID") or _maybe_int("statusId"),
        raw=msg,
    )


async def stream_private_events(
    conn: ClientConnection,
    *,
    timeout_s: float = DEFAULT_RECV_TIMEOUT_S,
) -> AsyncIterator[PrivateEvent]:
    """Yield ``Trading.*.Update`` events from a live private subscription.

    Stops when the recv times out or the server disconnects; callers should
    treat exhaustion as "no more events available" and fall back to PnL
    verification if work remains.
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            return
        try:
            raw = await asyncio.wait_for(conn.recv(), timeout=remaining)
        except (TimeoutError, asyncio.TimeoutError):
            return
        except websockets.ConnectionClosed:
            return
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue
        ev = _parse_event(raw)
        if ev is not None:
            yield ev
