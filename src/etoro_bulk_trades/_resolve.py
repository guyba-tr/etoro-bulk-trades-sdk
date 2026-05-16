"""Bidirectional instrument resolver.

Two paths under one ``resolve()`` call:

* **Symbol → metadata** — ``GET /market-data/search?internalSymbolFull={SYMBOL}``
  per symbol. The response array can contain partial matches
  (``MSFT``, ``MSFT.RTH``, ``MSFT.EUR``); we always pick the entry whose
  ``internalSymbolFull`` matches exactly. Symbols are uppercased by default;
  pass ``force_exact=True`` to send verbatim.
* **ID → metadata** — batched ``GET /market-data/instruments?instrumentIds=...``
  via :func:`etoro_bulk_trades._http.instruments_url` (literal commas, never
  percent-encoded). The batch ladder is **50 → 25** on
  :class:`PayloadTooLargeError` (HTTP 413/414); 429 / 5xx are bubbled to the
  HTTP layer's retry strategy and never trigger a shrink.

Casing reminder: the search endpoint returns ``instrumentId`` (lowercase d);
the metadata endpoint returns ``instrumentID`` (capital D). Both are mapped
into the SDK's :data:`InstrumentID` branded type.

Caching
-------
Resolutions are cached in a per-client :class:`InstrumentCache`. Both
directions (symbol → ref, id → ref) populate together so the next
``resolve()`` short-circuits.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from etoro_bulk_trades.errors import (
    HttpStatusError,
    PayloadTooLargeError,
    ResolutionError,
)
from etoro_bulk_trades.types import InstrumentID, InstrumentImage, InstrumentRef

if TYPE_CHECKING:
    from collections.abc import Iterable

    from etoro_bulk_trades._http import HttpClient

BATCH_LADDER: tuple[int, ...] = (50, 25)


@dataclass
class InstrumentCache:
    """Per-client in-memory cache populated on first resolve."""

    by_symbol: dict[str, InstrumentRef] = field(default_factory=dict)
    by_id: dict[int, InstrumentRef] = field(default_factory=dict)

    def put(self, ref: InstrumentRef) -> None:
        self.by_symbol[ref.symbol.upper()] = ref
        self.by_id[int(ref.instrument_id)] = ref

    def get_symbol(self, symbol: str) -> InstrumentRef | None:
        return self.by_symbol.get(symbol.upper())

    def get_id(self, instrument_id: int) -> InstrumentRef | None:
        return self.by_id.get(int(instrument_id))


def _select_image_url(images: list[dict[str, Any]] | None) -> tuple[InstrumentImage, ...]:
    """Normalize the wire image array into an immutable tuple.

    The actual variant selection (card SVG vs largest PNG) lives on the
    consumer side; here we just keep the metadata typed.
    """
    if not images:
        return ()
    out: list[InstrumentImage] = []
    for img in images:
        try:
            out.append(
                InstrumentImage(
                    uri=str(img["uri"]),
                    format=str(img.get("format", "")),
                    width=int(img["width"]) if img.get("width") is not None else None,
                    height=int(img["height"]) if img.get("height") is not None else None,
                    background_color=img.get("backgroundColor"),
                    text_color=img.get("textColor"),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return tuple(out)


def _ref_from_search_item(item: dict[str, Any]) -> InstrumentRef:
    """Map a ``/market-data/search`` item (lowercase ``instrumentId``)."""
    return InstrumentRef(
        instrument_id=cast(InstrumentID, int(item["instrumentId"])),
        symbol=str(
            item.get("internalSymbolFull") or item.get("symbolFull") or item.get("symbol", "")
        ),
        display_name=str(
            item.get("instrumentDisplayName") or item.get("displayName") or item.get("name", "")
        ),
        instrument_type=item.get("instrumentTypeDescription") or item.get("instrumentType"),
        exchange_id=int(item["exchangeID"]) if item.get("exchangeID") is not None else None,
        images=_select_image_url(item.get("images")),
    )


def _ref_from_metadata_item(item: dict[str, Any]) -> InstrumentRef:
    """Map a ``/market-data/instruments`` item (capital ``instrumentID``)."""
    return InstrumentRef(
        instrument_id=cast(InstrumentID, int(item["instrumentID"])),
        symbol=str(
            item.get("symbolFull") or item.get("internalSymbolFull") or item.get("symbol", "")
        ),
        display_name=str(
            item.get("instrumentDisplayName") or item.get("displayName") or item.get("name", "")
        ),
        instrument_type=item.get("instrumentTypeDescription") or item.get("instrumentType"),
        exchange_id=int(item["exchangeID"]) if item.get("exchangeID") is not None else None,
        images=_select_image_url(item.get("images")),
    )


async def _resolve_symbol(
    http: HttpClient,
    symbol: str,
    *,
    force_exact: bool,
) -> InstrumentRef | None:
    """Single-symbol search. Returns ``None`` on no exact match."""
    query = symbol if force_exact else symbol.upper()
    try:
        body = await http.request(
            "GET",
            "/market-data/search",
            params={"internalSymbolFull": query},
        )
    except HttpStatusError as exc:
        if exc.status_code == 404:
            return None
        raise

    items = body.get("items") if isinstance(body, dict) else None
    if not items:
        return None

    # Exact match first; fall back to the first hit if no exact match (rare,
    # but better than failing the whole resolve when the user gave an
    # unambiguous symbol that just doesn't have an exact field — we surface
    # via ResolutionError later if it doesn't match the user's expectation).
    exact = next(
        (i for i in items if str(i.get("internalSymbolFull", "")).upper() == query.upper()),
        None,
    )
    chosen = exact or items[0]
    return _ref_from_search_item(chosen)


async def _fetch_ids_chunk(
    http: HttpClient,
    chunk: list[int],
    *,
    start_batch_size: int,
) -> tuple[list[InstrumentRef], int]:
    """Fetch one chunk of IDs; on 413/414 shrink and recurse.

    Returns ``(refs_in_chunk, remaining_chunk_size_to_use)``. The caller
    handles re-chunking the rest of the list with the new size. 429 / 5xx
    are NOT swallowed here — the HTTP layer's typed retry/backoff
    (:data:`etoro_bulk_trades._http.GENERAL_429_BACKOFF_S`) owns those.
    """
    from etoro_bulk_trades._http import instruments_url

    batch_size = start_batch_size
    while True:
        try:
            body = await http.request(
                "GET",
                "",
                absolute_url=instruments_url(chunk[:batch_size]),
            )
        except PayloadTooLargeError:
            ladder_idx = BATCH_LADDER.index(batch_size)
            if ladder_idx + 1 < len(BATCH_LADDER):
                batch_size = BATCH_LADDER[ladder_idx + 1]
                chunk = chunk[:batch_size]
                continue
            return [], batch_size  # already at min; let caller drop
        items = body.get("instrumentDisplayDatas") if isinstance(body, dict) else None
        return ([_ref_from_metadata_item(it) for it in items] if items else []), batch_size


async def _resolve_ids_batch(
    http: HttpClient,
    ids: list[int],
) -> list[InstrumentRef]:
    """Batched ``/market-data/instruments`` lookup with adaptive 50 → 25 sizing.

    Per the ``etoro-api-conventions`` rule, the API rejects large batches
    with HTTP 413/414. We shrink **only** on those statuses; 429 / 5xx are
    handled by the HTTP layer's retry/backoff and never trigger a shrink
    (that would hide real rate-limit problems behind sizes that succeed by
    accident).

    Multiple batches run **concurrently** — the per-client rate limiter
    (60 rpm general) serializes the underlying HTTP sends when contention
    is high, so parallel `asyncio.gather` here is safe even for thousands
    of IDs. The HTTP layer retries 429s with the documented backoff
    (1s / 5s / 30s, three attempts) before bubbling
    :class:`RateLimitError`.
    """
    if not ids:
        return []

    # Build chunks at the starting batch size; per-chunk shrink is handled
    # inside _fetch_ids_chunk for the rare 413/414 cases.
    batch_size = BATCH_LADDER[0]
    chunks = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]
    results = await asyncio.gather(
        *(_fetch_ids_chunk(http, chunk, start_batch_size=batch_size) for chunk in chunks)
    )
    out: list[InstrumentRef] = []
    for refs, _ in results:
        out.extend(refs)
    return out


async def resolve(
    http: HttpClient,
    inputs: Iterable[str | int],
    *,
    cache: InstrumentCache,
    force_exact: bool = False,
) -> dict[str | int, InstrumentRef]:
    """Resolve a mix of symbols and instrument IDs to :class:`InstrumentRef`.

    Returns a dict keyed by the **caller's input** (string symbols or int IDs
    exactly as passed in). Raises :class:`ResolutionError` if any input
    couldn't be resolved.
    """
    deduped: list[str | int] = []
    seen: set[str | int] = set()
    for x in inputs:
        if x in seen:
            continue
        seen.add(x)
        deduped.append(x)

    symbols: list[str] = []
    ids: list[int] = []
    for x in deduped:
        if isinstance(x, int):
            ids.append(x)
        elif isinstance(x, str):
            try:
                ids.append(int(x))
            except ValueError:
                symbols.append(x)
        else:
            raise TypeError(f"resolve() inputs must be str or int, got {type(x).__name__}")

    # Cache hits short-circuit.
    out: dict[str | int, InstrumentRef] = {}
    pending_symbols: list[str] = []
    pending_ids: list[int] = []
    for sym in symbols:
        hit = cache.get_symbol(sym)
        if hit is not None:
            out[sym] = hit
        else:
            pending_symbols.append(sym)
    for iid in ids:
        hit = cache.get_id(iid)
        if hit is not None:
            out[iid] = hit
        else:
            pending_ids.append(iid)

    # Live-resolve symbols **concurrently**. The per-client rate limiter
    # (60 rpm general) serializes the underlying HTTP sends when the
    # window is saturated, and the HTTP layer's typed 429 backoff
    # (1s / 5s / 30s, up to 3 retries — see
    # :data:`etoro_bulk_trades._http.GENERAL_429_BACKOFF_S`) is the
    # fallback if eToro returns 429 anyway under bursty load. This keeps
    # bulk resolves linear in symbol count *budget* but sub-linear in
    # wall-clock for typical sizes (≤60 symbols all fly in parallel and
    # complete in one round-trip; larger sets queue inside the limiter).
    # Per https://api-portal.etoro.com/getting-started/rate-limits.
    if pending_symbols:
        symbol_refs = await asyncio.gather(
            *(_resolve_symbol(http, sym, force_exact=force_exact) for sym in pending_symbols)
        )
        for sym, ref in zip(pending_symbols, symbol_refs, strict=True):
            if ref is not None:
                cache.put(ref)
                out[sym] = ref

    # Batched ID resolution.
    if pending_ids:
        refs = await _resolve_ids_batch(http, pending_ids)
        for ref in refs:
            cache.put(ref)
        for iid in pending_ids:
            ref = cache.get_id(iid)
            if ref is not None:
                out[iid] = ref

    # Re-key by caller inputs and check for misses.
    result: dict[str | int, InstrumentRef] = {}
    unresolved: list[str | int] = []
    for x in deduped:
        if x in out:
            result[x] = out[x]
        else:
            unresolved.append(x)

    if unresolved:
        raise ResolutionError(unresolved=tuple(unresolved))

    return result
