"""Probe harness for the four open eToro-API ambiguities (A1, A4, A5, A7).

Run against the **demo** environment with a partner key pair OR a Bearer
access token (or both). Prints a short report; never writes anything other
than the trades it opens-then-closes for A1 / A4.

Usage:

.. code-block:: bash

    export ETORO_DEMO_API_KEY=...
    export ETORO_DEMO_USER_KEY=...
    # optionally:
    export ETORO_DEMO_BEARER=...
    export ETORO_DEMO_REFRESH=...
    export ETORO_DEMO_CLIENT_ID=...

    uv run python scripts/probe_open_questions.py

What it tests:

* **A1** — open a $5 AAPL position with ``InstrumentID`` (capital D). If
  rejected with a casing-related error, retry with ``InstrumentId``
  (lowercase d). Reports which one works.
* **A4** — same as A1 but using Bearer auth (skipped if no token in env).
* **A5** — ``GET /api/v1/me`` with API-key headers. Reports whether it
  returns ``gcid``/``realCid`` or 401s.
* **A7** — two back-to-back ``GET /pnl`` reads ~1s apart. Compares response
  bodies; if identical, the cache is per-user/env (per-key would have
  yielded a fresh read for the second call). Per-spec the cache is 10s,
  so identical bodies confirm the spec.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any

from etoro_bulk_trades._auth import (
    ApiKeyAuth,
    AuthHandle,
    BearerAuth,
    probe_environment,
)
from etoro_bulk_trades._http import HttpClient
from etoro_bulk_trades._resolve import InstrumentCache, resolve
from etoro_bulk_trades.errors import HttpStatusError

PROBE_SYMBOL = "AAPL"
PROBE_AMOUNT = 5.0


async def probe_a1(http: HttpClient, env: str) -> None:
    """A1 — confirm the casing of the ``InstrumentID`` request body field."""
    print("\n── A1: trade-execution InstrumentID casing")
    cache = InstrumentCache()
    refs = await resolve(http, [PROBE_SYMBOL], cache=cache)
    instrument_id = int(refs[PROBE_SYMBOL].instrument_id)
    print(f"   AAPL → instrument_id={instrument_id}")

    async def try_open(casing: str) -> tuple[int | None, Any]:
        body = {
            casing: instrument_id,
            "IsBuy": True,
            "Leverage": 1,
            "Amount": PROBE_AMOUNT,
        }
        try:
            response: Any = await http.request(
                "POST",
                f"/trading/execution/{env}/market-open-orders/by-amount",
                json=body,
            )
        except HttpStatusError as exc:
            return exc.status_code, exc.body
        return 200, response

    for casing in ("InstrumentID", "InstrumentId"):
        status, body = await try_open(casing)
        if status == 200 and isinstance(body, dict):
            order_id = body.get("orderForOpen", {}).get("orderID")
            print(f"   ✓ Casing '{casing}' worked. order_id={order_id}")
            position_id = body.get("orderForOpen", {}).get("positionID")
            if position_id is not None:
                try:
                    await http.request(
                        "POST",
                        f"/trading/execution/{env}/market-close-orders/positions/{position_id}",
                        json={"UnitsToDeduct": None},
                    )
                    print(f"   ↳ closed position {position_id}")
                except HttpStatusError as exc:
                    print(f"   ↳ close failed: {exc}")
            return
        print(f"   ✗ Casing '{casing}' rejected: status={status} body={_short(body)}")
    print("   Both casings failed; check credentials and try again.")


async def probe_a4(env: str) -> None:
    """A4 — repeat the A1 trade with Bearer auth."""
    print("\n── A4: Bearer-auth trade-execution support")
    access = os.environ.get("ETORO_DEMO_BEARER")
    if not access:
        print("   ⚠ Skipped (no ETORO_DEMO_BEARER set)")
        return
    handle = AuthHandle(
        BearerAuth(
            access_token=access,
            refresh_token=os.environ.get("ETORO_DEMO_REFRESH"),
            client_id=os.environ.get("ETORO_DEMO_CLIENT_ID"),
        )
    )
    http = HttpClient(auth_provider=handle)
    handle.bind(http)
    try:
        await probe_a1(http, env)
    finally:
        await http.aclose()


async def probe_a5(http: HttpClient) -> None:
    """A5 — does ``/api/v1/me`` work with API-key auth?"""
    print("\n── A5: /api/v1/me coverage with API-key auth")
    try:
        body = await http.request("GET", "/me")
    except HttpStatusError as exc:
        print(f"   ✗ /me rejected: status={exc.status_code} body={_short(exc.body)}")
        return
    if not isinstance(body, dict):
        print(f"   ✗ unexpected body type: {type(body).__name__}")
        return
    gcid = body.get("gcid")
    real_cid = body.get("realCid") or body.get("realCID")
    print(f"   ✓ /me returned gcid={gcid} realCid={real_cid}")


async def probe_a7(http: HttpClient, env: str) -> None:
    """A7 — PnL cache scope (per-user-per-env? per-key? other?)."""
    print("\n── A7: /pnl cache scope")
    body1: Any = await http.request("GET", f"/trading/info/{env}/pnl")
    await asyncio.sleep(1.0)
    body2: Any = await http.request("GET", f"/trading/info/{env}/pnl")
    identical = json.dumps(body1, sort_keys=True) == json.dumps(body2, sort_keys=True)
    print(
        f"   Two GETs ~1s apart {'returned identical' if identical else 'returned different'} bodies — "
        f"cache appears to be {'in effect' if identical else 'absent or shorter than 1s'}"
    )


def _short(body: Any, limit: int = 200) -> str:
    s = repr(body)
    return s if len(s) <= limit else s[:limit] + "…"


async def main() -> int:
    user_key = os.environ.get("ETORO_DEMO_USER_KEY")
    api_key = os.environ.get("ETORO_DEMO_API_KEY")
    if not user_key:
        print(
            "ETORO_DEMO_USER_KEY is required for probes A1/A5/A7.",
            file=sys.stderr,
        )
        return 2

    auth = (
        ApiKeyAuth(user_key=user_key, api_key=api_key) if api_key else ApiKeyAuth(user_key=user_key)
    )
    handle = AuthHandle(auth)
    http = HttpClient(auth_provider=handle)
    handle.bind(http)

    print("Detected env...")
    env = await probe_environment(http)
    print(f"   env={env}")
    if env != "demo":
        print("⚠ This script must be run against a DEMO credential.", file=sys.stderr)
        await http.aclose()
        return 3

    started = time.monotonic()
    try:
        await probe_a5(http)
        await probe_a7(http, env)
        await probe_a1(http, env)
    finally:
        await http.aclose()

    # Bearer probe uses its own client.
    await probe_a4(env)

    elapsed = time.monotonic() - started
    print(f"\nDone in {elapsed:.1f}s.")
    return 0


if __name__ == "__main__":
    httpx_logger = __import__("logging").getLogger("httpx")
    httpx_logger.setLevel("WARNING")
    raise SystemExit(asyncio.run(main()))
