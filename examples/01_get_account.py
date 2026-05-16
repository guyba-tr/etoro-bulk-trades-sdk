"""Print a summary of the connected account.

Reads ``ETORO_API_KEY``, ``ETORO_USER_KEY``, and optionally ``ETORO_ENV``
(``real`` or ``demo``) from the environment.

.. code-block:: bash

    export ETORO_API_KEY=...
    export ETORO_USER_KEY=...
    export ETORO_ENV=demo  # or real
    uv run python examples/01_get_account.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import cast

from etoro_bulk_trades import AsyncBulkTradesClient, Environment


async def main() -> int:
    api_key = os.environ.get("ETORO_API_KEY")
    user_key = os.environ.get("ETORO_USER_KEY")
    env_arg = os.environ.get("ETORO_ENV")
    if not api_key or not user_key:
        print("Set ETORO_API_KEY and ETORO_USER_KEY first.", file=sys.stderr)
        return 2
    env: Environment | None = cast("Environment", env_arg) if env_arg in ("real", "demo") else None

    async with AsyncBulkTradesClient.from_api_key(api_key, user_key) as client:
        info = await client.connect(env=env)
        print(f"Connected to {info.env} (auth_mode={info.auth_mode})")
        if info.gcid is not None:
            print(f"  gcid={info.gcid} realCid={info.real_cid}")

        snap = await client.get_account()
        print(f"\nEquity         : ${snap.equity}")
        print(f"Available Cash : ${snap.available_cash}")
        print(f"Total Invested : ${snap.total_invested}")
        print(f"Unrealized P&L : ${snap.unrealized_pnl_total}")
        print(f"Open positions : {len(snap.positions)}")
        print(f"Pending orders : {len(snap.pending_orders)}")
        print(f"Mirrors        : {len(snap.mirrors)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
