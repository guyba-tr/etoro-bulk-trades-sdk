"""Open a single $5 AAPL position and verify it.

WARNING: this places a real trade. Use a demo credential by setting
``ETORO_ENV=demo``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from typing import cast

from etoro_bulk_trades import (
    AsyncBulkTradesClient,
    CloseIntent,
    Environment,
    OpenIntent,
    TradeResult,
)


async def main() -> int:
    user_key = os.environ.get("ETORO_USER_KEY")
    api_key = os.environ.get("ETORO_API_KEY")
    env_arg = os.environ.get("ETORO_ENV", "demo")
    if not user_key:
        print("Set ETORO_USER_KEY first.", file=sys.stderr)
        return 2
    env: Environment = cast("Environment", env_arg)

    async with AsyncBulkTradesClient.from_api_key(user_key, api_key=api_key) as client:
        await client.connect(env=env)

        intent = OpenIntent(instrument="AAPL", amount=Decimal("5"))
        opened = await client.open_trade(intent)
        print(f"Open  → status={opened.status} order_id={opened.order_id}")

        verified = await client.verify_orders(opened, mode="pnl", timeout_s=30.0)
        assert isinstance(verified, TradeResult)
        print(f"Verify → status={verified.status} position_id={verified.position_id}")

        if verified.position_id is not None and verified.instrument_id is not None:
            closed = await client.close_trade(
                CloseIntent(
                    position_id=verified.position_id,
                    instrument_id=verified.instrument_id,
                )
            )
            print(f"Close → status={closed.status} order_id={closed.order_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
