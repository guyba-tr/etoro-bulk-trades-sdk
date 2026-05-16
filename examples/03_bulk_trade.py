"""Open three small positions from a single cash pool and verify them."""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from typing import cast

from etoro_bulk_trades import (
    AsyncBulkTradesClient,
    BulkTradePlan,
    Environment,
)


async def main() -> int:
    api_key = os.environ.get("ETORO_API_KEY")
    user_key = os.environ.get("ETORO_USER_KEY")
    env_arg = os.environ.get("ETORO_ENV", "demo")
    if not api_key or not user_key:
        print("Set ETORO_API_KEY and ETORO_USER_KEY first.", file=sys.stderr)
        return 2
    env: Environment = cast("Environment", env_arg)

    plan = BulkTradePlan(
        weights={
            "AAPL": Decimal("0.4"),
            "MSFT": Decimal("0.4"),
            "GOOG": Decimal("0.2"),
        },
        total_amount=Decimal("15"),
    )

    async with AsyncBulkTradesClient.from_api_key(api_key, user_key) as client:
        await client.connect(env=env)

        result = await client.execute_bulk_trade(
            plan,
            auto_verify=True,
            verify_mode="pnl",
            verify_timeout_s=30.0,
        )

        print(f"Equity anchor : ${result.equity_anchor}")
        print(f"Cash anchor   : ${result.cash_anchor}")
        print(f"Open buffer applied: {result.open_buffer_applied}")
        print()
        print(f"{'symbol':<8} {'status':<22} {'requested':>10} {'filled':>10}")
        for trade in result.trades:
            sym = trade.intent.instrument if hasattr(trade.intent, "instrument") else "—"
            requested = str(trade.requested_amount or "")
            filled = str(trade.filled_amount or "")
            print(f"{sym!s:<8} {trade.status:<22} {requested:>10} {filled:>10}")
        print()
        print(
            f"Summary  filled=${result.summary.total_filled_amount} "
            f"pending=${result.summary.total_pending_amount} "
            f"failed=${result.summary.total_failed_amount}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
