"""Rebalance an account to a target allocation.

The default plan targets 50% AAPL / 50% MSFT using current equity as the
pool. Use ``--dry-run`` (the default here) to preview the diff without
placing any trades.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from decimal import Decimal
from typing import cast

from etoro_bulk_trades import (
    AsyncBulkTradesClient,
    Environment,
    PendingOrdersExistError,
    RebalancePlan,
)


async def main() -> int:
    parser = argparse.ArgumentParser(description="Rebalance an eToro account.")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--execute", action="store_false", dest="dry_run")
    args = parser.parse_args()

    user_key = os.environ.get("ETORO_USER_KEY")
    api_key = os.environ.get("ETORO_API_KEY")
    env_arg = os.environ.get("ETORO_ENV", "demo")
    if not user_key:
        print("Set ETORO_USER_KEY first.", file=sys.stderr)
        return 2
    env: Environment = cast("Environment", env_arg)

    plan = RebalancePlan(
        target_weights={
            "AAPL": Decimal("0.5"),
            "MSFT": Decimal("0.5"),
        }
    )

    async with AsyncBulkTradesClient.from_api_key(user_key, api_key=api_key) as client:
        await client.connect(env=env)
        try:
            result = await client.rebalance(
                plan,
                dry_run=args.dry_run,
                auto_verify=not args.dry_run,
                verify_mode="pnl",
            )
        except PendingOrdersExistError as exc:
            print(f"Refusing to rebalance: {exc.pending_count} pending orders exist.")
            return 1

        print(f"{'symbol':<10} {'action':<10} {'current':>12} {'target':>12} {'delta':>12}")
        for delta in result.diff:
            print(
                f"{delta.instrument.symbol:<10} {delta.action:<10} "
                f"{delta.current_amount!s:>12} {delta.target_amount!s:>12} "
                f"{delta.delta_amount!s:>12}"
            )
        if not args.dry_run:
            print(
                f"\nPhase 1 closes: {len(result.phase_1_closes)} "
                f"| Phase 2 opens: {len(result.phase_2_opens)}"
            )
            print(f"Counts by status: {result.summary.counts_by_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
