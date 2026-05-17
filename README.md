# etoro-bulk-trades-sdk

[![CI](https://github.com/guyba-tr/etoro-bulk-trades-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/guyba-tr/etoro-bulk-trades-sdk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A typed Python SDK for the [eToro Public API](https://api-portal.etoro.com/).
Read your account, place trades — one at a time, in bulk, or as a full
portfolio rebalance — and get verified outcomes back.

## Install

```bash
pip install etoro-bulk-trades-sdk
```

## What you can do

| Capability | How |
|---|---|
| **Read your account** | `client.get_account()` → `AccountSnapshot` with cash, equity, positions, mirrors, pending orders. |
| **Place a single trade** | `client.open_trade(OpenIntent(...))` / `client.close_trade(CloseIntent(...))`. |
| **Place many trades at once** | `client.execute_bulk_trade(BulkTradePlan(weights={...}, total_amount=...))`. |
| **Rebalance to a target allocation** | `client.rebalance(RebalancePlan(target_weights={...}))` — closes what's over-weight, opens what's under-weight. |
| **Verify what landed** | `client.verify_orders(result)` flips per-trade statuses to `filled` / `pending_market_open` / `not_landed` / `failed`. Bulk and rebalance do this automatically. |
| **Resolve symbols and IDs** | `client.resolve_instruments(["AAPL", 1001, "BTC"])` accepts both forms in one call. |
| **Dedup re-runs** *(optional)* | Pass `idempotency_key=...` on any trade method with an `idempotency_store=` on the client to skip re-POSTing trades that already landed. See [Idempotency](#idempotency). |

## Quickstart

```python
import asyncio
from decimal import Decimal

from etoro_bulk_trades import (
    AsyncBulkTradesClient,
    BulkTradePlan,
    OpenIntent,
)


async def main() -> None:
    async with AsyncBulkTradesClient.from_api_key(
        user_key="...your per-user key...",
    ) as client:
        await client.connect(env="real")  # fails fast if the key is for demo

        account = await client.get_account()
        print(f"Equity: {account.equity}  Cash: {account.available_cash}")

        # Single trade
        opened = await client.open_trade(
            OpenIntent(instrument="AAPL", amount=Decimal("100"), is_buy=True),
        )
        print(opened.status, opened.order_id)

        # Bulk — verified by default
        bulk = await client.execute_bulk_trade(
            BulkTradePlan(
                weights={"AAPL": Decimal("0.5"), "MSFT": Decimal("0.5")},
                total_amount=Decimal("1000"),
            )
        )
        for trade in bulk.trades:
            print(trade.intent.instrument, trade.status)


asyncio.run(main())
```

A sync facade is available for scripts and notebooks — same nine methods,
no `async`:

```python
from etoro_bulk_trades import BulkTradesClient

with BulkTradesClient.from_api_key(user_key) as client:
    client.connect(env="demo")
    print(client.get_account().equity)
```

More end-to-end examples live under [`examples/`](./examples).

## The interface

### The client

Two top-level classes; both expose the same nine methods.

| Class | When to use |
|---|---|
| `AsyncBulkTradesClient` | Async code (FastAPI, asyncio scripts, async workers). |
| `BulkTradesClient` | Scripts, notebooks, anywhere you don't want `await`. |

```text
.from_api_key(user_key, *, api_key=None, idempotency_store=None)
.from_bearer(access_token, *, refresh_token=None, client_id=None,
             on_token_refresh=None, idempotency_store=None)
.connect(env="real"|"demo"|None)           # binds the client to one env
.close()                                   # release HTTP resources
.get_account()                             # → AccountSnapshot
.resolve_instruments(symbols_or_ids)       # → {input: InstrumentRef}
.open_trade(intent, *, idempotency_key=None)        # → TradeResult
.close_trade(intent, *, idempotency_key=None)       # → TradeResult
.execute_bulk_trade(plan, *, idempotency_key=None)  # → BulkTradeResult (verified)
.rebalance(plan, *, idempotency_key=None)           # → RebalanceResult (verified)
.verify_orders(result)                     # → upgrades statuses on a result
```

Every `idempotency_key`-bearing method is fully backward compatible —
omit it (and `idempotency_store`) and behaviour is identical to before.
See [Idempotency](#idempotency).

### Inputs you construct

All are immutable Pydantic v2 models — typos in keyword args fail at
construction.

| Model | Fields you care about |
|---|---|
| `OpenIntent` | `instrument` (symbol or ID), `amount` **or** `units` (exactly one), `is_buy`, `leverage`, optional `stop_loss_rate` / `take_profit_rate` / `trailing_stop_loss`. |
| `CloseIntent` | `position_id`, `instrument_id`, optional `units_to_deduct` (omit for a full close). |
| `BulkTradePlan` | `weights: dict[str|int, Decimal]` (must sum ≤ 1), `total_amount`, `is_buy`, `leverage`. |
| `RebalancePlan` | `target_weights`, optional `total_amount`, `is_buy`, `leverage`, `close_excluded`, `close_buffer_pct`. |

### Results you read

| Result | Carries |
|---|---|
| `TradeResult` | `status`, `order_id`, `position_id`, `requested_amount`, `filled_amount`, `error`. |
| `BulkTradeResult` | `trades: tuple[TradeResult, ...]`, `summary` (totals + counts), `equity_anchor`, `cash_anchor`. |
| `RebalanceResult` | `diff: tuple[RebalanceDelta, ...]`, `phase_1_closes`, `phase_2_opens`, `summary`. |
| `AccountSnapshot` | `equity`, `available_cash`, `total_invested`, `unrealized_pnl_total`, `positions`, `mirrors`, `pending_orders`. |
| `ConnectionInfo` | `env`, `auth_mode`, optional `gcid` / `real_cid` (when available). |

After a trade-execution call, `status` is one of:

| `status` | Meaning |
|---|---|
| `ok` | POST accepted with an `order_id`. |
| `filled` | Verified: position is open. |
| `pending_market_open` | Verified: queued for next market open. |
| `not_landed` | Verified: the POST didn't produce a position or pending order. |
| `failed` | POST was rejected (4xx) or auth failed. |
| `ambiguous` | POST outcome unknown (timeout); the verifier reconciles via `/pnl`. |
| `rate_limited_giveup` | POST was 429-throttled after the SDK exhausted retries. |

## Authentication

Two ways in — never mix them on the same client.

| Mode | Constructor | What you need |
|---|---|---|
| **API key** (most common) | `AsyncBulkTradesClient.from_api_key(user_key)` | A per-user key from **eToro Settings → Trading**. |
| **OAuth Bearer** | `AsyncBulkTradesClient.from_bearer(access_token, refresh_token=..., client_id=..., on_token_refresh=...)` | An access token plus (optionally) a refresh token. The SDK rotates the token on the first 401; your `on_token_refresh` callback persists the new pair. |

If a Bearer refresh fails (`invalid_grant`), the SDK raises
`SessionExpiredError` — surface a "Reconnect to eToro" flow rather than
retrying.

## Environment binding

`connect(env=...)` detects whether the credential belongs to `real` or
`demo`. Pass `env="real"` or `env="demo"` to assert the expected one —
the SDK raises `EnvironmentMismatchError` on a mismatch. Pass `env=None`
to auto-detect. A client is bound to its environment for its lifetime.

## Verification modes

`execute_bulk_trade` and `rebalance` verify their results by default. You
can override the mode with `verify_mode=`:

| Mode | Behaviour |
|---|---|
| `"ws"` (default) | WebSocket-first; falls back to `/pnl` if the socket drops or doesn't see the order. Best for close-heavy flows. |
| `"auto"` | 5-second WebSocket window, then `/pnl`. Best latency profile for opens. |
| `"pnl"` | Sleep through the 10-second `/pnl` cache, read once, classify. Slowest but always works. |

Call `verify_orders(result)` manually after `auto_verify=False` if you
want to defer verification.

## Idempotency

The eToro execution endpoints don't accept an idempotency key, so the
SDK provides one *client-side*. It's entirely opt-in — wire a store on
the client, then pass an `idempotency_key` on any trade method. Without
both, nothing changes.

```python
from etoro_bulk_trades import (
    AsyncBulkTradesClient,
    BulkTradePlan,
    InMemoryIdempotencyStore,
    OpenIntent,
)

store = InMemoryIdempotencyStore()
client = AsyncBulkTradesClient.from_api_key(user_key, idempotency_store=store)
await client.connect(env="real")

# First call: POSTs to eToro, caches the TradeResult under "trade-abc".
r1 = await client.open_trade(
    OpenIntent(instrument="AAPL", amount=Decimal("100")),
    idempotency_key="trade-abc",
)

# Re-run with the same key: returns the cached r1; no POST, no /pnl read.
r2 = await client.open_trade(
    OpenIntent(instrument="AAPL", amount=Decimal("100")),
    idempotency_key="trade-abc",
)
```

For `execute_bulk_trade` and `rebalance`, the `idempotency_key` is a
**batch** key; the SDK derives stable per-trade keys from it. Re-running
the same batch skips the trades that already landed and POSTs only the
rest — the natural shape for retrying a partial bulk:

```python
plan = BulkTradePlan(
    weights={"AAPL": Decimal("0.5"), "MSFT": Decimal("0.5")},
    total_amount=Decimal("1000"),
)
await client.execute_bulk_trade(plan, idempotency_key="rebalance-2026-05-17")
# AAPL succeeded, MSFT was ambiguous — retry:
await client.execute_bulk_trade(plan, idempotency_key="rebalance-2026-05-17")
# AAPL is skipped (cached); MSFT POSTs once more.
```

What gets cached: terminal statuses only (`ok`, `filled`,
`pending_market_open`, `failed`). `ambiguous`, `rate_limited_giveup`,
and `not_landed` are intentionally *not* cached so re-runs can retry
them.

Bring your own store: implement the `IdempotencyStore` protocol
(`async def get(key)` / `async def put(key, result)`) backed by Redis,
SQL, or whatever — the SDK uses it through the same code path.

## Exceptions

Every exception inherits from `EtoroSDKError`:

```text
EtoroSDKError
├── AuthError
│   ├── InvalidCredentialsError      # 401, no refresh available
│   ├── SessionExpiredError          # Bearer refresh failed
│   └── EnvironmentMismatchError     # connect(env=X), key is for Y
├── RateLimitError                   # 429 after retries
├── TransportError                   # 5xx / network drop
├── InstrumentResolutionError        # symbol or ID not found
├── InsufficientCashError            # planned > available cash
├── PendingOrdersExistError          # rebalance refuses to race the queue
└── RebalanceCashShortfallError      # phase-1 closes under-delivered
```

Single-trade calls raise these; bulk calls fold per-trade failures into
`TradeResult.status` + `TradeResult.error` so one failure can't take down
the rest of the batch.

## License

MIT
