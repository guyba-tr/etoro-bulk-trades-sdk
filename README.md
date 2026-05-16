# etoro-bulk-trades-sdk

[![CI](https://github.com/guyba-tr/etoro-bulk-trades-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/guyba-tr/etoro-bulk-trades-sdk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Async-first Python SDK for the [eToro Public API](https://api-portal.etoro.com/),
focused on **single trades, bulk trading, rebalancing, and verified order
outcomes**.

> **Status:** alpha. Treat outputs as advisory until you have run the probe
> harness against your own demo credentials (`scripts/probe_open_questions.py`).

## Highlights

- **Two equivalent auth modes** — `x-api-key` + `x-user-key`, or `Authorization: Bearer`
  (with optional refresh-token rotation via a callback). Never both at once.
- **Environment enforcement at `connect()`** — pass `env="real"` or `"demo"`
  to fail fast if the credential is bound to the other side.
- **Correct account math** — Available Cash, Total Invested, Profit/Loss and
  Equity computed verbatim per the eToro guides, including mirror-position
  dedup and the `closedPositionsNetProfit` adjustment.
- **Bidirectional resolver** — accepts symbols (`"AAPL"`) and instrument IDs
  (`1001`) in the same call; in-memory cache; literal-comma `/instruments`
  URL builder so encoders can't break batched lookups.
- **Bulk trades and rebalancing** with anchor-frozen sizing, ceiling math,
  the 1% open-buffer rule, and at-most-once execution discipline.
- **WebSocket-first verification** of executed orders, with a robust PnL
  fallback when the socket drops.
- **Sync facade** (`BulkTradesClient`) over the async core
  (`AsyncBulkTradesClient`) for scripts and notebooks.

## Install

```bash
pip install etoro-bulk-trades-sdk
# or, for development:
uv sync --extra dev
```

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
        api_key="...partner key...",
        user_key="...per-user key...",
    ) as client:
        info = await client.connect(env="real")  # raises if key is demo

        account = await client.get_account()
        print(f"Equity: {account.equity}, Cash: {account.available_cash}")

        # Single trade
        opened = await client.open_trade(
            OpenIntent(instrument="AAPL", amount=Decimal("100"), is_buy=True),
        )
        print(opened.status, opened.order_id)

        # Bulk
        plan = BulkTradePlan(
            weights={"AAPL": Decimal("0.5"), "MSFT": Decimal("0.5")},
            total_amount=Decimal("1000"),
        )
        bulk = await client.execute_bulk_trade(plan)  # auto-verifies via WS
        for trade in bulk.trades:
            print(trade.intent.instrument, trade.status)


asyncio.run(main())
```

The sync facade is a drop-in replacement for scripts and notebooks:

```python
from etoro_bulk_trades import BulkTradesClient

with BulkTradesClient.from_api_key(api_key, user_key) as client:
    client.connect(env="demo")
    snap = client.get_account()
```

## Authentication

The SDK supports both Public API auth modes documented at
<https://api-portal.etoro.com/getting-started/authentication>.

| Mode | Constructor | Headers used |
|---|---|---|
| Partner / API key | `AsyncBulkTradesClient.from_api_key(api_key, user_key)` | `x-api-key`, `x-user-key` |
| OAuth / Bearer | `AsyncBulkTradesClient.from_bearer(access_token, ...)` | `Authorization: Bearer ...` |

For Bearer auth, supply `refresh_token`, `client_id`, and an
`on_token_refresh: Callable[[TokenPair], None]` callback so the SDK can
rotate the token in-process and your application can persist the new pair.
The SDK does this transparently on the first 401 of a request; if the
refresh itself fails (`400 invalid_grant`), it raises
`SessionExpiredError` — surface a "Reconnect to eToro" flow rather than
retrying.

## Environment enforcement

`connect(env=...)` probes `GET /trading/info/real/pnl` once to detect the
credential's binding. The matrix:

| `env` arg | Key environment | Result |
|---|---|---|
| `None` | real | returns `ConnectionInfo(env="real")` |
| `None` | demo | returns `ConnectionInfo(env="demo")` |
| `"real"` | real | returns `ConnectionInfo(env="real")` |
| `"real"` | demo | raises `EnvironmentMismatchError` |
| `"demo"` | real | raises `EnvironmentMismatchError` |
| `"demo"` | demo | returns `ConnectionInfo(env="demo")` |

A client is bound to its environment for its lifetime — calling `connect`
twice with different `env` values raises.

## The four workflows

| Method | Purpose |
|---|---|
| `open_trade(intent)` | Single open by amount or by units (exactly one). |
| `close_trade(intent)` | Full or partial close by `position_id`. |
| `execute_bulk_trade(plan)` | Multi-position open from one cash pool, weight-based. |
| `rebalance(plan)` | Two-phase close-then-open to a target allocation. |

All four share the same execution disciplines (see
[the plan](./.cursor/plans/etoro-bulk-trades-sdk-v1_7e4e5058.plan.md) for the
full design notes):

- **Anchor freeze** — read `/pnl` once at workflow start; freeze
  `EQUITY_ANCHOR` and `CASH_ANCHOR`; never recompute mid-flow.
- **Ceilings, never targets** — `amount = floor(weight * total * 100) / 100`;
  the SDK never rounds up.
- **Open buffer** — when the plan would leave Available Cash below 1% of
  equity, shrink each open by 1% so per-trade fees can't push displayed
  cash negative.
- **At-most-once** — every trade-execution `POST` is sent at most once;
  timeouts / connection drops are recorded as `ambiguous` and reconciled by
  reading state in `verify_orders` (never by re-firing).

### Verification

`verify_orders` upgrades each `TradeStatus` to one of:

- `filled` — observed in `positions[]` (or WS confirmed).
- `pending_market_open` — present in `ordersForOpen[]`.
- `not_landed` — neither WS confirmed nor present in `/pnl`.
- `failed` — preserved from execution (the verifier never downgrades).

Modes:

- `ws` (default) — open a private WebSocket subscription and match by
  `OrderID`. On any drop or timeout, fall back to `pnl`.
- `pnl` — sleep 10s for the PnL cache, read `/pnl`, match by
  `instrumentID`. Always safe.
- `auto` — try `ws` for 5s, then fall back to `pnl`.

`execute_bulk_trade` and `rebalance` both default to `auto_verify=True`, so
their results have the verified statuses already applied.

## Exceptions

Every exception the SDK raises inherits from `EtoroSDKError`:

```
EtoroSDKError
├── AuthError
│   ├── InvalidCredentialsError      # 401, no refresh available
│   ├── SessionExpiredError          # Bearer refresh failed (invalid_grant)
│   └── EnvironmentMismatchError     # connect(env=X), key is for Y
├── RateLimitError                   # 429 after retries
├── TransportError                   # 5xx / network
├── PayloadTooLargeError             # 413/414, handled internally
├── ResolutionError                  # symbol / ID not found
├── InsufficientCashError            # bulk: planned > available
├── PendingOrdersExistError          # rebalance: pending opens exist
├── RebalanceCashShortfallError      # phase 1 closes under-delivered
├── CeilingViolationError            # post-fill: actual > expected
└── AmbiguousTradeError              # opt-in strict mode only
```

## Probing open API questions

`scripts/probe_open_questions.py` runs against a **demo** credential and
prints a short report covering four ambiguities that the public docs
don't pin down:

- **A1** — does the trade-execution body field need `InstrumentID` (capital D)
  or `InstrumentId` (lowercase d)? The SDK ships with capital D by default;
  the probe places a $5 AAPL position to confirm and reverts immediately.
- **A4** — does Bearer auth work on trade-execution endpoints?
- **A5** — does `GET /api/v1/me` accept API-key headers and return
  `gcid` / `realCid`?
- **A7** — is the `/pnl` cache scoped per-user-per-env, per-key, or
  per-request-ID?

```bash
export ETORO_DEMO_API_KEY=...
export ETORO_DEMO_USER_KEY=...
uv run python scripts/probe_open_questions.py
```

## Development

```bash
uv sync --extra dev
uv run ruff check .
uv run ruff format .
uv run mypy
uv run pytest tests/unit
```

Integration tests against the demo environment are opt-in and skipped by
default:

```bash
export ETORO_DEMO_API_KEY=...
export ETORO_DEMO_USER_KEY=...
uv run pytest tests/integration -m integration
```

## License

MIT
