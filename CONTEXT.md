# eToro Bulk Trades SDK — Domain Context

The shared vocabulary for this codebase. Every internal module name and
every public API term in this glossary is **load-bearing** — agents and
contributors should use these terms exactly, not synonyms.

## Language

**Anchor snapshot**
The single read of `/trading/info/{env}/pnl` taken at the start of a
workflow. `EQUITY_ANCHOR` and `CASH_ANCHOR` are derived from it and
**never** recomputed mid-flow. `rebalance` deliberately takes a second
anchor after Phase 1 because each phase has its own snapshot.
_Avoid_: snapshot (ambiguous — every `/pnl` read produces one), portfolio.

**Outcome**
The classifier's verdict on one trade-execution POST. A typed value
(`status` + `error` + `next_action` + `order_id` + `filled_amount`) that
sits between the HTTP exception layer and the public `TradeResult`. The
single source of truth for "which row of the at-most-once decision table
fired."
_Avoid_: status (`Outcome` carries one — the status alone is a substring).

**Decision table** _(at-most-once)_
The seven-row table in the `etoro-api-conventions` rule § "Trade-execution
endpoints have NO idempotency key" that maps a POST outcome (2xx / 4xx /
401 / 429 / 5xx / timeout) to a recovery action. `_at_most_once.py` is
the executable form of that table. Adding a new outcome class means
editing exactly that file plus its test.
_Avoid_: retry policy (the table is *more* than retry — it also decides
when not to retry).

**Sizing**
The pure-math layer that turns an `OpenIntent` plus an anchor snapshot
into a USD `Amount` field. Implements ceilings (floor to cents, never
round up), the close-side ceil buffer (round up close amounts to absorb
fees), and the 1% open buffer (shrink opens when post-trade cash would
drop below 1% of equity). Lives in `_sizing.py`.

**Rebalance planning**
The pure-math layer that turns a `RebalancePlan` + an anchor snapshot
into `(RebalanceDelta[], CloseIntent[])`. Lives in
`_rebalance_planning.py` as `build_diff` + `select_positions_for_close`.
Distinct from execution — planning is deterministic; execution is the
HTTP round-trips.

**PnlReader**
The single seam for `/trading/info/{env}/pnl` reads. Owns the wire
shape, the 10-second per-user cache TTL, and the environment-segmented
path. Lives in `_pnl.py`. Both `_execute` and `_verify` go through it.
_Avoid_: snapshot reader (it does more — it also classifies positions
for the verifier).

**Summary**
The roll-up of per-trade results into a `BulkTradeSummary` /
`RebalanceSummary`. Lives in `_summary.py`. Called twice per workflow:
once when execution finishes (pre-verification), once after the verifier
upgrades statuses.

**Anchor freeze**
The discipline: read `/pnl` once, freeze the anchor, never re-read for
sizing. The `Anchor snapshot` is the *data*; the **freeze** is the
*rule*. Violations create the "sized against stale prices" bug class.

**Pre-existing position IDs**
The set of `PositionID` values an instrument already had **before** a
trade was placed. Captured at sizing time, carried on every
`TradeResult.pre_existing_position_ids`, consumed by the verifier to
identify the *new* position safely. Without this, the verifier can't
distinguish "the trade I just placed" from a position the user opened
yesterday — and closing the wrong position is unrecoverable.

**Verification modes**
`ws` (default), `auto`, `pnl`. WS opens a private WebSocket and matches
by `OrderID`. PnL sleeps 10s for the cache window then reads `/pnl`
once. `auto` tries WS for 5s, then falls back. Documented under
`VerifyMode` in `types.py`.

**Idempotency key** _(client-side)_
The eToro execution endpoints do **not** accept an idempotency key —
the SDK provides a *client-side* one. A caller-supplied
`idempotency_key` on `open_trade` / `close_trade` (per-trade) or
`execute_bulk_trade` / `rebalance` (batch) is combined with the
`instrument_id` (opens) or `position_id` (closes) to form a stable
key in the configured `IdempotencyStore`. Only terminal statuses
(`ok` / `filled` / `pending_market_open` / `failed`) are cached;
`ambiguous` / `rate_limited_giveup` / `not_landed` are intentionally
**not** cached so callers can retry. Default store is
`NullIdempotencyStore` (no-op); opt in with `InMemoryIdempotencyStore`
or a bring-your-own implementation of the `IdempotencyStore` protocol.
_Avoid_: "request ID" (eToro's `x-request-id` is a per-attempt trace
header; the idempotency key is a per-intent dedup key — they don't mix).

## Module map

| Module | Owns |
|---|---|
| `_http` | HTTP transport, 401-then-refresh-once, retry by error class |
| `_auth` | `AuthContext` (`ApiKeyAuth` \| `BearerAuth`), env probing, token refresh |
| `_pnl` | `/pnl` reads + `ClassifiedPositions` projection |
| `_account` | Pure formulas turning a `clientPortfolio` JSON into `AccountSnapshot` |
| `_instrument_resolution` | Symbol ↔ instrument-ID resolution with adaptive batching |
| `_sizing` | Ceiling/floor + open buffer math |
| `_rebalance_planning` | Diff + close-position selection |
| `_at_most_once` | The decision-table classifier (one `Outcome` per POST) |
| `_idempotency` | Optional client-side dedup: `IdempotencyStore` protocol + default impls + key derivation |
| `_summary` | Roll-up summarisers (called by `_execute` and `_verify`) |
| `_execute` | Orchestration: single + bulk + rebalance workflows |
| `_verify` | Status upgrade via WS or PnL reconciliation |
| `_ws` | WebSocket client for the `private` topic |
| `_ratelimit` | Sliding-window limiters (60 rpm general, 20 rpm execution) |
| `types` | Pydantic models, branded IDs, enums |
| `errors` | Typed exception hierarchy |
| `client` / `_sync` | Public `AsyncBulkTradesClient` + sync facade |

## Relationships

- An **Anchor snapshot** belongs to one workflow run.
- A trade-execution POST always produces exactly one **Outcome**.
- A `TradeResult` carries the **Outcome**'s `status` plus the
  workflow's `pre_existing_position_ids` for that instrument.
- The **PnlReader** is the only module that knows the `/pnl` wire shape.

## Flagged ambiguities

- "snapshot" used to mean both the **Anchor snapshot** and any `/pnl`
  read — resolved: the workflow-frozen one is the `Anchor snapshot`;
  every read is `read_snapshot(...)` and produces an `AccountSnapshot`.
- "classify" was overloaded across `_classify_open_response` (success
  parsing) and `_pnl_classify` (verification projection) — resolved:
  the at-most-once classifier returns `Outcome`s; the verifier
  projection returns `ClassifiedPositions`. Different concepts, distinct
  names.
