# CLI and Kafka contracts

Put `--config /absolute/path/account.json` before the subcommand, or set
`FANGDE_CONFIG`. The default is `config/fangde.config.json` in this project.

```bash
fangde holdings
fangde orders                         # alias for today-orders
fangde history-orders --from 2026-10-01 --to 2026-10-26
fangde session-authenticate           # password login, trading unlock, holdings check
fangde market-order --symbol AAPL --quantity 1 --side Buy
fangde limit-order --symbol AAPL --quantity 1 --side Buy --price 100
fangde limit-order --symbol AAPL --quantity 1 --side Buy --price 100 --kill
fangde cancel-order EXACT_ORDER_NO
fangde cancel-all
```

Order/cancel examples above are previews. Append `--execute` only when you intend
to send the operation. `--dry-run` and `--no-submit` remain preview aliases for
compatibility. `--kill` requests the emulated FOK workflow. Prices and symbols
above are illustrative, not suggested trades.

US premarket and after-hours trading is enabled by default through
`api.extendedHoursMarketOrders: true`. During premarket (04:00–09:30 New York)
and after-hours (normally 16:00–20:00), `MARKET_ORDER` and `market-order` become
DAY limit orders using the fresh Kafka offers for buys or bids for sells.
The ticket sends Fangde's `extended_trading=1`. A converted market command can
remain unfilled if prices move away from its limit; it is not a broker market order.
Explicit limits, including emulated FOK, also set the extended-hours flag.
Regular-session market commands remain broker market orders.

The calendar handles DST, holidays and early closes. After an early close,
the supported after-hours window ends four hours after that close, capped at
20:00 New York. Overnight market commands remain unsupported. No extra Kafka
field or CLI switch is needed for premarket/after-hours conversion; setting
`api.extendedHoursMarketOrders` to `false` disables that conversion.

Password-enabled sessions recover automatically during reads. Before each order
or cancellation, the client validates login and performs a fresh trading-password
unlock, even when holdings reads succeed with a saved trading token.
`session-authenticate` forces login/unlock explicitly; add `--retry` only
after investigating an earlier failed/interrupted password attempt. Neither form
submits or cancels an order. Set passwords with `python -m fangde.password_setup
--session /private/account.session.json` using masked local prompts.

The default runner topics match GigaMoney:

| Direction | Topic | Contract |
|---|---|---|
| Consume | `trading-commands` | JSON commands, filtered by exact `account_id` |
| Consume | `price-books` | Timestamped books, cached by a separate reader |
| Publish | `account-details` | Account cash/positions and `trading_enabled` |
| Publish | `order-status` | KTrader v1 snapshots with per-order sequence |

Supported command `type` values are `MARKET_ORDER`, `LIMIT_ORDER`,
`LIMIT_ORDER_FOK`, `CANCEL_OPEN_ORDERS`, and `SET_TRADING_ENABLED`.
Limit commands also accept `time_in_force: "FOK"` or `cancel_unfilled: true`.
`qty_shares` is required; notional orders and short sales are unsupported.

```json
{
  "account_id": "FANGDE-A",
  "command_id": "unique-command-id",
  "ts": "2026-10-27T14:00:00Z",
  "type": "LIMIT_ORDER",
  "symbol": "AAPL",
  "side": "BUY",
  "qty_shares": 1,
  "limit_price": "100.00"
}
```

Use a current UTC `ts`; the example timestamp will become stale. Missing/old
timestamps are rejected by the default five-minute command age rule. Use a unique
`command_id` for each intent. The same ID with the same completed order returns
its recorded result; changing its order parameters is rejected. If no ID is
provided, the runner derives one from topic/partition/offset.

For troubleshooting, `Command received` logs the trading fields and the Kafka
topic, partition and offset, excluding arbitrary extra message fields.
`Command finished` retains that Kafka location, outcome and commit decision.
Prepared order journals also retain the command's Kafka location and the selected
price-book location. Authentication failures identify `validate_login`,
`unlock_trading` or `submit`, plus the broker code and endpoint; workflow logs
include the prepared trading session, effective type and price.
Share-availability logs distinguish submitted sells from strategies awaiting
their trigger. If authentication cannot be recovered, the runner stops intake and
exits with `authentication_required` in `runner-status`, including the failed
command ID and Kafka location. A successful holdings read does not verify trading
authorization. Restore it using `session-authenticate` before restarting the runner;
the journal prevents replay of a previously dispatched, rejected command.

A `SET_TRADING_ENABLED` command uses the same account/ID/timestamp envelope plus
`"trading_enabled": true` or `false`. Enabling persists across restarts. The
configuration default is used only when no state has been saved. New orders are
ignored and committed while disabled; cancellation remains available.

Books use `symbol`, integer nanosecond `update_time`, and `offer_side`/`bid_side`
objects containing their own `update_time` and `levels`. Each level has `prc` and
`total_qty` (or `qty`). For a buy the reader walks offers; for a sell it walks bids.
Insufficient visible depth uses the outermost available level, so partial fills
remain possible. The cache refreshes every 30 seconds by default, rejects
future-dated/inconsistent books, and enforces a maximum age of five minutes.

Orders have an independent broker polling worker targeting **0.5 seconds**
start-to-start (`kafka.orderPollIntervalSeconds`). Positions and cash target one
second (`kafka.holdingsIntervalSeconds`). Slow broker responses, authentication
renewal and financial writes can extend the actual cadence; requests never overlap
within a poller or accumulate catch-up work. Order monitoring continues during
order-action delays and holdings failures. Its logs include measured cadence.
Normal broker reads use independent, validated session snapshots, so a slow
holdings response does not hold the order poller. Session access is queued fairly;
renewal and financial writes retain the exclusive lock. Reads rejected after a
concurrent renewal reuse the latest credentials before attempting another renewal.
Completed order/cancel handling wakes order polling and triggers an immediate
holdings refresh, with no default post-order delay.

Submission/cancellation confirmation persists each known order change immediately
and wakes a separate Kafka delivery worker with zero producer linger. Kafka delays
do not block new broker observations. Existing installations should set
`kafka.orderPollIntervalSeconds` to `0.5`, `kafka.holdingsIntervalSeconds` to `1`,
and `api.postOrderPublishDelaySeconds` to `0`.

Account details are published on startup and whenever cash, positions (including
prices and P/L), or trading-enabled state changes. Observation timestamps and
position ordering alone do not cause publication. A failed delivery does not
advance the last-published state and is retried on the next poll. The account
payload preserves GigaMoney's `account_id`, `account_num_id`, `cash`,
`cash_by_currency`, `positions`, `ts`, and `trading_enabled` fields.

Order snapshots use the JSON array key `[account_id,order_id]` and schema version
1. The daily list discovers orders, including older open strategies. Known
nonterminal orders omitted from that list are queried by exact broker ID until
terminal, including after midnight or restart. Histories are queried only on
explicit request. Absence from a list never implies cancellation or rejection.
External/app-created orders can be published, without a fabricated command ID.
Unknown statuses are labeled `UNKNOWN`; missing/invalid data is reported rather
than invented. Partial cancellations preserve cumulative filled quantity.

Order status records are persisted before publication and marked delivered only
after Kafka acknowledgment. A durable per-order queue retains newer observations
while earlier deliveries fail. Retries preserve payload and sequence, survive
midnight, and continue when broker reads fail. Poll-time-only changes and unchanged
states after restart are suppressed. Stale list responses cannot decrease fills
or return a terminal order to working status. An unavailable order query does not
prevent independently verified holdings from being published. The exact KTrader schema is in
`tests/fixtures/order-status.v1.schema.json`.
Fangde can renew the displayed creation date on standing strategy orders; their
first persisted creation time is retained only when exact ID, symbol, side, type,
and quantity match. Ordinary-order creation-time conflicts remain errors.

Valid order commands discarded before submission (including an unresolved prior
submission, disabled trading, and expiry) receive durable `REJECTED` feedback before
their Kafka command offset is committed. These records retain the command ID and
use a deterministic `local-rejected:<sha256>` ID, explicitly indicating a local
intent rather than a broker ticket, with zero fills and an explanatory message.
Their durable record prevents submission on redelivery. Failure to persist that
feedback stops intake without committing the command. Dispatched or ambiguous
submissions are never inferred rejected: their dispatch marker and exact broker
ID remain authoritative. A lost acknowledgment without an exact broker ID still
requires operator reconciliation; no financial request is automatically replayed.

No real broker or Kafka connection is made by the test suite; HTTP tests use only
local loopback and synthetic credentials.
