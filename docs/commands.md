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

Password-enabled sessions recover automatically during reads and before financial
dispatch. `session-authenticate` forces login/unlock explicitly; add `--retry` only
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

Holdings are published at startup, after completed order/cancel handling, and
every 30 seconds measured from the preceding update's completion. The account
payload preserves GigaMoney's `account_id`, `account_num_id`, `cash`,
`cash_by_currency`, `positions`, `ts`, and `trading_enabled` fields.

Order snapshots use the JSON array key `[account_id,order_id]` and schema version
1. Only orders created today in `api.orderTimezone` (New York by default) are
published. CLI today queries retain everything Fangde puts in its today list,
including older open strategies. Histories are queried only on explicit request.
External/app-created orders can be published, without a fabricated command ID.
Unknown statuses are labeled `UNKNOWN`; missing/invalid data is reported rather
than invented. Partial cancellations preserve cumulative filled quantity.

Order status records are persisted before publication and marked delivered only
after Kafka acknowledgment. Retries preserve payload and sequence. Poll-time-only
changes do not produce new states; a disappeared order is not assumed canceled.
An unavailable order query does not prevent independently verified holdings from
being published. The exact KTrader schema is in
`tests/fixtures/order-status.v1.schema.json`.

No real broker or Kafka connection is made by the test suite; HTTP tests use only
local loopback and synthetic credentials.
