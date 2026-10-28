# Fangde API automation for Linux

Fangde adapter for the command formats and Linux workflow in
[GigaMoney-Automation-Linux](https://github.com/mhliu94/GigaMoney-Automation-Linux),
based on commit `031709523a00607a50f1caaabf05afca23081c61`.

Use the rooted Pixel 8 Pro on the home Windows computer to export each account's
authenticated session. Each Linux server receives its own account configuration,
session file, Kafka consumer group, and persistent working directory.

## Implemented

| Function | Fangde behavior |
|---|---|
| Holdings | USD cash and US stock positions; average cost, market value, daily P/L |
| Market order | Broker market order during the regular US session |
| Extended-hours market command | Converted to a DAY limit using the opposite side of a fresh Kafka book |
| Limit order | DAY limit; price rounding follows GigaMoney's cent rounding toward the order side |
| Emulated FOK | Submit a limit, wait, then cancel an unfilled remainder and confirm broker status |
| Cancellation | One exact order number or all currently active, cancelable US orders |
| Today's orders | Fangde's today endpoint; retains broker-listed carryover/strategy orders |
| History | Optional date-range query with complete pagination |
| Kafka | Commands, periodic/post-order account snapshots, cached price books |
| Order status | KTrader v1 snapshots, exact string IDs, durable acknowledged outbox |
| Command center | HTTPS login, CSRF protection, runner start/stop, logs and machine health |
| Operations | Session inspection/import, account binding, persistent order journal, systemd unit rendering |
| Authentication | Password login after logout/expiry, independent trading-password unlock, atomic token storage |

The emulated FOK workflow **can partially fill** and cannot roll back fills. It
is not an exchange-native all-or-nothing FOK instruction. A cancel acknowledgment
is followed by status checks; a fill racing with cancellation is reported as a fill.

## Validation and limits

The adapter's holdings, today-order, order-detail, security-settings and paginated
history reads have been checked against Fangde 7.7.1.
Order submission/cancellation payloads were implemented from the app's code and
tested with simulated responses. **No live financial write was performed.**

Linux service behavior and the actual Kafka cluster still need deployment
validation. The offline suite covers transport, account binding, status mapping,
partial fills, uncertain submissions, cancellation races, command handling,
price books, outbox recovery, and command-center HTTP authentication.

Password login and trading unlock were verified live on 2026-10-27, followed by
holdings reads. A password-enabled export lets Linux recover a rejected login
and unlock a missing/expired trading session. It uses password authentication;
no Fangde refresh-token mechanism is assumed. SMS verification is outside scope.

Reads can resume after successful recovery. Order submission/cancellation first
checks authentication with a holdings read; a dispatched order or cancellation
is never automatically replayed. A failed/interrupted password attempt is latched
to prevent repeated attempts. The runner then reports `authentication_required`.
After correcting credentials, explicitly retry or import a fresh export.

The private export contains password-equivalent values as well as tokens and
device headers. Base64 is not encryption. Password setup upgrades the account
binding to version 3, distinguishing brokerage accounts that share one user.
Switching accounts on the phone can revoke sessions; check the assigned server.
Long-term token lifetime and remote-server access still need deployment validation.

Current trading support is US stocks, whole-share buys and long sells, DAY
market/limit orders. Portfolio publishing stops on unsupported assets rather
than publishing an incomplete account. Existing strategy orders are queryable
and cancelable when their status is understood; creating strategy orders is not
part of this adapter.

All new orders require a fresh Kafka price-book reference, including explicit
limits and regular-session markets, because Fangde's app supplies a reference
price. For a market order this reference is **not a price cap**. The calendar uses
`exchange_calendars` XNYS sessions, including holidays, DST and early closes;
it cannot detect unexpected closures or instrument halts. Overnight market
commands are rejected. Explicit limits retain the GigaMoney extended-session
behavior. See [protocol notes](docs/protocol.md).

## Use

See [deployment and account exports](docs/deployment.md),
[commands and Kafka contracts](docs/commands.md), and
[validation record](VALIDATION.md).

The original `fangde_holdings.py` and `export_session.py` remain available with
their original configuration format. See [legacy instructions](docs/legacy-holdings.md).
Use `python -m fangde.session_export` for the full package's account-bound exports.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[kafka,dev]'
.venv/bin/python -m pytest -q
.venv/bin/fangde --help
```

The direct CLI defaults to a preview for orders and cancellations. Use `--execute`
to send them yourself. The Kafka runner executes commands after trading is enabled;
new configurations default to trading disabled. Cancellation commands remain
available while new-order intake is disabled.

Keep `work/api/orders/`, `work/api/order-status/`, and `work/trading-state.json`
when updating or moving an account. They contain duplicate protection, publication
sequences and the account's enabled state. Do not share one state directory between accounts.

`config/server.example.json` is the operational template. The larger
`config/fangde.config.example.json` lists all inherited settings, including dormant
command-center emulator settings. The backend is API-only; Linux needs the private
account export but no phone, emulator, Android SDK, interception proxy, or GigaMoney
signing profile. Configure passwords locally on Windows as described in deployment.
