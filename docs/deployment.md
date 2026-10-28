# One Windows exporter, one account per Linux server

## Export on Windows

Use Python 3.10+ and ADB on the home computer. The exporter itself uses only the
Python standard library. From the extracted project directory:

```powershell
python -m fangde.session_export --adb C:\path\to\adb.exe --serial DEVICE_SERIAL --account-id FANGDE-A --output C:\private\FANGDE-A.session.json
```

Before exporting, sign in to that account and unlock trading through Fangde's
normal verification flow on the phone. Use a different logical ID and output
file for each account. The exporter verifies version 7.7.1, checks that both reads
of each MMKV file agree, and requires exactly one trading token for the active
user. It prints only the label and SHA-256 identity binding, never tokens.

To renew the same account use the same arguments plus `--replace`. Replacement
refuses a different broker identity or label. A changed app version or multiple
trading sessions requires inspection; no account is guessed. The current exporter
uses the verified Pixel 8 Pro device-header profile.

For automatic login and trading unlock, add passwords through local masked
prompts after the phone export:

```powershell
python -m pip install -e ".[bootstrap]"
python -m fangde.password_setup --session C:\private\FANGDE-A.session.json
```

Enter the login phone/email, the two-letter phone region (for example `CN`, `HK`
or `US`), login password, and trading password. The tool reads the active account,
logs in, selects that exact brokerage account, unlocks trading, and verifies
holdings. It replaces the export only after success. This setup needs no recorder
or Frida attachment. Password entry is once per account or password change.

The resulting version-3 identity digest differs from the token-only version-2
digest. Use the digest printed after password setup in the server configuration.
For password changes, rerun password setup on that account's private export.
Do not replace a version-3 profile with the token-only phone exporter; use a new
output file if bootstrapping from the phone again.

The session includes password-equivalent values and is a private JSON file, not
an encrypted archive. Base64 encoding does not protect the passwords. Transfer it directly
to its assigned server using your SSH/SCP connection. Do not put it in Git, chat,
the shared source ZIP, or another account's working directory.

## Configure each Linux server

Install Python 3.10+ with venv support, extract the code, and enter its directory:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[kafka]'
cp config/server.example.json config/fangde.config.json
chmod 600 config/fangde.config.json /private/FANGDE-A.session.json
.venv/bin/fangde session-inspect /private/FANGDE-A.session.json
```

Edit these configuration fields:

| Setting | Value |
|---|---|
| `fangde.accountId` | Exact exported logical account ID and Kafka `account_id` |
| `fangde.accountNumId` | Optional existing KTrader numeric account ID |
| `api.subjectSha256` | Identity digest printed by `session-inspect` |
| `paths.workDir`, `paths.logDir` | Absolute private state/log directories on this server |
| `kafka.bootstrapServers` | Your Kafka cluster |
| `kafka.consumerGroupId` | **Unique per account**; e.g. `fangde-account-a` |
| `kafka.*Topic` | Existing command, account-details, order-status and price-book topics |
| `kafka.clientConfig` | Your SASL/TLS options, if required |
| `monitor.serverId` | This Linux server's display name |
| `monitor.adminUsername`, `monitor.adminPassword` | Separate dashboard credentials |
| `monitor.certFile`, `monitor.keyFile` | TLS certificate/key paths |
| `monitor.host`, `monitor.port`, `monitor.publicHost` | Intended listen address and hostname |

Different accounts must not share a Kafka consumer group: a group distributes
messages among servers, while each server deliberately ignores other accounts.
Use exactly one active runner per brokerage account. Relative paths resolve from
the installed project root; absolute paths make separate deployments clearer.

Import and validate without sending an order:

```bash
.venv/bin/fangde session-import /private/FANGDE-A.session.json
.venv/bin/fangde validate-config
.venv/bin/fangde holdings
.venv/bin/fangde today-orders
.venv/bin/fangde price-books-refresh
.venv/bin/fangde limit-order --symbol AAPL --side Buy --quantity 1 --price 100
```

The last command is a **preview** and its price is only an illustrative input.
Supply your own intended order if validating trading. Session import checks both
the logical ID and broker identity, then atomically writes `api.sessionPath`
(default `work/api/session.json`). It does not start services. Files written on
Linux use mode 600; working state directories use 700 where created by the API code.

## Run and supervise

After configuring and validating the server, the operator can run:

```bash
.venv/bin/fangde runner
.venv/bin/fangde runner-status
.venv/bin/fangde runner-stop
```

For a user systemd service, render the unit on that Linux server so its paths and
interpreter are correct:

```bash
.venv/bin/fangde render-service
mkdir -p ~/.config/systemd/user
cp work/systemd/fangde-runner.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now fangde-runner.service
```

The generated unit path is printed; use that path if `paths.workDir` was changed.
If systemd manages the runner, use systemd for lifecycle control; a force-stop
from the dashboard can be restarted by systemd's `Restart=on-failure` setting.
Alternatively let the dashboard start/stop the runner directly, without enabling
the systemd runner service.

The HTTPS command center is launched separately:

```bash
.venv/bin/fangde monitor-start
.venv/bin/fangde monitor-status
.venv/bin/fangde monitor-stop
```

Use the configured TLS URL. The dashboard requires its own login and protects
control requests with CSRF/session checks. It shows the runner and machine health;
no server emulator is needed. Its listener defaults to `127.0.0.1`.

## Expired sessions and uncertain requests

With a password-enabled export, main-session expiry (`1001`) and trading locks
(`1101`, `1102`) trigger bounded automatic recovery during reads. A holdings
preflight restores authentication before an order/cancellation is dispatched.
The session file is locked across processes and tokens are saved atomically.

Incorrect credentials, interrupted authentication, unsupported responses, or
additional verification stop automatic attempts. SMS is not implemented.
After investigating the failure and correcting credentials, explicitly retry:

```bash
fangde session-authenticate --retry
```

This command performs password login, trading unlock, and a holdings read. It
places no order. Restart the runner after resolving `authentication_required`.
Token-only exports retain the phone re-export/import procedure. A command
explicitly rejected for authentication after dispatch is marked rejected; use a
new command ID for a new intent after inspecting the broker. No financial request
is replayed automatically after authentication fails.

If the connection fails after sending an order, the broker may have accepted it.
The journal blocks new submissions until the exact result is established. There
is no verified broker lookup by the local command ID. Do not delete the journal
or send a replacement order just because the response was lost.

After inspecting the app or `today-orders`, an operator can associate the exact
broker order with its unresolved local command:

```bash
.venv/bin/fangde reconcile-order --command-id ORIGINAL_COMMAND_ID --order-id EXACT_BROKER_ORDER_NO
```

This verifies symbol, side, type, quantity and limit price using a read-only detail
query. It does not prove identity from those attributes alone: the operator must
identify the correct ticket. It saves the exact order ID locally without submitting
or canceling. On the next runner start, recovery confirms the order; an unfinished
emulated FOK may then send its pending cancellation. If no matching order can be
established, preserve the journal and inspect the broker/account state before
resolving it manually. A cancellation with an uncertain response is not resent.
