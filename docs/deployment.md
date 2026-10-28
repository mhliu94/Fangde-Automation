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
digest. The Linux installer derives this binding from the exported file.
For password changes, rerun password setup on that account's private export.
Do not replace a version-3 profile with the token-only phone exporter; use a new
output file if bootstrapping from the phone again.

The session includes password-equivalent values and is a private JSON file, not
an encrypted archive. Base64 encoding does not protect the passwords. Transfer it directly
to its assigned server using your SSH/SCP connection. Do not put it in Git, chat,
the shared source ZIP, or another account's working directory.

Combine the export and Kafka address into one deployment file on Windows:

```powershell
python -m fangde.deployment bundle --session C:\private\FANGDE-A.session.json --output C:\private\FANGDE-A.deployment.json --kafka-bootstrap broker.example.com:9092
```

Pass `--settings C:\private\server-settings.json` to include optional overrides,
such as Kafka SASL/TLS settings, topic names, numeric account ID or monitor options.
The settings file uses the same structure as `config/server.example.json`; include
only the fields you need. The bundle command rejects a mismatched account binding
and refuses to overwrite an existing output. Use a new filename for an updated bundle.
The deployment file contains credentials; transfer it privately to its assigned server.

## Configure each Linux server

Install Git and Python 3.10+ with venv support. On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install git python3 python3-venv
git clone https://github.com/mhliu94/Fangde-Automation.git
cd Fangde-Automation
chmod 600 /private/FANGDE-A.deployment.json
./install.sh /private/FANGDE-A.deployment.json
./start.sh --check
```

Setup creates `.venv`, installs the Kafka dependencies, derives the logical account
ID and broker binding, and writes a private runtime configuration. No login, broker
request, Kafka connection or service startup occurs during setup or `--check`.
`FANGDE_PYTHON=/path/to/python3.12 ./install.sh ...` selects another Python version.

An existing plain session/password export is also accepted:

```bash
./install.sh /private/FANGDE-A.session.json --kafka-bootstrap broker.example.com:9092
```

The default state directory is `${XDG_STATE_HOME:-$HOME/.local/state}/fangde/account-FANGDE-A`.
Use `--state-dir /absolute/private/directory` to choose another location outside
the checkout. It contains `server.config.json`, `session.json`, `work/` and `logs/`.
The checkout's ignored `config/deployment.json` points to the runtime configuration;
commands work from any current directory. An explicit `--config` or `FANGDE_CONFIG`
overrides that selection.

Do not reuse a checkout or state directory for a different account. Setup checks
both the logical label and broker identity. Existing manual configurations remain
supported by the direct CLI; use a fresh checkout for managed installation.

For optional customization, edit the private `server.config.json` printed by setup:

| Setting | Value |
|---|---|
| `fangde.accountId` | Derived exported logical ID and Kafka `account_id`; keep unchanged |
| `fangde.accountNumId` | Optional existing KTrader numeric account ID |
| `api.subjectSha256` | Derived broker identity binding; keep unchanged |
| `paths.workDir`, `paths.logDir` | Derived private state/log directories; preserve on update |
| `kafka.bootstrapServers` | Your Kafka cluster |
| `kafka.consumerGroupId` | Derived as `fangde-FANGDE-A`; must be unique per account |
| `kafka.*Topic` | Existing command, account-details, order-status and price-book topics |
| `kafka.clientConfig` | Your SASL/TLS options, if required |
| `monitor.serverId` | This Linux server's display name |
| `monitor.adminUsername`, `monitor.adminPassword` | Separate dashboard credentials |
| `monitor.certFile`, `monitor.keyFile` | TLS certificate/key paths |
| `monitor.host`, `monitor.port`, `monitor.publicHost` | Intended listen address and hostname |

The minimal bundle uses the default Kafka client settings with no SASL/TLS overrides.
If the cluster requires authentication, supply `kafka.clientConfig` in the private
settings file. Certificate/key paths in the bundle are resolved relative to its
location on Linux; the referenced files must also be transferred to that server.

Different accounts must not share a Kafka consumer group: a group distributes
messages among servers, while each server deliberately ignores other accounts.
Use exactly one active runner per brokerage account. Relative paths resolve from
the installed project root; absolute paths make separate deployments clearer.

Validate reads after installation, when ready to connect:

```bash
.venv/bin/fangde validate-config
.venv/bin/fangde holdings
.venv/bin/fangde today-orders
.venv/bin/fangde price-books-refresh
.venv/bin/fangde limit-order --symbol AAPL --side Buy --quantity 1 --price 100
```

The last command is a **preview** and its price is only an illustrative input.
Supply your own intended order if validating trading. Private files use mode 600
and the managed state directory uses 700. New configurations start with trading
disabled; existing enabled state is preserved during updates.

## Run and supervise

The trading script is the Kafka runner. Start it in the foreground with:

```bash
./start.sh
```

From a separate terminal, its existing status/stop commands are available:

```bash
.venv/bin/fangde runner-status
.venv/bin/fangde runner-stop
```

To enable live trading and run in the background with `nohup`, use:

```bash
./start_live.sh --check
./start_live.sh
```

`--check` only validates local configuration; it does not enable trading or start
anything. The normal invocation enables new orders, persists that state across
restarts, and appends output to the account's `logs/runner.log` (or configured
`monitor.runnerLog`). It prints the PID and log path. Use `runner-status` to check
startup and `runner-stop` to stop this background process. `FANGDE_CONFIG` can
select a different configuration. The monitor is not started.

Stop an existing runner first. For a systemd-managed runner, use
`systemctl --user stop fangde-ACCOUNT_ID-runner.service`, replacing `ACCOUNT_ID`
with the configured account ID. The live launcher refuses to start a second
runner; the process also holds its exclusive lock before changing trading state.
`nohup` survives terminal logout but does not restart a crashed process or start
it after a reboot. Use the systemd setup below for supervision.

The HTTPS monitor is a separate foreground process. Set `monitor.adminPassword`,
`monitor.certFile` and `monitor.keyFile` in the runtime configuration, then run:

```bash
./start-monitor.sh --check
./start-monitor.sh
```

Starting either process does not start the other. The dashboard requires its own
login and protects control requests with CSRF/session checks. Its listener defaults
to `127.0.0.1`; configure the intended address and TLS hostname for remote access.
Existing `fangde monitor-start`, `monitor-status` and `monitor-stop` commands remain
available for manual background operation.

For systemd supervision, install the two independent user units:

```bash
./install.sh /private/FANGDE-A.deployment.json --systemd
systemctl --user enable --now fangde-FANGDE-A-runner.service
```

`--systemd` writes both units and reloads the user manager; it does not enable or
start either service. Unit names use the exported account ID. To start the monitor
separately, after configuring its password and TLS files:

```bash
systemctl --user enable --now fangde-FANGDE-A-monitor.service
```

For startup without an interactive login, the administrator can enable the user's
systemd lingering with `sudo loginctl enable-linger USERNAME`. Both units append to
separate files in the account's `logs/` directory. Generated unit paths are printed
by setup, and each service runs a local configuration check before starting.
If systemd manages the runner, use systemd for lifecycle control; a force-stop
from the dashboard can be restarted by systemd's `Restart=on-failure` setting.
Alternatively let the dashboard start/stop the runner directly, without enabling
the systemd runner service.

## Update an installation

Stop both processes before changing code or dependencies. If using systemd:

```bash
systemctl --user stop fangde-FANGDE-A-runner.service fangde-FANGDE-A-monitor.service
git pull --ff-only
./install.sh /private/FANGDE-A.deployment.json --systemd
systemctl --user start fangde-FANGDE-A-runner.service
```

Start the monitor separately if needed. For foreground operation, stop the commands
with Ctrl+C, omit `--systemd`, and restart the chosen command. Setup refuses to update
an active runner. Reinstallation preserves the installed configuration, renewed
tokens, order journal, publication records and trading-enabled state. It does not
reset these from an older uploaded file.

To intentionally replace credentials after re-exporting the same account, pass
`--refresh-session`. To apply settings from an updated bundle or `--settings` file,
pass `--update-settings`. To move to a fresh checkout on the same server, supply the
existing `--state-dir` so that account state is retained. Stop the old installation first.

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
