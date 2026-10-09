# Fangde Automation

A read-only Python client for Fangde securities holdings, asset summaries, and account lists. It uses an existing authenticated app session and runs without an Android phone or emulator. Python 3.10+ and system TLS certificates are required; there are no third-party Python dependencies.

The protocol was inspected in Fangde Android app `com.fdzq.app`, version 7.7.1. Direct holdings, summary, and account-list requests were verified on Windows. Linux server deployment has not yet been tested. This project uses an undocumented app API and is not an official Fangde SDK.

## Setup

For a fresh clone, copy the template and populate the session credentials and device identifiers:

```bash
cp config.example.json config.json
chmod 600 config.json
```

On Windows, use `Copy-Item config.example.json config.json` instead. If a populated `config.json` already exists, retain it. The configuration template references `config.schema.json` for editor validation. Actual tokens belong only in a private config file.

Validate the config locally without contacting Fangde:

```bash
python3 fangde_holdings.py --check-config
```

On Windows, replace `python3` with your Python launcher, such as `python` or `py -3`.

## Retrieve account data

```bash
python3 fangde_holdings.py holdings --output data/holdings.json --csv data/holdings.csv
python3 fangde_holdings.py summary --output data/summary.json
python3 fangde_holdings.py accounts --output data/accounts.json
```

The client creates the output directory as needed. Without `--output`, it prints JSON to stdout. Use `--config /secure/path/config.json` to select another config; the default is `config.json` beside the script, regardless of the working directory.

Holdings JSON includes normalized positions, the full portfolio data, and a UTC retrieval timestamp. Values retain the precision returned by Fangde. The `currency` field uses the broker's ISO currency string; its separate `ccy` field can be a numeric internal identifier. Prices are broker snapshot values. The retrieval timestamp is not a quote timestamp.

The holdings command returns the securities portfolio associated with the configured token. It does not aggregate other accounts, options, futures, or funds. The summary command returns the broker's broader asset summary. The account-list command redacts returned token fields.

## Credentials and session renewal

`config.json` contains an existing login session, not a permanent API key. Keep it private. Fangde controls session expiry, revocation, device restrictions, and verification requirements. The token may grant more access than this read-only client exposes.

If Fangde requires renewed authentication, sign in through the app and export a fresh session on a computer connected to the rooted phone:

```bash
python3 export_session.py --adb /path/to/adb --replace
```

For multiple attached devices, add `--serial YOUR_DEVICE_SERIAL`. The export utility reads only the app's session and device-ID MMKV files and makes no network requests. ADB and a rooted phone are needed for export only. Its app metadata targets the inspected 7.7.1 / Pixel 8 Pro setup; a different version or device may require adjustment.

The optional `session.trade_token` field is empty by default. A trade token was not needed for the verified holdings requests. The client does not request SMS codes, log in, refresh tokens, or bypass verification. Trading and transfers are not implemented.

## Linux deployment

Copy or clone the source onto the server. Transfer the populated `config.json` separately through a secure channel; it will not come from GitHub. Then:

```bash
cd /path/to/fangde-automation
chmod 600 config.json
python3 fangde_holdings.py --check-config
python3 fangde_holdings.py --output data/holdings.json --csv data/holdings.csv
```

There is no installation step or `pip install` requirement. The client writes owner-only output files on Unix. Access from a different server IP may trigger Fangde verification.

## GitHub hygiene

The `.gitignore` excludes real configs, environment secrets, account snapshots, `data/`, temporary app inspections, private archives, logs, and Python caches. `config.example.json` and `config.schema.json` are intended to be committed. Keep arbitrary result filenames inside `data/` so that they are excluded too.

Before publishing, review the staged paths and changes:

```bash
git status --short
git diff --cached --stat
git diff --cached
```

Never force-add credentials or account data. Ignore rules do not remove files already committed to Git history. Repository documentation and tests use no personal portfolio data or real device identifiers.

## Error handling

Exit code `0` means success; `2` means a configuration, connection, protocol, or API error. Failed requests preserve prior output files, so callers should check the exit code and `retrieved_at` before using an existing snapshot. Malformed responses are reported as errors rather than empty portfolios.

Requests use validated HTTPS, do not follow redirects, and ignore inherited proxy settings. Only the three read operations below are implemented. They use HTTP POST because that is how the app defines them; they do not submit trades.

| Operation | Endpoint |
| --- | --- |
| `holdings` | `https://apitrade.techfx88.com/trade/index/portfolio` |
| `summary` | `https://apitrade.techfx88.com/trade/asset/summary2` |
| `accounts` | `https://apitrade.techfx88.com/user/index/account_list` |

The request signature follows the inspected app: `_t` is Unix epoch seconds and `_sign` is lowercase MD5 of `token + _t + "U7KK2GV5GH68"`. The suffix is a public app protocol constant, not an account secret. A success response has `status == 0` and a `data` payload.

## Tests

```bash
python3 -m unittest -v
```

Tests run offline with synthetic credentials. They cover request signing, endpoint restrictions, redirects, API errors, credential redaction, response validation, numeric precision, CSV formulas, output protection, and session export parsing. They make no broker requests.
