#!/usr/bin/env python3
"""Read Fangde holdings with an existing authenticated session (Python 3.10+)."""

import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE_URL = "https://apitrade.techfx88.com"
# These are read operations, although the app uses HTTP POST for them.
READ_PATHS = {
    "holdings": "/trade/index/portfolio",
    "summary": "/trade/asset/summary2",
    "accounts": "/user/index/account_list",
}
SIGNING_SUFFIX = "U7KK2GV5GH68"  # Public app protocol constant, not an account secret.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
DEFAULT_CONFIG = Path(__file__).resolve().with_name("config.json")
PRIVATE_KEYS = {"token", "tradetoken", "password", "authorization", "cookie"}
COLUMNS = ("symbol", "name", "exchange", "currency", "quantity", "available_quantity",
           "average_cost", "last_price", "unrealized_pl")


class FangdeError(Exception):
    """An error safe to display without exposing request credentials."""


def private_write(path, text):
    """Atomic UTF-8 write, owner-only mode on Unix, no intermediate public file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == "posix":
            os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_config(path):
    try:
        config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        raise FangdeError("Cannot read config JSON. Use config.example.json or export_session.py.") from None
    if not isinstance(config, dict) or not isinstance(config.get("session"), dict):
        raise FangdeError("Config requires a session object.")
    token = config["session"].get("token")
    if not isinstance(token, str) or (not token and not isinstance(config.get('authentication'),dict)) or token.startswith("REPLACE_"):
        raise FangdeError("Set session.token in config.json to your current Fangde login token.")
    for key in ("token", "trade_token"):
        value = config["session"].get(key, "")
        if not isinstance(value, str) or any(ord(c) < 32 for c in value):
            raise FangdeError("Session credentials must be strings without control characters.")
    headers = config.get("headers", {})
    allowed = {"Version", "Agent", "Agent-Name", "brand", "Device-Id", "uniqueId",
               "Device-Name", "Device-Version", "Software", "package", "Accept-Language",
               "Channel", "apptheme"}
    if not isinstance(headers, dict) or set(headers) - allowed:
        raise FangdeError("Config contains unsupported HTTP headers.")
    for value in headers.values():
        if not isinstance(value, str) or any(ord(c) < 32 or ord(c) > 126 for c in value):
            raise FangdeError("Header values must be printable ASCII strings.")
    timeout = config.get("timeout_seconds", 30)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 1 <= timeout <= 120:
        raise FangdeError("timeout_seconds must be between 1 and 120.")
    if os.name == "posix" and Path(path).stat().st_mode & 0o077:
        raise FangdeError("Config is readable by other users. Run chmod 600 on your config file.")
    return config


def redact(value, secrets=()):
    if isinstance(value, dict):
        return {k: ("[REDACTED]" if re.sub(r"[^a-z]", "", k.lower()) in PRIVATE_KEYS
                    else redact(v, secrets)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, secrets) for v in value]
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[REDACTED]")
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, config, opener=None):
        self.config = config
        self.session = config["session"]
        # Do not forward credentials to redirects or an inherited HTTP proxy.
        self.opener = opener or urllib.request.build_opener(
            urllib.request.ProxyHandler({}), NoRedirect(),
            urllib.request.HTTPSHandler(context=ssl.create_default_context()))

    def request(self, operation):
        if operation not in READ_PATHS:
            raise FangdeError("Only holdings, summary, and account-list reads are implemented.")
        token = self.session["token"]
        stamp = str(int(time.time()))
        fields = {"token": token}
        if operation != "accounts":
            fields["force"] = "1"
            if self.session.get("trade_token"):
                fields["tradeToken"] = self.session["trade_token"]
        fields.update(_t=stamp, _sign=hashlib.md5(
            (token + stamp + SIGNING_SUFFIX).encode("utf-8")).hexdigest())
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json",
                   "User-Agent": "okhttp/4.12.0", **self.config.get("headers", {})}
        request = urllib.request.Request(BASE_URL + READ_PATHS[operation],
            data=urllib.parse.urlencode(fields).encode("ascii"), headers=headers, method="POST")
        try:
            with self.opener.open(request, timeout=self.config.get("timeout_seconds", 30)) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
            if code in (401, 403):
                raise FangdeError(f"HTTP {code}: session rejected. Renew login through Fangde; verification may be required.") from None
            if 300 <= code < 400:
                raise FangdeError("Fangde returned a redirect. Stopped without forwarding credentials.") from None
            raise FangdeError(f"Fangde returned HTTP {code}; no holdings were saved.") from None
        except (urllib.error.URLError, TimeoutError, socket.timeout, ssl.SSLError, OSError):
            raise FangdeError("Connection failed. Check network access, TLS certificates, and server clock; no holdings were saved.") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise FangdeError("Response exceeded 8 MiB; stopped without saving incomplete holdings.")
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError):
            raise FangdeError("Fangde returned invalid JSON; no holdings were saved.") from None
        if not isinstance(result, dict) or "status" not in result:
            raise FangdeError("Unexpected response schema: missing Fangde status.")
        secrets = (token, self.session.get("trade_token", ""))
        status = result["status"]
        if isinstance(status, bool) or str(status) != "0":
            message = str(redact(result.get("info") or result.get("message") or "Request rejected", secrets))
            message = re.sub(r"[\x00-\x1f\x7f]", " ", message)[:250]
            status_text = re.sub(r"[^0-9-]", "", str(status))[:12] or "unknown"
            raise FangdeError(f"Fangde status {status_text}: {message}. No holdings saved. If login or verification is requested, complete it through Fangde and update config.json.")
        if "data" not in result:
            raise FangdeError("Unexpected response schema: missing data.")
        return redact(result["data"], secrets)


def normalize_holdings(data):
    if not isinstance(data, dict) or not isinstance(data.get("products"), list):
        raise FangdeError("Unexpected holdings schema: expected data.products array; refusing to report empty holdings.")
    holdings = []
    for row in data["products"]:
        if not isinstance(row, dict) or not row.get("symbol") or row.get("qty") is None:
            raise FangdeError("A holding has no symbol or quantity; refusing to return partial holdings.")
        holdings.append({
            "symbol": row["symbol"], "name": row.get("name_en") or row.get("name", ""),
            "exchange": row.get("exchange", ""), "currency": row.get("currency") or row.get("ccy", ""),
            "quantity": row["qty"], "available_quantity": row.get("avail_sell_qty"),
            "average_cost": row.get("avg_cost"), "last_price": row.get("last_price"),
            "unrealized_pl": row.get("pl"),
        })
    return holdings


def csv_text(rows):
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS)
    writer.writeheader()
    for row in rows:
        safe = {}
        for key, value in row.items():
            text = "" if value is None else str(value)
            # Prevent spreadsheet formulas while retaining ordinary negative numbers.
            numeric = bool(re.fullmatch(r"-?\d+(?:\.\d+)?", text))
            if text.lstrip().startswith(("=", "+", "-", "@")) and not numeric:
                text = "'" + text
            safe[key] = text
        writer.writerow(safe)
    return buffer.getvalue()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", nargs="?", choices=READ_PATHS, default="holdings")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, help="Write a private JSON result; otherwise print JSON")
    parser.add_argument("--csv", type=Path, help="Also write a holdings CSV")
    parser.add_argument("--check-config", action="store_true", help="Validate config locally without contacting Fangde")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.check_config:
            print("Config valid. Session token present; credentials were not printed or sent.")
            return 0
        if args.csv and args.operation != "holdings":
            raise FangdeError("--csv is only supported for holdings.")
        paths = [args.config.resolve()] + [p.resolve() for p in (args.output, args.csv) if p]
        if len(set(paths)) != len(paths):
            raise FangdeError("Config, JSON output, and CSV output must be different files.")
        data = Client(config).request(args.operation)
        result = {"retrieved_at": datetime.now(timezone.utc).isoformat(),
                  "source": BASE_URL + READ_PATHS[args.operation], "operation": args.operation}
        if args.operation == "holdings":
            rows = normalize_holdings(data)
            result.update(holdings=rows, portfolio=data, holding_count=len(rows))
            if args.csv:
                private_write(args.csv, csv_text(rows))
        else:
            result["data"] = data
        output = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
        if args.output:
            private_write(args.output, output)
            print(f"Saved {args.operation} to {args.output}")
            if args.operation == "holdings":
                print(f"Holdings returned: {len(rows)}")
        else:
            print(output, end="")
        return 0
    except (FangdeError, OSError) as error:
        message = str(error) if isinstance(error, FangdeError) else "Cannot write result file. Check the output path and permissions."
        print("Error: " + message, file=sys.stderr)
        return 2


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    raise SystemExit(main())
