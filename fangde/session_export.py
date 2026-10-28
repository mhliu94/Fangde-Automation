#!/usr/bin/env python3
"""One-time, local ADB export of Fangde's session to config.json. No network calls."""

import argparse
import json
from pathlib import Path
import struct
import subprocess
import sys
import hashlib
import re
from datetime import datetime, timezone

from .legacy_holdings import FangdeError, private_write

APP_ROOT = "/data/user/0/com.fdzq.app/"


def varint(data, offset, end):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= end:
            raise FangdeError("Truncated MMKV data; open Fangde and retry export.")
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise FangdeError("Invalid MMKV integer.")


def read_mmkv_strings(data):
    """Decode unencrypted MMKV records, retaining the last value for each key."""
    if len(data) < 5:
        raise FangdeError("Missing or unreadable MMKV session file.")
    end = 4 + struct.unpack_from("<I", data)[0]
    if end > len(data) or end < 5:
        raise FangdeError("Unsupported MMKV file layout; export requires reinspection.")
    _, offset = varint(data, 4, end)
    records = {}
    while offset < end:
        key_size, offset = varint(data, offset, end)
        if key_size == 0 or offset + key_size > end:
            raise FangdeError("Invalid MMKV key boundary.")
        try:
            key = data[offset:offset + key_size].decode("utf-8")
        except UnicodeError:
            raise FangdeError("Session appears encrypted or uses an unsupported format.") from None
        offset += key_size
        size, offset = varint(data, offset, end)
        if offset + size > end:
            raise FangdeError("Invalid MMKV value boundary.")
        raw = data[offset:offset + size]
        offset += size
        records.pop(key, None)
        if not raw:
            continue
        length, begin = varint(raw, 0, len(raw))
        if begin + length == len(raw):
            try:
                records[key] = raw[begin:].decode("utf-8")
            except UnicodeError:
                pass
    return records


def select_trade_token(session):
    """Select only an unambiguous saved trading session for the logged-in user."""
    suid = session.get("SUID", "")
    if not suid:
        raise FangdeError("No active user identifier. Sign in through Fangde first.")
    candidates = [value for key, value in session.items()
                  if key.startswith(suid + "_") and key.endswith("_TRADE_TOKEN") and value]
    if not candidates:
        raise FangdeError("No saved trading session. Unlock trading in Fangde and retry.")
    if len(candidates) != 1:
        raise FangdeError("Multiple saved trading sessions; account selection needs inspection. Nothing exported.")
    return candidates[0]


def account_binding(session):
    select_trade_token(session)
    suid = session['SUID']
    key = next(key for key, value in session.items()
               if key.startswith(suid + '_') and key.endswith('_TRADE_TOKEN') and value)
    return hashlib.sha256(('fangde\0' + suid + '\0' + key).encode()).hexdigest()


def export(adb, serial, output, account_id, replace=False):
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', account_id):
        raise FangdeError('Use a logical account ID containing letters, numbers, dots, underscores or hyphens.')
    output = Path(output)
    if output.exists() and not replace:
        raise FangdeError('Export exists; use --replace to renew the same account.')
    command = [adb] + (["-s", serial] if serial else [])
    version = subprocess.run(command + ['shell', 'dumpsys', 'package', 'com.fdzq.app'],
                             check=True, capture_output=True, timeout=20).stdout.decode('utf-8', errors='replace')
    if not re.search(r'\bversionName=7\.7\.1(?:\s|$)', version):
        raise FangdeError('This exporter was verified for Fangde 7.7.1. Inspect the installed version before exporting.')

    def read_file(relative):
        # Fixed app-specific paths: never execute input from the device.
        args = command + ["exec-out", "su", "-c", "cat " + APP_ROOT + relative]
        first = subprocess.run(args, check=True, capture_output=True, timeout=20).stdout
        second = subprocess.run(args, check=True, capture_output=True, timeout=20).stdout
        if first != second:
            raise FangdeError("Fangde changed its session while exporting. Retry once it is idle.")
        return read_mmkv_strings(second)

    session = read_file("filesmmkv/session_app_shared")
    identity = read_file("files/mmkv/UUID")
    token = session.get("TOKEN", "")
    if not token:
        raise FangdeError("No saved Fangde login token. Sign in through Fangde first.")
    device_id = identity.get("UUID", "")
    if not device_id:
        raise FangdeError("No saved Fangde device ID. Open Fangde and retry.")
    config = {
        "version": 2,
        "identity": {"account_id": account_id, "subject_sha256": account_binding(session)},
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "session": {"token": token, "trade_token": select_trade_token(session)},
        "headers": {
            "Version": "7.7.1", "Agent": "Android", "Agent-Name": "Pixel 8 Pro",
            "brand": "google", "Device-Id": device_id, "uniqueId": device_id,
            "Device-Name": "Android", "Software": "fd_app", "package": "com.fdzq.app",
            "Accept-Language": "zh-CN",
        },
        "timeout_seconds": 30,
    }
    if output.exists():
        try:
            previous = json.loads(output.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            raise FangdeError('Existing export cannot be verified; choose a new output path.') from None
        if previous.get('identity') != config['identity']:
            raise FangdeError('Refusing to overwrite another account. Use a separate output file.')
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_write(output, json.dumps(config, indent=2) + "\n")
    return config['identity']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adb", default="adb")
    parser.add_argument("--serial", help="Required when multiple Android devices are attached")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--account-id", required=True, help="Stable logical ID used by this account's Linux server and Kafka commands")
    parser.add_argument("--replace", action="store_true", help="Replace an existing config with a fresh session")
    args = parser.parse_args(argv)
    if args.output.exists() and not args.replace:
        parser.error("Config already exists. Use --replace to export a new session.")
    try:
        identity = export(args.adb, args.serial, args.output, args.account_id, args.replace)
    except (FangdeError, OSError, subprocess.SubprocessError) as error:
        message = str(error) if isinstance(error, FangdeError) else "ADB export failed. Check USB debugging, device selection, and root authorization."
        print("Error: " + message, file=sys.stderr)
        return 2
    print(f"Saved session to {args.output}. Credential values were not printed.")
    print(json.dumps(identity))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
