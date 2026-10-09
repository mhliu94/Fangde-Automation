#!/usr/bin/env python3
"""One-time, local ADB export of Fangde's session to config.json. No network calls."""

import argparse
import json
from pathlib import Path
import struct
import subprocess
import sys

from fangde_holdings import FangdeError, private_write

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


def export(adb, serial, output):
    command = [adb] + (["-s", serial] if serial else [])

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
        "session": {"token": token, "trade_token": ""},
        "headers": {
            "Version": "7.7.1", "Agent": "Android", "Agent-Name": "Pixel 8 Pro",
            "brand": "google", "Device-Id": device_id, "uniqueId": device_id,
            "Device-Name": "Android", "Software": "fd_app", "package": "com.fdzq.app",
            "Accept-Language": "zh-CN",
        },
        "timeout_seconds": 30,
    }
    private_write(output, json.dumps(config, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adb", default="adb")
    parser.add_argument("--serial", help="Required when multiple Android devices are attached")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--replace", action="store_true", help="Replace an existing config with a fresh session")
    args = parser.parse_args()
    if args.output.exists() and not args.replace:
        parser.error("Config already exists. Use --replace to export a new session.")
    try:
        export(args.adb, args.serial, args.output)
    except (FangdeError, OSError, subprocess.SubprocessError) as error:
        message = str(error) if isinstance(error, FangdeError) else "ADB export failed. Check USB debugging, device selection, and root authorization."
        print("Error: " + message, file=sys.stderr)
        return 2
    print(f"Saved session to {args.output}. Credential values were not printed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
