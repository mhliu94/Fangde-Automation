import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse

import fangde_holdings as fd
from export_session import read_mmkv_strings


def config():
    return {"session": {"token": "test-token", "trade_token": ""},
            "headers": {"Version": "7.7.1", "Agent": "Android"}, "timeout_seconds": 30}


class Response(io.BytesIO):
    def __init__(self, value):
        super().__init__(json.dumps(value).encode())


class Opener:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def open(self, request, timeout):
        self.calls.append(request)
        if isinstance(self.value, Exception):
            raise self.value
        return Response(self.value)


class HoldingsTests(unittest.TestCase):
    def test_signature_and_read_only_destination(self):
        opener = Opener({"status": 0, "data": {"products": []}})
        with patch.object(fd.time, "time", return_value=1700000000):
            fd.Client(config(), opener).request("holdings")
        request = opener.calls[0]
        fields = urllib.parse.parse_qs(request.data.decode())
        self.assertEqual(request.full_url, "https://apitrade.techfx88.com/trade/index/portfolio")
        self.assertEqual(fields["_sign"], [hashlib.md5(b"test-token1700000000U7KK2GV5GH68").hexdigest()])
        self.assertEqual(fields["force"], ["1"])
        self.assertNotIn("tradeToken", fields)
        self.assertNotIn("test-token", request.full_url)

    def test_no_arbitrary_endpoint_or_trade_call(self):
        opener = Opener({})
        with self.assertRaises(fd.FangdeError):
            fd.Client(config(), opener).request("/trade/index/orderActionAdd")
        self.assertEqual(opener.calls, [])

    def test_redirects_never_forward_credentials(self):
        self.assertIsNone(fd.NoRedirect().redirect_request(None, None, 307, "", {}, "https://elsewhere.test"))
        error = urllib.error.HTTPError(fd.BASE_URL, 307, "redirect", {}, None)
        with self.assertRaisesRegex(fd.FangdeError, "without forwarding"):
            fd.Client(config(), Opener(error)).request("holdings")

    def test_api_failure_is_not_empty_holdings(self):
        with self.assertRaisesRegex(fd.FangdeError, "status 42") as caught:
            fd.Client(config(), Opener({"status": 42, "info": "expired test-token"})).request("holdings")
        self.assertNotIn("test-token", str(caught.exception))

    def test_unknown_status_rejected(self):
        for status in (None, False, {}, "", 0.0):
            with self.subTest(status=status), self.assertRaises(fd.FangdeError):
                fd.Client(config(), Opener({"status": status, "data": {"products": []}})).request("holdings")

    def test_missing_products_is_not_zero_positions(self):
        for data in ({}, {"products": None}, [], {"products": [{}]}):
            with self.subTest(data=data), self.assertRaises(fd.FangdeError):
                fd.normalize_holdings(data)
        self.assertEqual(fd.normalize_holdings({"products": []}), [])

    def test_values_remain_exact(self):
        rows = fd.normalize_holdings({"products": [{"symbol": "EXAMPLE", "qty": "0.123456789",
            "avg_cost": "123456.789012345", "ccy": "2", "currency": "USD", "last_price": "0.0001", "pl": "-1.25"}]})
        self.assertEqual(rows[0]["quantity"], "0.123456789")
        self.assertEqual(rows[0]["average_cost"], "123456.789012345")
        self.assertEqual(rows[0]["unrealized_pl"], "-1.25")
        self.assertEqual(rows[0]["currency"], "USD")

    def test_account_tokens_redacted(self):
        data = {"accounts": [{"token": "another-account-token", "trade_token": "secret"}]}
        result = fd.Client(config(), Opener({"status": 0, "data": data})).request("accounts")
        self.assertNotIn("another-account-token", json.dumps(result))
        self.assertNotIn("secret", json.dumps(result))

    def test_config_headers_cannot_override_host(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.json"
            data = config()
            data["headers"]["Host"] = "elsewhere.test"
            fd.private_write(path, json.dumps(data))
            with self.assertRaises(fd.FangdeError):
                fd.load_config(path)

    def test_csv_formula_escaped_but_negative_pl_preserved(self):
        row = dict.fromkeys(fd.COLUMNS, "")
        row.update(name="=DDE()", unrealized_pl="-123.45")
        text = fd.csv_text([row])
        self.assertIn("'=DDE()", text)
        self.assertIn("-123.45", text)
        self.assertNotIn("'-123.45", text)

    def test_failure_preserves_last_successful_output(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fd.private_write(root / "config.json", json.dumps(config()))
            fd.private_write(root / "holdings.json", "previous result")
            with patch.object(fd.Client, "request", side_effect=fd.FangdeError("expired")), contextlib.redirect_stderr(io.StringIO()):
                code = fd.main(["--config", str(root / "config.json"), "--output", str(root / "holdings.json")])
            self.assertEqual(code, 2)
            self.assertEqual((root / "holdings.json").read_text(), "previous result")

    def test_output_cannot_overwrite_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.json"
            fd.private_write(path, json.dumps(config()))
            with patch.object(fd.Client, "request") as request, contextlib.redirect_stderr(io.StringIO()):
                code = fd.main(["--config", str(path), "--output", str(path)])
            self.assertEqual(code, 2)
            request.assert_not_called()
            self.assertEqual(json.loads(path.read_text()), config())

    def test_private_atomic_write(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.json"
            fd.private_write(path, "one")
            fd.private_write(path, "two")
            self.assertEqual(path.read_text(), "two")
            self.assertEqual(len(list(Path(folder).iterdir())), 1)
            if os.name == "posix":
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)


def vi(value):
    data = bytearray()
    while value > 127:
        data.append((value & 127) | 128)
        value >>= 7
    data.append(value)
    return bytes(data)


def record(key, value):
    key = key.encode()
    raw = b"" if value is None else vi(len(value.encode())) + value.encode()
    return vi(len(key)) + key + vi(len(raw)) + raw


class ExportTests(unittest.TestCase):
    def test_mmkv_latest_value_and_deleted_keys(self):
        payload = vi(123456) + record("TOKEN", "old") + record("TOKEN", "new") + record("gone", "x") + record("gone", None)
        data = struct.pack("<I", len(payload)) + payload + b"\x00" * 32
        self.assertEqual(read_mmkv_strings(data), {"TOKEN": "new"})

    def test_truncated_mmkv_rejected(self):
        with self.assertRaises(fd.FangdeError):
            read_mmkv_strings(struct.pack("<I", 999) + b"123")


if __name__ == "__main__":
    unittest.main()
