import copy
import ast
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fangde.config import DEFAULTS, Settings, find_adb, load_settings, validate_config
from fangde.errors import ConfigurationError
from fangde.setup import init_config, render_service
from fangde.monitor import in_scheduled_restart_window
from datetime import datetime, timezone


class ConfigTests(unittest.TestCase):

    def test_price_cache_accepts_legacy_age_and_requires_positive_poll_interval(self):
        validate_config({"kafka": {"priceBookMaxAgeSeconds": None, "priceBookPollIntervalSeconds": 30}})
        with self.assertRaises(ConfigurationError):
            validate_config({"kafka": {"priceBookPollIntervalSeconds": 0}})

    def test_all_literal_operator_settings_are_in_the_catalog(self):
        root = Path(__file__).resolve().parents[1]
        for file in [*(root / "fangde").glob("*.py"), *(root / "scripts").glob("*.py")]:
            for node in ast.walk(ast.parse(file.read_text())):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or node.func.attr not in {"get", "require"} or not node.args:
                    continue
                if ast.unparse(node.func.value) not in {"settings", "self.settings", "self.controller.settings", "settings()", "settings_for_capture()"}:
                    continue
                key = node.args[0]
                if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                    continue
                value = DEFAULTS
                for part in key.value.split("."):
                    with self.subTest(file=file.name, setting=key.value):
                        self.assertIn(part, value)
                        value = value[part]

    def test_example_lists_every_config_field_without_machine_paths(self):
        path = Path(__file__).resolve().parents[1] / "config/fangde.config.example.json"
        example = json.loads(path.read_text())
        self.assertEqual(example, DEFAULTS)
        self.assertNotIn("/home/ubuntu", path.read_text())
        validate_config(example)

    def test_invalid_or_misspelled_fields_fail_without_exposing_values(self):
        for value in ({"api": {"requestTimeoutSecond": "secret"}},
                      {"kafka": {"holdingsIntervalSeconds": 0}},
                      {"api": {"readAttempts": 0}},
                      {"monitor": {"restart": {"timezone": "invalid"}}},
                      {"kafka": {"clientConfig": {"enable.auto.commit": True}}}):
            with self.subTest(value=value), self.assertRaises(ConfigurationError) as error:
                validate_config(value)
            self.assertNotIn("secret", str(error.exception))

    def test_config_selection_and_paths_are_independent_of_current_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            init_config(config)
            data = json.loads(config.read_text())
            data["paths"] = {"workDir": str(root / "account-a-state"), "logDir": str(root / "account-a-logs")}
            config.write_text(json.dumps(data))
            with patch.dict(os.environ, {"FANGDE_CONFIG": str(config)}):
                settings = load_settings()
            self.assertEqual(settings.session_path, root / "account-a-state/api/session.json")
            self.assertEqual(settings.log_dir, root / "account-a-logs")
            if os.name == "posix": self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                init_config(config)

    def test_loaded_accounts_do_not_share_mutable_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "account.json"
            path.write_text('{}')
            first, second = load_settings(path), load_settings(path)
            first.data["kafka"]["clientConfig"]["sasl.password"] = "private-account-a"
            self.assertEqual(second.get("kafka.clientConfig"), {})
            self.assertEqual(DEFAULTS["kafka"]["clientConfig"], {})

    @unittest.skipUnless(os.name == "posix", "systemd uses Linux paths")
    def test_service_uses_selected_interpreter_config_and_paths_with_spaces(self):
        with tempfile.TemporaryDirectory(prefix="account setup ") as directory:
            root = Path(directory)
            settings = Settings({"paths": {"workDir": str(root / "state"), "logDir": str(root / "logs")},
                                 "service": {"name": "account-a", "restartSeconds": 17}}, root / "account a.json", root)
            with patch("fangde.setup.sys.executable", str(root / ".venv/bin/python")):
                result = render_service(settings)
            unit = Path(result["unit"]).read_text()
            self.assertIn('--config "' + str(settings.path) + '" runner', unit)
            self.assertIn("RestartSec=17", unit)
            self.assertFalse(result["started"])
            self.assertNotIn("/home/ubuntu", unit)

    def test_restart_timezone_disabled_and_midnight_window(self):
        settings = Settings({"monitor": {"restart": {"enabled": True, "timezone": "UTC", "start": "23:30", "end": "00:30"}}}, Path("x"), Path("."))
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 9, 10, 0, 10, tzinfo=timezone.utc), settings))
        settings.data["monitor"]["restart"]["enabled"] = False
        self.assertFalse(in_scheduled_restart_window(datetime(2026, 9, 10, 0, 10, tzinfo=timezone.utc), settings))




if __name__ == "__main__":
    unittest.main()
