import http.client
import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from fangde.config import Settings
from fangde.monitor import (
    ActionResult,
    FangdeController,
    FangdeServer,
    LoginLimiter,
    RedirectServer,
    StatusMonitor,
    in_scheduled_restart_window,
    parse_android_app_meminfo,
    parse_android_memory_summary,
    verify_admin_credentials,
)


class ScheduledRestartWindowTests(unittest.TestCase):
    def test_window_uses_daylight_aware_eastern_time(self):
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)))
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 1, 15, 1, 1, tzinfo=timezone.utc)))

    def test_window_includes_only_the_900_pm_minute(self):
        self.assertFalse(in_scheduled_restart_window(datetime(2026, 8, 15, 0, 0, 59, tzinfo=timezone.utc)))
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 8, 15, 1, 0, 59, tzinfo=timezone.utc)))
        self.assertFalse(in_scheduled_restart_window(datetime(2026, 8, 15, 1, 1, tzinfo=timezone.utc)))


class FakeRunner:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []

    def __call__(self, arguments, *, cwd, timeout):
        self.calls.append(list(arguments))
        if self.responses:
            return self.responses.pop(0)
        return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()


class FakeProcess:
    def __init__(self, pid=2468, returncode=None):
        self.pid = pid
        self.returncode = returncode

    def poll(self):
        return self.returncode


class MonitorTests(unittest.TestCase):
    def test_api_health_and_status_do_not_probe_android(self):
        self.settings.data["fangde"]["backend"] = "api"
        controller = FangdeController(self.settings, machine_sampler=lambda: {
            "cpu_percent": 20, "memory_percent": 30, "memory_total_bytes": 16 * 1024**3,
            "memory_available_bytes": 11 * 1024**3})
        with patch.object(controller, "emulator_status", side_effect=AssertionError("must not probe Android")), \
             patch.object(controller, "_android_memory", side_effect=AssertionError("must not use ADB")):
            status = controller.collect_status(60)
        self.assertEqual(status["account"]["backend"], "api")
        self.assertEqual(status["health"]["overall_level"], "healthy")
        self.assertEqual([row["id"] for row in status["health"]["metrics"]], ["machine_cpu", "machine_memory"])

    def test_api_scheduled_restart_is_skipped_even_when_enabled(self):
        self.settings.data["fangde"]["backend"] = "api"
        self.settings.data["monitor"]["restart"] = {"enabled": True}
        controller = FangdeController(self.settings)
        controller.stop_script = Mock()
        controller.start_script = Mock()
        controller.stop_emulator = Mock()
        controller._start_emulator = Mock()
        for hour, minute in [(0, 1), (0, 30), (1, 0)]:
            with self.subTest(hour=hour, minute=minute):
                result = controller.scheduled_restart_if_due(
                    datetime(2026, 8, 15, hour, minute, tzinfo=timezone.utc),
                    waiter=lambda _: False,
                )
                self.assertIsNone(result)
        controller.stop_script.assert_not_called()
        controller.start_script.assert_not_called()
        controller.stop_emulator.assert_not_called()
        controller._start_emulator.assert_not_called()
        self.assertFalse(controller.emulator_run_path.exists())

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "logs").mkdir()
        (self.root / "work").mkdir()
        self.settings = Settings({
            "fangde": {"accountId": "GIGA-A", "package": "com.fdzq.app"},
            "adb": {"serial": "emulator-5554", "path": ""},
            "monitor": {"avd": "Pixel_10", "runnerLog": "logs/runner.log"},
        }, self.root / "config.json", self.root)

    def test_holdings_stats_are_current_session_latest_ten(self):
        started = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)
        lines = ["2026-08-10 07:59:59,000 INFO Holdings query completed in 999.000 seconds."]
        lines += [
            f"2026-08-10 08:{number:02d}:00,000 INFO Holdings query completed in {number:.3f} seconds."
            for number in range(1, 13)
        ]
        (self.root / "logs" / "runner.log").write_text("\n".join(lines), encoding="utf-8")
        controller = FangdeController(self.settings)

        result = controller.holdings_query_performance({
            "running": True, "pid": 123, "started_at": started.isoformat(),
        })

        self.assertTrue(result["available"])
        self.assertEqual(result["attempts_in_session"], 12)
        self.assertEqual(result["sample_count"], 10)
        self.assertEqual(result["average_seconds"], 7.5)
        self.assertEqual(result["fastest_seconds"], 3.0)
        self.assertEqual(result["slowest_seconds"], 12.0)

    def test_holdings_stats_do_not_reuse_stopped_session(self):
        result = FangdeController(self.settings).holdings_query_performance({
            "running": False, "started_at": None,
        })
        self.assertFalse(result["available"])
        self.assertEqual(result["sample_count"], 0)

    def test_holdings_stats_fall_back_to_publication_schedule_for_old_runner(self):
        (self.root / "logs" / "runner.log").write_text(
            "2026-08-10 08:00:05,000 INFO Published holdings with 2 positions to partition=0 offset=1.\n"
            "2026-08-10 08:02:07,000 INFO Published holdings with 2 positions to partition=0 offset=2.\n"
            "2026-08-10 08:04:09,000 INFO Published holdings with 2 positions to partition=0 offset=3.\n",
            encoding="utf-8",
        )
        self.settings.data["kafka"] = {"holdingsIntervalSeconds": 120}
        result = FangdeController(self.settings).holdings_query_performance({
            "running": True, "pid": 123, "started_at": "2026-08-10T08:00:00Z",
        })
        self.assertTrue(result["available"])
        self.assertTrue(result["estimated"])
        self.assertEqual(result["average_seconds"], 7.0)
        self.assertEqual(result["fastest_seconds"], 5.0)
        self.assertEqual(result["slowest_seconds"], 9.0)





    def test_control_logs_one_outcome_and_elapsed_time(self):
        for response in (ActionResult(True, "started"), ActionResult(False, "could not start"), RuntimeError("failed")):
            with self.subTest(response=response):
                controller = FangdeController(self.settings)
                controller.start_script = Mock()
                if isinstance(response, Exception):
                    controller.start_script.side_effect = response
                else:
                    controller.start_script.return_value = response
                with self.assertLogs("fangde.monitor", level="INFO") as logs:
                    result = controller.perform("script/start")
                outcomes = [record.getMessage() for record in logs.records
                            if record.getMessage().startswith("Control action finished")]
                self.assertEqual(len(outcomes), 1)
                self.assertIn("outcome=success" if result.ok else "outcome=failure", outcomes[0])
                self.assertIn("elapsed_s=", outcomes[0])
                self.assertFalse(controller._action_lock.locked())

    def test_start_script_uses_venv_python_config_and_detached_process(self):
        calls = []

        def popen(arguments, **kwargs):
            calls.append((arguments, kwargs))
            return FakeProcess()

        controller = FangdeController(self.settings, popen=popen)
        with patch.object(controller, "script_status", side_effect=[{"running": False}, {"running": True}]):
            result = controller.start_script()

        self.assertTrue(result.ok)
        self.assertEqual(calls[0][0][1:3], ["-m", "fangde.cli"])
        self.assertIn(str(self.settings.path), calls[0][0])
        self.assertTrue(calls[0][1]["start_new_session"])
        self.assertEqual(calls[0][1]["env"]["FANGDE_RUN_LOG"], str(self.root / "logs" / "runner.log"))

    def test_stop_script_requests_validated_signal(self):
        controller = FangdeController(self.settings)
        with patch.object(controller, "script_status", return_value={"running": True, "pid": 4321}), patch(
            "fangde.monitor.request_runner_stop", return_value={"requested": True, "pid": 4321}
        ) as request:
            result = controller.stop_script()

        self.assertTrue(result.ok)
        request.assert_called_once_with(self.settings, force=True)














class DummyController:
    def __init__(self):
        self.actions = []

    def collect_status(self, interval):
        return self.status()

    def public_config(self):
        return {"account_id": "GIGA-A", "server_id": "test", "device": "emulator-5554", "avd": "Pixel_10"}

    def status(self):
        return {
            "account": self.public_config(),
            "script": {"running": True, "state": "running", "pid": 123},
            "emulator": {"running": True, "state": "running"},
            "holdings_queries": {"sample_count": 1, "average_seconds": 4.0},
            "health": {"overall_level": "healthy", "metrics": []},
            "checked_at": "2026-08-10T08:00:00Z", "next_check_at": "2026-08-10T08:01:00Z",
        }

    def perform(self, action):
        self.actions.append(action)
        return ActionResult(True, "done")


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.controller = DummyController()
        self.monitor = StatusMonitor(self.controller, 60)
        self.monitor.refresh()
        self.server = FangdeServer(
            ("127.0.0.1", 0), self.monitor, admin_password="test-dashboard-password",
        )
        self.server.is_tls = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def connection(self):
        return http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)

    def login(self, connection):
        body = json.dumps({"username": "admin", "password": "test-dashboard-password"})
        connection.request("POST", "/api/login", body=body, headers={
            "Content-Type": "application/json", "Content-Length": str(len(body)),
        })
        response = connection.getresponse()
        payload = json.loads(response.read())
        self.assertEqual(response.status, 200)
        cookie = response.getheader("Set-Cookie")
        self.assertIn("Secure", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)
        return cookie.split(";", 1)[0], payload["csrf_token"]

    def test_health_and_status_routes(self):
        connection = self.connection()
        try:
            connection.request("GET", "/healthz")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(json.loads(response.read())["ok"])
            connection.request("GET", "/api/status")
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 401)
            cookie, _ = self.login(connection)
            connection.request("GET", "/api/status", headers={"Cookie": cookie})
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read())["account"]["account_id"], "GIGA-A")
        finally:
            connection.close()

    def test_control_requires_custom_header(self):
        connection = self.connection()
        try:
            cookie, csrf = self.login(connection)
            connection.request("POST", "/api/script/start", body="{}", headers={
                "Cookie": cookie, "Content-Type": "application/json", "Content-Length": "2",
            })
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 403)

            connection.request("POST", "/api/script/start", body="{}", headers={
                "Cookie": cookie, "Content-Type": "application/json", "Content-Length": "2",
                "X-Fangde-Action": "1", "X-CSRF-Token": csrf,
            })
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(json.loads(response.read())["ok"])
            self.assertEqual(self.controller.actions, ["script/start"])
        finally:
            connection.close()

    def test_credentials_and_login_lockout(self):
        self.assertTrue(verify_admin_credentials("admin", "test-password", "test-password"))
        self.assertFalse(verify_admin_credentials("admin", "wrong", "test-password"))
        limiter = LoginLimiter()
        for _ in range(4):
            self.assertEqual(limiter.record_failure("192.0.2.1"), 0)
        self.assertGreater(limiter.record_failure("192.0.2.1"), 0)
        self.assertGreater(limiter.retry_after("192.0.2.1"), 0)

    def test_http_redirect_uses_standard_https_without_explicit_port(self):
        redirect = RedirectServer(("127.0.0.1", 0), "monitor.example.test", 443)
        thread = threading.Thread(target=redirect.serve_forever, daemon=True)
        thread.start()
        connection = http.client.HTTPConnection("127.0.0.1", redirect.server_port, timeout=5)
        try:
            connection.request("GET", "/dashboard?view=live")
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 308)
            self.assertEqual(
                response.getheader("Location"),
                "https://monitor.example.test/dashboard?view=live",
            )
        finally:
            connection.close()
            redirect.shutdown()
            redirect.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
