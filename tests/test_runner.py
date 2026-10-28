import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fangde.config import Settings
from fangde.diagnostics import command_context
from fangde.errors import SafetyError
from fangde.lock import EmulatorLock
from fangde.runner import (
    TradingRunner, command_is_stale, command_plan, holdings_snapshot, nullable_number,
)


class FakeMessage:
    def partition(self):
        return 0

    def offset(self):
        return 123


class FakeProducer:
    def __init__(self, *, error=None, remaining=0, report=True):
        self.error = error
        self.remaining = remaining
        self.report = report
        self.key = None
        self.value = None

    def produce(self, topic, *, key, value, on_delivery):
        self.key = key
        self.value = json.loads(value)
        if self.report:
            on_delivery(self.error, FakeMessage())

    def flush(self, timeout):
        return self.remaining


class RunnerTests(unittest.TestCase):
    def settings(self):
        root = Path(tempfile.mkdtemp())
        return Settings({
            "fangde": {"accountId": "GIGA-A", "accountNumId": 7},
        }, root / "config.json", root)

    def test_live_start_enables_saved_state_before_intake(self):
        runner = TradingRunner(self.settings())
        runner._set_trading_enabled(False)
        with patch.object(runner, '_operation'), patch.object(runner, '_run_kafka') as intake:
            intake.side_effect = lambda: self.assertTrue(runner._trading_enabled())
            runner.run(live=True)
        self.assertTrue(TradingRunner(runner.settings)._trading_enabled())
        self.assertEqual(runner.state_path.stat().st_mode & 0o777, 0o600)

    def test_live_start_cannot_enable_another_running_process(self):
        runner = TradingRunner(self.settings())
        runner._set_trading_enabled(False)
        before = runner.state_path.read_bytes()
        with EmulatorLock(runner.settings.work_dir / 'runner.lock', 0):
            with self.assertRaises(SafetyError):
                runner.run(live=True)
        self.assertEqual(runner.state_path.read_bytes(), before)

    def test_normal_start_preserves_saved_trading_state(self):
        runner = TradingRunner(self.settings())
        for enabled in (False, True):
            runner._set_trading_enabled(enabled)
            before = runner.state_path.read_bytes()
            with patch.object(runner, '_operation'), patch.object(runner, '_run_kafka'):
                runner.run()
            self.assertEqual(runner.state_path.read_bytes(), before)

    def test_cli_forwards_explicit_live_start(self):
        from fangde.cli import main
        with patch('fangde.cli.load_settings', return_value=self.settings()), \
             patch('fangde.cli.TradingRunner') as runner:
            self.assertEqual(main(['runner', '--live']), 0)
        runner.return_value.run.assert_called_once_with(live=True)

    def test_routing_and_guards(self):
        market = command_plan({
            "type": "MARKET_ORDER", "symbol": "bili", "side": "BUY", "qty_shares": 2,
        })
        self.assertEqual(market.arguments["symbol"], "BILI")
        self.assertEqual(market.arguments["quantity"], "2")
        fok = command_plan({
            "type": "LIMIT_ORDER", "time_in_force": "FOK", "symbol": "BILI",
            "side": "SELL", "qty_shares": 3, "limit_price": 17.4,
        })
        self.assertTrue(fok.arguments["kill"])
        with self.assertRaises(ValueError):
            command_plan({
                "type": "MARKET_ORDER", "symbol": "BILI", "side": "BUY",
                "notional_usd": 100,
            })

    def test_snapshot_contract(self):
        snapshot = holdings_snapshot({
            "queriedAt": "2026-07-10T12:00:00Z",
            "cash": {"balances": [{"currency": "USD", "balance": "$1,234.50"}]},
            "positions": [{
                "symbol": "bili", "quantity": "12.5", "cost": "$17.25",
                "marketPrice": "$18.50", "marketValue": "$231.25",
                "dailyPL": "($15.00)", "dailyPLPercent": "6.94%",
            }],
        }, self.settings(), True)
        self.assertEqual(set(snapshot), {
            "account_id", "account_num_id", "cash", "cash_by_currency", "positions", "ts",
            "trading_enabled",
        })
        self.assertEqual(snapshot["cash"], 1234.5)
        self.assertEqual(snapshot["positions"][0]["avg_price"], 17.25)
        self.assertEqual(snapshot["positions"][0]["dailyPL"], -15.0)

    def test_staleness_and_number_cleaning(self):
        now = datetime.now(timezone.utc)
        self.assertTrue(command_is_stale({}, 300, now=now))
        self.assertTrue(command_is_stale({"ts": (now - timedelta(seconds=301)).isoformat()}, 300, now=now))
        self.assertFalse(command_is_stale({"ts": now.isoformat()}, 300, now=now))
        self.assertEqual(nullable_number("($1,234.50)"), -1234.5)

    def test_publish_requires_successful_delivery_report(self):
        snapshot = {"account_id": "A", "positions": []}
        producer = FakeProducer()
        self.assertEqual(TradingRunner._publish_snapshot(producer, "account-details", snapshot), (0, 123))
        self.assertEqual(producer.key, "")
        self.assertEqual(producer.value, snapshot)

        with self.assertRaisesRegex(RuntimeError, "rejected"):
            TradingRunner._publish_snapshot(
                FakeProducer(error="Topic authorization failed"), "account-details", snapshot
            )
        with self.assertRaisesRegex(RuntimeError, "remain queued"):
            TradingRunner._publish_snapshot(
                FakeProducer(remaining=1, report=False), "account-details", snapshot
            )
        with self.assertRaisesRegex(RuntimeError, "no delivery report"):
            TradingRunner._publish_snapshot(
                FakeProducer(report=False), "account-details", snapshot
            )


class RunnerLoopTests(unittest.TestCase):
    @staticmethod
    def command_results(logs):
        prefix = "Command finished "
        return [json.loads(record.getMessage()[len(prefix):]) for record in logs.records
                if record.getMessage().startswith(prefix)]

    def run_command(
        self, command, *, result=None, trading_enabled=True, commit_failed=True,
        refresh_error=None, delivery_error=None, later_polls=(),
        query_seconds=0, delivery_seconds=0, holdings_interval=120, backend="api",
        order_delivery_error=None,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings({
                "fangde": {
                    "accountId": "GIGA-A", "tradingEnabledDefault": trading_enabled, "backend": backend,
                },
                "kafka": {
                    "bootstrapServers": "unused:9092", "commandTopic": "commands",
                    "accountDetailsTopic": "account-details", "consumerGroupId": "test",
                    "commitFailedCommands": commit_failed,
                },
            }, root / "config.json", root)
            if holdings_interval is not None:
                settings.data["kafka"]["holdingsIntervalSeconds"] = holdings_interval
            runner = TradingRunner(settings)
            command = {
                "account_id": "GIGA-A", "command_id": "test-order",
                "ts": datetime.now(timezone.utc).isoformat(),
                "symbol": "BILI", "side": "BUY", "qty_shares": 2, "limit_price": 17.4,
                **command,
            }
            message = Mock()
            message.error.return_value = None
            message.value.return_value = json.dumps(command).encode()
            now = 0
            polls = iter([(10, message), *[(when, None) for when in later_polls]])
            events = []
            snapshots = []
            queries = 0
            producer = FakeProducer()
            original_publish_orders = runner._publish_orders

            def publish_orders(*args):
                if order_delivery_error is not None:
                    raise order_delivery_error
                return original_publish_orders(*args)

            def poll(timeout):
                nonlocal now
                events.append("poll")
                item = next(polls, None)
                if item is None:
                    runner.running = False
                    return None
                when, value = item
                now = max(now, when)
                return value

            def operation(kind, arguments):
                nonlocal queries, now
                events.append(kind)
                if kind == "recover":
                    return {"ok": True, "recovered": 0}
                if kind != "holdings":
                    if isinstance(result, Exception):
                        raise result
                    return result if result is not None else {"ok": True, "submitted": True}
                queries += 1
                now += query_seconds
                if queries > 1:
                    producer.error = delivery_error
                    if refresh_error:
                        raise refresh_error
                return {"positions": [{"symbol": "BILI", "quantity": queries, "cost": 17.4}]}

            def publish(producer, topic, snapshot, **kwargs):
                nonlocal now
                events.append("publish")
                snapshots.append(snapshot)
                self.assertEqual(topic, "account-details")
                now += delivery_seconds
                return TradingRunner._publish_snapshot(producer, topic, snapshot)

            def sleep(seconds):
                nonlocal now
                events.append(("sleep", seconds))
                now += seconds

            consumer = Mock()
            consumer.poll.side_effect = poll
            consumer.commit.side_effect = lambda **kwargs: events.append("commit")
            kafka = SimpleNamespace(
                Consumer=Mock(return_value=consumer), Producer=Mock(return_value=producer),
                KafkaException=RuntimeError,
            )
            with (
                patch.dict(sys.modules, {"confluent_kafka": kafka}),
                patch("fangde.runner.signal.signal"),
                patch("fangde.runner.time.monotonic", side_effect=lambda: now),
                patch("fangde.runner.time.sleep", side_effect=sleep),
                patch.object(runner, "_operation", side_effect=operation) as operations,
                patch.object(runner, "_publish_snapshot", side_effect=publish),
                patch.object(runner, "_publish_orders", side_effect=publish_orders),
            ):
                runner.run()
            consumer.commit.assert_called_once_with(message=message, asynchronous=False)
            consumer.close.assert_called_once_with()
            return events, snapshots, operations


    def test_unsuccessful_or_unsubmitted_orders_do_not_refresh(self):
        for result in ({"ok": False, "submitted": True}, {"ok": True, "submitted": False}):
            with self.subTest(result=result):
                events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, result=result)
                self.assertEqual(len(snapshots), 1)
                self.assertEqual(events.count("holdings"), 1)

    def test_api_post_action_delay_includes_cancel_and_acknowledged_failures(self):
        from fangde.api_trading import OrderUncertain
        for kind, operation, result in (
            ("MARKET_ORDER", "market", None),
            ("LIMIT_ORDER", "limit", None),
            ("LIMIT_ORDER_FOK", "limit", OrderUncertain("Cancel unconfirmed", submitted=True)),
            ("CANCEL_OPEN_ORDERS", "cancel", {"dryRun": False, "canceledCount": 1}),
            ("CANCEL_OPEN_ORDERS", "cancel", OrderUncertain("Some orders remain open")),
        ):
            with self.subTest(kind=kind, result=result):
                events, _, _ = self.run_command({"type": kind}, backend="api", result=result)
                self.assertEqual(events, ["recover", "holdings", "publish", "poll", operation,
                    "commit", ("sleep", 2), "holdings", "publish", "poll"])

    def test_api_delayed_publication_resets_periodic_interval_from_delivery(self):
        events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, backend="api",
            query_seconds=4, delivery_seconds=2, holdings_interval=30, later_polls=(47, 48))
        self.assertEqual(events, ["recover", "holdings", "publish", "poll", "market", "commit",
            ("sleep", 2), "holdings", "publish", "poll", "poll", "holdings", "publish", "poll"])
        self.assertEqual(len(snapshots), 3)












    def test_ignored_commands_are_distinct_from_successful_actions(self):
        for command, options, reason in (
            ({"type": "MARKET_ORDER", "ts": "2000-01-01T00:00:00Z"}, {}, "stale"),
            ({"type": "MARKET_ORDER"}, {"trading_enabled": False}, "trading_disabled"),
            ({"type": "UNSUPPORTED"}, {}, "unsupported_type"),
        ):
            with self.subTest(reason=reason), self.assertLogs("fangde", level="INFO") as logs:
                self.run_command(command, **options)
            commands = self.command_results(logs)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["outcome"], "ignored")
            self.assertEqual(commands[0]["reason"], reason)
            self.assertTrue(commands[0]["committed"])

    def test_publication_failure_does_not_change_successful_command_outcome(self):
        with self.assertLogs("fangde", level="INFO") as logs:
            self.run_command({"type": "MARKET_ORDER"}, delivery_error="Delivery rejected")
        commands = self.command_results(logs)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["outcome"], "success")
        failures = [record.getMessage() for record in logs.records
                    if "Holdings publication failed" in record.getMessage()]
        self.assertEqual(len(failures), 1)
        self.assertIn("stage=publish trigger=post_order command_id=test-order", failures[0])



if __name__ == "__main__":
    unittest.main()
