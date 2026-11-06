import copy
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fangde.config import Settings
from fangde.diagnostics import command_context, command_source_context
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

    def test_holdings_publish_only_changed_data_and_retry_failed_delivery(self):
        runner = TradingRunner(self.settings())
        producer = FakeProducer()
        snapshot = holdings_snapshot({
            "cash": {"balances": [{"currency": "USD", "balance": "100"}]},
            "positions": [{"symbol": "AAPL", "quantity": "2", "cost": "10"},
                          {"symbol": "MSFT", "quantity": "1", "cost": "20"}],
        }, runner.settings, True)
        with patch.object(producer, "produce", wraps=producer.produce) as produce:
            self.assertIsNotNone(runner._publish_holdings(producer, "details", snapshot))
            reordered = dict(snapshot, ts="2026-11-05T12:00:00Z", positions=list(reversed(snapshot["positions"])))
            self.assertIsNone(runner._publish_holdings(producer, "details", reordered))
            produce.assert_called_once()
            producer.error = "delivery failure"
            changed = dict(snapshot, cash=90)
            with self.assertRaises(RuntimeError):
                runner._publish_holdings(producer, "details", changed)
            producer.error = None
            self.assertIsNotNone(runner._publish_holdings(producer, "details", changed))
            self.assertIsNone(runner._publish_holdings(producer, "details", changed))
            self.assertEqual(produce.call_count, 3)

    def test_all_account_fields_trigger_publication_and_restart_sends_baseline(self):
        baseline = holdings_snapshot({"positions": [{"symbol": "AAPL", "quantity": 2, "cost": 10}]},
                                     self.settings(), True)
        changes = [dict(baseline, cash=1), dict(baseline, cash_by_currency={"HKD": 10}),
                   dict(baseline, trading_enabled=False), dict(baseline, positions=[])]
        for field in ("qty", "avg_price", "marketPrice", "marketValue", "dailyPL", "dailyPLPercent"):
            changed = copy.deepcopy(baseline)
            changed["positions"][0][field] = 123
            changes.append(changed)
        for changed in changes:
            with self.subTest(changed=changed):
                runner = TradingRunner(self.settings())
                producer = FakeProducer()
                runner._publish_holdings(producer, "details", baseline)
                self.assertIsNotNone(runner._publish_holdings(producer, "details", changed))
                self.assertIsNone(runner._publish_holdings(producer, "details", changed))
                self.assertIsNotNone(TradingRunner(runner.settings)._publish_holdings(producer, "details", changed))


class RunnerLoopTests(unittest.TestCase):
    def test_one_second_polling_checks_orders_even_when_holdings_are_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings({"fangde": {"accountId": "A"}, "kafka": {
                "bootstrapServers": "unused", "commandTopic": "commands",
                "accountDetailsTopic": "details", "consumerGroupId": "test",
                "pollTimeoutSeconds": 30,
            }}, root / "config.json", root)
            runner = TradingRunner(settings)
            now = 0.0
            starts = []
            waits = []
            def operation(kind, arguments):
                nonlocal now
                if kind == "recover":
                    return {}
                starts.append(now)
                now += 0.2
                return {"positions": [], "orders": [{"observation": len(starts)}],
                        "queriedAt": datetime.fromtimestamp(now, timezone.utc).isoformat()}
            def poll(timeout):
                nonlocal now
                waits.append(timeout)
                now += timeout
                if len(starts) == 3:
                    runner.running = False
                return None
            producer = FakeProducer()
            consumer = Mock()
            consumer.poll.side_effect = poll
            kafka = SimpleNamespace(Consumer=Mock(return_value=consumer), Producer=Mock(return_value=producer),
                                    KafkaException=RuntimeError)
            with patch.dict(sys.modules, {"confluent_kafka": kafka}), patch("fangde.runner.signal.signal"), \
                 patch("fangde.runner.time.monotonic", side_effect=lambda: now), \
                 patch.object(runner, "_operation", side_effect=operation), \
                 patch.object(runner, "_publish_orders") as orders, \
                 patch.object(producer, "produce", wraps=producer.produce) as publish:
                runner.run()
            self.assertEqual(starts, [0, 1, 2])
            for wait in waits:
                self.assertAlmostEqual(wait, 0.8)
            self.assertEqual(orders.call_count, 3)
            self.assertEqual([call.args[1] for call in orders.call_args_list],
                             [[{"observation": n}] for n in (1, 2, 3)])
            publish.assert_called_once()

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
            message.partition.return_value = 0
            message.offset.return_value = 123
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
                try:
                    runner.run()
                except OSError:
                    consumer.commit.assert_not_called()
                    raise
            consumer.commit.assert_called_once_with(message=message, asynchronous=False)
            consumer.close.assert_called_once_with()
            return events, snapshots, operations

    def test_command_receipt_and_outcome_include_kafka_location_without_extra_payload(self):
        with self.assertLogs("fangde", level="INFO") as logs:
            self.run_command({"type": "MARKET_ORDER", "password": "private-extra-field",
                              "metadata": {"token": "private-nested-field"}})
        prefix = "Command received "
        received = [json.loads(record.getMessage()[len(prefix):]) for record in logs.records
                    if record.getMessage().startswith(prefix)]
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["source"], {"topic": "commands", "partition": 0, "offset": 123})
        self.assertEqual(received[0]["command"]["qty_shares"], 2)
        self.assertEqual(self.command_results(logs)[0]["source"], received[0]["source"])
        self.assertNotIn("private-", "\n".join(logs.output))
        self.assertIsNone(command_source_context.get())

    def test_stale_command_receipt_is_recorded_without_execution(self):
        with self.assertLogs("fangde", level="INFO") as logs:
            _, _, operations = self.run_command({"type": "MARKET_ORDER", "ts": "2026-01-01T00:00:00Z"})
        self.assertTrue(any(record.getMessage().startswith("Command received ") for record in logs.records))
        self.assertEqual(self.command_results(logs)[0]["reason"], "stale")
        self.assertNotIn("market", [call.args[0] for call in operations.call_args_list])

    def test_blocked_command_has_durable_rejection_before_commit(self):
        from fangde.order_status import OrderStatusPublisher, rejection_id
        from fangde.workflows import OrderUncertain
        original = TradingRunner._reject_unsubmitted
        snapshots = []
        def reject(runner, command, reason):
            result = original(runner, command, reason)
            snapshots.append(OrderStatusPublisher(runner.settings)._read(rejection_id(command['command_id']))['pending'])
            return result
        with patch.object(TradingRunner, '_reject_unsubmitted', reject):
            _, _, operations = self.run_command({'type': 'LIMIT_ORDER'},
                result=OrderUncertain('A previous submission is unresolved'))
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0]['status'], 'REJECTED')
        self.assertEqual(snapshots[0]['command_id'], 'test-order')
        self.assertEqual([call.args[0] for call in operations.call_args_list].count('limit'), 1)

    def test_failed_rejection_persistence_stops_without_committing(self):
        from fangde.workflows import OrderUncertain
        with patch.object(TradingRunner, '_reject_unsubmitted', side_effect=OSError('disk full')), \
             self.assertRaisesRegex(OSError, 'disk full'):
            self.run_command({'type': 'LIMIT_ORDER'}, result=OrderUncertain('blocked'))

    def test_ambiguous_dispatched_order_is_never_locally_rejected(self):
        from fangde.workflows import OrderUncertain
        with patch.object(TradingRunner, '_reject_unsubmitted') as reject:
            self.run_command({'type': 'LIMIT_ORDER'}, result=OrderUncertain('lost ACK', submitted=True))
        reject.assert_not_called()

    def test_rejected_command_redelivery_cannot_submit_after_restart(self):
        from fangde.order_status import OrderStatusPublisher
        with patch.object(OrderStatusPublisher, 'has_rejection', return_value=True):
            with self.assertLogs('fangde', level='INFO') as logs:
                _, _, operations = self.run_command({'type': 'LIMIT_ORDER'})
        self.assertNotIn('limit', [call.args[0] for call in operations.call_args_list])
        self.assertEqual(self.command_results(logs)[0]['reason'], 'previously_rejected')


    def test_unsuccessful_or_unsubmitted_orders_do_not_refresh(self):
        for result in ({"ok": False, "submitted": True}, {"ok": True, "submitted": False}):
            with self.subTest(result=result):
                events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, result=result)
                self.assertEqual(len(snapshots), 1)
                self.assertEqual(events.count("holdings"), 1)

    def test_api_post_action_refresh_includes_cancel_and_acknowledged_failures(self):
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
                    "commit", "holdings", "publish", "poll"])

    def test_periodic_interval_is_measured_from_query_start(self):
        events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, backend="api",
            query_seconds=4, delivery_seconds=2, holdings_interval=30, later_polls=(39, 40))
        self.assertEqual(events, ["recover", "holdings", "publish", "poll", "market", "commit",
            "holdings", "publish", "poll", "poll", "holdings", "publish", "poll"])
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
