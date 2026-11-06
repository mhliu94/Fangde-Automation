"""No network: polling, recovery and independent change delivery."""
import copy
import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from fangde.api import TransportError
from fangde.broker import ApiTrading
from fangde.config import DEFAULTS, Settings, validate_config
from fangde.errors import ConfigurationError
from fangde.order_monitor import OrderMonitor
from fangde.order_status import OrderStatusPublisher, rejection_id
from fangde.runner import TradingRunner
from tests.test_api_trading import Broker, raw_order


class OrderMonitorTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        data = copy.deepcopy(DEFAULTS)
        data['fangde']['accountId'] = 'TEST'
        self.settings = Settings(data, root / 'config.json', root)
        self.broker = Broker()
        self.api = ApiTrading(self.settings, client=self.broker)
        self.publisher = OrderStatusPublisher(self.settings)
        self.command = {'command_id': 'blocked', 'account_id': 'TEST', 'type': 'LIMIT_ORDER',
                        'ts': datetime.now(timezone.utc).isoformat(), 'symbol': 'BIYA',
                        'side': 'SELL', 'qty_shares': 100, 'limit_price': 2.28}

    def test_missing_daily_order_is_queried_by_exact_id_until_terminal(self):
        row = raw_order()
        identifier = row['order_no']
        self.broker.orders[identifier] = row
        self.api.poll_order_status()
        self.assertIn(identifier, self.publisher.tracked_orders())
        self.broker.orders[identifier].update(order_status='8', order_status_desc='全部成交', exec_qty='2')
        original = self.api._read
        def read(name, params=None):
            return {'list': []} if name == 'orders' else original(name, params)
        self.api._read = read
        self.api.poll_order_status()
        record = self.publisher._read(identifier)
        self.assertEqual(record['queued'][-1]['status'], 'FILLED')
        self.assertEqual(self.publisher.tracked_orders(), {})
        before = len(self.broker.calls)
        self.api.poll_order_status()
        self.assertEqual(len(self.broker.calls), before)

    def test_failed_list_and_malformed_other_row_do_not_hide_known_order(self):
        row = raw_order()
        self.broker.orders[row['order_no']] = row
        self.api.poll_order_status()
        original = self.api._read
        def unavailable(name, params=None):
            if name == 'orders':
                raise TransportError('offline list')
            return original(name, params)
        self.api._read = unavailable
        self.assertEqual(self.api.poll_order_status()['observed'], 1)
        self.api._read = original
        self.broker.orders['malformed'] = raw_order(order_no='malformed', create_time='', local_create_time='')
        self.broker.orders[row['order_no']].update(order_status='7', order_status_desc='部分成交', exec_qty='1')
        result = self.api.poll_order_status()
        self.assertEqual(result, {'observed': 1, 'errors': 1})
        self.assertEqual(self.publisher._read(row['order_no'])['queued'][-1]['filled_quantity'], 1)

    def test_durable_rejection_requires_absent_dispatch_marker(self):
        runner = TradingRunner(self.settings)
        self.assertTrue(runner._reject_unsubmitted(self.command, 'prior command unresolved'))
        self.assertTrue(self.publisher.has_rejection('blocked'))
        snapshot = self.publisher._read(rejection_id('blocked'))['pending']
        self.assertEqual((snapshot['status'], snapshot['quantity'], snapshot['filled_quantity']), ('REJECTED', 100, 0))
        self.assertEqual(snapshot['command_id'], 'blocked')
        self.assertIn('Not submitted to broker', snapshot['message'])
        for fields in ({'sent_at': 1, 'phase': 'ambiguous'}, {'order_id': 'broker-id'}):
            self.api.journal.save('dispatched', {'account': 'TEST', 'command_id': 'dispatched', **fields})
            self.assertFalse(runner._reject_unsubmitted(dict(self.command, command_id='dispatched'), 'timeout'))
            self.assertFalse(self.publisher.has_rejection('dispatched'))

    def test_half_second_schedule_is_start_to_start_without_overlapping(self):
        runner = TradingRunner(self.settings)
        with patch('fangde.order_monitor.ApiTrading'):
            monitor = OrderMonitor(runner, Mock())
        now, starts, waits = [0.0], [], []
        def poll():
            starts.append(now[0])
            now[0] += 0.2
            return {'observed': 0, 'errors': 0}
        def wait(seconds):
            waits.append(seconds)
            now[0] += seconds
            if len(starts) == 3:
                monitor.stop.set()
        monitor.api.poll_order_status.side_effect = poll
        with patch('fangde.order_monitor.time.monotonic', side_effect=lambda: now[0]), \
             patch.object(monitor.poll_now, 'wait', side_effect=wait):
            monitor._poll()
        self.assertEqual(starts, [0, 0.5, 1])
        for delay in waits:
            self.assertAlmostEqual(delay, 0.3)

    def test_delivery_continues_while_broker_query_is_blocked(self):
        runner = TradingRunner(self.settings)
        entered, release, delivered = threading.Event(), threading.Event(), threading.Event()
        with patch('fangde.order_monitor.ApiTrading'):
            monitor = OrderMonitor(runner, Mock())
        def poll():
            entered.set()
            release.wait(3)
            return {'observed': 0, 'errors': 0}
        monitor.api.poll_order_status.side_effect = poll
        runner._publish_orders = Mock(side_effect=lambda *args: delivered.set())
        monitor.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertTrue(delivered.wait(1))
            self.assertFalse(release.is_set())
        finally:
            release.set()
            monitor.close()
        self.assertTrue(all(not thread.is_alive() for thread in monitor.threads))

    def test_invalid_polling_interval_rejected(self):
        for interval in (0, -1, float('inf')):
            with self.assertRaises(ConfigurationError):
                validate_config({'kafka': {'orderPollIntervalSeconds': interval}})
