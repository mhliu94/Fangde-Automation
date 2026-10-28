"""Stateful offline Fangde double: no test connects to the real broker."""
import copy
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from fangde.api import ApiResult, ApiError, TransportError, AuthenticationRequired
from fangde.api_trading import ApiTrading, OrderUncertain
from fangde.broker import normalize_order, market_session
from fangde.config import Settings, DEFAULTS
from fangde.diagnostics import command_context


def broker_order(identifier='9007199254740993123', **extra):
    return {'order_id_str':identifier, 'counter_id':'ST/US/BIYA', 'action':1,
            'order_type':'LO', 'price':'2.28', 'submitted_at':str(int(time.time())-1),
            'time_in_force':0, 'currency':'USD', 'quantity':'2', 'executed_qty':'0',
            'executed_price':'0', 'status':'NewStatus', **extra}


def raw_order(**extra):
    return {'order_no':'9007199254740993123', 'exchange':'US', 'symbol':'BIYA',
        'asset_type':'Stock', 'bs_flag':'B', 'order_type':'PL', 'qty':'2', 'price':'2.28',
        'exec_qty':'0','exec_price':'0','order_status':'2','order_status_desc':'已报',
        'enable_cancel':'1', 'create_time':'2026-10-27 08:11:00',
        'local_create_time':'2026-10-26 20:11:00','local_timezone':'美东', **extra}


class Broker:
    def __init__(self):
        self.calls=[]
        self.orders={}
        self.fail_submit=False
        self.cancel_final='6'
        self.fill_on_cancel=False
        self.partial=False
        self.history=[]
        self.positions={'products':[{'exchange':'US','asset_type':'Stock','symbol':'BIYA',
            'qty':'10','avail_sell_qty':'8','avg_cost':'2','last_price':'2.3',
            'market_value':'23','today_pl':'1','today_pl_percent':'4.55'}],
            'cash':[{'ccy':'USD','amt':'123.45'}]}

    def read(self, name, params=None):
        self.calls.append(('read',name,copy.deepcopy(params)))
        if name=='settings':
            data={'FDHongKong':{'order_type':[{'exchange':'US','types':[{'code':'M'},{'code':'PL'}]}],
                              'duration_type':[{'exchange':'US','types':[{'code':'DayOrder'}]}]}}
        elif name=='security': data={'exchange':'US','symbol':params['symbol']}
        elif name=='positions': data=self.positions
        elif name=='orders': data={'list':list(self.orders.values())}
        elif name=='order_detail': data=self.orders[params['order_no']]
        elif name=='order_history': data=self.history[int(params['page'])-1]
        else: raise AssertionError(name)
        return ApiResult(name,200,0,1,'0',{'data':copy.deepcopy(data)})

    def write(self, name, body, *, before_send):
        before_send()
        self.calls.append(('write',name,copy.deepcopy(body)))
        if name=='submit':
            row=raw_order(symbol=body['counter_id'].split('/')[-1],
                bs_flag='B' if body['action']=='1' else 'S', qty=body['quantity'],
                order_type='PL' if body['order_type']=='LO' else 'M',price=body.get('price','2.3'))
            if self.partial: row.update(order_status='7',order_status_desc='部分成交',exec_qty='1',exec_price='2.28')
            self.orders[row['order_no']]=row
            if self.fail_submit: raise TransportError('simulated timeout after acceptance')
            return {'order_id_str':row['order_no']}
        row=self.orders[body['order_id']]
        if self.fill_on_cancel:
            row.update(order_status='8',order_status_desc='全部成交',exec_qty=row['qty'],exec_price=row['price'])
        elif self.cancel_final:
            row.update(order_status=self.cancel_final,order_status_desc='已撤单' if self.cancel_final=='6' else '部成部撤',enable_cancel='0')
        return {}


class TradingTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        root=Path(temp.name)
        data=copy.deepcopy(DEFAULTS)
        data['fangde']['accountId']='TEST'
        data['api'].update(orderActionDelaySeconds=0,fokWaitSeconds=0,cancelPollDelaysSeconds=[0,0])
        self.settings=Settings(data,root/'config.json',root)
        self.broker=Broker()
        self.books=SimpleNamespace(price=lambda *args:SimpleNamespace(price='2.29',details=lambda:{}))
        self.trading=ApiTrading(self.settings,client=self.broker,price_books=self.books,sleeper=lambda _:None)
        token=command_context.set('test-command');self.addCleanup(command_context.reset,token)

    def writes(self,name): return [x for x in self.broker.calls if x[:2]==('write',name)]

    def test_preview_does_not_submit(self):
        result=self.trading.limit_order('BIYA','2.28','2','buy',dry_run=True)
        self.assertFalse(result['submitted']);self.assertEqual(self.writes('submit'),[])

    def test_submission_ack_is_new_not_filled_and_replay_is_deduplicated(self):
        first=self.trading.limit_order('BIYA','2.28','2','Buy')
        second=self.trading.limit_order('BIYA','2.28','2','Buy')
        self.assertEqual(first['status'],'NewStatus')
        self.assertEqual(first['filledQty'],'0')
        self.assertTrue(second['deduplicated']);self.assertEqual(len(self.writes('submit')),1)

    def test_same_command_id_with_different_intent_rejected(self):
        self.trading.limit_order('BIYA','2.28','2','Buy')
        with self.assertRaises(ValueError): self.trading.limit_order('BIYA','2.28','3','Buy')
        self.assertEqual(len(self.writes('submit')),1)

    def test_authentication_failure_stops_workflow_and_does_not_leave_ambiguous_ticket(self):
        def rejected(name,body,*,before_send):
            before_send()
            raise AuthenticationRequired('Renew on phone')
        self.broker.write=rejected
        with self.assertRaises(AuthenticationRequired):self.trading.limit_order('BIYA','2.28','2','Buy')
        self.assertEqual(self.trading.journal.read('test-command')['phase'],'rejected')

    def test_operator_reconciliation_matches_intent_without_resubmitting(self):
        self.broker.fail_submit=True
        with self.assertRaises(OrderUncertain):self.trading.limit_order('BIYA','2.28','2','Buy')
        result=self.trading.reconcile_order('test-command','9007199254740993123')
        self.assertFalse(result['resubmitted'])
        self.assertEqual(self.trading.recover_pending()['recovered'],1)
        self.assertEqual(len(self.writes('submit')),1)

    def test_fok_and_separate_cancel_share_ambiguous_cancellation(self):
        self.broker.cancel_final=None
        with self.assertRaises(OrderUncertain):self.trading.limit_order('BIYA','2.28','2','Buy',kill=True)
        with self.assertRaises(OrderUncertain):self.trading.cancel_order('9007199254740993123')
        self.assertEqual(len(self.writes('withdraw')),1)

    def test_lost_ack_blocks_replay_and_other_submissions(self):
        self.broker.fail_submit=True
        for _ in range(2):
            with self.assertRaises(OrderUncertain):self.trading.limit_order('BIYA','2.28','2','Buy')
        token=command_context.set('different')
        try:
            with self.assertRaises(OrderUncertain):self.trading.limit_order('BIYA','2.28','2','Buy')
        finally:command_context.reset(token)
        self.assertEqual(len(self.writes('submit')),1)

    def test_partial_fok_cancels_remainder_but_reports_existing_fill(self):
        self.broker.partial=True;self.broker.cancel_final='5'
        result=self.trading.limit_order('BIYA','2.28','2','Buy',kill=True)
        self.assertEqual((result['status'],result['filledQty']),('PartialWithdrawal','1'))
        self.assertEqual(len(self.writes('withdraw')),1)

    def test_fill_race_is_reported_filled_not_canceled(self):
        self.broker.fill_on_cancel=True
        result=self.trading.limit_order('BIYA','2.28','2','Buy',kill=True)
        self.assertEqual((result['status'],result['filledQty']),('FilledStatus','2'))

    def test_unconfirmed_cancel_is_not_repeated(self):
        row=raw_order();self.broker.orders[row['order_no']]=row
        self.broker.cancel_final=None
        for _ in range(2):
            with self.assertRaises(OrderUncertain):self.trading.cancel_order(row['order_no'])
        self.assertEqual(len(self.writes('withdraw')),1)

    def test_terminal_cancel_is_noop(self):
        row=raw_order(order_status='8',order_status_desc='全部成交',exec_qty='2')
        self.broker.orders[row['order_no']]=row
        result=self.trading.cancel_order(row['order_no'])
        self.assertFalse(result['canceled']);self.assertEqual(self.writes('withdraw'),[])

    def test_non_cancelable_does_not_send(self):
        row=raw_order(enable_cancel='0');self.broker.orders[row['order_no']]=row
        with self.assertRaises(OrderUncertain):self.trading.cancel_order(row['order_no'])
        self.assertEqual(self.writes('withdraw'),[])

    def test_sell_availability_prevents_shorting(self):
        with self.assertRaises(ValueError):self.trading.limit_order('BIYA','2.28','9','Sell')
        self.assertEqual(self.writes('submit'),[])

    def test_fractional_quantity_rejected(self):
        with self.assertRaises(ValueError):self.trading.limit_order('BIYA','2.28','1.5','Buy')
        self.assertEqual(self.writes('submit'),[])

    def test_regular_market_and_extended_market_conversion(self):
        for session,expected in [('regular','MO'),('premarket','LO'),('afterhours','LO')]:
            with patch('fangde.broker.market_session',return_value=session):
                result=self.trading.market_order('BIYA','2','Buy',dry_run=True)
            self.assertEqual(result['ticket']['order_type'],expected)
            self.assertEqual(result['ticket']['_reference_price'],'2.29')

    def test_stale_book_blocks_submission(self):
        self.books.price=lambda *args:(_ for _ in ()).throw(ValueError('stale book'))
        with self.assertRaises(ValueError):self.trading.limit_order('BIYA','2.28','2','Buy')
        self.assertEqual(self.writes('submit'),[])

    def test_holdings_and_order_snapshot_mapping(self):
        row=raw_order();self.broker.orders[row['order_no']]=row
        result=self.trading.query()
        self.assertEqual(result['positions'][0]['cost'],'2')
        self.assertEqual(result['orders'][0]['status'],'NEW')
        self.assertEqual(result['orderStatusErrors'],0)

    def test_strategy_status_does_not_use_normal_order_enum(self):
        self.assertEqual(normalize_order(raw_order(order_type='3',order_status='3',order_status_desc='已撤单'),self.settings)['status'],'CanceledStatus')
        self.assertTrue(normalize_order(raw_order(order_type='3',order_status='3',order_status_desc='unrecognized'),self.settings)['status'].startswith('UNKNOWN'))
        self.assertEqual(normalize_order(raw_order(order_type='3',order_status='0',order_status_desc='待触发'),self.settings)['status'],'NotReported')

    def test_broker_timezone_matches_local_timestamp(self):
        first=normalize_order(raw_order(),self.settings)['submitted_at']
        second=normalize_order(raw_order(local_create_time=''),self.settings)['submitted_at']
        self.assertEqual(first,second);self.assertEqual(first,'2026-10-27T00:11:00+00:00')

    def test_history_reads_all_pages_and_rejects_duplicates(self):
        self.broker.history=[{'list':[raw_order(order_no=str(n))], 'page_info':{'page':str(n),'page_sum':'2'}} for n in (1,2)]
        self.assertEqual(len(self.trading.history_orders('2026-10-01','2026-10-26')['orders']),2)
        self.broker.history[1]['list'][0]['order_no']='1'
        with self.assertRaises(ValueError):self.trading.history_orders('2026-10-01','2026-10-26')

    def test_calendar_holiday_dst_earlyclose_and_overnight(self):
        for stamp in ('2026-07-03T15:00:00+00:00','2026-10-27T00:01:00+00:00'):
            with self.assertRaises(ValueError):market_session(datetime.fromisoformat(stamp))
        self.assertEqual(market_session(datetime.fromisoformat('2026-10-26T13:31:00+00:00')),'regular')
        self.assertEqual(market_session(datetime.fromisoformat('2026-11-02T14:31:00+00:00')),'regular')
        self.assertEqual(market_session(datetime.fromisoformat('2026-11-27T18:01:00+00:00')),'afterhours')
