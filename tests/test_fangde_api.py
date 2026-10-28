import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from fangde.api import (ApiClient, TradingApiClient, AuthenticationRequired,
                        TransportError, ApiError, save_session)
from fangde.config import Settings, DEFAULTS
from fangde.session_import import import_export, inspect_export
from fangde.session_export import account_binding, select_trade_token, export
from fangde.legacy_holdings import FangdeError, SIGNING_SUFFIX
from fangde import cli


def state():
    return {'version':2,'identity':{'account_id':'TEST','subject_sha256':'a'*64},
        'session':{'token':'synthetic-login-token','trade_token':'synthetic-trading-token'},
        'headers':{'Version':'7.7.1','Device-Id':'synthetic-device'}}


class Response(io.BytesIO):
    pass


class Opener:
    def __init__(self,data=None,error=None):
        self.data=data if data is not None else {'status':0,'data':{}}
        self.error=error; self.calls=[]
    def open(self,req,**kwargs):
        self.calls.append(req)
        if self.error:raise self.error
        return Response(json.dumps(self.data).encode())


class TransportTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        root=Path(temp.name);data=copy.deepcopy(DEFAULTS)
        data['fangde']['accountId']='TEST';data['api']['subjectSha256']='a'*64
        self.settings=Settings(data,root/'config.json',root)
        save_session(self.settings.session_path,state())
        self.opener=Opener();self.client=TradingApiClient(self.settings,opener=self.opener)
        self.body={'counter_id':'ST/US/BIYA','action':'1','order_type':'LO',
                   'price':'2.28','quantity':'2','_reference_price':'2.29','force_only_rth':'2'}

    def test_account_label_or_subject_mismatch_stops_before_network(self):
        for field,value in [('account_id','OTHER'),('subject_sha256','b'*64)]:
            wrong=state();wrong['identity'][field]=value
            save_session(self.settings.session_path,wrong)
            with self.assertRaises(AuthenticationRequired):self.client.read('positions')
        self.assertEqual(self.opener.calls,[])

    def test_today_get_has_current_signature_and_no_trade_token(self):
        with patch('fangde.api.time.time',return_value=1234):self.client.read('orders')
        request=self.opener.calls[0]; fields=parse_qs(urlsplit(request.full_url).query)
        self.assertEqual(request.method,'GET');self.assertEqual(fields['bs_flag'],['B,S'])
        self.assertNotIn('tradeToken',fields)
        expected=hashlib.md5(('synthetic-login-token1234'+SIGNING_SUFFIX).encode()).hexdigest()
        self.assertEqual(fields['_sign'],[expected])

    def test_limit_wire_matches_app_form_and_exact_id(self):
        self.opener.data={'status':'0','data':{'order_no':'9007199254740993123'}}
        sent=Mock();result=self.client.write('submit',self.body,before_send=sent)
        sent.assert_called_once();self.assertEqual(result['order_id_str'],'9007199254740993123')
        request=self.opener.calls[0];fields=parse_qs(request.data.decode(),keep_blank_values=True)
        self.assertEqual(request.method,'POST')
        self.assertEqual(urlsplit(request.full_url).path,'/trade/index/orderActionAdd')
        expected={'symbol':'BIYA','exchange':'US','bs_flag':'B','order_type':'PL','qty':'2','price':'2.28',
            'market_price':'2.29','duration_type':'DayOrder','valid_end_date':'','extended_trading':'1',
            'coupon_id':'','need_transfer':'','token':'synthetic-login-token','tradeToken':'synthetic-trading-token'}
        self.assertEqual({k:v[0] for k,v in fields.items() if k not in ('_t','_sign')},expected)

    def test_regular_market_wire_and_cancel_id(self):
        self.opener.data={'status':0,'data':{'order_no':'ticket-1'}}
        self.client.write('submit',dict(self.body,order_type='MO',force_only_rth='1'),before_send=lambda:None)
        fields=parse_qs(self.opener.calls[-1].data.decode())
        self.assertEqual((fields['order_type'],fields['price'],fields['extended_trading']),(['M'],['2.29'],['0']))
        self.client.write('withdraw',{'order_id':'ticket-1'},before_send=lambda:None)
        self.assertEqual(parse_qs(self.opener.calls[-1].data.decode())['order_no'],['ticket-1'])

    def test_invalid_order_values_fail_before_callback(self):
        for changes in ({'quantity':'1.5'},{'quantity':'NaN'},{'action':'short'},
                        {'counter_id':'ST/HK/0005'},{'order_type':'MO','force_only_rth':'2'}):
            sent=Mock()
            with self.assertRaises(ValueError):self.client.write('submit',dict(self.body,**changes),before_send=sent)
            sent.assert_not_called()
        self.assertEqual(self.opener.calls,[])

    def test_authentication_rejection_and_error_messages_do_not_leak_tokens(self):
        for data,error_type in [({'status':1001,'info':'synthetic-login-token'},AuthenticationRequired),
                                ({'status':8,'info':'bad token synthetic-login-token'},AuthenticationRequired),
                                ({'status':9,'info':'Insufficient funds','echo':'synthetic-trading-token'},ApiError)]:
            self.opener.data=data
            with self.assertRaises(error_type) as caught:self.client.read('positions')
            self.assertNotIn('synthetic-',str(caught.exception))

    def test_timeout_write_is_single_send(self):
        self.opener.error=URLError('synthetic-login-token')
        with self.assertRaises(TransportError) as caught:self.client.write('submit',self.body,before_send=lambda:None)
        self.assertEqual(len(self.opener.calls),1);self.assertNotIn('synthetic-',str(caught.exception))

    def test_authentication_scope_uses_broker_code_without_localized_message(self):
        for code,scope in [(1001,'login'),(1101,'trade'),(1102,'trade'),(1107,'unknown'),(1401,'unknown')]:
            self.opener.data={'status':str(code),'info':'','data':None}
            with self.assertRaises(AuthenticationRequired) as caught:self.client.read('positions')
            self.assertEqual((caught.exception.scope,caught.exception.api_code),(scope,code))

    def test_numeric_ack_is_not_coerced_to_order_id(self):
        self.opener.data={'status':0,'data':{'order_no':9007199254740993123}}
        with self.assertRaises(TransportError):self.client.write('submit',self.body,before_send=lambda:None)

    def test_export_import_binding_and_redacted_inspection(self):
        source=self.settings.root/'export.json';save_session(source,state())
        result=inspect_export(source)
        self.assertTrue(result['has_trading_session']);self.assertNotIn('synthetic-',json.dumps(result))
        self.assertTrue(import_export(self.settings,source)['ok'])
        other=state();other['identity']['account_id']='OTHER';save_session(source,other)
        with self.assertRaises(AuthenticationRequired):import_export(self.settings,source)

    def test_mmkv_selection_is_account_specific_and_binding_stable_across_renewal(self):
        data={'SUID':'person1','person1_TRADE_TOKEN':'trade1','person2_TRADE_TOKEN':'other'}
        self.assertEqual(select_trade_token(data),'trade1')
        first=account_binding(data);data['person1_TRADE_TOKEN']='renewed'
        self.assertEqual(account_binding(data),first)
        data['person1_other_TRADE_TOKEN']='ambiguous'
        with self.assertRaises(FangdeError):account_binding(data)

    def test_export_refuses_newer_unreviewed_app_before_reading_session(self):
        with patch('fangde.session_export.subprocess.run',return_value=Mock(stdout=b'versionName=8.0.0')) as run:
            with self.assertRaises(FangdeError):export('adb','device',self.settings.root/'export.json','TEST')
            self.assertEqual(run.call_count,1)

    def test_cli_mutations_default_to_preview(self):
        save_session(self.settings.path,{'fangde':{'accountId':'TEST'}})
        cases=[(['market-order','--symbol','BIYA','--quantity','2','--side','Buy'],'market_order'),
               (['limit-order','--symbol','BIYA','--quantity','2','--price','2.28','--side','Buy'],'limit_order'),
               (['cancel-all'],'cancel_all'),(['cancel-order','ticket-1'],'cancel_order')]
        for args,method in cases:
            for execute in (False,True):
                fake=Mock();getattr(fake,method).return_value={}
                with patch('fangde.cli._automation',return_value=fake),patch('fangde.cli._json'):
                    code=cli.main(['--config',str(self.settings.path),*args,*(['--execute'] if execute else [])])
                self.assertEqual(code,0);self.assertEqual(getattr(fake,method).call_args.kwargs['dry_run'],not execute)
