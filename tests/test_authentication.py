import base64
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock,patch
from urllib.error import URLError
from urllib.parse import parse_qs,urlsplit

from fangde.api import ApiClient,TradingApiClient,AuthenticationRequired,TransportError,load_session,save_session
from fangde.authentication import PASSWORD_PREFIX,LOGIN_PATH,UNLOCK_PATH,validate_profile,account_digest
from fangde.config import DEFAULTS,Settings
from fangde.session_import import inspect_export
from fangde.password_setup import configure_passwords,phone_cipher
from fangde.credential_export import build_profile

PORTFOLIO='/trade/index/portfolio'
ADD='/trade/index/orderActionAdd'
ACCOUNTS='/user/index/account_list'

def encoded(value):return base64.encodebytes(PASSWORD_PREFIX+value.encode()).decode()

def state():
    subject={'suid':'123','broker':'FD','trade_account':'TEST1','trade_key':'123_FD_TEST1_TRADE_TOKEN'}
    binding=hashlib.sha256(('fangde\0'+subject['suid']+'\0'+subject['trade_key']).encode()).hexdigest()
    return {'version':2,'identity':{'account_id':'TEST','subject_sha256':binding},
        'session':{'token':'old-login','trade_token':'old-trade'},'headers':{'Version':'7.7.1','Device-Id':'fake-device'},
        'authentication':{'version':1,'subject':subject,'login':{'fields':{'phone_rsa':'synthetic-rsa',
            'region':'CN','password':encoded('fake-login-password')},'signing_seed':'13800000000'},
            'trade_password':encoded('fake-trade-password')}}

def login(user=None):
    return {'status':0,'data':{'index':0,'list':[user or {'suid':123,'broker':'FD','trade_account':'TEST1','token':'new-login'}]}}

def unlock():return {'status':0,'data':{'tradeToken':'new-trade','random_code':''}}
def ok(data=None):return {'status':0,'data':{} if data is None else data}

class Sequence:
    def __init__(self,*items):self.items=list(items);self.calls=[]
    def open(self,request,**kwargs):
        self.calls.append(request)
        if not self.items:raise AssertionError('Unexpected extra request')
        path,result=self.items.pop(0)
        if urlsplit(request.full_url).path!=path:raise AssertionError('Unexpected endpoint')
        if isinstance(result,Exception):raise result
        return io.BytesIO(json.dumps(result).encode())

class AuthenticationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        root=Path(temporary.name);data=copy.deepcopy(DEFAULTS);self.state=state()
        data['fangde']['accountId']='TEST';data['api']['subjectSha256']=self.state['identity']['subject_sha256']
        self.settings=Settings(data,root/'config.json',root)
        save_session(self.settings.session_path,self.state)

    def client(self,*sequence):
        self.opener=Sequence(*sequence)
        return TradingApiClient(self.settings,opener=self.opener)

    def test_logged_off_read_restores_login_and_trade_with_fresh_signature(self):
        client=self.client((PORTFOLIO,{'status':1001}),(LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        with patch('fangde.authentication.time.time',return_value=2345):result=client.read('positions')
        self.assertTrue(result.ok)
        saved=load_session(self.settings.session_path)
        self.assertEqual(saved['session'],{'token':'new-login','trade_token':'new-trade'})
        login_request=self.opener.calls[1]
        self.assertEqual(urlsplit(login_request.full_url).hostname,'api-apimsg.techfx88.com')
        form=parse_qs(login_request.data.decode())
        self.assertEqual(form['_t'],['2345']);self.assertNotIn('token',form)
        self.assertEqual(form['_sign'],[hashlib.md5(b'138000000002345U7KK2GV5GH68').hexdigest()])
        self.assertNotIn('fake-',json.dumps(inspect_export(self.settings.session_path)))

    def test_locked_trade_renews_without_login(self):
        client=self.client((PORTFOLIO,{'status':1102}),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        self.assertTrue(client.read('positions').ok)
        self.assertEqual(load_session(self.settings.session_path)['session']['token'],'old-login')

    def test_empty_tokens_restore_both(self):
        self.state['session']={'token':'','trade_token':''};save_session(self.settings.session_path,self.state)
        client=self.client((LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        self.assertTrue(client.read('positions').ok)

    def test_missing_trade_checks_main_session_before_unlock(self):
        self.state['session']['trade_token']='';save_session(self.settings.session_path,self.state)
        client=self.client((ACCOUNTS,{'status':1001}),(LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        self.assertTrue(client.read('positions').ok)

    def test_bad_password_does_not_repeat_across_calls_or_clients(self):
        client=self.client((PORTFOLIO,{'status':1101}),(UNLOCK_PATH,{'status':1113,'info':'fake-trade-password'}),
                           (PORTFOLIO,{'status':1101}))
        with self.assertRaises(AuthenticationRequired) as caught:client.read('positions')
        self.assertNotIn('fake-',str(caught.exception))
        with self.assertRaises(AuthenticationRequired):ApiClient(self.settings,opener=self.opener).read('positions')
        self.assertEqual(len(self.opener.calls),3)
        self.assertTrue(load_session(self.settings.session_path)['authentication']['blocked']['trade'])

    def test_login_network_failure_persists_attempt_and_never_echoes_exception(self):
        client=self.client((PORTFOLIO,{'status':1001}),(LOGIN_PATH,URLError('fake-login-password')))
        with self.assertRaises(AuthenticationRequired) as caught:client.read('positions')
        self.assertNotIn('fake-',str(caught.exception))
        self.assertTrue(load_session(self.settings.session_path)['authentication']['blocked']['login'])

    def test_different_account_response_keeps_original_tokens(self):
        wrong={'suid':987,'broker':'FD','trade_account':'OTHER','token':'wrong-token'}
        client=self.client((PORTFOLIO,{'status':1001}),(LOGIN_PATH,login(wrong)))
        with self.assertRaises(AuthenticationRequired):client.read('positions')
        self.assertEqual(load_session(self.settings.session_path)['session'],self.state['session'])

    def test_selects_exact_account_instead_of_response_index(self):
        response=login();response['data']['list'].insert(0,{'suid':456,'broker':'FD','trade_account':'OTHER','token':'wrong-token'})
        client=self.client((PORTFOLIO,{'status':1001}),(LOGIN_PATH,response),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        self.assertTrue(client.read('positions').ok)
        self.assertEqual(load_session(self.settings.session_path)['session']['token'],'new-login')

    def test_additional_verification_stops_without_followup_requests(self):
        challenge=unlock();challenge['data']['random_code']='synthetic-challenge'
        client=self.client((PORTFOLIO,{'status':1101}),(UNLOCK_PATH,challenge))
        with self.assertRaises(AuthenticationRequired):client.read('positions')
        self.assertEqual(len(self.opener.calls),2)
        self.assertEqual(load_session(self.settings.session_path)['session']['trade_token'],'old-trade')

    def test_profile_mismatch_stops_before_network(self):
        self.state['authentication']['subject']['trade_account']='OTHER';save_session(self.settings.session_path,self.state)
        client=self.client()
        with self.assertRaises(AuthenticationRequired):client.read('positions')
        self.assertEqual(self.opener.calls,[])

    def ticket(self):
        return {'counter_id':'ST/US/TEST','action':'1','order_type':'LO','price':'1',
                'quantity':'1','_reference_price':'1','force_only_rth':'1'}

    def test_write_recovers_before_send_then_never_replays_auth_rejection(self):
        client=self.client((PORTFOLIO,{'status':1101}),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()),(ADD,{'status':1001}))
        before=Mock()
        with self.assertRaises(AuthenticationRequired):client.write('submit',self.ticket(),before_send=before)
        before.assert_called_once()
        self.assertEqual(len(self.opener.calls),4)

    def test_failed_preflight_does_not_mark_or_dispatch_order(self):
        client=self.client((PORTFOLIO,{'status':1101}),(UNLOCK_PATH,{'status':1107}))
        before=Mock()
        with self.assertRaises(AuthenticationRequired):client.write('submit',self.ticket(),before_send=before)
        before.assert_not_called()
        self.assertEqual(len(self.opener.calls),2)

    def test_uncertain_order_is_never_replayed(self):
        client=self.client((PORTFOLIO,ok()),(ADD,URLError('lost response')))
        with self.assertRaises(TransportError):client.write('submit',self.ticket(),before_send=lambda:None)
        self.assertEqual(len(self.opener.calls),2)

    def test_authentication_attempts_are_bounded_on_repeated_expiry(self):
        client=self.client((PORTFOLIO,{'status':1001}),(LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,{'status':1001}))
        with self.assertRaises(AuthenticationRequired):client.read('positions')
        self.assertEqual(len(self.opener.calls),4)

    def test_explicit_retry_is_required_to_clear_interrupted_attempt(self):
        self.state['authentication']['blocked']={'login':True,'trade':True};save_session(self.settings.session_path,self.state)
        client=self.client((LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        with self.assertRaises(AuthenticationRequired):client.authenticate()
        self.assertEqual(self.opener.calls,[])
        self.assertTrue(client.authenticate(retry=True)['holdings_verified'])
        self.assertEqual(load_session(self.settings.session_path)['authentication']['blocked'],{})

    def test_email_login_has_no_phone_signature(self):
        profile=self.state['authentication'];profile['login']={'fields':{'email':'synthetic@example.invalid',
            'password':encoded('fake-login-password')},'signing_seed':None}
        save_session(self.settings.session_path,self.state)
        client=self.client((LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        client.authenticate()
        self.assertNotIn('_sign',parse_qs(self.opener.calls[0].data.decode()))

    def test_invalid_password_encoding_is_rejected_locally(self):
        for value in ('plain-password','!!!!',base64.b64encode(b'wrong-prefix').decode()):
            profile=state();profile['authentication']['trade_password']=value
            with self.assertRaises(AuthenticationRequired):validate_profile(profile)

    def test_concurrent_clients_renew_only_once(self):
        client=self.client((PORTFOLIO,{'status':1001}),(LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),
                           (PORTFOLIO,ok()),(PORTFOLIO,ok()))
        def read():return ApiClient(self.settings,opener=self.opener).read('positions').ok
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(lambda _:read(),range(2))),[True,True])
        self.assertEqual(len(self.opener.calls),5)

    def test_local_password_setup_verifies_reads_before_replacing_export(self):
        client=self.client((LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        with patch('fangde.password_setup.phone_cipher',return_value='fresh-cipher'):
            result=configure_passwords(self.settings.session_path,'13800000000','CN',
                                      'fake-login-password','fake-trade-password',opener=self.opener)
        self.assertTrue(result['holdings_verified'])
        saved=load_session(self.settings.session_path)
        self.assertEqual(saved['session']['token'],'new-login')
        self.assertEqual(saved['version'],3)
        self.assertEqual(saved['identity']['subject_sha256'],account_digest(saved['authentication']['subject']))

    def test_password_setup_wrong_account_preserves_export(self):
        wrong={'suid':987,'broker':'FD','trade_account':'OTHER','token':'wrong-token'}
        client=self.client((LOGIN_PATH,login(wrong)))
        before=self.settings.session_path.read_bytes()
        with patch('fangde.password_setup.phone_cipher',return_value='fresh-cipher'):
            with self.assertRaises(AuthenticationRequired):
                configure_passwords(self.settings.session_path,'13800000000','CN',
                                    'fake-login-password','fake-trade-password',opener=self.opener)
        self.assertEqual(self.settings.session_path.read_bytes(),before)

    def test_phone_encryption_uses_fresh_rsa_padding(self):
        first,second=phone_cipher('13800000000'),phone_cipher('13800000000')
        self.assertEqual(len(base64.b64decode(first)),256)
        self.assertNotEqual(first,second)

    def test_password_setup_matches_selected_broker_when_global_key_is_shared(self):
        subject=self.state['authentication']['subject'];subject['trade_key']='123_TRADE_TOKEN'
        binding=hashlib.sha256(b'fangde\0' + b'123\0' + b'123_TRADE_TOKEN').hexdigest()
        self.state['identity']['subject_sha256']=binding;save_session(self.settings.session_path,self.state)
        response=login();response['data']['list'].append({'suid':123,'broker':'OTHER','trade_account':'OTHER1','token':'wrong-token'})
        client=self.client((LOGIN_PATH,response),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        with patch('fangde.password_setup.phone_cipher',return_value='fresh-cipher'):
            result=configure_passwords(self.settings.session_path,'13800000000','CN',
                                      'fake-login-password','fake-trade-password',opener=self.opener)
        self.assertTrue(result['holdings_verified'])
        self.assertEqual(load_session(self.settings.session_path)['session']['token'],'new-login')

    def test_first_password_setup_reads_active_account_before_login(self):
        self.state.pop('authentication');save_session(self.settings.session_path,self.state)
        user=login()['data']['list'][0]
        client=self.client(('/user/index/info',ok(user)),(LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        with patch('fangde.password_setup.phone_cipher',return_value='fresh-cipher'):
            self.assertTrue(configure_passwords(self.settings.session_path,'13800000000','CN',
                'fake-login-password','fake-trade-password',opener=self.opener)['holdings_verified'])

    def test_capture_export_checks_current_token_and_signature(self):
        from urllib.parse import urlencode
        profile=self.state['authentication']
        form={**profile['login']['fields'],'_t':'1234',
              '_sign':hashlib.md5(b'138000000001234U7KK2GV5GH68').hexdigest()}
        response=login();response['data']['list'][0]['token']='old-login'
        row={'host':'api-apimsg.techfx88.com','path':LOGIN_PATH,'method':'POST',
             'response_status':200,'response_body':json.dumps(response),'request_body':urlencode(form),
             'request_headers':{'Device-Id':'fake-device'},'signing_seed':'13800000000'}
        path=self.settings.root/'capture.jsonl';path.write_text(json.dumps(row)+'\n')
        result=build_profile(self.state,path,profile['trade_password'])
        self.assertEqual(result['subject'],profile['subject'])
        row['signing_seed']='13900000000';path.write_text(json.dumps(row)+'\n')
        with self.assertRaises(AuthenticationRequired):build_profile(self.state,path,profile['trade_password'])

    def test_cancellation_auth_rejection_is_not_replayed(self):
        path='/trade/index/orderActionCancel'
        client=self.client((PORTFOLIO,ok()),(path,{'status':1102}))
        before=Mock()
        with self.assertRaises(AuthenticationRequired):client.write('withdraw',{'order_id':'synthetic-order'},before_send=before)
        before.assert_called_once();self.assertEqual(len(self.opener.calls),2)

    def test_version_three_rejects_other_broker_sharing_a_global_trade_key(self):
        original=state();original['version']=3
        subject=original['authentication']['subject'];subject['trade_key']='123_TRADE_TOKEN'
        original['identity']['subject_sha256']=account_digest(subject)
        validate_profile(original)
        wrong=copy.deepcopy(original);wrong['authentication']['subject']['broker']='OTHER'
        with self.assertRaises(AuthenticationRequired):validate_profile(wrong)

    def test_version_three_password_setup_can_renew_existing_profile(self):
        self.state['version']=3
        self.state['identity']['subject_sha256']=account_digest(self.state['authentication']['subject'])
        save_session(self.settings.session_path,self.state)
        client=self.client((LOGIN_PATH,login()),(UNLOCK_PATH,unlock()),(PORTFOLIO,ok()))
        with patch('fangde.password_setup.phone_cipher',return_value='fresh-cipher'):
            result=configure_passwords(self.settings.session_path,'13800000000','CN',
                                      'fake-login-password','fake-trade-password',opener=self.opener)
        self.assertEqual(result['subject_sha256'],self.state['identity']['subject_sha256'])
