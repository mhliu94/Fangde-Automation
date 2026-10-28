"""Account-bound password login and trading unlock; no verification-code flow."""
from __future__ import annotations
import base64
import hashlib
import http.client
import json
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from . import legacy_holdings as fd
from .api import AuthenticationRequired, save_session

LOGIN_HOST = "https://api-apimsg.techfx88.com"
LOGIN_PATH = "/user/login/index"
TRADE_HOST = "https://apitrade.techfx88.com"
UNLOCK_PATH = "/user/trade/clientAuth"
PASSWORD_PREFIX = b"fdzqSH#2017|4|1"


def account_digest(subject):
    """Bind the specific brokerage account even when trade tokens are global."""
    return hashlib.sha256(('fangde-account\0'+'\0'.join(subject[k] for k in
                          ('suid','broker','trade_account'))).encode()).hexdigest()


def password_valid(value):
    if not isinstance(value,str) or not 1 <= len(value) <= 8192:
        return False
    try:
        decoded=base64.b64decode(''.join(value.split()),validate=True)
    except (ValueError,UnicodeError):
        return False
    return decoded.startswith(PASSWORD_PREFIX) and len(decoded)>len(PASSWORD_PREFIX)


def scalar(value):
    if type(value) not in (str,int):
        raise AuthenticationRequired("Authentication profile has an invalid account identifier.")
    return str(value)


def validate_profile(state):
    profile=state.get('authentication')
    if not isinstance(profile,dict) or profile.get('version')!=1:
        raise AuthenticationRequired("Export this account's password-login and trading-unlock credentials first.")
    subject=profile.get('subject')
    if not isinstance(subject,dict) or set(subject)!={'suid','broker','trade_account','trade_key'}:
        raise AuthenticationRequired("Authentication profile has no complete account binding.")
    for value in subject.values():
        if not isinstance(value,str) or not value or '\0' in value:
            raise AuthenticationRequired("Authentication profile has an invalid account binding.")
    key=subject['trade_key']
    if key not in (subject['suid']+'_TRADE_TOKEN',
                   '_'.join((subject['suid'],subject['broker'],subject['trade_account'],'TRADE_TOKEN'))):
        raise AuthenticationRequired("Authentication profile trading account does not match its binding.")
    binding=hashlib.sha256(('fangde\0'+subject['suid']+'\0'+key).encode()).hexdigest()
    if state.get('version')==3:binding=account_digest(subject)
    if binding!=state['identity']['subject_sha256']:
        raise AuthenticationRequired("Authentication credentials belong to a different account.")
    login=profile.get('login')
    if not isinstance(login,dict) or set(login)!={'fields','signing_seed'}:
        raise AuthenticationRequired("Invalid password-login profile.")
    fields=login['fields']
    if not isinstance(fields,dict) or set(fields)-{'phone_rsa','region','email','password'}:
        raise AuthenticationRequired("Unsupported password-login fields.")
    if not all(isinstance(v,str) and len(v)<=8192 for v in fields.values()):
        raise AuthenticationRequired("Invalid password-login field values.")
    if bool(fields.get('phone_rsa'))==bool(fields.get('email')):
        raise AuthenticationRequired("Password login requires one phone or email identity.")
    seed=login['signing_seed']
    if fields.get('phone_rsa'):
        if not isinstance(seed,str) or not re.fullmatch(r'\+?\d{5,20}',seed):
            raise AuthenticationRequired("Phone login has no verified signing identity.")
    elif seed is not None:
        raise AuthenticationRequired("Email login must not use a phone signing identity.")
    if not password_valid(fields.get('password')) or not password_valid(profile.get('trade_password')):
        raise AuthenticationRequired("Password credentials have an unsupported encoding.")
    return profile


def require_no_challenge(value):
    if any(value.get(k) not in (None,'',0,'0',False) for k in
           ('random_code','second_auth','need_second_auth','need_auth')):
        raise AuthenticationRequired("Password authentication did not complete; additional verification is outside scope.")


def select_user(data,subject):
    if not isinstance(data,dict) or not isinstance(data.get('list'),list):
        raise AuthenticationRequired("Password login returned an unsupported account response.")
    require_no_challenge(data)
    matches=[row for row in data['list'] if isinstance(row,dict) and all(
        scalar(row.get(key))==subject[key] for key in ('suid','broker','trade_account'))]
    if len(matches)!=1:
        raise AuthenticationRequired("Password login did not return exactly the configured brokerage account.")
    require_no_challenge(matches[0])
    return matches[0]


def token_value(value):
    if not isinstance(value,str) or not value or len(value)>8192 or any(ord(c)<33 for c in value):
        raise AuthenticationRequired("Authentication response did not contain a valid session token.")
    return value


class PasswordAuthentication:
    """Called while holding the session lock; persist before and after each attempt."""
    def __init__(self,settings,opener=None):
        self.settings,self.opener=settings,opener

    def _post(self,state,host,path,fields,seed):
        fields=dict(fields)
        if seed is not None:
            stamp=str(int(time.time()))
            fields.update(_t=stamp,_sign=hashlib.md5((seed+stamp+fd.SIGNING_SUFFIX).encode()).hexdigest())
        headers={'Accept':'application/json','User-Agent':'okhttp/4.12.0',**state.get('headers',{}),
                 'Content-Type':'application/x-www-form-urlencoded'}
        request=Request(host+path,data=urlencode(fields).encode('ascii'),headers=headers,method='POST')
        opener=self.opener or fd.Client(state).opener
        maximum=int(self.settings.get('api.maxResponseBytes',4194304))
        try:
            with opener.open(request,timeout=float(self.settings.get('api.requestTimeoutSeconds',15))) as response:
                raw=response.read(maximum+1)
        except HTTPError as error:
            status=error.code
            error.close()
            raise AuthenticationRequired(f"Password authentication failed with HTTP {status}; automatic attempts stopped.") from None
        except (URLError,TimeoutError,OSError,http.client.HTTPException):
            raise AuthenticationRequired("Password authentication had a network failure; automatic attempts stopped.") from None
        if len(raw)>maximum:
            raise AuthenticationRequired("Authentication response exceeded the size limit.")
        try:result=json.loads(raw)
        except (ValueError,UnicodeError):
            raise AuthenticationRequired("Authentication returned invalid JSON.") from None
        if not isinstance(result,dict) or type(result.get('status')) not in (int,str) or not re.fullmatch(r'-?\d+',str(result['status'])):
            raise AuthenticationRequired("Authentication returned an unsupported response.")
        code=int(result['status'])
        if code!=0:
            raise AuthenticationRequired(f"Password authentication was rejected (code {code}); automatic attempts stopped.",api_code=code)
        data=result.get('data')
        if not isinstance(data,dict):
            raise AuthenticationRequired("Authentication success response is missing data.")
        require_no_challenge(data)
        return data

    def renew(self,state,scope,*,retry=False):
        if scope not in ('login','trade'):
            raise AuthenticationRequired("This authentication failure cannot be renewed automatically.")
        profile=validate_profile(state)
        failures=profile.setdefault('blocked',{})
        if not isinstance(failures,dict):
            raise AuthenticationRequired("Invalid authentication attempt state.")
        if failures.get(scope) and not retry:
            raise AuthenticationRequired("An earlier password attempt failed or was interrupted; inspect credentials before session-authenticate --retry.",scope=scope)
        # A crash, rejected password, or uncertain response cannot create a retry loop.
        failures[scope]=True
        save_session(self.settings.session_path,state)
        if scope=='login':
            login=profile['login']
            data=self._post(state,LOGIN_HOST,LOGIN_PATH,login['fields'],login['signing_seed'])
            user=select_user(data,profile['subject'])
            token=token_value(user.get('token'))
            state['session'].update(token=token,trade_token='')
        else:
            token=token_value(state['session'].get('token'))
            data=self._post(state,TRADE_HOST,UNLOCK_PATH,{'token':token,'password':profile['trade_password']},token)
            state['session']['trade_token']=token_value(data.get('tradeToken'))
        failures.pop(scope,None)
        profile['last_'+scope+'_renewal_unix']=int(time.time())
        save_session(self.settings.session_path,state)
