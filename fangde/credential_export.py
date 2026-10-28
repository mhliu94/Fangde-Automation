"""Build an account-bound password profile from a verified local login capture."""
import hashlib
import hmac
import json
from pathlib import Path
from urllib.parse import parse_qs
from .api import AuthenticationRequired
from .authentication import LOGIN_PATH,LOGIN_HOST,validate_profile,select_user
from .legacy_holdings import SIGNING_SUFFIX


def build_profile(state,capture_path,trade_password):
    records=[]
    try:
        for line in Path(capture_path).read_text(encoding='utf-8-sig').splitlines():
            row=json.loads(line)
            if row.get('host')==LOGIN_HOST.removeprefix('https://') and row.get('path')==LOGIN_PATH:
                records.append(row)
    except (OSError,ValueError,AttributeError):
        raise AuthenticationRequired('Cannot read the private password-login capture.') from None
    if not records:
        raise AuthenticationRequired('No supported password-login capture was found.')
    row=records[-1]
    try:
        response=json.loads(row['response_body'])
        parsed=parse_qs(row['request_body'],keep_blank_values=True,strict_parsing=True)
        if any(len(v)!=1 for v in parsed.values()):raise ValueError()
        fields={k:v[0] for k,v in parsed.items()}
        if row['response_status']!=200 or response['status'] not in (0,'0') or row['method']!='POST':raise ValueError()
        users=response['data']['list']
        if not isinstance(users,list):raise ValueError()
    except (KeyError,TypeError,ValueError):
        raise AuthenticationRequired('Latest password-login capture is not a successful supported exchange.') from None
    matches=[]
    for user in users:
        if not isinstance(user,dict):continue
        subject={k:str(user.get(k,'')) for k in ('suid','broker','trade_account')}
        for key in (subject['suid']+'_TRADE_TOKEN','_'.join((*subject.values(),'TRADE_TOKEN'))):
            digest=hashlib.sha256(('fangde\0'+subject['suid']+'\0'+key).encode()).hexdigest()
            if digest==state['identity']['subject_sha256'] and user.get('token')==state['session']['token']:
                matches.append({**subject,'trade_key':key})
    if len(matches)!=1:
        raise AuthenticationRequired('Login capture does not match the active exported account and token.')
    device_headers={k.lower():v for k,v in row['request_headers'].items()}
    if device_headers.get('device-id')!=state['headers'].get('Device-Id'):
        raise AuthenticationRequired('Login capture belongs to a different device profile.')
    seed=row.get('signing_seed')
    if fields.get('phone_rsa'):
        if not isinstance(seed,str):raise AuthenticationRequired('Captured phone login has no signing identity.')
        expected=hashlib.md5((seed+fields.get('_t','')+SIGNING_SUFFIX).encode()).hexdigest()
        if not hmac.compare_digest(expected,fields.get('_sign','')):
            raise AuthenticationRequired('Captured login signature does not match the signing identity.')
    elif seed is not None or '_t' in fields or '_sign' in fields:
        raise AuthenticationRequired('Unsupported email-login signature.')
    fields.pop('_t',None);fields.pop('_sign',None)
    profile={'version':1,'subject':matches[0],
             'login':{'fields':fields,'signing_seed':seed},'trade_password':trade_password}
    validate_profile({**state,'authentication':profile})
    select_user(response['data'],profile['subject'])
    return profile
