"""Add password renewal to one phone export using local, masked password prompts."""
import argparse
import base64
import copy
import getpass
import hashlib
from pathlib import Path
import re
import sys
import tempfile

from .api import ApiClient,AuthenticationRequired,load_session,save_session
from .authentication import (LOGIN_HOST,LOGIN_PATH,PASSWORD_PREFIX,PasswordAuthentication,
                             require_no_challenge,select_user,token_value,validate_profile)
from .authentication import account_digest
from .config import Settings,DEFAULTS
from .lock import EmulatorLock

# Production public key from Fangde 7.7.1's FDEncryptionHelper. This is not a secret.
PHONE_PUBLIC_KEY=b'''-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwLWt55mgZRE3o+lpB6dc
3B0WZsU/aDg14xbz8DGxwe7PnHKrbYaLLOcO7bsKgPvmyPKB6mL7EK30vIU/KSp1
wipCsNTj1aRXCy+KmFce6S/JWA0dji+RplFrc3Qc1shDAqjNAtlxwS+1en5R7wWn
2JB9O6hyiaZzD3xUbqbJTmb0UeEA4ppWp48RyzH2nDO0F4OPk9DQm6P4xSf0Mqrx
owHNb14OpVOh/o+cyfDGwjIvCJkklRXvMEoi4lSworqtMLKaW0Y9OLvYtVS6K4sG
90Aibf3SqhWhxYC9/3UVHP0wtn9J5HgOlNSx2EHP54/1O3XpQSASiW+1VV1c3sDW
GQIDAQAB
-----END PUBLIC KEY-----'''


def encode_password(value):
    if not isinstance(value,str) or not value or len(value)>1024:
        raise ValueError('Enter a nonempty password of at most 1024 characters.')
    return base64.encodebytes(PASSWORD_PREFIX+value.encode('utf-8')).decode('ascii')


def phone_cipher(phone):
    if not re.fullmatch(r'\+?\d{5,20}',phone):
        raise ValueError('Enter the phone number without spaces.')
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
        from cryptography.hazmat.primitives.asymmetric import padding
    except ImportError:
        raise ValueError("Phone setup needs the bootstrap extra: pip install -e '.[bootstrap]'.") from None
    key=load_pem_public_key(PHONE_PUBLIC_KEY)
    return base64.b64encode(key.encrypt(phone.encode(),padding.PKCS1v15())).decode('ascii')


def login_fields(identifier,region,password):
    if '@' in identifier:
        if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+',identifier):
            raise ValueError('Enter a valid login email address.')
        return {'email':identifier,'password':encode_password(password)},None
    if not re.fullmatch(r'[A-Z]{2}',region):raise ValueError('Enter the two-letter login country/region code, such as CN, HK or US.')
    return {'phone_rsa':phone_cipher(identifier),'region':region,'password':encode_password(password)},identifier


def bound_subject(data,binding,expected=None):
    if not isinstance(data,dict) or not isinstance(data.get('list'),list):
        raise AuthenticationRequired('Password login returned no supported account list.')
    require_no_challenge(data)
    matches=[]
    for user in data['list']:
        if not isinstance(user,dict):continue
        if any(type(user.get(k)) not in (str,int) for k in ('suid','broker','trade_account')):continue
        subject={k:str(user[k]) for k in ('suid','broker','trade_account')}
        if expected is not None and any(subject[k]!=expected[k] for k in subject):continue
        if expected is not None and account_digest(subject)==binding:
            matches.append({**subject,'trade_key':expected['trade_key']})
            continue
        for key in (subject['suid']+'_TRADE_TOKEN','_'.join((*subject.values(),'TRADE_TOKEN'))):
            if hashlib.sha256(('fangde\0'+subject['suid']+'\0'+key).encode()).hexdigest()==binding:
                matches.append({**subject,'trade_key':key})
    if len(matches)!=1:raise AuthenticationRequired('These credentials do not identify exactly the exported brokerage account.')
    select_user(data,matches[0])
    return matches[0]


def configure_passwords(session_path,identifier,region,login_password,trade_password,*,opener=None):
    session_path=Path(session_path)
    fields,seed=login_fields(identifier,region,login_password)
    encoded_trade=encode_password(trade_password)
    with EmulatorLock(Path(str(session_path)+'.lock'),300):
        original=load_session(session_path)
        config=copy.deepcopy(DEFAULTS)
        config['fangde']['accountId']=original['identity']['account_id']
        config['api']['subjectSha256']=original['identity']['subject_sha256']
        # Stage privately beside the export and replace only after a holdings check.
        with tempfile.TemporaryDirectory(prefix='.fangde-password-setup-',dir=session_path.parent) as directory:
            settings=Settings(config,Path(directory)/'config.json',Path(directory))
            auth=PasswordAuthentication(settings,opener)
            if 'authentication' in original:
                expected=validate_profile(original)['subject']
            else:
                # A user can have multiple brokerage accounts sharing one global
                # trade-token key. Bind to the account selected on the phone.
                info=ApiClient(settings,opener=opener)._send(original,'POST','/user/index/info',{'need_source':'0'}).payload['data']
                expected=bound_subject({'list':[info]},original['identity']['subject_sha256'])
            data=auth._post(original,LOGIN_HOST,LOGIN_PATH,fields,seed)
            subject=bound_subject(data,original['identity']['subject_sha256'],expected)
            state=copy.deepcopy(original)
            state['authentication']={'version':1,'subject':subject,'login':{'fields':fields,'signing_seed':seed},
                                     'trade_password':encoded_trade}
            state['version']=3
            state['identity']['subject_sha256']=account_digest(subject)
            config['api']['subjectSha256']=state['identity']['subject_sha256']
            validate_profile(state)
            state['session']={'token':token_value(select_user(data,subject).get('token')),'trade_token':''}
            save_session(settings.session_path,state)
            auth.renew(state,'trade')
            ApiClient(settings,opener=opener).read('positions')
            save_session(session_path,load_session(settings.session_path))
    return {'ok':True,'account_id':original['identity']['account_id'],
            'subject_sha256':state['identity']['subject_sha256'],'password_renewal':True,'holdings_verified':True}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session',required=True,type=Path)
    args=parser.parse_args(argv)
    try:
        state=load_session(args.session)
        print('Account: '+state['identity']['account_id'])
        print('Enter credentials locally. Passwords will not be echoed.')
        identifier=input('Login phone number or email: ').strip()
        region=input('Login country/region code (CN, HK, US, etc.): ').strip().upper() if '@' not in identifier else ''
        login_password=getpass.getpass('Login password: ')
        trade_password=getpass.getpass('Trading password: ')
        result=configure_passwords(args.session,identifier,region,login_password,trade_password)
    except (AuthenticationRequired,ValueError) as error:
        print('Error: '+str(error),file=sys.stderr);return 2
    except (OSError,EOFError,KeyboardInterrupt):
        print('Password setup did not complete.',file=sys.stderr);return 2
    print('Saved verified password renewal for '+result['account_id']+'. Credentials were not printed.')
    print('Use this api.subjectSha256 on the matching server: '+result['subject_sha256'])
    return 0

if __name__=='__main__':raise SystemExit(main())
