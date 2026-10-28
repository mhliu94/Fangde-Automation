"""Validate and atomically install a phone export for one configured account."""
from .api import load_session, save_session, AuthenticationRequired
from .lock import EmulatorLock


def inspect_export(path):
    state = load_session(path)
    if 'authentication' in state:
        from .authentication import validate_profile
        validate_profile(state)
    return {**state['identity'], 'exported_at': state.get('exported_at'),
            'has_login': bool(state['session'].get('token')),
            'has_trading_session': bool(state['session'].get('trade_token')),
            'has_password_renewal': 'authentication' in state}


def import_export(settings, source):
    state = load_session(source)
    if 'authentication' in state:
        from .authentication import validate_profile
        validate_profile(state)
    expected = {'account_id': settings.require('fangde.accountId'),
                'subject_sha256': settings.require('api.subjectSha256')}
    if state['identity'] != expected:
        raise AuthenticationRequired('Export identity does not match this server configuration.')
    # Same operation lock as orders: a replacement cannot change credentials
    # halfway through one trading workflow.
    with EmulatorLock(settings.work_dir / 'api/operation.lock', 300):
        with EmulatorLock(type(settings.session_path)(str(settings.session_path)+'.lock'),300):
            if settings.session_path.exists() and load_session(settings.session_path)['identity'] != expected:
                raise AuthenticationRequired('Existing server session belongs to another account.')
            save_session(settings.session_path, state)
    return {'ok': True, 'account_id': expected['account_id'], 'sessionPath': str(settings.session_path)}
