"""One-file Linux setup; installation and local checks never contact a broker."""
from __future__ import annotations
import argparse
import copy
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import sys
import tempfile

from .api import ApiClient, SessionError, load_session, save_session
from .authentication import validate_profile
from .config import DEFAULTS, Settings, _merge, load_settings, project_root, validate_config
from .errors import ConfigurationError, SafetyError
from .lock import EmulatorLock
from .setup import render_service

FORMAT = 'fangde-deployment'
INSTALLATION = 'fangde-installation'


class DeploymentError(RuntimeError):
    """Credential-free setup failure."""


def private_json(path):
    path = Path(path).expanduser()
    try:
        if path.stat().st_size > 4 * 1024 * 1024:
            raise DeploymentError('Account/settings file exceeds the size limit.')
        if os.name == 'posix' and path.stat().st_mode & 0o077:
            raise DeploymentError('Account/settings file must be private; run chmod 600 on it.')
        value = json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        raise DeploymentError('Cannot read the account/settings JSON file.') from None
    if not isinstance(value, dict):
        raise DeploymentError('Account/settings JSON must contain an object.')
    return value


def credentials(value):
    # Reuse all session/header/profile checks without weakening file-mode checks.
    with tempfile.TemporaryDirectory(prefix='fangde-validate-') as temporary:
        path = Path(temporary) / 'session.json'
        save_session(path, value)
        state = load_session(path)
        if 'authentication' in state:
            validate_profile(state)
        return state


def read_account(path):
    value = private_json(path)
    if value.get('format') == FORMAT:
        if value.get('version') != 1 or set(value) - {'format', 'version', 'credentials', 'settings'}:
            raise DeploymentError('Unsupported deployment file format.')
        settings = value.get('settings', {})
        if not isinstance(settings, dict):
            raise DeploymentError('Deployment settings must be an object.')
        validate_config(settings)
        return credentials(value.get('credentials')), settings
    if 'format' in value:
        raise DeploymentError('Unsupported account file format.')
    return credentials(value), {}


def require_binding(settings, identity):
    for group, key, expected in (('fangde', 'accountId', identity['account_id']),
                                 ('api', 'subjectSha256', identity['subject_sha256'])):
        if settings.get(group, {}).get(key) not in (None, '', expected):
            raise DeploymentError('Settings belong to a different account.')


def require_kafka(address):
    if not isinstance(address, str) or not address:
        raise DeploymentError('Set settings.kafka.bootstrapServers in the deployment file, or use --kafka-bootstrap.')
    for broker in address.split(','):
        match = re.fullmatch(r'(?:[A-Za-z0-9_.-]+|\[[0-9A-Fa-f:]+\]):([0-9]{1,5})', broker)
        if not match or not 1 <= int(match[1]) <= 65535:
            raise DeploymentError('Kafka bootstrap servers must be host:port entries separated by commas.')


def bundle(session, output, *, settings_path=None, kafka_bootstrap=None):
    state, options = read_account(session)
    if settings_path:
        options = _merge(options, private_json(settings_path))
    if kafka_bootstrap:
        options.setdefault('kafka', {})['bootstrapServers'] = kafka_bootstrap
    validate_config(options)
    require_binding(options, state['identity'])
    require_kafka(options.get('kafka', {}).get('bootstrapServers'))
    output = Path(output).expanduser().absolute()
    if output.exists():
        raise DeploymentError('Deployment output already exists; choose another filename.')
    save_session(output, {'format': FORMAT, 'version': 1, 'credentials': state, 'settings': options})
    return {'ok': True, 'account_id': state['identity']['account_id'], 'file': str(output)}


def private_directory(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.name == 'posix':
        path.chmod(0o700)


def installation_pointer(root):
    path = root / 'config/deployment.json'
    if not path.exists():
        return None
    value = private_json(path)
    if value.get('format') != INSTALLATION or not isinstance(value.get('config'), str):
        raise DeploymentError('Invalid installation pointer.')
    config = Path(value['config'])
    if not config.is_absolute():
        raise DeploymentError('Installation config path must be absolute.')
    return config.resolve()


def normalize_paths(options, source_dir):
    for group, key in (('monitor', 'certFile'), ('monitor', 'keyFile')):
        value = options.get(group, {}).get(key)
        if value:
            options[group][key] = str((source_dir / Path(value).expanduser()).resolve())
    client = options.get('kafka', {}).get('clientConfig', {})
    for key in ('ssl.ca.location', 'ssl.certificate.location', 'ssl.key.location', 'sasl.kerberos.keytab'):
        if client.get(key):
            client[key] = str((source_dir / Path(client[key]).expanduser()).resolve())


def install(account_file, **options):
    root = Path(options.pop('root', None) or project_root()).resolve()
    with EmulatorLock(root / 'work/deployment-install.lock', 30):
        return _install(account_file, root=root, **options)


def _install(account_file, *, state_dir=None, settings_path=None, kafka_bootstrap=None,
            refresh_session=False, update_settings=False, systemd=False, root=None):
    root = Path(root or project_root()).resolve()
    state, options = read_account(account_file)
    if settings_path:
        extra = private_json(settings_path)
        normalize_paths(extra, Path(settings_path).expanduser().resolve().parent)
        options = _merge(options, extra)
    normalize_paths(options, Path(account_file).expanduser().resolve().parent)
    if kafka_bootstrap:
        options.setdefault('kafka', {})['bootstrapServers'] = kafka_bootstrap
    validate_config(options)
    require_binding(options, state['identity'])
    identity = state['identity']
    selected = installation_pointer(root)
    if selected and private_json(selected).get('fangde', {}).get('accountId') != identity['account_id']:
        raise DeploymentError('This checkout is assigned to another account. Use a separate checkout.')
    if not selected and (root / 'config/fangde.config.json').exists():
        raise DeploymentError('This checkout has a manual config. Use a fresh checkout for managed installation.')
    if state_dir:
        directory = Path(state_dir).expanduser().resolve()
    elif selected:
        directory = selected.parent
    else:
        base = Path(os.environ.get('XDG_STATE_HOME') or Path.home() / '.local/state').expanduser()
        directory = (base / 'fangde' / ('account-' + identity['account_id'])).resolve()
    if directory.is_relative_to(root):
        raise DeploymentError('Keep the account state directory outside the repository checkout.')
    config_path = directory / 'server.config.json'
    if selected and selected != config_path:
        raise DeploymentError('This checkout already points to another state directory. Preserve that directory when updating.')
    marker_path = directory / 'deployment.json'
    if directory.exists() and not marker_path.exists() and any(p.name != '.install.lock' for p in directory.iterdir()):
        raise DeploymentError('State directory is not an existing Fangde installation or an empty directory.')
    private_directory(directory)
    with EmulatorLock(directory / '.install.lock', 30):
        if marker_path.exists():
            marker = private_json(marker_path)
            if marker.get('format') != INSTALLATION or marker.get('identity') != identity:
                raise DeploymentError('State directory belongs to another account.')
        existing = private_json(config_path) if config_path.exists() else None
        if existing:
            validate_config(existing)
            require_binding(existing, identity)
        if existing and not update_settings:
            config = existing
            if kafka_bootstrap and kafka_bootstrap != config.get('kafka', {}).get('bootstrapServers'):
                raise DeploymentError('Use --update-settings to change an installed Kafka address.')
        else:
            config = _merge(existing or {}, options)
        config = copy.deepcopy(config)
        config.setdefault('fangde', {}).update(accountId=identity['account_id'], backend='api')
        config['fangde'].setdefault('tradingEnabledDefault', False)
        config['paths'] = {'workDir': str(directory / 'work'), 'logDir': str(directory / 'logs')}
        config.setdefault('api', {}).update(subjectSha256=identity['subject_sha256'], sessionPath=str(directory / 'session.json'))
        config.setdefault('kafka', {}).setdefault('consumerGroupId', 'fangde-' + identity['account_id'])
        if not config['kafka']['consumerGroupId']:
            config['kafka']['consumerGroupId'] = 'fangde-' + identity['account_id']
        config.setdefault('service', {})['name'] = 'fangde-' + identity['account_id'] + '-runner'
        validate_config(config)
        require_kafka(config['kafka'].get('bootstrapServers'))
        settings = Settings(_merge(DEFAULTS, config), config_path, root)
        session_path = directory / 'session.json'
        if session_path.exists():
            current = load_session(session_path)
            if current['identity'] != identity:
                raise DeploymentError('Installed session belongs to another account.')
            if 'authentication' in current and not refresh_session:
                validate_profile(current)
        # The runner holds runner.lock for its entire lifetime, including recovery.
        # Taking it with timeout zero prevents changing a running account.
        private_directory(directory / 'work')
        private_directory(directory / 'logs')
        with EmulatorLock(settings.work_dir / 'runner.lock', 0):
            with EmulatorLock(settings.work_dir / 'api/operation.lock', 30):
                with EmulatorLock(Path(str(session_path) + '.lock'), 30):
                    save_session(marker_path, {'format': INSTALLATION, 'version': 1, 'identity': identity})
                    replaced = not session_path.exists() or refresh_session
                    if replaced:
                        save_session(session_path, state)
                    save_session(config_path, config)
                    save_session(root / 'config/deployment.json', {'format': INSTALLATION, 'config': str(config_path)})
        units = []
        if sys.platform == 'linux':
            units = [render_service(settings, component=component)['unit'] for component in ('runner', 'monitor')]
        if systemd:
            install_units(units)
        return {'ok': True, 'account_id': identity['account_id'], 'config': str(config_path),
                'state_dir': str(directory), 'session_replaced': replaced, 'units': units,
                'services_installed': systemd, 'started': False}


def install_units(units):
    if sys.platform != 'linux' or not units:
        raise DeploymentError('User systemd services require Linux.')
    target = Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'systemd/user'
    private_directory(target)
    for filename in units:
        source = Path(filename)
        destination = target / source.name
        if destination.exists() and not destination.read_text().startswith('# Managed by Fangde deployment'):
            raise DeploymentError('An existing systemd unit is not managed by this installer.')
    for filename in units:
        source = Path(filename)
        from .legacy_holdings import private_write
        private_write(target / source.name, source.read_text())
    result = subprocess.run(['systemctl', '--user', 'daemon-reload'], capture_output=True)
    if result.returncode:
        raise DeploymentError('Units were written, but the user systemd manager is unavailable. No service was started.')


def check(config=None, *, component='runner'):
    settings = load_settings(config)
    with ApiClient(settings)._session():
        pass
    if component == 'runner':
        require_kafka(settings.get('kafka.bootstrapServers'))
        settings.require('kafka.consumerGroupId')
        try:
            import confluent_kafka  # noqa: F401
        except ImportError:
            raise DeploymentError('Kafka dependency is missing; run install.sh first.') from None
        client = settings.kafka_client_config
        for name in ('ssl.ca.location', 'ssl.certificate.location', 'ssl.key.location', 'sasl.kerberos.keytab'):
            if client.get(name) and not Path(client[name]).is_file():
                raise DeploymentError('A configured Kafka certificate/key file is missing.')
    elif component == 'monitor':
        settings.require('monitor.adminPassword')
        settings.require('monitor.certFile')
        settings.require('monitor.keyFile')
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(settings.resolve_path(settings.get('monitor.certFile')),
                                    settings.resolve_path(settings.get('monitor.keyFile')))
        except (OSError, ssl.SSLError):
            raise DeploymentError('Monitor TLS certificate/key is missing or invalid.') from None
    else:
        raise DeploymentError('Unknown service component.')
    return {'ok': True, 'component': component, 'config': str(settings.path),
            'account_id': settings.require('fangde.accountId'), 'network_checked': False}


def start(component, config=None, *, check_only=False):
    result = check(config, component=component)
    if check_only:
        return result
    os.execv(sys.executable, [sys.executable, '-m', 'fangde.cli', '--config', result['config'], component])


def preflight():
    selected = installation_pointer(project_root())
    if selected:
        settings = load_settings(selected)
        with EmulatorLock(settings.work_dir / 'runner.lock', 0):
            pass
    return {'ok': True, 'installed': selected is not None, 'started': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('preflight', help='check that the installed runner is stopped before updating dependencies')
    pack = sub.add_parser('bundle', help='combine account credentials and server settings into one private file')
    pack.add_argument('--session', required=True, type=Path)
    pack.add_argument('--output', required=True, type=Path)
    pack.add_argument('--settings', type=Path)
    pack.add_argument('--kafka-bootstrap')
    setup = sub.add_parser('install', help='install one account locally; never starts services')
    setup.add_argument('account_file', type=Path)
    setup.add_argument('--state-dir', type=Path)
    setup.add_argument('--settings', type=Path)
    setup.add_argument('--kafka-bootstrap')
    setup.add_argument('--refresh-session', action='store_true')
    setup.add_argument('--update-settings', action='store_true')
    setup.add_argument('--systemd', action='store_true', help='install separate user units without enabling or starting them')
    for name in ('runner', 'monitor'):
        launch = sub.add_parser(name, help='start only the ' + name)
        launch.add_argument('--config', type=Path)
        launch.add_argument('--check', action='store_true', help='validate local setup without network calls or startup')
    args = parser.parse_args(argv)
    if os.name == 'posix':
        os.umask(0o077)
    try:
        if args.command == 'preflight':
            result = preflight()
        elif args.command == 'bundle':
            result = bundle(args.session, args.output, settings_path=args.settings, kafka_bootstrap=args.kafka_bootstrap)
        elif args.command == 'install':
            result = install(args.account_file, state_dir=args.state_dir, settings_path=args.settings,
                kafka_bootstrap=args.kafka_bootstrap, refresh_session=args.refresh_session,
                update_settings=args.update_settings, systemd=args.systemd)
        else:
            result = start(args.command, args.config, check_only=args.check)
        print(json.dumps(result, indent=2))
        return 0
    except (DeploymentError, ConfigurationError, SessionError, SafetyError) as error:
        print(json.dumps({'ok': False, 'message': str(error)}), file=sys.stderr)
        return 2
    except (OSError, ValueError, TypeError):
        print(json.dumps({'ok': False, 'message': 'Deployment failed; check file permissions and JSON types.'}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
