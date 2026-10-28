"""Linux CI integration checks with synthetic credentials and no broker calls."""
import datetime
import json
from pathlib import Path
import subprocess
import sys

from fangde.api import load_session, save_session
from fangde.authentication import account_digest
from fangde.deployment import bundle
from tests.test_authentication import state


def prepare(base):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    account = state()
    account['version'] = 3
    account['identity']['subject_sha256'] = account_digest(account['authentication']['subject'])
    save_session(base / 'account.session.json', account)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1)).sign(key, hashes.SHA256()))
    (base / 'test-cert.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (base / 'test-key.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    (base / 'test-key.pem').chmod(0o600)
    save_session(base / 'settings.json', {'monitor': {'adminPassword': 'synthetic-dashboard-password',
        'certFile': str(base / 'test-cert.pem'), 'keyFile': str(base / 'test-key.pem')}})
    bundle(base / 'account.session.json', base / 'account.deployment.json',
           settings_path=base / 'settings.json', kafka_bootstrap='127.0.0.1:1')


def reinstall(base):
    directory = base / 'private state'
    session = directory / 'session.json'
    account = load_session(session)
    account['session']['token'] = 'synthetic-renewed-token'
    save_session(session, account)
    journal = directory / 'work/api/orders/synthetic.json'
    enabled = directory / 'work/trading-state.json'
    save_session(journal, {'phase': 'accepted', 'order_id': 'synthetic-order'})
    save_session(enabled, {'enabled': True})
    paths = (session, journal, enabled, directory / 'server.config.json')
    before = {path: path.read_bytes() for path in paths}
    root = Path(__file__).resolve().parent.parent
    subprocess.run([str(root / 'install.sh'), str(base / 'account.deployment.json')], check=True)
    for path, content in before.items():
        assert path.read_bytes() == content, f'Reinstall changed {path.name}'
    assert session.stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700
    assert not (directory / 'work/runner.pid.json').exists()
    assert not (directory / 'work/monitor.pid.json').exists()
    print(json.dumps({'ok': True, 'preserved_state': True, 'services_started': False}))


if __name__ == '__main__':
    {'prepare': prepare, 'reinstall': reinstall}[sys.argv[1]](Path(sys.argv[2]))
