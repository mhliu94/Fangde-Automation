"""Exercise the shell launcher with synthetic credentials and a fake nohup."""
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest

from fangde.api import save_session
from fangde.lock import EmulatorLock
from tests.test_authentication import state


@unittest.skipUnless(os.name == 'posix', 'Linux launcher')
class LiveLauncherTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='fangde live ')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        repo = Path(__file__).resolve().parents[1]
        self.script = self.root / 'start_live.sh'
        shutil.copy2(repo / 'start_live.sh', self.script)
        python = self.root / '.venv/bin/python'
        python.parent.mkdir(parents=True)
        python.write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' "$@"\n')
        python.chmod(0o700)
        account = state()
        save_session(self.root / 'session.json', account)
        save_session(self.root / 'config.json', {
            'fangde': {'accountId': 'TEST'},
            'api': {'sessionPath': str(self.root / 'session.json'),
                    'subjectSha256': account['identity']['subject_sha256']},
            'paths': {'workDir': str(self.root / 'work'), 'logDir': str(self.root / 'logs')},
            'kafka': {'bootstrapServers': 'broker.example.invalid:9092', 'consumerGroupId': 'test'},
        })
        self.capture = self.root / 'launch.json'
        fake = self.root / 'nohup'
        fake.write_text('#!' + sys.executable + '\n' + '''
import json, os, sys, time
from pathlib import Path
Path(os.environ['LAUNCH_CAPTURE']).write_text(json.dumps({
    'args': sys.argv[1:], 'pid': os.getpid(), 'stdin': sys.stdin.read(),
    'log': os.environ.get('FANGDE_RUN_LOG')}))
print('Synthetic runner output', flush=True)
time.sleep(30)
''')
        fake.chmod(0o700)
        self.env = {**os.environ, 'FANGDE_CONFIG': str(self.root / 'config.json'),
                    'PYTHONPATH': str(repo), 'PATH': str(self.root) + os.pathsep + os.environ['PATH'],
                    'LAUNCH_CAPTURE': str(self.capture)}
        self.addCleanup(self.stop_fake_runner)

    def stop_fake_runner(self):
        if self.capture.exists():
            try:
                os.kill(json.loads(self.capture.read_text())['pid'], signal.SIGTERM)
            except ProcessLookupError:
                pass

    def launch(self, *args):
        return subprocess.run([str(self.script), *args], cwd=self.root, env=self.env,
                              capture_output=True, text=True, timeout=10)

    def test_nohup_background_launch_redirects_io_and_requests_live_mode(self):
        result = self.launch()
        self.assertEqual(result.returncode, 0, result.stderr)
        launched = json.loads(self.capture.read_text())
        self.assertEqual(launched['args'][-4:], ['-m', 'fangde.cli', 'runner', '--live'])
        self.assertEqual(launched['stdin'], '')
        log = self.root / 'logs/runner.log'
        self.assertEqual(launched['log'], str(log))
        self.assertIn('Synthetic runner output', log.read_text())
        os.kill(launched['pid'], 0)
        self.assertFalse((self.root / 'work/trading-state.json').exists())

    def test_check_does_not_launch_or_enable_trading(self):
        result = self.launch('--check')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.capture.exists())
        self.assertFalse((self.root / 'work/trading-state.json').exists())

    def test_active_runner_is_rejected_before_launch(self):
        with EmulatorLock(self.root / 'work/runner.lock', 0):
            result = self.launch()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Runner already running', result.stderr)
        self.assertFalse(self.capture.exists())
        self.assertFalse((self.root / 'work/trading-state.json').exists())
