import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch,Mock

from fangde.api import load_session,save_session,AuthenticationRequired
from fangde.authentication import account_digest
from fangde.config import load_settings,default_config_path,ConfigurationError
from fangde.deployment import (bundle,install,check,start,main,read_account,install_units,
                               DeploymentError,require_kafka,preflight)
from fangde.errors import SafetyError
from fangde.lock import EmulatorLock
from tests.test_authentication import state as authentication_state


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='fangde deploy ');self.addCleanup(temporary.cleanup)
        self.base=Path(temporary.name)
        self.repo=self.base/'checkout';self.repo.mkdir()
        self.directory=self.base/'private account'
        self.state=authentication_state();self.state['version']=3
        self.state['identity']['subject_sha256']=account_digest(self.state['authentication']['subject'])
        self.source=self.base/'account.session.json';save_session(self.source,self.state)
        self.account=self.base/'account.deployment.json'
        bundle(self.source,self.account,kafka_bootstrap='broker.example.invalid:9092')

    def install(self,**kwargs):
        return install(self.account,state_dir=self.directory,root=self.repo,**kwargs)

    def test_one_file_install_derives_identity_and_paths_without_network(self):
        with patch('urllib.request.OpenerDirector.open',side_effect=AssertionError('unexpected network')):
            result=self.install()
            settings=load_settings(result['config'])
            self.assertEqual(settings.get('fangde.accountId'),'TEST')
            self.assertEqual(settings.get('api.subjectSha256'),self.state['identity']['subject_sha256'])
            self.assertEqual(settings.get('kafka.consumerGroupId'),'fangde-TEST')
            self.assertEqual(settings.get('kafka.bootstrapServers'),'broker.example.invalid:9092')
            self.assertFalse(settings.get('fangde.tradingEnabledDefault'))
            self.assertEqual(settings.session_path,self.directory/'session.json')
            self.assertTrue(check(result['config'])['ok'])
        self.assertFalse(result['started'])
        pointer=json.loads((self.repo/'config/deployment.json').read_text())
        self.assertEqual(pointer['config'],result['config'])
        self.assertNotIn('fake-login-password',json.dumps(result))
        if os.name=='posix':
            for path in (settings.session_path,settings.path):self.assertEqual(path.stat().st_mode&0o777,0o600)
            self.assertEqual(self.directory.stat().st_mode&0o777,0o700)

    def test_existing_plain_session_is_supported_with_kafka_argument(self):
        result=install(self.source,state_dir=self.directory,root=self.repo,kafka_bootstrap='broker.example.invalid:9092')
        self.assertTrue(result['ok'])

    def test_reinstall_preserves_renewed_credentials_journal_and_enabled_state(self):
        result=self.install();settings=load_settings(result['config'])
        current=load_session(settings.session_path);current['session']['token']='renewed-synthetic-login'
        save_session(settings.session_path,current)
        journal=settings.work_dir/'api/orders/intent.json';save_session(journal,{'phase':'accepted','order_id':'synthetic-order'})
        enabled=settings.work_dir/'trading-state.json';save_session(enabled,{'enabled':True})
        before={p:p.read_bytes() for p in (settings.session_path,journal,enabled,settings.path)}
        result=self.install()
        self.assertFalse(result['session_replaced'])
        for path,data in before.items():self.assertEqual(path.read_bytes(),data)

    def test_refresh_session_requires_explicit_flag(self):
        result=self.install();settings=load_settings(result['config'])
        current=load_session(settings.session_path);current['session']['token']='renewed-synthetic-login'
        save_session(settings.session_path,current)
        result=self.install(refresh_session=True)
        self.assertTrue(result['session_replaced'])
        self.assertEqual(load_session(settings.session_path)['session'],self.state['session'])

    def test_another_account_cannot_replace_the_checkout_or_state(self):
        first=self.install();before=Path(first['config']).read_bytes()
        other=copy.deepcopy(self.state);other['identity']['account_id']='OTHER'
        file=self.base/'other.session.json';save_session(file,other)
        with self.assertRaises(DeploymentError):
            install(file,state_dir=self.directory,root=self.repo,kafka_bootstrap='broker.example.invalid:9092')
        self.assertEqual(Path(first['config']).read_bytes(),before)

    def test_another_broker_with_same_label_is_rejected(self):
        self.install()
        other=copy.deepcopy(self.state);other['authentication']['subject']['broker']='OTHER'
        other['authentication']['subject']['trade_key']='123_OTHER_TEST1_TRADE_TOKEN'
        other['identity']['subject_sha256']=account_digest(other['authentication']['subject'])
        file=self.base/'other.session.json';save_session(file,other)
        with self.assertRaises(DeploymentError):
            install(file,state_dir=self.directory,root=self.repo,kafka_bootstrap='broker.example.invalid:9092')

    def test_checkout_can_be_replaced_without_resetting_account(self):
        first=self.install();current=load_session(self.directory/'session.json')
        current['session']['token']='renewed-synthetic-login';save_session(self.directory/'session.json',current)
        second_repo=self.base/'new checkout';second_repo.mkdir()
        second=install(self.account,state_dir=self.directory,root=second_repo)
        self.assertFalse(second['session_replaced'])
        self.assertEqual(second['config'],first['config'])

    def test_running_runner_blocks_install_updates(self):
        first=self.install();settings=load_settings(first['config'])
        with EmulatorLock(settings.work_dir/'runner.lock',0):
            with self.assertRaises(SafetyError):self.install(refresh_session=True)

    def test_configuration_update_is_explicit(self):
        result=self.install()
        with self.assertRaises(DeploymentError):self.install(kafka_bootstrap='new.example.invalid:9092')
        self.install(kafka_bootstrap='new.example.invalid:9092',update_settings=True)
        self.assertEqual(load_settings(result['config']).get('kafka.bootstrapServers'),'new.example.invalid:9092')

    def test_preflight_checks_existing_runner_without_starting_anything(self):
        with patch('fangde.deployment.project_root',return_value=self.repo):
            self.assertFalse(preflight()['installed'])
            result=self.install();settings=load_settings(result['config'])
            self.assertTrue(preflight()['installed'])
            with EmulatorLock(settings.work_dir/'runner.lock',0):
                with self.assertRaises(SafetyError):preflight()

    def test_missing_broker_and_mismatched_settings_fail_before_account_install(self):
        with self.assertRaises(DeploymentError):install(self.source,state_dir=self.directory,root=self.repo)
        self.assertFalse((self.directory/'session.json').exists())
        settings=self.base/'wrong-settings.json';save_session(settings,{'fangde':{'accountId':'OTHER'}})
        with self.assertRaises(DeploymentError):self.install(settings_path=settings)

    def test_existing_unmanaged_directory_is_not_overwritten(self):
        self.directory.mkdir();valuable=self.directory/'keep.txt';valuable.write_text('preserve')
        with self.assertRaises(DeploymentError):self.install()
        self.assertEqual(valuable.read_text(),'preserve')

    def test_account_state_cannot_live_in_checkout(self):
        with self.assertRaises(DeploymentError):install(self.account,state_dir=self.repo/'runtime',root=self.repo)

    def test_installed_pointer_selects_config_independent_of_cwd(self):
        result=self.install()
        with patch('fangde.config.project_root',return_value=self.repo),patch.dict(os.environ,{'FANGDE_CONFIG':''}):
            self.assertEqual(default_config_path(),Path(result['config']))
            self.assertEqual(load_settings().get('fangde.accountId'),'TEST')

    def test_explicit_environment_config_wins_over_pointer(self):
        self.install()
        with patch('fangde.config.project_root',return_value=self.repo),patch.dict(os.environ,{'FANGDE_CONFIG':str(self.base/'override.json')}):
            self.assertEqual(default_config_path(),self.base/'override.json')

    def test_start_commands_exec_only_the_selected_process(self):
        for component in ('runner','monitor'):
            with patch('fangde.deployment.check',return_value={'ok':True,'config':'/private/config.json'}),patch('fangde.deployment.os.execv') as execute:
                start(component)
                self.assertEqual(execute.call_args.args[1][-1],component)
                self.assertEqual(execute.call_count,1)
                execute.reset_mock();start(component,check_only=True);execute.assert_not_called()

    def test_monitor_configuration_is_not_required_by_runner(self):
        result=self.install()
        self.assertTrue(check(result['config'],component='runner')['ok'])
        with self.assertRaises(ConfigurationError):check(result['config'],component='monitor')

    def test_bundle_rejects_overwrite_and_does_not_print_secrets(self):
        with self.assertRaises(DeploymentError):bundle(self.source,self.account,kafka_bootstrap='broker.example.invalid:9092')
        invalid=self.base/'bad.json';save_session(invalid,{'private-password':'fake-login-password'})
        output=io.StringIO()
        with redirect_stderr(output):code=main(['install',str(invalid),'--state-dir',str(self.directory)])
        self.assertEqual(code,2);self.assertNotIn('fake-login-password',output.getvalue())

    @unittest.skipUnless(os.name=='posix','POSIX file modes')
    def test_world_readable_input_is_rejected(self):
        self.account.chmod(0o644)
        with self.assertRaises(DeploymentError):read_account(self.account)

    def test_kafka_address_validation(self):
        for value in ('broker.invalid:9092','one.invalid:9092,two.invalid:9093','[::1]:9092'):require_kafka(value)
        for value in ('','https://broker:9092','user:password@broker:9092','broker:0','broker:99999','broker:9092\nsecret'):
            with self.assertRaises(DeploymentError):require_kafka(value)

    @unittest.skipUnless(os.name=='posix','Linux service paths')
    def test_units_are_separate_and_install_does_not_start_them(self):
        with patch('fangde.deployment.sys.platform','linux'):
            result=self.install()
        self.assertEqual(len(result['units']),2)
        runner,monitor=(Path(p).read_text() for p in result['units'])
        self.assertIn(' runner\n',runner);self.assertIn(' monitor\n',monitor)
        self.assertNotIn('-monitor.service',runner);self.assertNotIn('-runner.service',monitor)
        self.assertNotIn(self.state['session']['token'],runner+monitor)
        destination=self.base/'user config'
        with patch.dict(os.environ,{'XDG_CONFIG_HOME':str(destination)}),patch('fangde.deployment.subprocess.run',return_value=Mock(returncode=0)) as run:
            install_units(result['units'])
        run.assert_called_once_with(['systemctl','--user','daemon-reload'],capture_output=True)
