import json
import unittest
from fangde.diagnostics import Workflow, error_detail
from fangde.config import Settings
from pathlib import Path

class DiagnosticTests(unittest.TestCase):
    def test_secret_values_removed_from_error_messages(self):
        settings=Settings({'monitor':{'adminPassword':'private-admin'},
            'kafka':{'clientConfig':{'sasl.password':'private-kafka'}}},Path('x'),Path('.'))
        result=error_detail(RuntimeError('private-admin private-kafka'),settings)
        self.assertEqual(result,'<redacted> <redacted>')

    def test_stage_and_attempt_are_recorded(self):
        settings=Settings({},Path('x'),Path('.'))
        workflow=Workflow('holdings',settings,{})
        with self.assertLogs('fangde',level='INFO') as logs:
            workflow.attempt(2)
            workflow.stage='read_positions'
            workflow.emit('finished',outcome='success')
        value=json.loads(logs.records[-1].getMessage().split('Workflow finished ',1)[1])
        self.assertEqual((value['stage'],value['attempts'],value['outcome']),('read_positions',2,'success'))
