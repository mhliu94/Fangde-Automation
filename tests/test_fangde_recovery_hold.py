from unittest.mock import Mock,patch
from fangde.runner import TradingRunner
from fangde.api_trading import OrderUncertain
from fangde.config import Settings

def test_uncertain_startup_preserves_state_and_runs_reporting_with_trading_blocked(tmp_path):
    settings=Settings({'fangde':{'accountId':'A','accountNumId':1,'backend':'api','tradingEnabledDefault':True}},tmp_path/'config.json',tmp_path)
    runner=TradingRunner(settings)
    runner._set_trading_enabled(True)
    before=runner.state_path.read_bytes()
    with patch.object(runner,'_operation',side_effect=OrderUncertain('unresolved')) as recover, patch.object(runner,'_run_kafka') as run:
        def check():
            assert runner._recovery_blocked
            assert not runner._trading_enabled()
        run.side_effect=check
        runner.run()
    recover.assert_called_once_with('recover',{})
    run.assert_called_once()
    assert runner.state_path.read_bytes()==before

def test_observation_only_does_not_subscribe_poll_or_commit_commands(tmp_path,monkeypatch):
    import sys
    from types import SimpleNamespace
    settings=Settings({'fangde':{'accountId':'A','accountNumId':1,'backend':'api'},'kafka':{
        'bootstrapServers':'unused','commandTopic':'trading-commands','accountDetailsTopic':'account-details','consumerGroupId':'test'}},tmp_path/'config.json',tmp_path)
    runner=TradingRunner(settings);runner._recovery_blocked=True
    consumer,producer=Mock(),Mock()
    monkeypatch.setitem(sys.modules,'confluent_kafka',SimpleNamespace(Consumer=lambda conf:consumer,Producer=lambda conf:producer,KafkaException=RuntimeError))
    def observation(*args):
        runner.running=False
        return {'positions':[], 'cash':{'balances':[{'currency':'USD','balance':'1'}]}}
    with patch.object(runner,'_operation',side_effect=observation),patch.object(runner,'_publish_account_update',return_value=(0,0)):
        runner._run_kafka()
    consumer.subscribe.assert_not_called();consumer.poll.assert_not_called();consumer.commit.assert_not_called()
