"""Switchover records read through the ZooKeeper and CLI boundaries."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.zk import Zookeeper, ZookeeperConfig
from src.zk_client import ZkNoNodeError
from src.cluster_state import ClusterState
from src import helpers

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli


@pytest.fixture
def cluster(monkeypatch):
    records = {}
    client = MagicMock()
    client.lock_version.return_value = None
    client.get.side_effect = records.get
    def get_with_version(path):
        if path not in records:
            raise ZkNoNodeError(path)
        return records[path], 0

    client.get_with_version.side_effect = get_with_version
    client.create_if_absent.side_effect = lambda path, value: records.setdefault(path, value) == value
    client.write.side_effect = lambda path, value, **kwargs: records.__setitem__(path, value) or True
    client.exists.side_effect = lambda path: path in records
    client.is_alive.return_value = True
    client.get_children.return_value = []
    client.make_lock.return_value.contenders.return_value = ['leader']
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'leader'))
    monkeypatch.setattr('src.cli.create_zk', lambda **kwargs: zk)
    config = MagicMock()
    config.getfloat.return_value = 1.0
    return SimpleNamespace(records=records, client=client, zk=zk, config=config)


@pytest.mark.parametrize('missing', ['client_hostname', 'state', 'sync_state', 'replay_lag_msec'])
def test_short_cli_requires_present_replica_fields(cluster, monkeypatch, missing):
    row = {'client_hostname': 'replica', 'state': 'streaming', 'sync_state': 'sync', 'replay_lag_msec': 0}
    del row[missing]
    cluster.records['replics_info'] = json.dumps([row])
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: cluster.zk)
    with pytest.raises(KeyError) as error:
        cli.show_info(SimpleNamespace(short=True, json=True), cluster.config)
    assert error.value.args == (missing,)


@pytest.mark.parametrize('values, expected', [
    ({'state': 'streaming', 'sync_state': 'sync', 'replay_lag_msec': 0}, 'streaming, sync_state sync, replay_lag_msec 0'),
    ({'state': '', 'sync_state': '', 'replay_lag_msec': None}, ', sync_state , replay_lag_msec None'),
])
def test_short_cli_preserves_replica_values(cluster, monkeypatch, capsys, values, expected):
    cluster.records['replics_info'] = json.dumps([{'client_hostname': 'replica', **values}])
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: cluster.zk)
    cli.show_info(SimpleNamespace(short=True, json=True), cluster.config)
    assert json.loads(capsys.readouterr().out)['replics_info'] == {'replica': expected}


@pytest.mark.parametrize('deprecated_yes', [False, True])
def test_cli_switchover_is_noninteractive_with_dataclass_plan(cluster, monkeypatch, caplog, deprecated_yes):
    cluster.records['all_hosts/replica/ha'] = ''
    cluster.records['cluster_state'] = ClusterState.initial('leader', 7, {'leader', 'replica'}).to_json()
    confirm = MagicMock(side_effect=AssertionError('switchover must not prompt'))
    monkeypatch.setattr(helpers, 'confirm', confirm)
    opts = SimpleNamespace(primary='leader', timeline=7, destination='replica', timeout=1,
                           reset=False, yes=deprecated_yes, replicas=None, block=False)
    with caplog.at_level('INFO'):
        cli.switchover(opts, cluster.config)
    confirm.assert_not_called()
    assert json.loads(cluster.records['switchover_request'])['target_primary'] == 'replica'
    assert json.loads(cluster.records['switchover_request'])['source_primary'] == 'leader'
    assert 'Switchover request' in caplog.text
    assert ('The -y/--yes option is deprecated' in caplog.text) is deprecated_yes
