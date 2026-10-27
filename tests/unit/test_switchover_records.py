"""Switchover records read through the ZooKeeper and CLI boundaries."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.zk import Zookeeper, ZookeeperConfig
from src.zk_client import ZkClientError
from src.exceptions import SwitchoverException
from src import helpers
from src.types import DbState

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli
    from src.utils import Switchover


@pytest.fixture
def cluster(monkeypatch):
    records = {}
    client = MagicMock()
    client.lock_version.return_value = None
    client.get.side_effect = records.get
    client.write.side_effect = lambda path, value, **kwargs: records.__setitem__(path, value) or True
    client.exists.side_effect = lambda path: path in records
    client.is_alive.return_value = True
    client.get_children.return_value = []
    client.make_lock.return_value.contenders.return_value = ['leader']
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'leader'))
    monkeypatch.setattr('src.utils.create_zk', lambda **kwargs: zk)
    config = MagicMock()
    config.getfloat.return_value = 1.0
    switch = Switchover(conf=config, primary='leader', timeline=7)
    return SimpleNamespace(records=records, client=client, zk=zk, switch=switch, config=config)


@pytest.mark.parametrize('payload', [None, {}, {'hostname': None}, {'hostname': 'leader', 'timeline': '7'},
                                     {'timeline': True}, {'timeline': 7.5}, {'timeline': 'bad'}])
def test_readers_keep_absent_empty_partial_and_timeline_values(cluster, payload):
    if payload is not None:
        cluster.records['switchover/master'] = json.dumps(payload)
    assert cluster.zk.get_state().to_dict()['switchover'] == payload
    info = cluster.switch.state().info
    assert (info.to_dict() if info is not None else None) == (payload or {})


@pytest.mark.parametrize('timeline', ['7', 'bad'])
def test_cli_writes_string_timeline_override(cluster, monkeypatch, timeline):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--timeline', timeline])
    opts = cli.parse_args()
    switch = Switchover(conf=cluster.config, primary='leader', timeline=opts.timeline)
    assert switch.plan_switchover()
    assert switch.perform(block=False)
    assert json.loads(cluster.records['switchover/master']) == {'hostname': 'leader', 'timeline': timeline, 'destination': None}


@pytest.mark.parametrize('progress, expected', [('scheduled', 'scheduled'), ('candidate_found', 'candidate_found'),
                                               ('', ''), (None, False), ('failed', False)])
def test_in_progress_preserves_return_value_and_constraints(cluster, progress, expected):
    cluster.records['switchover/state'] = progress
    cluster.records['switchover/master'] = '{"hostname":"leader","timeline":7}'
    assert cluster.switch.in_progress() == expected
    assert cluster.switch.in_progress(primary='leader') is False
    assert cluster.switch.in_progress(timeline=8) is False
    assert cluster.switch.in_progress(timeline=7) == expected


def test_in_progress_tolerates_transport_failure_only_when_requested(cluster):
    cluster.client.get.side_effect = ZkClientError('unavailable')
    assert cluster.switch.in_progress(return_true_on_zk_fail=True) is True


def test_reset_keeps_empty_task_distinct_from_absence(cluster):
    cluster.records['switchover/master'] = '{"hostname":"leader","timeline":7}'
    cluster.switch.reset(force=True)
    assert cluster.records['switchover/master'] == '{}'
    assert cluster.zk.get_switchover_primary_info() is not None
    assert not cluster.zk.get_switchover_primary_info()


@pytest.mark.parametrize('deprecated_yes', [False, True])
def test_cli_switchover_is_noninteractive_with_dataclass_plan(cluster, monkeypatch, caplog, deprecated_yes):
    cluster.records['all_hosts/replica/ha'] = ''
    confirm = MagicMock(side_effect=AssertionError('switchover must not prompt'))
    monkeypatch.setattr(helpers, 'confirm', confirm)
    opts = SimpleNamespace(primary='leader', timeline=7, destination='replica', timeout=1,
                           reset=False, yes=deprecated_yes, replicas=None, block=False)
    with caplog.at_level('INFO'):
        cli.switchover(opts, cluster.config)
    confirm.assert_not_called()
    assert json.loads(cluster.records['switchover/master']) == {
        'hostname': 'leader', 'timeline': 7, 'destination': 'replica',
    }
    assert cluster.records['switchover/state'] == 'scheduled'
    assert 'switchover leader (timeline: 7)' in caplog.text
    assert ('The -y/--yes option is deprecated' in caplog.text) is deprecated_yes
