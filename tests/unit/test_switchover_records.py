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


@pytest.mark.parametrize('stored', [None, 'null', '{}'])
def test_absent_switchover_is_absent_in_both_readers(cluster, stored):
    if stored is not None:
        cluster.records['switchover/master'] = stored
    assert cluster.zk.get_state().switchover is None
    assert cluster.switch.state().info is None


@pytest.mark.parametrize('payload, expected', [
    ({'hostname': 'leader', 'timeline': '7'}, {'hostname': 'leader', 'timeline': 7, 'destination': None}),
    ({'hostname': None}, {'hostname': None, 'timeline': None, 'destination': None}),
    ({'timeline': None, 'destination': ''}, {'hostname': None, 'timeline': None, 'destination': ''}),
    ({'primary': 'legacy', 'timeline': '42'}, {'hostname': None, 'timeline': 42, 'destination': None}),
])
def test_partial_legacy_tasks_keep_meaning_and_write_fixed_schema(cluster, payload, expected):
    cluster.records['switchover/master'] = json.dumps(payload)
    assert cluster.zk.get_state().to_dict()['switchover'] == expected
    assert cluster.switch.state().info.to_dict() == expected


@pytest.mark.parametrize('invalid', ['bad', '', '7.5', 7.5, True, False, [], {}])
@pytest.mark.parametrize('reader', ['daemon', 'cli'])
def test_invalid_timeline_rejects_record_without_reset(cluster, invalid, reader):
    cluster.records.update({
        'switchover/master': json.dumps({'hostname': 'leader', 'timeline': invalid}),
        'switchover/state': 'scheduled', 'failover_state': 'promoting',
    })
    before = dict(cluster.records)
    with pytest.raises(ValueError, match='switchover/master.*timeline|timeline.*switchover/master') as error:
        if reader == 'daemon':
            cluster.zk.get_state()
        else:
            cluster.switch.state()
    assert repr(invalid) in str(error.value)
    assert 'expected' in str(error.value)
    assert cluster.records == before
    cluster.client.delete.assert_not_called()


def test_cli_logs_actionable_timeline_error(cluster, monkeypatch, caplog):
    cluster.records['switchover/master'] = '{"hostname":"leader","timeline":"bad"}'
    monkeypatch.setattr(cli, 'parse_args', lambda: SimpleNamespace(action=cli.show_info, config_file='unused', short=False, json=True))
    monkeypatch.setattr(cli, 'read_config', lambda **kwargs: cluster.config, raising=False)
    monkeypatch.setattr(cli, 'init_logging', lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: cluster.zk)
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 1
    assert 'switchover/master' in caplog.text
    assert 'timeline' in caplog.text
    assert "'bad'" in caplog.text
    assert 'expected' in caplog.text


def test_cli_timeline_is_written_as_a_number(cluster, monkeypatch):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--timeline', '7'])
    opts = cli.parse_args()
    switch = Switchover(conf=cluster.config, primary='leader', timeline=opts.timeline)
    assert switch.plan_switchover()
    assert switch.perform(block=False)
    assert json.loads(cluster.records['switchover/master']) == {'hostname': 'leader', 'timeline': 7, 'destination': None}
    assert cluster.records['switchover/state'] == 'scheduled'


@pytest.mark.parametrize('phase, expected', [(None, False), ('failed', False), ('scheduled', True), ('candidate_found', True), ('', False)])
def test_in_progress_reports_whether_switching_is_active(cluster, phase, expected):
    cluster.records['switchover/state'] = phase
    assert cluster.switch.in_progress() is expected


@pytest.mark.parametrize('path', ['switchover/state', 'switchover/master', 'failover_state', 'replics_info'])
def test_in_progress_keeps_waiting_on_transport_failure_in_any_read(cluster, path, caplog):
    def read(key):
        if key == path:
            raise ZkClientError('connection lost')
        return cluster.records.get(key)
    cluster.client.get.side_effect = read
    assert cluster.switch.in_progress(return_true_on_zk_fail=True) is True
    assert 'Failed to get switchover state' in caplog.text


def test_perform_timeout_still_reports_phase(cluster):
    assert cluster.switch.plan_switchover()
    with pytest.raises(SwitchoverException, match='timeout exceeded, current status: scheduled'):
        cluster.switch.perform(timeout=0)


@pytest.mark.parametrize('field', ['application_name', 'sync_state'])
def test_legacy_null_replica_strings_are_empty_in_zk_output(cluster, field):
    cluster.records['replics_info'] = json.dumps([{field: None}])
    assert cluster.zk.get_state().to_dict()['replics_info'][0][field] == ''


@pytest.mark.parametrize('stored, expected', [
    ('{}', None), ('null', None),
    ('{"hostname":null}', {'hostname': None, 'timeline': None, 'destination': None}),
    ('{"hostname":"leader","timeline":"7"}', {'hostname': 'leader', 'timeline': 7, 'destination': None}),
])
def test_status_and_cli_expose_normalized_switchover(cluster, stored, expected, tmp_path, monkeypatch, capsys):
    cluster.records['switchover/master'] = stored
    state = cluster.zk.get_state()
    helpers.write_status_file(DbState(), state, str(tmp_path))
    assert json.loads((tmp_path / 'pgconsul.status').read_text())['zk_state']['switchover'] == expected
    cluster.config.get.return_value = str(tmp_path)
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: cluster.zk)
    cli.show_info(SimpleNamespace(short=False, json=True), cluster.config)
    assert json.loads(capsys.readouterr().out)['switchover'] == expected


def test_cli_rejects_invalid_timeline_argument(monkeypatch, capsys):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--timeline', 'bad'])
    with pytest.raises(SystemExit) as error:
        cli.parse_args()
    assert error.value.code == 2
    assert '--timeline' in capsys.readouterr().err
