import json
import sys
from configparser import RawConfigParser
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from src import helpers
from src.zk import Zookeeper, ZookeeperConfig
from src.zk_client import ZkClient

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli


PRIMARY = 'vla-primary.example.net'
PREFERRED = 'rc1a-replica.example.net'
ALTERNATIVE = 'edge-replica.example.net'


def replica(host, write_lag=0, state='streaming'):
    return {
        'application_name': helpers.app_name_from_fqdn(host),
        'write_location_diff': write_lag,
        'state': state,
    }


@pytest.fixture
def cluster(monkeypatch):
    conf = RawConfigParser()
    conf.read(Path(__file__).parents[1] / 'conf' / 'pgconsul.conf')
    prefix = conf.get('global', 'zk_lockpath_prefix')
    hosts = [PRIMARY, PREFERRED, ALTERNATIVE]
    nodes = {
        'timeline': '7',
        'quorum': json.dumps([PREFERRED, ALTERNATIVE]),
        'replics_info': json.dumps([replica(PREFERRED), replica(ALTERNATIVE)]),
        f'all_hosts/{PREFERRED}/prio': '100',
        f'all_hosts/{ALTERNATIVE}/prio': '50',
    }
    ha_paths = {f'all_hosts/{host}/ha' for host in hosts}
    lock_holders = {f'{prefix}alive/{host}': [host] for host in hosts}
    lock_holders[f'{prefix}leader'] = [PRIMARY]

    def make_lock(path, _contender):
        return Mock(contenders=Mock(side_effect=lambda: lock_holders.get(path, [])), acquire=Mock(return_value=True))

    def write(path, data, *, makepath):
        nodes[path] = data
        return True

    transport = Mock(spec=ZkClient)
    transport.get.side_effect = nodes.get
    transport.get_children.side_effect = {'all_hosts': hosts}.get
    transport.exists.side_effect = ha_paths.__contains__
    transport.make_lock.side_effect = make_lock
    transport.ensure_path.return_value = True
    transport.is_connected.return_value = True
    transport.write.side_effect = write
    zk = Zookeeper(transport, ZookeeperConfig(release_lock_after_acquire_failed=False, timeout=1, path_prefix=prefix))
    monkeypatch.setattr(cli.utils, 'create_zk', Mock(return_value=zk))
    monkeypatch.setattr(cli, 'read_config', Mock(return_value=conf))
    monkeypatch.setattr(cli, 'init_logging', Mock())
    return SimpleNamespace(conf=conf, transport=transport, nodes=nodes, ha_paths=ha_paths, lock_holders=lock_holders, prefix=prefix)


def run_switchover(monkeypatch, *args):
    monkeypatch.setattr(sys, 'argv', ['pgconsul-util', 'switchover', *args])
    cli.entry()


def assert_scheduled(cluster, destination):
    task_write, state_write = cluster.transport.write.call_args_list
    task_path, payload = task_write.args
    assert task_path == 'switchover/master'
    assert json.loads(payload) == {'hostname': PRIMARY, 'timeline': 7, 'destination': destination}
    assert state_write.args == ('switchover/state', 'scheduled')


@pytest.mark.parametrize('value', ['', '   ', ',vla-', 'vla-,', 'vla-,,rc1a-'])
def test_cli_rejects_empty_host_prefix(value, cluster, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_switchover(monkeypatch, '--from', value)
    assert exc.value.code == 2
    assert 'non-empty host prefixes' in capsys.readouterr().err
    cli.read_config.assert_not_called()


def test_cli_rejects_destination_with_from(cluster, monkeypatch):
    with pytest.raises(SystemExit) as exc:
        run_switchover(monkeypatch, '--from', 'vla-', '--destination', ALTERNATIVE)
    assert exc.value.code == 2
    cli.read_config.assert_not_called()


@pytest.mark.parametrize('prefixes, expected', [('vla-', PREFERRED), (' vla-, rc1a- ', ALTERNATIVE)])
def test_cli_schedules_best_replica_outside_prefixes(cluster, monkeypatch, prefixes, expected):
    run_switchover(monkeypatch, '--from', prefixes)
    assert_scheduled(cluster, expected)


def test_cli_prefers_wal_position_over_priority(cluster, monkeypatch):
    cluster.nodes['replics_info'] = json.dumps([replica(PREFERRED, write_lag=20), replica(ALTERNATIVE)])
    run_switchover(monkeypatch, '--from', 'vla-')
    assert_scheduled(cluster, ALTERNATIVE)


def test_cli_uses_quorum_to_select_replica(cluster, monkeypatch):
    cluster.nodes['quorum'] = json.dumps([ALTERNATIVE])
    run_switchover(monkeypatch, '--from', 'vla-')
    assert_scheduled(cluster, ALTERNATIVE)


def test_cli_can_select_outside_quorum_when_data_loss_is_allowed(cluster, monkeypatch):
    cluster.conf.set('replica', 'allow_potential_data_loss', 'yes')
    cluster.nodes['quorum'] = json.dumps([PREFERRED])
    run_switchover(monkeypatch, '--from', 'vla-,rc1a-')
    assert_scheduled(cluster, ALTERNATIVE)


@pytest.mark.parametrize('prefixes, quorum, replicas', [
    ('vla-,rc1a-,edge-', [PREFERRED, ALTERNATIVE], [replica(PREFERRED), replica(ALTERNATIVE)]),
    ('vla-,rc1a-', [PREFERRED], [replica(PREFERRED), replica(ALTERNATIVE)]),
    ('vla-,rc1a-', [PREFERRED, ALTERNATIVE], [replica(PREFERRED)]),
    ('vla-,rc1a-', [PREFERRED, ALTERNATIVE], [replica(ALTERNATIVE, state='catchup')]),
    ('vla-,rc1a-', [PREFERRED, ALTERNATIVE], [replica(ALTERNATIVE, write_lag=None)]),
])
def test_cli_does_not_schedule_without_eligible_replica(cluster, caplog, monkeypatch, prefixes, quorum, replicas):
    cluster.nodes['quorum'] = json.dumps(quorum)
    cluster.nodes['replics_info'] = json.dumps(replicas)
    with pytest.raises(SystemExit) as exc:
        run_switchover(monkeypatch, '--from', prefixes)
    assert exc.value.code == 1
    assert 'No eligible HA replica' in caplog.text
    cluster.transport.write.assert_not_called()
    cluster.transport.ensure_path.assert_not_called()


def test_cli_does_not_schedule_to_dead_replica(cluster, monkeypatch):
    del cluster.lock_holders[f'{cluster.prefix}alive/{ALTERNATIVE}']
    with pytest.raises(SystemExit) as exc:
        run_switchover(monkeypatch, '--from', 'vla-,rc1a-')
    assert exc.value.code == 1
    cluster.transport.write.assert_not_called()


def test_cli_does_not_schedule_to_non_ha_replica(cluster, monkeypatch):
    cluster.ha_paths.remove(f'all_hosts/{ALTERNATIVE}/ha')
    with pytest.raises(SystemExit) as exc:
        run_switchover(monkeypatch, '--from', 'vla-,rc1a-')
    assert exc.value.code == 1
    cluster.transport.write.assert_not_called()


def test_cli_skips_switchover_when_primary_is_already_outside_prefixes(cluster, monkeypatch, capsys):
    run_switchover(monkeypatch, '--from', 'rc1a-,edge-', '--block')
    assert f'Primary {PRIMARY} is already outside' in capsys.readouterr().out
    cluster.transport.get_children.assert_not_called()
    cluster.transport.write.assert_not_called()
    cluster.transport.ensure_path.assert_not_called()


@pytest.mark.parametrize('args, destination', [([], None), (['--destination', ALTERNATIVE], ALTERNATIVE)])
def test_cli_schedules_switchover_without_from(cluster, monkeypatch, args, destination):
    run_switchover(monkeypatch, *args)
    assert_scheduled(cluster, destination)
