from configparser import RawConfigParser
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src import helpers
from src.exceptions import SwitchoverException
from src.main import Pgconsul
from src.replication_manager import ReplicationManager
from src.zk import Zookeeper

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src.cli import parse_args, parse_excluded_dcs, switchover
    from src.utils import Switchover


PRIMARY = 'vla-primary.example.net'
PREFERRED = 'rc1a-replica.example.net'
ALTERNATIVE = 'rc1b-replica.example.net'


@pytest.mark.parametrize('hostname,dc', [
    ('rc1a-host', 'vla'), ('rc1b-host', 'sas'), ('rc1d-host', 'klg'),
    ('rc1e-host', 'nrg'), ('rc1k-host', 'nub'), ('kz1-a-host', 'kz1-a'),
    ('vla-host', 'vla'), ('klg-host', 'klg'), ('sas-host', 'sas'),
    ('orn-host', 'orn'), ('dct-host', 'dct'), ('bnt-host', 'bnt'), ('mow-host', 'mow'),
    ('legacyh', 'sas'), ('legacyk', 'vla'), ('legacyn', 'klg'), ('legacys', 'orn'),
    ('legacyt', 'dct'), ('legacyu', 'bnt'), ('legacyv', 'mow'), ('legacyf', 'myt'),
])
def test_get_dc_by_fqdn(hostname, dc):
    assert helpers.get_dc_by_fqdn(hostname + '.example.net') == dc


@pytest.mark.parametrize('hostname', ['', 'unknown-node', 'rc1z-hostk', 'kz1-b-hosth'])
def test_unknown_hostname_is_rejected(hostname):
    with pytest.raises(ValueError, match='Unable to determine DC'):
        helpers.get_dc_by_fqdn(hostname)


@pytest.mark.parametrize('value,expected', [
    (' rc1a, vla,rc1b, sas ', ['sas', 'vla']),
    ('rc1d,rc1e,rc1k,kz1-a', ['klg', 'kz1-a', 'nrg', 'nub']),
    ('orn,dct,bnt,mow,myt', ['bnt', 'dct', 'mow', 'myt', 'orn']),
])
def test_normalize_dcs(value, expected):
    assert parse_excluded_dcs(value) == expected


@pytest.mark.parametrize('value', ['', ' ', 'vla,,sas', ',vla', 'vla,', 'typo', 'h', 'rc1a-'])
def test_cli_rejects_invalid_dcs(value, monkeypatch, capsys):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--from', value])
    with pytest.raises(SystemExit) as exc:
        parse_args()
    assert exc.value.code == 2
    assert 'supported values:' in capsys.readouterr().err


def test_cli_normalizes_dcs_and_keeps_blocking_options(monkeypatch):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--from', 'vla,rc1b', '-b', '-t', '300'])
    opts = parse_args()
    assert opts.excluded_dcs == ['sas', 'vla']
    assert opts.block is True
    assert opts.timeout == 300


def test_cli_rejects_destination_with_from(monkeypatch):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--from', 'vla', '-d', ALTERNATIVE])
    with pytest.raises(SystemExit) as exc:
        parse_args()
    assert exc.value.code == 2


@pytest.fixture
def conf():
    conf = RawConfigParser()
    conf.read(Path(__file__).parents[1] / 'conf' / 'pgconsul.conf')
    return conf


def replica(host, write_lag=0, replay_lag=0, state='streaming'):
    return {
        'application_name': helpers.app_name_from_fqdn(host),
        'write_location_diff': write_lag,
        'replay_lag_msec': replay_lag,
        'state': state,
    }


@pytest.fixture
def cluster():
    zk = MagicMock(spec=Zookeeper)
    zk.TIMELINE_INFO_PATH = Zookeeper.TIMELINE_INFO_PATH
    zk.get_ha_hosts.return_value = [PRIMARY, PREFERRED, ALTERNATIVE]
    zk.get_replics_info.return_value = [replica(PREFERRED), replica(ALTERNATIVE)]
    zk.get.side_effect = lambda *args, **kwargs: zk.get_replics_info.return_value
    zk.get_quorum.return_value = [PREFERRED, ALTERNATIVE]
    zk.is_host_alive.return_value = True
    zk.get_host_prio.side_effect = lambda host: '100' if host == PREFERRED else '50'
    zk.get_current_lock_holder.return_value = PRIMARY
    zk.noexcept_get.return_value = None
    zk.get_alive_hosts.return_value = [PRIMARY, PREFERRED, ALTERNATIVE]
    return zk


@pytest.fixture
def daemon(cluster, monkeypatch):
    daemon = Pgconsul.__new__(Pgconsul)
    daemon.zk = cluster
    daemon.db = MagicMock()
    daemon.db.get_role.return_value = 'primary'
    daemon.config = SimpleNamespace(allow_potential_data_loss=False, max_allowed_switchover_lag_ms=100, min_failover_timeout=0)
    daemon._replication_manager = ReplicationManager.__new__(ReplicationManager)
    daemon._replication_manager._zk = cluster
    cluster.get_switchover_primary_info.return_value = {'hostname': PRIMARY, 'timeline': 1, 'excluded_dcs': ['vla']}
    cluster.get_switchover_state.return_value = 'scheduled'
    cluster.get_timeline.return_value = 1
    cluster.get_ha_replics.return_value = [PREFERRED, ALTERNATIVE]
    monkeypatch.setattr('src.main.helpers.get_hostname', lambda: PRIMARY)
    return daemon


def check_switchover(daemon):
    zk = daemon.zk
    return daemon._check_primary_switchover({'replics_info': zk.get_replics_info.return_value}, {
        zk.SWITCHOVER_ROOT_PATH: zk.get_switchover_primary_info.return_value,
        zk.LAST_FAILOVER_TIME_PATH: None, zk.LAST_SWITCHOVER_TIME_PATH: None, zk.FAILOVER_STATE_PATH: None,
    })


def test_excludes_preferred_candidate_with_warning(daemon, caplog):
    assert check_switchover(daemon) == ALTERNATIVE
    assert f'Preferred switchover candidate {PREFERRED}' in caplog.text
    assert ALTERNATIVE in caplog.text


@pytest.mark.parametrize('primary,dc', [(PRIMARY, 'vla'), (PREFERRED, 'vla'), (ALTERNATIVE, 'sas')])
@pytest.mark.parametrize('blocking_args', [[], ['-b']])
def test_cli_skips_switchover_when_primary_dc_is_not_listed(cluster, conf, monkeypatch, capsys, primary, dc, blocking_args):
    cluster.get_current_lock_holder.return_value = primary
    cluster.get_replics_info.return_value = []
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--from', 'klg,rc1e'] + blocking_args)
    with patch('src.utils.create_zk', return_value=cluster):
        switchover(parse_args(), conf)
    assert capsys.readouterr().out == f'Master locale in {dc}\n'
    cluster.get.assert_not_called()
    cluster.get_ha_hosts.assert_not_called()
    cluster.get_alive_hosts.assert_not_called()
    cluster.write.assert_not_called()
    cluster.try_acquire_lock.assert_not_called()


@pytest.mark.parametrize('allow_data_loss,quorum,lag,expected', [
    (False, False, 0, None), (False, True, 0, ALTERNATIVE),
    (False, True, 101, None), (False, True, None, None),
    (True, False, 101, ALTERNATIVE), (True, False, None, None),
])
def test_same_safety_checks_with_and_without_from(daemon, allow_data_loss, quorum, lag, expected):
    daemon.config.allow_potential_data_loss = allow_data_loss
    daemon.zk.get_quorum.return_value = [ALTERNATIVE] if quorum else []
    daemon.zk.get_replics_info.return_value = [replica(ALTERNATIVE, replay_lag=lag)]
    task = daemon.zk.get_switchover_primary_info.return_value
    for excluded_dcs in ([], ['vla']):
        task['excluded_dcs'] = excluded_dcs
        assert check_switchover(daemon) == expected
    daemon.zk.write.assert_not_called()


@pytest.mark.parametrize('lag', [101, None])
def test_excluding_priority_replica_does_not_force_promotion_of_lagging_replica(daemon, lag):
    daemon.zk.get_replics_info.return_value = [replica(PREFERRED), replica(ALTERNATIVE, replay_lag=lag)]
    assert daemon._get_switchover_candidate() == ALTERNATIVE
    assert check_switchover(daemon) is None


def test_daemon_reselects_after_candidates_change(daemon):
    assert daemon._get_switchover_candidate() == ALTERNATIVE
    daemon.zk.get_replics_info.return_value = [replica(PREFERRED)]
    assert daemon._get_switchover_candidate() is None
    daemon.zk.get_replics_info.return_value.append(replica(ALTERNATIVE))
    assert daemon._get_switchover_candidate() == ALTERNATIVE


def test_unknown_replica_dc_is_skipped(daemon, caplog):
    unknown = 'unknown-node.example.net'
    daemon.zk.get_ha_hosts.return_value.append(unknown)
    daemon.zk.get_quorum.return_value.append(unknown)
    daemon.zk.get_replics_info.return_value.append(replica(unknown, write_lag=-1))
    assert daemon._get_switchover_candidate() == ALTERNATIVE
    assert 'Unable to determine DC' in caplog.text


def test_cli_does_not_write_when_all_replica_dcs_are_excluded(cluster, conf, monkeypatch, caplog):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--from', 'vla,rc1b'])
    with patch('src.utils.create_zk', return_value=cluster), pytest.raises(SystemExit) as exc:
        switchover(parse_args(), conf)
    assert exc.value.code == 1
    assert 'All HA replica DCs are excluded' in caplog.text
    cluster.write.assert_not_called()
    cluster.try_acquire_lock.assert_not_called()


def test_cli_stores_restriction_without_pinning_destination(cluster, conf, monkeypatch):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--from', 'rc1a'])
    with patch('src.utils.create_zk', return_value=cluster):
        switchover(parse_args(), conf)
    task = cluster.write.call_args_list[0].args[1]
    assert task['hostname'] == PRIMARY
    assert task['destination'] is None
    assert task['excluded_dcs'] == ['vla']
    assert cluster.write.call_args_list[1].args[1] == 'scheduled'
    cluster.get_quorum.assert_not_called()


def test_unknown_primary_dc_fails_before_writing(cluster, conf):
    cluster.get_current_lock_holder.return_value = 'unknown-node.example.net'
    with patch('src.utils.create_zk', return_value=cluster):
        switch = Switchover(conf=conf, excluded_dcs=['vla'])
    with pytest.raises(SwitchoverException, match='Unable to determine DC'):
        switch.plan_switchover()
    cluster.write.assert_not_called()


def test_existing_switchover_payload_has_no_new_fields(cluster, conf):
    with patch('src.utils.create_zk', return_value=cluster):
        switch = Switchover(conf=conf, timeline=1)
    assert switch.plan_switchover()
    assert switch.perform(block=False)
    assert cluster.write.call_args_list[0].args[1] == {'hostname': PRIMARY, 'timeline': 1, 'destination': None}
