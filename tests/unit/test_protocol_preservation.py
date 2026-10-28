"""Pinned pre-dataclass writer contracts at SQL, ZK and CLI boundaries."""
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src import helpers
with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli
from src.types import DbState, ReplicaInfo, WalReceiverInfo
from tests.unit.state_fixtures import state_cluster, state_consul
from tests.unit.test_db_state import postgres
from tests.unit.state_samples import LEGACY_REPLICA_ROW


def test_sql_replica_row_is_published_without_normalization(postgres, state_cluster):
    row = {**LEGACY_REPLICA_ROW, 'sent_location_diff': 11, 'replay_location_diff': 23, 'backend_start_ts': 12345}
    with patch.object(postgres.db, '_get', return_value=[row]) as query:
        rows = postgres.db.get_replics_info('primary')
    for field in ('client_addr', 'sent_location_diff', 'replay_location_diff', 'backend_start_ts'):
        assert field in query.call_args.args[0]
    zk = state_cluster.zk
    assert zk.try_acquire_lock()
    assert zk.write_replics_info(rows)
    assert zk.write_host_replics_info(rows, 'me')
    assert json.loads(state_cluster.records['replics_info']) == [row]
    assert json.loads(state_cluster.records['all_hosts/me/replics_info']) == [row]


@pytest.mark.parametrize('role', ['primary', 'replica'])
@pytest.mark.parametrize('field', ['sent_lsn', 'write_lsn', 'replay_lsn'])
@pytest.mark.parametrize('lsn_value', ['0/1234ABCD', '0/0', None])
def test_replica_lsn_from_sql_survives_publication_cache_and_cli(postgres, state_cluster, tmp_path, monkeypatch, capsys, role, field, lsn_value):
    row = {**LEGACY_REPLICA_ROW, field: lsn_value}
    with patch.object(postgres.db, '_get', return_value=[row]) as query:
        replicas = postgres.db.get_replics_info(role)
    assert f'{field}::text AS {field}' in ' '.join(query.call_args.args[0].split())
    assert getattr(replicas[0], field) == lsn_value
    zk = state_cluster.zk
    assert zk.try_acquire_lock()
    assert zk.write_replics_info(replicas)
    assert zk.write_host_replics_info(replicas, 'me')
    assert getattr(zk.get_replics_info()[0], field) == lsn_value
    assert getattr(zk.get_host_replics_info('me')[0], field) == lsn_value
    postgres.db.save_state(DbState(replics_info=replicas))
    cache = json.loads((tmp_path / '.pgconsul_db_state.cache').read_text())
    assert cache['replics_info'][0][field] == lsn_value
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: zk)
    config = SimpleNamespace(get=lambda *args: str(tmp_path))
    cli.show_info(SimpleNamespace(short=False, json=True), config)
    assert json.loads(capsys.readouterr().out)['replics_info'][0][field] == lsn_value


def test_nullable_receiver_survives_cache_and_zk(postgres, state_cluster):
    row = {'pid': 42, 'status': 'streaming', 'slot_name': None,
           'last_msg_receipt_time_msec': None, 'conninfo': None}
    with patch.object(postgres.db, '_get', return_value=[row]):
        receiver = postgres.db._get_wal_receiver_info()
    assert state_cluster.zk.write_host_wal_receiver(receiver, 'me')
    assert json.loads(state_cluster.records['all_hosts/me/wal_receiver']) == row
    assert DbState.from_dict({'wal_receiver': row}).to_dict() == {'wal_receiver': row}


@pytest.mark.parametrize('value', ['7', 'bad', 7.5, True, None])
def test_switchover_reader_preserves_timeline_type(state_cluster, value):
    record = {'hostname': 'old', 'timeline': value}
    state_cluster.records['switchover/master'] = json.dumps(record)
    assert state_cluster.zk.get_state().to_dict()['switchover'] == record


def test_cli_timeline_override_remains_a_string(monkeypatch):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'switchover', '--timeline', '7'])
    assert cli.parse_args().timeline == '7'


def test_unknown_priority_keeps_baseline_error():
    with pytest.raises(TypeError):
        helpers.get_oldest_replica([ReplicaInfo.from_dict({'application_name': 'r', 'write_location_diff': 1, 'priority': None})])


def test_unknown_lag_keeps_baseline_error():
    rows = [ReplicaInfo.from_dict({'application_name': name, 'write_location_diff': lag, 'priority': 1}) for name, lag in [('a', None), ('b', 1)]]
    with pytest.raises(TypeError):
        helpers.get_oldest_replica(rows)


def test_primary_collection_keeps_sessions_query(postgres):
    state = postgres.db.get_state().to_dict()
    assert state['sessions_ratio'] == 0
    assert state['replication_state'] == ('async', None)
    assert 'primary_fqdn' not in state
    assert 'connection_timed_out' not in state


def test_zk_info_keeps_lock_version_ssn_and_null_leader(state_cluster):
    state_cluster.transport.lock_version.return_value = '0000000021'
    state_cluster.records['all_hosts/r/ha'] = ''
    state = state_cluster.zk.get_state().to_dict()
    assert state['lock_version'] == '0000000021'
    assert state['last_leader'] is None
    assert state['synchronous_standby_names'] == {'r': (None, None)}
