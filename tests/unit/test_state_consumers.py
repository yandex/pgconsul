import json
from unittest.mock import patch

import pytest

from tests.unit.state_samples import DB_DEFAULTS
from tests.unit.state_fixtures import state_cluster, state_consul  # noqa: F401

from src import helpers
from src.zk_client import ZkClientError
from src.types import DbState, MaintenanceState, ReplicaInfo, SsnInfo, SwitchoverPrimaryInfo, ZkState


def test_status_file_keeps_nested_null_empty_records_and_named_ssn(tmp_path):
    db_state = {
        'alive': False, 'running': True, 'role': None,
        'connection_timed_out': True, 'prev_state': {},
    }
    zk_state = {
        'alive': True, 'timeline': 7, 'lock_holder': None,
        'last_failover_time': None, 'last_switchover_time': None,
        'failover_state': None, 'failover_must_be_reset': False,
        'current_promoting_host': None, 'switchover/candidate': None,
        'maintenance': {'status': None, 'ts': None},
        'switchover': {'hostname': None, 'timeline': None, 'destination': None}, 'switchover/state': None,
        'switchover/side_replicas': [], 'last_leader': 'old',
        'single_node': False, 'replics_info': None,
        'synchronous_standby_names': {'host1': {'value': 'ANY 1 (host2)', 'last_update': '123.0'}},
    }
    state = ZkState(
        alive=True, timeline=7, lock_holder=None, maintenance=MaintenanceState(),
        switchover=SwitchoverPrimaryInfo(), switchover_state=None,
        switchover_side_replicas=[], last_leader='old', single_node=False, replics_info=None,
        synchronous_standby_names={'host1': SsnInfo(value='ANY 1 (host2)', last_update='123.0')},
    )
    with patch('src.helpers.time.time', return_value=1234.5):
        helpers.write_status_file(DbState.from_dict(db_state), state, str(tmp_path))
    result = json.loads((tmp_path / 'pgconsul.status').read_text())
    assert result == {
        'db_state': {**DB_DEFAULTS, 'running': True, 'connection_timed_out': True},
        'zk_state': zk_state,
        'ts': 1234.5,
    }


def test_replica_sort_uses_wal_position_then_priority_without_mutating_input():
    replicas = [
        {'application_name': 'behind', 'write_location_diff': 20, 'priority': 999},
        {'application_name': 'low_priority', 'write_location_diff': 0, 'priority': 10},
        {'application_name': 'winner', 'write_location_diff': 0, 'priority': 100},
    ]
    replicas = [ReplicaInfo.from_dict(row) for row in replicas]
    assert helpers.get_oldest_replica(replicas) == 'winner'
    assert replicas[0].application_name == 'behind'
    assert helpers.get_oldest_replica([]) is None


@pytest.mark.parametrize('differences', [(None, 100), (100, None), (None, None)])
def test_replica_selection_waits_for_comparable_wal_positions(differences, caplog):
    replicas = [
        ReplicaInfo(application_name=f'replica{i}', write_location_diff=diff, priority=i)
        for i, diff in enumerate(differences)
    ]
    assert helpers.get_oldest_replica(replicas) is None
    assert 'unknown write_location_diff' in caplog.text


@pytest.mark.parametrize('difference', [None, 0, 100])
def test_single_replica_does_not_need_wal_position_comparison(difference):
    replica = ReplicaInfo(application_name='only', write_location_diff=difference)
    assert helpers.get_oldest_replica([replica]) == 'only'


def test_quorum_only_includes_alive_streaming_hosts():
    replicas = [
        {'application_name': 'host1', 'state': 'streaming'},
        {'application_name': 'host2', 'state': 'catchup'},
        {'application_name': 'host3', 'state': 'streaming'},
        {'application_name': None, 'state': None},
    ]
    replicas = [ReplicaInfo.from_dict(row) for row in replicas]
    with patch('src.helpers.app_name_from_fqdn', side_effect=lambda host: host.split('.')[0]):
        assert helpers.make_current_replics_quorum(replicas, ['host1.example', 'host2.example']) == {'host1.example'}


@pytest.mark.parametrize('timeline, replicas, publish', [
    (None, [], False), (0, [], False), (6, [], False),
    (7, None, False), (7, [], True),
])
def test_primary_publishes_replica_snapshot_only_for_matching_timeline(state_consul, state_cluster, timeline, replicas, publish):
    cluster = state_cluster
    cluster.holders['leader'] = ['me']
    cluster.records.update({
        'replics_info': '[{"application_name":"previous"}]',
        'all_hosts/me/replics_info': '[{"application_name":"previous"}]',
        'failover_must_be_reset': '', 'failover_state': 'promoting',
    })
    state = cluster.zk.get_state()
    state.timeline = timeline
    snapshot = state.to_dict()
    state_consul.primary_iter(DbState(timeline=7, replics_info=replicas), state)
    for path in ('replics_info', 'all_hosts/me/replics_info'):
        assert json.loads(cluster.records[path]) == ([] if publish else [{'application_name': 'previous'}])
    assert state.to_dict() == snapshot


@pytest.mark.parametrize('stored_priority, other_priority, expected_winner', [
    (None, '-5', 'host1'), ('0', '-5', 'host1'), ('-10', '-5', 'host2'),
    (None, '5', 'host2'), ('0', '5', 'host2'), ('200', '5', 'host1'),
])
def test_switchover_selects_candidate_using_known_ha_priorities(state_consul, state_cluster, stored_priority, other_priority, expected_winner):
    cluster = state_cluster
    state_consul.config.allow_potential_data_loss = True
    cluster.holders.update({'leader': ['me'], 'alive/me': ['me'], 'alive/host1': ['host1'], 'alive/host2': ['host2']})
    rows = [
        {'application_name': 'host1', 'state': 'streaming', 'write_location_diff': 0, 'replay_lag_msec': 0, 'priority': 99},
        {'application_name': 'host2', 'state': 'streaming', 'write_location_diff': 0, 'replay_lag_msec': 0},
        {'application_name': 'unknown', 'write_location_diff': 100, 'priority': 999},
    ]
    cluster.records.update({
        'timeline': '7', 'switchover/master': '{"hostname":"me","timeline":7}',
        'switchover/state': 'scheduled', 'all_hosts/me/ha': '',
        'all_hosts/host1/ha': '', 'all_hosts/host2/ha': '', 'all_hosts/host2/prio': other_priority,
    })
    if stored_priority is not None:
        cluster.records['all_hosts/host1/prio'] = stored_priority
    read = cluster.transport.get.side_effect

    def get(path):
        assert path != 'all_hosts/unknown/prio', 'Non-HA replicas have no host priority'
        return read(path)

    cluster.transport.get.side_effect = get
    state_consul.db.get_replics_info.return_value = [ReplicaInfo.from_dict(row) for row in rows]
    state_consul.primary_iter(DbState(timeline=7, replics_info=state_consul.db.get_replics_info()), cluster.zk.get_state())
    assert cluster.records['switchover/candidate'] == expected_winner


@pytest.mark.parametrize('priority_fields', [{}, {'priority': None}, {'priority': 0}])
def test_unknown_priority_sorts_as_zero(priority_fields):
    replicas = [
        ReplicaInfo(application_name='negative', write_location_diff=0, priority=-10),
        ReplicaInfo.from_dict({'application_name': 'default', 'write_location_diff': 0, **priority_fields}),
    ]
    assert helpers.get_oldest_replica(replicas) == 'default'


def test_primary_switchover_closes_pooler_despite_failed_replica_refresh(state_consul, state_cluster):
    cluster = state_cluster
    state_consul.config.switchover_replica_turn_timeout = 1
    rows = [ReplicaInfo(application_name='replica1', state='streaming', replay_lag_msec=0)]
    cluster.records.update({
        'timeline': '7', 'all_hosts/me/ha': '', 'all_hosts/replica1/ha': '',
        'switchover/master': '{"hostname":"me","timeline":7,"destination":"replica1"}',
        'switchover/state': 'scheduled',
    })
    cluster.holders.update({'leader': ['me'], 'alive/me': ['me'], 'alive/replica1': ['replica1']})
    state_consul.db.get_replics_info.return_value = rows
    write = cluster.transport.write.side_effect
    failed_publications = []

    def publish(path, value, **kwargs):
        if path == 'replics_info' and cluster.records['switchover/state'] == 'candidate_found':
            failed_publications.append(json.loads(value))
            raise ZkClientError('replica refresh unavailable')
        result = write(path, value, **kwargs)
        if path == 'switchover/state' and value == 'initiated':
            # The remote replica accepts the published switchover request.
            cluster.records[path] = 'candidate_found'
        return result

    cluster.transport.write.side_effect = publish
    state_consul.primary_iter(DbState(timeline=7, replics_info=rows), cluster.zk.get_state())
    assert failed_publications
    assert cluster.records['switchover/candidate'] == 'replica1'
    assert json.loads(cluster.records['switchover/side_replicas']) == []
    assert cluster.records['switchover/state'] == 'candidate_found'
    state_consul.db.checkpoint.assert_called()
    state_consul.db.pgpooler.assert_called_with('stop')
    # Catch-up is unavailable within the configured zero deadline; do not stop PG or release its lock.
    state_consul.db.stop_postgresql.assert_not_called()
    assert cluster.holders['leader'] == ['me']
