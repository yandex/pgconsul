import json
from unittest.mock import patch

import pytest

from tests.unit.state_samples import DB_DEFAULTS
from tests.unit.state_fixtures import state_cluster, state_consul  # noqa: F401

from src import helpers
from src.zk_client import ZkClientError
from src.types import DbState, MaintenanceState, ReplicaInfo, SsnInfo, SwitchoverPrimaryInfo, ZkState


def test_status_file_keeps_nested_records_and_ssn_pairs(tmp_path):
    db_state = {
        'alive': False, 'running': True, 'role': None,
        'connection_timed_out': True, 'prev_state': {},
    }
    zk_state = {
        'alive': True, 'timeline': 7, 'lock_holder': None,
        'last_failover_time': None, 'last_switchover_time': None,
        'failover_state': None, 'failover_must_be_reset': False,
        'current_promoting_host': None, 'switchover/candidate': None, 'lock_version': None,
        'maintenance': {'status': None, 'ts': None},
        'switchover': {'hostname': None, 'timeline': None, 'destination': None}, 'switchover/state': None,
        'switchover/side_replicas': [], 'last_leader': 'old',
        'single_node': False, 'replics_info': None,
        'synchronous_standby_names': {'host1': ['ANY 1 (host2)', '123.0']},
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
        'db_state': db_state,
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
def test_replica_selection_keeps_legacy_unknown_lag_results(differences, caplog):
    replicas = [
        ReplicaInfo(application_name=f'replica{i}', write_location_diff=diff, priority=i)
        for i, diff in enumerate(differences)
    ]
    if None in differences and differences != (None, None):
        with pytest.raises(TypeError):
            helpers.get_oldest_replica(replicas)
    else:
        assert helpers.get_oldest_replica(replicas) == 'replica1'


@pytest.mark.parametrize('difference', [None, 0, 100])
def test_single_replica_does_not_need_wal_position_comparison(difference):
    replica = ReplicaInfo(application_name='only', write_location_diff=difference, priority=1)
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


@pytest.mark.parametrize('priority_fields', [{}, {'priority': None}, {'priority': 0}])
def test_unknown_priority_keeps_missing_key_and_null_errors(priority_fields):
    replicas = [
        ReplicaInfo(application_name='negative', write_location_diff=0, priority=-10),
        ReplicaInfo.from_dict({'application_name': 'default', 'write_location_diff': 0, **priority_fields}),
    ]
    if 'priority' not in priority_fields:
        with pytest.raises(KeyError, match='priority'):
            helpers.get_oldest_replica(replicas)
    elif priority_fields['priority'] is None:
        with pytest.raises(TypeError):
            helpers.get_oldest_replica(replicas)
    else:
        assert helpers.get_oldest_replica(replicas) == 'default'
