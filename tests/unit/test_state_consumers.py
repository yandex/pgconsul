import json
from unittest.mock import MagicMock, patch

import pytest

from src import helpers
from src.failover_election import FailoverElection
from src.main import Pgconsul
from src.zk import Zookeeper
from src.types import DbState, ReplicaInfo, ZkState


def test_status_file_keeps_nested_null_empty_records_and_ssn_arrays(tmp_path):
    db_state = {
        'alive': False, 'running': True, 'role': None,
        'connection_timed_out': True, 'prev_state': {},
    }
    zk_state = {
        'alive': True, 'timeline': 7, 'lock_holder': None,
        'maintenance': {'status': None, 'ts': None},
        'switchover': {}, 'switchover/state': None,
        'switchover/side_replicas': [], 'last_leader': 'old',
        'single_node': False, 'replics_info': None,
        'synchronous_standby_names': {'host1': ('ANY 1 (host2)', 123.0)}, 'replics_info_written': None,
    }
    with patch('src.helpers.time.time', return_value=1234.5):
        helpers.write_status_file(DbState.from_dict(db_state), ZkState.from_dict(zk_state), str(tmp_path))
    result = json.loads((tmp_path / 'pgconsul.status').read_text())
    assert result == {
        'db_state': db_state,
        'zk_state': {**zk_state, 'synchronous_standby_names': {'host1': ['ANY 1 (host2)', 123.0]}},
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


def test_election_ignores_unknown_nullable_and_missing_votes():
    zk = MagicMock()
    zk.get_ha_hosts.return_value = ['host1.example', 'host2.example']
    zk.get_election_host_vote.side_effect = [(10, 100), None]
    replicas = [
        {'application_name': None}, {'application_name': 'unknown'},
        {'application_name': 'host1'}, {'application_name': 'host2'},
    ]
    replicas = [ReplicaInfo.from_dict(row) for row in replicas]
    election = FailoverElection(zk, 10, replicas, MagicMock(), False, 100, '0/A', 1)
    with patch('src.helpers.app_name_from_fqdn', side_effect=lambda host: host.split('.')[0]):
        votes = election._collect_votes()
    assert votes == {'host1.example': (10, 100)}
    assert election._determine_election_winner({'host1': (10, 100), 'host2': (11, 0), 'host3': (11, 1)}) == 'host3'
    assert zk.get_election_host_vote.call_count == 2


@pytest.mark.parametrize('timeline,result,expected', [(6, True, None), (7, False, False), (7, True, True)])
def test_store_replicas_preserves_three_valued_write_status(timeline, result, expected):
    instance = Pgconsul.__new__(Pgconsul)
    instance.zk = MagicMock()
    instance.zk.TIMELINE_INFO_PATH = Zookeeper.TIMELINE_INFO_PATH
    instance.zk.write_replics_info.return_value = result
    instance.write_host_stat = MagicMock()
    db_state = DbState.from_dict({'timeline': 7, 'replics_info': []})
    zk_state = ZkState.from_dict({Zookeeper.TIMELINE_INFO_PATH: timeline})
    assert instance._store_replics_info(db_state, zk_state) is (timeline == 7)
    assert zk_state.replics_info_written is expected
    assert instance.zk.write_replics_info.call_count == (timeline == 7)
    assert zk_state.to_dict()['replics_info_written'] is expected


def test_priority_added_only_to_known_ha_replicas():
    instance = Pgconsul.__new__(Pgconsul)
    instance.zk = MagicMock()
    instance.zk.get_ha_hosts.return_value = ['host1.example', 'host2.example']
    instance.zk.get_host_prio.side_effect = ['200', None]
    instance.zk.get_replics_info.return_value = [
        {'application_name': 'host1'}, {'application_name': 'host2'}, {'application_name': 'unknown'},
    ]
    instance.zk.get_replics_info.return_value = [
        ReplicaInfo.from_dict(row) for row in instance.zk.get_replics_info.return_value
    ]
    with patch('src.helpers.app_name_from_fqdn', side_effect=lambda host: host.split('.')[0]):
        result = instance._get_extended_replica_infos()
    assert [row.to_dict() for row in result] == [
        {'application_name': 'host1', 'priority': 200},
        {'application_name': 'host2', 'priority': None},
        {'application_name': 'unknown'},
    ]
