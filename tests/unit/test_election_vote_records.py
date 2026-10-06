"""Election records preserve winner selection and the persisted vote layout."""
from unittest.mock import MagicMock, patch

import pytest

from src.failover_election import FailoverElection
from src.replication_manager import ReplicationManager, ReplicationManagerConfig
from src.types import ElectionVote, ReplicaInfo
from tests.unit.test_db_state import zk_transport, zookeeper  # noqa: F401
from tests.unit.state_fixtures import state_cluster, state_consul  # noqa: F401


@pytest.mark.parametrize('rows, winner', [
    ([(10, 100), (11, 0)], 'host2'),
    ([(11, 0), (11, 100)], 'host2'),
    ([(11, 100), (11, 100)], 'host1'),
    ([None, (11, 100)], 'host2'),
    ([None, None], None),
])
def test_election_publishes_winner_by_lsn_then_priority(rows, winner):
    zk = MagicMock()
    zk.get_current_lock_holder.return_value = None
    zk.get_ha_hosts.return_value = ['host1', 'host2']
    zk.get_election_host_vote.side_effect = [
        ElectionVote(lsn=row[0], priority=row[1]) if row is not None else None for row in rows
    ]
    zk.try_acquire_lock.return_value = True
    replicas = [ReplicaInfo(application_name='host1'), ReplicaInfo(application_name='host2')]
    election = FailoverElection(zk, 0, replicas, MagicMock(), False, 100, '0/B', 1)
    with patch('src.helpers.get_hostname', return_value=winner or 'host1'), patch('src.failover_election.time.sleep'):
        assert election.make_election(election_loser_timeout=0) is (winner is not None)
    if winner is None:
        zk.write_election_winner.assert_not_called()
    else:
        zk.write_election_winner.assert_called_once_with(winner)


@pytest.mark.parametrize('lsn, priority', [('11', '100'), (None, '100'), ('11', None)])
def test_vote_reader_keeps_existing_zk_nodes(zookeeper, lsn, priority):
    zk, records = zookeeper
    path = 'election_vote/host1'
    if lsn is not None:
        records[path + '/lsn'] = lsn
    if priority is not None:
        records[path + '/prio'] = priority
    before = dict(records)
    vote = zk.get_election_host_vote('host1')
    if lsn is None or priority is None:
        assert vote is None
    else:
        assert vote == ElectionVote(lsn=11, priority=100)
    assert records == before


@pytest.mark.parametrize('votes, quorum_size, winner', [
    ({'host1': (10, 100)}, 1, 'host1'),
    ({'host1': (10, 100)}, 2, None),
    ({'host1': (10, 100), 'host2': (11, 0), 'host3': (11, 1)}, 1, 'host3'),
])
def test_election_filters_unknown_and_missing_votes_before_selecting_winner(state_cluster, state_consul, votes, quorum_size, winner):
    cluster = state_cluster
    for host in ('host1', 'host2', 'host3'):
        cluster.records[f'all_hosts/{host}/ha'] = ''
    # Non-HA hosts must not supply a winning vote or satisfy the quorum.
    cluster.records.update({'election_vote/unknown/lsn': '999', 'election_vote/unknown/prio': '999'})
    rows = [ReplicaInfo.from_dict(row) for row in [
        {'application_name': None}, {'application_name': 'unknown'},
        {'application_name': 'host1'}, {'application_name': 'host2'}, {'application_name': 'host3'},
    ]]
    write = cluster.transport.write.side_effect

    def publish(path, value, **kwargs):
        # Participants vote in the registration phase, after old votes are removed.
        if path == 'election_status' and value == 'registration':
            for host, (lsn, priority) in votes.items():
                cluster.records[f'election_vote/{host}/lsn'] = str(lsn)
                cluster.records[f'election_vote/{host}/prio'] = str(priority)
        if path == 'election_winner':
            cluster.holders['leader'] = [value]
        return write(path, value, **kwargs)

    cluster.transport.write.side_effect = publish
    replication = ReplicationManager(
        ReplicationManagerConfig(100, 0.0, 'count', '0-0', '0-0', 100.0, 0.0, 0.0), state_consul.db, cluster.zk,
    )
    election = FailoverElection(cluster.zk, 1, rows, replication, True, 100, '0/A', quorum_size)
    assert election.make_election(election_loser_timeout=0) is False
    assert cluster.records.get('election_winner') == winner
