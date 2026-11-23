"""Election selection need not wait when every eligible replica has voted."""

import time
from unittest.mock import MagicMock

from src.failover_election import FailoverElection, STATUS_CLEANUP, STATUS_DONE, STATUS_REGISTRATION, STATUS_SELECTION
from src.types import ElectionVote, ReplicaInfo


def test_selection_starts_early_after_all_votes():
    zk = MagicMock()
    statuses = []
    zk.get_ha_hosts.return_value = ['host1', 'host2']
    zk.get_election_host_vote.side_effect = lambda host, **kwargs: ElectionVote(10 if host == 'host1' else 11, 100)
    zk.get_current_lock_holder.side_effect = lambda lock: 'host2' if lock == zk.PRIMARY_LOCK_PATH and STATUS_DONE in statuses else None
    zk.try_acquire_lock.return_value = True
    zk.delete_election_vote.return_value = True
    zk.write_election_status.side_effect = lambda status: statuses.append(status) or True
    zk.write_election_vote.return_value = True
    zk.write_election_winner.return_value = True
    replication_manager = MagicMock()
    replication_manager.is_promote_safe.return_value = True
    replica_infos = [ReplicaInfo(application_name='host1'), ReplicaInfo(application_name='host2')]
    election = FailoverElection(zk, 10, replica_infos, replication_manager, False, 100, '0/A', 2)

    started = time.monotonic()
    assert election.make_election(election_loser_timeout=0) is False
    assert time.monotonic() - started < 3
    assert statuses == [STATUS_CLEANUP, STATUS_REGISTRATION, STATUS_SELECTION, STATUS_DONE]
    zk.write_election_winner.assert_called_once_with('host2')
