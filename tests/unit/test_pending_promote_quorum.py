# encoding: utf-8
from unittest.mock import MagicMock

from src.helpers import app_name_from_fqdn
from src.replication_manager import (
    QuorumReplicationManager,
    ReplicationManagerConfig,
    SingleSyncReplicationManager,
)


HOSTS = [f'pgconsul{number}.test' for number in range(1, 5)]


class FakeZk:
    """
    ZK as the quorum manager uses it here: the recorded quorum, readable and writable.
    """

    QUORUM_PATH = 'quorum'

    def __init__(self, quorum_hosts):
        self._values = {self.QUORUM_PATH: quorum_hosts}

    def get(self, key, preproc=None):
        return self._values.get(key)

    def write(self, key, data, preproc=None):
        self._values[key] = data
        return True

    def write_ssn_on_changes(self, value):
        pass


def config() -> ReplicationManagerConfig:
    return ReplicationManagerConfig(
        priority=0,
        primary_unavailability_timeout=1,
        change_replication_metric='count',
        weekday_change_hours='0-0',
        weekend_change_hours='0-0',
        overload_sessions_ratio=75,
        before_async_unavailability_timeout=15,
    )


def make(db, zk) -> QuorumReplicationManager:
    return QuorumReplicationManager(config(), db, zk)


def streaming(hosts):
    return [{'application_name': app_name_from_fqdn(host), 'state': 'streaming'} for host in hosts]


def test_the_quorum_of_a_pending_promote_is_not_published():
    # the promote may still fail, and the quorum would then describe no primary at all
    db, zk = MagicMock(), MagicMock()

    make(db, zk).set_replication_before_promote(HOSTS[1:])

    assert db.change_replication_type.call_args[0][0] == 'ANY 2(pgconsul2_test,pgconsul3_test,pgconsul4_test)'
    assert zk.write.call_args_list == []


def test_a_failed_promote_leaves_the_quorum_that_took_the_commits():
    # the candidate installs the quorum it would govern and fails to promote; the hosts
    # that outlive it still have to measure themselves against the primary's quorum
    zk = FakeZk(HOSTS)
    manager = make(MagicMock(), zk)

    manager.set_replication_before_promote(HOSTS[1:])

    survivors = HOSTS[2:]
    assert manager.is_promote_safe(survivors, streaming(survivors)) is False


def test_single_sync_replication_has_nothing_to_set_before_promote():
    db, zk = MagicMock(), MagicMock()

    SingleSyncReplicationManager(config(), db, zk).set_replication_before_promote(HOSTS[1:])

    assert db.method_calls == []
    assert zk.method_calls == []
