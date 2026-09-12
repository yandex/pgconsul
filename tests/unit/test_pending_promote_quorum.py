# encoding: utf-8
from unittest.mock import MagicMock

from src.replication_manager import QuorumReplicationManager, ReplicationManagerConfig


HOSTS = ['pgconsul1.test', 'pgconsul2.test', 'pgconsul3.test']


def make(db, zk) -> QuorumReplicationManager:
    config = ReplicationManagerConfig(
        priority=0,
        primary_unavailability_timeout=1,
        change_replication_metric='count',
        weekday_change_hours='0-0',
        weekend_change_hours='0-0',
        overload_sessions_ratio=75,
        before_async_unavailability_timeout=15,
    )
    return QuorumReplicationManager(config, db, zk)


def test_the_quorum_of_a_pending_promote_is_not_published():
    # the promote may still fail, and the quorum would then describe no primary at all
    db, zk = MagicMock(), MagicMock()
    db.get_replication_state.return_value = ('sync', 'ANY 1(pgconsul1_test)')

    make(db, zk).update_replication_type(None, None, set_quorum_to=HOSTS[1:])

    assert db.change_replication_type.call_args[0][0] == 'ANY 1(pgconsul2_test,pgconsul3_test)'
    assert zk.write.call_args_list == []
