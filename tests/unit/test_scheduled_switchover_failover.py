from configparser import RawConfigParser
from unittest.mock import Mock, patch

from src.main import pgconsul


def test_scheduled_switchover_without_primary_lock_enters_failover_with_autofailover_enabled():
    config = RawConfigParser()
    config['global'] = {'autofailover': 'yes'}
    daemon = pgconsul.__new__(pgconsul)
    daemon.config = config
    daemon.zk = Mock(SWITCHOVER_STATE_PATH='switchover/state')
    daemon.zk.get_current_lock_holder.return_value = None

    with patch.object(pgconsul, '_accept_failover', return_value=True) as accept_failover:
        assert daemon._accept_switchover({'switchover/state': 'scheduled'}) is True

    accept_failover.assert_called_once_with(switchover_in_progress=True)


def test_scheduled_switchover_with_primary_lock_waits():
    daemon = pgconsul.__new__(pgconsul)
    daemon.zk = Mock(SWITCHOVER_STATE_PATH='switchover/state')
    daemon.zk.get_current_lock_holder.return_value = 'primary'

    with patch.object(pgconsul, '_accept_failover') as accept_failover:
        assert daemon._accept_switchover({'switchover/state': 'scheduled'}) is False

    accept_failover.assert_not_called()
