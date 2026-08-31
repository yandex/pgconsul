import importlib
from unittest.mock import MagicMock, patch

import src

src.read_config = MagicMock()
Switchover = importlib.import_module('src.utils').Switchover


def test_perform_checks_switchover_once_more_at_timeout_boundary():
    """Regression for pgconsul_util.feature:402."""
    switchover = Switchover.__new__(Switchover)
    switchover._zk = MagicMock()
    switchover._zk.get_alive_hosts.return_value = []
    switchover._plan = {'primary': 'primary', 'timeline': 1}
    switchover._new_primary = 'candidate'
    switchover._conf = MagicMock()
    switchover._conf.getboolean.return_value = False
    switchover._log = MagicMock()
    switchover._initiate_switchover = MagicMock(return_value=True)
    switchover.in_progress = MagicMock(side_effect=['candidate_found', 'promoted', False])
    switchover._wait_for_primary = MagicMock()
    switchover.state = MagicMock(return_value={'progress': None})
    switchover._wait_for_replicas = MagicMock()

    with patch('src.utils.time.sleep'):
        assert switchover.perform(timeout=1) is True

    assert switchover.in_progress.call_count == 3


def test_initiate_writes_manager_owned_protocol_record():
    switchover = Switchover.__new__(Switchover)
    switchover._zk = MagicMock()
    switchover._zk.TIMELINE_INFO_PATH = 'timeline'
    switchover._zk.get_switchover_record.return_value = ({}, 3)
    switchover._log = MagicMock()
    switchover._lock = MagicMock()
    switchover.state = MagicMock(return_value={})
    with patch('src.utils.time.time', return_value=100):
        assert switchover._initiate_switchover('primary', 5, 'candidate') is True

    record = switchover._zk.write_switchover_record.call_args.args[0]
    assert record['protocol_version'] == 2
    assert record['operation_id']
    assert record['hostname'] == 'primary'
    assert record['started_at'] == 100
    assert 'deadline_at' not in record


def test_perform_returns_failure_without_waiting_for_new_primary():
    switchover = Switchover.__new__(Switchover)
    switchover._zk = MagicMock()
    switchover._zk.get_alive_hosts.return_value = []
    switchover._plan = {'primary': 'primary', 'timeline': 1}
    switchover._new_primary = 'candidate'
    switchover._conf = MagicMock()
    switchover._log = MagicMock()
    switchover._initiate_switchover = MagicMock(return_value=True)
    switchover.in_progress = MagicMock(return_value=False)
    switchover.state = MagicMock(return_value={
        'progress': 'failed', 'info': {'failure_reason': 'timeout'},
    })
    switchover._wait_for_primary = MagicMock()

    assert switchover.perform(timeout=1) is False
    switchover._wait_for_primary.assert_not_called()
