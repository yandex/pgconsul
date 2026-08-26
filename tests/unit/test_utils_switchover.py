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
