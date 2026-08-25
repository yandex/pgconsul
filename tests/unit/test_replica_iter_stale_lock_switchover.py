# coding: utf8
"""An HA replica already following the candidate ignores the stale lock."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul


@pytest.mark.parametrize('phase', ['initiated', 'primary_shut'])
def test_no_role_logic_when_already_following_candidate(phase):
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(primary_switch_disable_archive_restore=False)
    inst._return_to_cluster = MagicMock()
    inst.change_primary = MagicMock()
    inst.zk.SWITCHOVER_STATE_PATH = 'state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'root'
    inst.zk.SWITCHOVER_CANDIDATE = 'candidate'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'side'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'

    zk_state = {
        'state': phase,
        'root': {'hostname': 'primary.example', 'timeline': 1},
        'candidate': 'candidate.example',
        'lock_holder': 'primary.example',
    }
    with patch('src.main.helpers.get_hostname', return_value='replica.example'):
        assert inst.handle_switchover(
            {'role': 'replica', 'primary_fqdn': 'candidate.example'}, zk_state,
        ) is True

    inst._return_to_cluster.assert_not_called()
    inst.change_primary.assert_not_called()
