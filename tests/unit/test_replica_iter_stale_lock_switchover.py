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
    inst.zk.SWITCHOVER_RECORD_PATH = 'record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'

    zk_state = {
        'record': {
            'hostname': 'primary.example', 'timeline': 1, 'phase': phase,
            'candidate': 'candidate.example',
        },
        'version': 1,
        'lock_holder': 'primary.example',
    }
    with patch('src.main.helpers.get_hostname', return_value='replica.example'):
        assert inst.handle_switchover(
            {'role': 'replica', 'primary_fqdn': 'candidate.example'}, zk_state,
        ) is True

    inst._return_to_cluster.assert_not_called()
    inst.change_primary.assert_not_called()
