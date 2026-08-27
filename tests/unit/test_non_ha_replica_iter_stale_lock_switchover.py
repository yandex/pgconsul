# coding: utf8
"""A cascading replica already following the candidate does not rewind."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul


_OLD_PRIMARY = 'primary.example'
_CANDIDATE = 'candidate.example'
_REPLICA = 'cascade.example'


@pytest.mark.parametrize('phase', ['initiated', 'primary_shut'])
def test_no_return_to_cluster_when_already_following_candidate(phase):
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(
        stream_from=_OLD_PRIMARY,
        primary_switch_disable_archive_restore=False,
    )
    inst._return_to_cluster = MagicMock()
    inst.zk.SWITCHOVER_RECORD_PATH = 'record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'

    zk_state = {
        'record': {
            'hostname': _OLD_PRIMARY, 'timeline': 1, 'phase': phase,
            'candidate': _CANDIDATE, 'side_replicas': [_REPLICA],
        },
        'version': 1,
        'lock_holder': _OLD_PRIMARY,
    }
    with patch('src.main.helpers.get_hostname', return_value=_REPLICA):
        assert inst.handle_switchover(
            {'role': 'replica', 'primary_fqdn': _CANDIDATE}, zk_state,
        ) is True

    inst._return_to_cluster.assert_not_called()
