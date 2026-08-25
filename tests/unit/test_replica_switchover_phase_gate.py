# coding: utf8
"""Side replicas follow a candidate only after candidate preparation starts."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.main import Pgconsul


_PRIMARY = 'primary.example'
_CANDIDATE = 'candidate.example'
_REPLICA = 'replica.example'


def _instance():
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(primary_switch_disable_archive_restore=False)
    inst._return_to_cluster = MagicMock()
    inst.zk.SWITCHOVER_STATE_PATH = 'state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'root'
    inst.zk.SWITCHOVER_CANDIDATE = 'candidate'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'side'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'
    return inst


def _state(phase):
    return {
        'state': phase,
        'root': {'hostname': _PRIMARY, 'timeline': 1},
        'candidate': _CANDIDATE,
        'lock_holder': _PRIMARY,
    }


def test_replica_waits_during_sync_set():
    inst = _instance()
    with patch('src.main.helpers.get_hostname', return_value=_REPLICA):
        assert inst.handle_switchover(
            {'role': 'replica', 'primary_fqdn': _PRIMARY}, _state('sync_set'),
        ) is True
    inst._return_to_cluster.assert_not_called()


def test_replica_follows_candidate_during_initiated():
    inst = _instance()
    with patch('src.main.helpers.get_hostname', return_value=_REPLICA):
        assert inst.handle_switchover(
            {'role': 'replica', 'primary_fqdn': _PRIMARY}, _state('initiated'),
        ) is True
    inst._return_to_cluster.assert_called_once_with(
        _CANDIDATE, 'replica', is_dead=False,
    )
