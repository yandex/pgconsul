# coding: utf8
"""Switchover is handled before role-based iteration, including dead PG."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul


_PRIMARY = 'primary.example'


def _instance():
    inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(primary_switch_disable_archive_restore=False)
    inst._executor = MagicMock()
    inst._sw_machine = MagicMock()
    inst._cand_machine = MagicMock()
    inst._timings = MagicMock()
    inst._local_states = {'switchover_primary': MagicMock()}
    inst._local_states['switchover_primary'].read.return_value = None
    inst.zk.PRIMARY_LOCK_PATH = 'master'
    inst.zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'
    return inst


def _state(phase):
    return {
        'lock_holder': _PRIMARY,
        'switchover/record': {
            'hostname': _PRIMARY, 'timeline': 1, 'phase': phase,
            'candidate': 'candidate.example', 'side_replicas': [],
        },
        'switchover_version': 1,
        'timeline': 1,
    }


@pytest.mark.parametrize('phase', ['pooler_stopped', 'pg_stopped'])
def test_dead_primary_is_handled_by_blocking_switchover(phase):
    inst = _instance()
    observation = MagicMock()
    inst._build_switchover_observation = MagicMock(return_value=observation)

    with patch('src.main.helpers.get_hostname', return_value=_PRIMARY):
        assert inst.handle_switchover(
            {'alive': False, 'role': 'primary'}, _state(phase),
        ) is True

    inst._executor.run.assert_called_once_with(inst._sw_machine, observation)
    inst.zk.release_if_hold.assert_not_called()


def test_observation_does_not_query_dead_postgres():
    inst = _instance()
    inst.zk.get_current_lock_holder.return_value = _PRIMARY
    inst.zk.get_ha_replics.return_value = []

    with patch('src.main.helpers.get_hostname', return_value=_PRIMARY):
        assert inst.handle_switchover(
            {'alive': False, 'role': 'primary'}, _state('pg_stopped'),
        ) is True

    inst.db.get_replics_info.assert_not_called()
    inst.db.get_role.assert_not_called()
    inst._executor.run.assert_called_once()
