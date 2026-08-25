# coding: utf8
"""The blocking handler runs only the candidate machine on a candidate."""

from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul


_PRIMARY = 'primary.example'
_CANDIDATE = 'candidate.example'


@pytest.mark.parametrize('phase', ['candidate_found', 'promoted'])
def test_candidate_machine_does_not_fall_through_to_replica_logic(phase):
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst._executor = MagicMock()
    inst._sw_machine = MagicMock()
    inst._cand_machine = MagicMock()
    observation = MagicMock()
    inst._build_switchover_observation = MagicMock(return_value=observation)
    inst._return_to_cluster = MagicMock()
    inst.zk.SWITCHOVER_STATE_PATH = 'state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'root'
    inst.zk.SWITCHOVER_CANDIDATE = 'candidate'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'side'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'

    zk_state = {
        'state': phase,
        'root': {'hostname': _PRIMARY, 'timeline': 1},
        'candidate': _CANDIDATE,
        'lock_holder': _PRIMARY,
    }
    with patch('src.main.helpers.get_hostname', return_value=_CANDIDATE):
        assert inst.handle_switchover({'role': 'replica'}, zk_state) is True

    inst._executor.run.assert_called_once_with(inst._cand_machine, observation)
    inst._return_to_cluster.assert_not_called()
