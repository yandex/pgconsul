# coding: utf8
"""Failed switchover enters fallback through its own state machine."""

from unittest.mock import MagicMock, patch

from src.main import Pgconsul
from src.switchover import SwitchoverRoute


def test_failed_switchover_without_primary_runs_global_machine():
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst._executor = MagicMock()
    inst._sw_machine = MagicMock()
    inst._cand_machine = MagicMock()
    observation = MagicMock()
    inst._build_switchover_observation = MagicMock(return_value=observation)
    inst.zk.SWITCHOVER_RECORD_PATH = 'record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'

    zk_state = {
        'record': {
            'hostname': 'primary.example', 'timeline': 1, 'phase': 'failed',
            'candidate': 'candidate.example',
        },
        'version': 1,
        'lock_holder': None,
    }
    with patch('src.main.helpers.get_hostname', return_value='replica.example'):
        assert inst.handle_switchover({'role': 'replica'}, zk_state) is True

    assert inst._build_switchover_observation.call_args.kwargs['route'] == SwitchoverRoute.GLOBAL
    inst._executor.run.assert_called_once_with(inst._sw_machine, observation)
