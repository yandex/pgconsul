# coding: utf8
"""Invalid switchover metadata is resolved by the blocking state machine."""

from unittest.mock import MagicMock, patch

from src.main import Pgconsul
from src.switchover import SwitchoverPhase


def test_invalid_phase_is_routed_as_failed():
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst._executor = MagicMock()
    inst._sw_machine = MagicMock()
    inst._cand_machine = MagicMock()
    inst._build_switchover_observation = MagicMock(return_value=MagicMock())
    inst.zk.SWITCHOVER_RECORD_PATH = 'record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'

    zk_state = {
        'record': {'hostname': 'old-primary', 'timeline': 1, 'phase': 'broken'},
        'version': 1,
        'lock_holder': 'new-primary',
    }
    with patch('src.main.helpers.get_hostname', return_value='replica'):
        assert inst.handle_switchover({'role': 'replica'}, zk_state) is True

    record = inst._build_switchover_observation.call_args.args[0]
    assert record.phase == SwitchoverPhase.FAILED
    inst._executor.run.assert_called_once()
