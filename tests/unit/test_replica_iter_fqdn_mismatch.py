# coding: utf8
"""Regression test for failed switchover fallback with an FQDN mismatch."""

from unittest.mock import MagicMock

from src.commands import InitializeFailover, TransitionTo
from src.main import Pgconsul
from src.switchover import (
    PrimarySwitchoverMachine,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


def test_failed_switchover_starts_fallback_despite_replica_source_mismatch():
    """FAILED is global and must not be hidden by replica primary_fqdn."""
    inst = Pgconsul.__new__(Pgconsul)
    inst.zk = MagicMock()
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover_state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover_root'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'switchover_side_replicas'
    inst.zk.SWITCHOVER_CANDIDATE = 'switchover_candidate'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst._sw_machine = PrimarySwitchoverMachine()
    inst._cand_machine = MagicMock()
    inst._executor = MagicMock()

    old_primary = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
    candidate = 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'
    record = SwitchoverRecord(
        hostname=old_primary,
        timeline=1,
        phase=SwitchoverPhase.FAILED,
        candidate=candidate,
    )
    observation = SwitchoverObservation(
        record=record,
        my_hostname='pgconsul_postgresql2_1.pgconsul_pgconsul_net',
        role='replica',
        zk_timeline=1,
        last_role_transition_ts=None,
        ha_replics=frozenset(),
        replics_info=[],
        streaming_replicas=(),
        candidate_alive=None,
        lock_holder=None,
        switchover_started_ts=1.0,
        downtime_started_ts=1.0,
        all_side_replicas_turned=False,
        current_time=2.0,
    )
    inst._build_switchover_observation = MagicMock(return_value=observation)

    captured_plan = []

    def run(machine, obs):
        captured_plan.extend(machine.plan(obs))
        return True

    inst._executor.run.side_effect = run
    zk_state = {
        'lock_holder': None,
        'switchover_state': 'failed',
        'switchover_root': {
            'hostname': old_primary,
            'timeline_info': 1,
        },
        'switchover_candidate': candidate,
        'switchover_side_replicas': [],
    }
    db_state = {
        'role': 'replica',
        'timeline': 1,
        # The replica now follows the failed candidate, not the old primary.
        'primary_fqdn': candidate,
    }

    assert inst.handle_switchover(db_state, zk_state) is True

    assert [type(command) for command in captured_plan] == [
        InitializeFailover,
        TransitionTo,
    ]
    assert captured_plan[1].phase == SwitchoverPhase.FALLBACK
