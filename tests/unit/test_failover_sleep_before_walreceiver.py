
# encoding: utf-8
"""Red test: sleep_before_disable_walreceiver must produce Log+Sleep plan.

Reproduces: failover_with_network_inconsistency.feature:143-144 — both replicas
must log "Sleep for test purposes before disabling walreceiver".

Root cause: after refactoring to the state-machine ADR-0007, the
logging.debug + time.sleep before disable_wal_receiver were dropped from
coordinator.plan_detected() and participant never got a plan for DETECTED phase.
"""

import time

import pytest

from src.commands import DisableWalReceiver, Log, Sleep
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
    FailoverRecord,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEBUG_MSG = 'Sleep for test purposes before disabling walreceiver'


def _make_coord_obs(
    phase: FailoverPhase = FailoverPhase.DETECTED,
    is_primary_unreachable: bool = True,
    last_failover_ts: float | None = None,
    last_primary_availability_ts: float = 0.0,
    zk_timeline: int = 5,
    local_timeline: int = 5,
    allow_data_loss: bool = False,
    autofailover: bool = True,
    failover_timer_started: bool = False,
    downtime_timer_started: bool = False,
    host_lsn: int = 100,
    host_priority: int = 1,
) -> FailoverObservation:
    record = FailoverRecord(phase=phase)
    return FailoverObservation(
        record=record,
        my_hostname='host1',
        role='replica',
        fallback_role=None,
        lock_holder=None,
        is_coordinator=True,
        election_status=None,
        election_winner=None,
        votes={},
        ha_replics=frozenset({'host2', 'host3'}),
        alive_hosts=['host2', 'host3'],
        replics_info=[
            {'application_name': 'host2', 'state': 'streaming'},
        ],
        host_lsn=host_lsn,
        host_priority=host_priority,
        last_failover_ts=last_failover_ts,
        last_primary_availability_ts=last_primary_availability_ts,
        is_primary_unreachable=is_primary_unreachable,
        is_replaying_wal=False,
        switchover_in_progress=False,
        failover_timer_started=failover_timer_started,
        downtime_timer_started=downtime_timer_started,
        zk_timeline=zk_timeline,
        local_timeline=local_timeline,
        allow_data_loss=allow_data_loss,
        quorum_size=2,
        autofailover=autofailover,
        current_time=time.time(),
    )


def _make_part_obs(
    phase: FailoverPhase = FailoverPhase.DETECTED,
    host_lsn: int = 100,
    host_priority: int = 1,
) -> FailoverObservation:
    record = FailoverRecord(phase=phase)
    return FailoverObservation(
        record=record,
        my_hostname='host2',
        role='replica',
        fallback_role=None,
        lock_holder=None,
        is_coordinator=False,  # participant
        election_status=None,
        election_winner=None,
        votes={},
        ha_replics=frozenset({'host1', 'host3'}),
        alive_hosts=['host1', 'host3'],
        replics_info=[],
        host_lsn=host_lsn,
        host_priority=host_priority,
        last_failover_ts=None,
        last_primary_availability_ts=0.0,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        switchover_in_progress=False,
        failover_timer_started=False,
        downtime_timer_started=False,
        zk_timeline=5,
        local_timeline=5,
        allow_data_loss=False,
        quorum_size=2,
        autofailover=True,
        current_time=time.time(),
    )


def _plan_types(plan: list) -> list[str]:
    return [type(cmd).__name__ for cmd in plan]


# ---------------------------------------------------------------------------
# Coordinator tests
# ---------------------------------------------------------------------------

class TestCoordinatorSleepBeforeWalreceiver:
    """Coordinator.plan_walreceiver_disabling must emit Log+Sleep before DisableWalReceiver.

    After the phase split, sleep+disable moved from plan_detected to
    plan_walreceiver_disabling. Tests updated accordingly.
    """

    def test_no_sleep_when_zero(self) -> None:
        """When sleep=0, plan_walreceiver_disabling must NOT include Log or Sleep."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=0.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        plan = machine.plan_walreceiver_disabling(_make_coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING))

        msgs = [cmd.message for cmd in plan if isinstance(cmd, Log)]
        assert not any(_DEBUG_MSG in m for m in msgs), (
            'No debug Log expected when sleep=0'
        )
        sleeps = [cmd for cmd in plan if isinstance(cmd, Sleep)]
        assert sleeps == [], 'No Sleep expected when sleep=0'

    def test_log_and_sleep_when_nonzero(self) -> None:
        """When sleep=5, plan_walreceiver_disabling must include Log + Sleep(5) before DisableWalReceiver."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=5.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        plan = machine.plan_walreceiver_disabling(_make_coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING))

        # Log with the expected message must be present
        debug_logs = [
            cmd for cmd in plan
            if isinstance(cmd, Log) and _DEBUG_MSG in cmd.message
        ]
        assert debug_logs, (
            f'Expected Log containing "{_DEBUG_MSG}" in coordinator plan_walreceiver_disabling. '
            f'Got plan: {plan}'
        )

        # Sleep(5) must be present
        sleeps = [cmd for cmd in plan if isinstance(cmd, Sleep)]
        assert sleeps, 'Expected Sleep in coordinator plan_walreceiver_disabling'
        assert sleeps[0].seconds == 5.0, f'Expected Sleep(5), got {sleeps[0]}'

        # Log+Sleep must come before DisableWalReceiver
        log_idx = next(i for i, cmd in enumerate(plan) if isinstance(cmd, Log) and _DEBUG_MSG in cmd.message)
        sleep_idx = next(i for i, cmd in enumerate(plan) if isinstance(cmd, Sleep))
        disable_idx = next(i for i, cmd in enumerate(plan) if isinstance(cmd, DisableWalReceiver))
        assert log_idx < disable_idx, 'Log must precede DisableWalReceiver'
        assert sleep_idx < disable_idx, 'Sleep must precede DisableWalReceiver'


# ---------------------------------------------------------------------------
# Participant tests  (the bug: DETECTED phase returns empty plan currently)
# ---------------------------------------------------------------------------

class TestParticipantSleepBeforeWalreceiver:
    """Participant.plan must emit Log+Sleep+DisableWalReceiver in WALRECEIVER_DISABLING phase.

    After phase split: participant no longer acts in DETECTED (returns []).
    Sleep+disable moved to WALRECEIVER_DISABLING, which coordinator writes
    after gates pass. Both replicas log the debug message in this new phase.
    failover_with_network_inconsistency.feature:143-144 checks BOTH replicas.
    """

    def test_participant_walreceiver_disabling_emits_log_when_sleep_nonzero(self) -> None:
        """With sleep=5, participant plan for WALRECEIVER_DISABLING must include Log."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=5.0)
        machine = FailoverParticipantMachine(config=cfg)
        plan = machine.plan(_make_part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING))

        debug_logs = [
            cmd for cmd in plan
            if isinstance(cmd, Log) and _DEBUG_MSG in cmd.message
        ]
        assert debug_logs, (
            f'Expected participant Log containing "{_DEBUG_MSG}" for WALRECEIVER_DISABLING phase. '
            f'Got plan: {plan}'
        )

    def test_participant_walreceiver_disabling_emits_sleep_when_nonzero(self) -> None:
        """With sleep=5, participant plan for WALRECEIVER_DISABLING must include Sleep(5)."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=5.0)
        machine = FailoverParticipantMachine(config=cfg)
        plan = machine.plan(_make_part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING))

        sleeps = [cmd for cmd in plan if isinstance(cmd, Sleep)]
        assert sleeps, f'Expected Sleep in participant WALRECEIVER_DISABLING plan. Got: {plan}'
        assert sleeps[0].seconds == 5.0

    def test_participant_walreceiver_disabling_emits_disable_wal_receiver(self) -> None:
        """Participant must disable walreceiver in WALRECEIVER_DISABLING phase."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=5.0)
        machine = FailoverParticipantMachine(config=cfg)
        plan = machine.plan(_make_part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING))

        disables = [cmd for cmd in plan if isinstance(cmd, DisableWalReceiver)]
        assert disables, f'Expected DisableWalReceiver in participant WALRECEIVER_DISABLING plan. Got: {plan}'

    def test_participant_walreceiver_disabling_no_debug_when_zero(self) -> None:
        """With sleep=0, participant WALRECEIVER_DISABLING plan must NOT contain debug Log."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=0.0)
        machine = FailoverParticipantMachine(config=cfg)
        plan = machine.plan(_make_part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING))

        debug_logs = [
            cmd for cmd in plan
            if isinstance(cmd, Log) and _DEBUG_MSG in cmd.message
        ]
        assert debug_logs == [], 'No debug Log expected when sleep=0'

    def test_participant_detected_empty(self) -> None:
        """Participant returns [] in DETECTED — waits for coordinator to advance.

        Participant must not act in DETECTED. Voting happens in REGISTRATION
        via plan_vote (after walreceiver is disabled in WALRECEIVER_DISABLING).
        """
        machine = FailoverParticipantMachine()
        plan = machine.plan(_make_part_obs(phase=FailoverPhase.DETECTED, host_lsn=42))
        assert plan == [], f'Expected [] in DETECTED, got: {plan}'
