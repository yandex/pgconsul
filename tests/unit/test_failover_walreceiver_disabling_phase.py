# encoding: utf-8
"""Red tests: WALRECEIVER_DISABLING phase separates gate check from walreceiver ops.

Reproduces: failover_with_network_inconsistency.feature:157 — replicas never vote
because coordinator is stuck in DETECTED phase after primary recovers.

Root cause: coordinator.plan_detected() re-checks is_primary_unreachable on every
iteration. When primary comes back (is_primary_unreachable=False), gates fail → []
returned → coordinator never transitions → participants loop forever in DETECTED
doing Sleep+DisableWalReceiver with no effect.

Fix: split DETECTED into two phases:
  DETECTED              — coordinator gate check only, transitions to WALRECEIVER_DISABLING
  WALRECEIVER_DISABLING — both coordinator and participant do sleep+disable,
                          coordinator transitions to GATES_PASSED, no re-check of gates

This ensures that once failover is committed (DETECTED written to ZK), the
sleep+disable step executes unconditionally regardless of primary availability.

BDD scenario: failover_with_network_inconsistency.feature:157
"""

import time

import pytest

from src.commands import DisableWalReceiver, FailoverTransitionTo, Log, Sleep
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


def _coord_obs(
    phase: FailoverPhase = FailoverPhase.DETECTED,
    is_primary_unreachable: bool = True,
    allow_data_loss: bool = True,
    alive_hosts: list | None = None,
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
        alive_hosts=alive_hosts if alive_hosts is not None else ['host2', 'host3'],
        replics_info=[{'application_name': 'host2', 'state': 'streaming'}],
        host_lsn=100,
        host_priority=1,
        last_failover_ts=None,
        last_primary_availability_ts=0.0,
        is_primary_unreachable=is_primary_unreachable,
        is_replaying_wal=False,
        switchover_in_progress=False,
        failover_timer_started=False,
        downtime_timer_started=False,
        zk_timeline=5,
        local_timeline=5,
        allow_data_loss=allow_data_loss,
        quorum_size=2,
        autofailover=True,
        current_time=time.time(),
    )


def _part_obs(
    phase: FailoverPhase = FailoverPhase.DETECTED,
    is_primary_unreachable: bool = True,
) -> FailoverObservation:
    record = FailoverRecord(phase=phase)
    return FailoverObservation(
        record=record,
        my_hostname='host2',
        role='replica',
        fallback_role=None,
        lock_holder=None,
        is_coordinator=False,
        election_status=None,
        election_winner=None,
        votes={},
        ha_replics=frozenset({'host1', 'host3'}),
        alive_hosts=['host1', 'host3'],
        replics_info=[],
        host_lsn=100,
        host_priority=1,
        last_failover_ts=None,
        last_primary_availability_ts=0.0,
        is_primary_unreachable=is_primary_unreachable,
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


def _cmd_types(plan: list) -> list[str]:
    return [type(cmd).__name__ for cmd in plan]


# ---------------------------------------------------------------------------
# Phase existence
# ---------------------------------------------------------------------------


class TestWalreceiverDisablingPhaseExists:
    """FailoverPhase.WALRECEIVER_DISABLING must exist."""

    def test_phase_enum_has_walreceiver_disabling(self) -> None:
        # Fails until WALRECEIVER_DISABLING is added to FailoverPhase.
        assert hasattr(FailoverPhase, 'WALRECEIVER_DISABLING'), (
            'FailoverPhase must have WALRECEIVER_DISABLING value'
        )

    def test_phase_value_is_string(self) -> None:
        assert str(FailoverPhase.WALRECEIVER_DISABLING) == 'walreceiver_disabling'

    def test_phase_is_active(self) -> None:
        """WALRECEIVER_DISABLING must be included in is_active() — failover is ongoing."""
        record = FailoverRecord(phase=FailoverPhase.WALRECEIVER_DISABLING)
        assert record.is_active()


# ---------------------------------------------------------------------------
# Coordinator: DETECTED → WALRECEIVER_DISABLING (not GATES_PASSED directly)
# ---------------------------------------------------------------------------


class TestCoordinatorDetectedTransition:
    """plan_detected must transition to WALRECEIVER_DISABLING (MDB-41951).

    plan_detected checks gates and transitions to WALRECEIVER_DISABLING.
    Walreceiver is disabled before voting; get_wal_receive_lsn falls back
    to pg_last_wal_receive_lsn when lwaldump crashes after disable.
    """

    def test_transitions_to_walreceiver_disabling(self) -> None:
        machine = FailoverCoordinatorMachine()
        obs = _coord_obs(phase=FailoverPhase.DETECTED)
        plan = machine.plan(obs)
        transitions = [c for c in plan if isinstance(c, FailoverTransitionTo)]
        assert transitions, 'Expected FailoverTransitionTo in plan_detected'
        assert transitions[-1].phase == FailoverPhase.WALRECEIVER_DISABLING, (
            f'Expected WALRECEIVER_DISABLING, got {transitions[-1].phase}'
        )

    def test_no_disable_walreceiver_in_detected(self) -> None:
        """DisableWalReceiver must NOT be in plan_detected — moved to WALRECEIVER_DISABLING."""
        machine = FailoverCoordinatorMachine()
        obs = _coord_obs(phase=FailoverPhase.DETECTED)
        plan = machine.plan(obs)
        assert not any(isinstance(c, DisableWalReceiver) for c in plan), (
            'DisableWalReceiver must not appear in plan_detected; '
            'it belongs to plan_walreceiver_disabling'
        )


# ---------------------------------------------------------------------------
# Coordinator: WALRECEIVER_DISABLING — sleep + disable + transition GATES_PASSED
# ---------------------------------------------------------------------------


class TestCoordinatorWalreceiverDisabling:
    """plan_walreceiver_disabling: sleep (optional) + disable + transition GATES_PASSED.

    This phase runs unconditionally — no gate re-check. Even if primary has
    recovered (is_primary_unreachable=False), the plan executes.
    """

    def test_includes_disable_walreceiver(self) -> None:
        machine = FailoverCoordinatorMachine()
        obs = _coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        assert any(isinstance(c, DisableWalReceiver) for c in plan), (
            'DisableWalReceiver must be in plan_walreceiver_disabling'
        )

    def test_transitions_to_gates_passed(self) -> None:
        # WALRECEIVER_DISABLING → GATES_PASSED (open registration, vote).
        # Walreceiver is disabled before voting; get_wal_receive_lsn falls
        # back to pg_last_wal_receive_lsn when lwaldump crashes (MDB-41951).
        machine = FailoverCoordinatorMachine()
        obs = _coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        transitions = [c for c in plan if isinstance(c, FailoverTransitionTo)]
        assert transitions, 'Expected FailoverTransitionTo in plan_walreceiver_disabling'
        assert transitions[-1].phase == FailoverPhase.GATES_PASSED, (
            f'Expected GATES_PASSED, got {transitions[-1].phase}'
        )

    def test_no_gate_recheck_when_primary_returned(self) -> None:
        """Even when is_primary_unreachable=False, plan must not be empty.

        This is the core of the bug: coordinator must proceed even if primary
        recovered, because failover is already committed (phase written to ZK).
        """
        machine = FailoverCoordinatorMachine()
        # Primary came back — in old code this caused plan_detected to return []
        obs = _coord_obs(
            phase=FailoverPhase.WALRECEIVER_DISABLING,
            is_primary_unreachable=False,
        )
        plan = machine.plan(obs)
        assert plan, (
            'plan_walreceiver_disabling must not be empty even when primary is reachable. '
            'Failover is committed — gates must not be re-checked here.'
        )
        assert any(isinstance(c, DisableWalReceiver) for c in plan)

    def test_sleep_before_disable_when_nonzero(self) -> None:
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=5.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        obs = _coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        sleeps = [c for c in plan if isinstance(c, Sleep)]
        assert sleeps, 'Expected Sleep in plan_walreceiver_disabling when sleep>0'
        assert sleeps[0].seconds == 5.0

    def test_no_sleep_when_zero(self) -> None:
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=0.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        obs = _coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        assert not any(isinstance(c, Sleep) for c in plan)

    def test_log_before_sleep_when_nonzero(self) -> None:
        """Log+Sleep must precede DisableWalReceiver (test log message preserved)."""
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=3.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        obs = _coord_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)

        debug_logs = [
            c for c in plan
            if isinstance(c, Log) and 'Sleep for test purposes' in c.message
        ]
        assert debug_logs, 'Expected debug Log with sleep message'

        log_idx = next(i for i, c in enumerate(plan) if isinstance(c, Log) and 'Sleep for test purposes' in c.message)
        sleep_idx = next(i for i, c in enumerate(plan) if isinstance(c, Sleep))
        disable_idx = next(i for i, c in enumerate(plan) if isinstance(c, DisableWalReceiver))
        assert log_idx < disable_idx, 'Log must precede DisableWalReceiver'
        assert sleep_idx < disable_idx, 'Sleep must precede DisableWalReceiver'


# ---------------------------------------------------------------------------
# Participant: DETECTED → empty (waits for coordinator to advance)
# ---------------------------------------------------------------------------


class TestParticipantDetectedEmpty:
    """Participant returns [] in DETECTED — waits for coordinator to advance.

    Participant must not act in DETECTED. Voting happens in REGISTRATION
    via plan_vote (after walreceiver is disabled in WALRECEIVER_DISABLING).
    """

    def test_participant_detected_empty(self) -> None:
        """Participant returns [] in DETECTED — no action."""
        machine = FailoverParticipantMachine()
        obs = _part_obs(phase=FailoverPhase.DETECTED)
        plan = machine.plan(obs)
        assert plan == [], f'Expected [] in DETECTED, got: {plan}'


# ---------------------------------------------------------------------------
# Participant: WALRECEIVER_DISABLING — sleep + disable (no transition)
# ---------------------------------------------------------------------------


class TestParticipantWalreceiverDisabling:
    """Participant does sleep+disable in WALRECEIVER_DISABLING.

    Participant has no FailoverTransitionTo — coordinator owns phase transitions.
    """

    def test_includes_disable_walreceiver(self) -> None:
        machine = FailoverParticipantMachine()
        obs = _part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        assert any(isinstance(c, DisableWalReceiver) for c in plan), (
            f'Participant must DisableWalReceiver in WALRECEIVER_DISABLING. Got: {plan}'
        )

    def test_no_transition(self) -> None:
        """Participant must not write FailoverTransitionTo — only coordinator transitions."""
        machine = FailoverParticipantMachine()
        obs = _part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        assert not any(isinstance(c, FailoverTransitionTo) for c in plan), (
            'Participant must not write FailoverTransitionTo in WALRECEIVER_DISABLING'
        )

    def test_sleep_when_nonzero(self) -> None:
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=7.0)
        machine = FailoverParticipantMachine(config=cfg)
        obs = _part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        sleeps = [c for c in plan if isinstance(c, Sleep)]
        assert sleeps, f'Expected Sleep in participant WALRECEIVER_DISABLING. Got: {plan}'
        assert sleeps[0].seconds == 7.0

    def test_no_sleep_when_zero(self) -> None:
        cfg = FailoverMachineConfig(sleep_before_disable_walreceiver=0.0)
        machine = FailoverParticipantMachine(config=cfg)
        obs = _part_obs(phase=FailoverPhase.WALRECEIVER_DISABLING)
        plan = machine.plan(obs)
        assert not any(isinstance(c, Sleep) for c in plan)

    def test_executes_even_when_primary_returned(self) -> None:
        """Participant must disable walreceiver even when primary is reachable.

        The key test: even after primary recovered (is_primary_unreachable=False),
        participant must still execute DisableWalReceiver in WALRECEIVER_DISABLING.
        This is the BDD scenario: primary comes back, but replicas already committed
        to failover and must vote (which requires walreceiver to be disabled first).
        """
        machine = FailoverParticipantMachine()
        obs = _part_obs(
            phase=FailoverPhase.WALRECEIVER_DISABLING,
            is_primary_unreachable=False,
        )
        plan = machine.plan(obs)
        assert any(isinstance(c, DisableWalReceiver) for c in plan), (
            'Participant must disable walreceiver even if primary has recovered'
        )
