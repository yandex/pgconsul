# coding: utf8
"""Regression tests for independent failover coordinator and participant steps.

Reproduces MDB-41951 behave failure: failover_with_network_inconsistency
feature, scenario "Failover will happen". When the failover winner is also
the coordinator and the winner run on the same host. Both paths must still run
independently: the coordinator only changes global state, while the participant
does local primary ownership and promotion work.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.commands import ClearFailoverDesiredPrimary, Decision, Promote
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
)


def _plan(machine, observation):
    return machine.decide(observation).plan


def _make_instance():
    from src.main import PgconsulConfig, Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='',
        working_dir='/tmp',
        iteration_timeout=0.0,
        quorum_commit=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='2',
        stream_from=None,
        autofailover=True,
        switchover_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        close_detached_after=0.0,
        start_pooler=False,
        recovery_timeout=0.0,
        can_delayed=False,
        primary_switch_disable_archive_restore=False,
        primary_switch_checks=0,
        primary_switch_restart=False,
        primary_unavailability_timeout=0.0,
        walreceiver_disable_timeout=0.0,
        min_failover_timeout=0.0,
        change_replication_type=False,
        sync_replication_in_maintenance=False,
        promote_checkpoint_sql=None,
        failure_name=None,
        failure_count=100000000,
        sleep_before_disable_walreceiver=0.0,
        election_lsn_read_sleep=0.0,
        election_loser_timeout=0,
    )
    inst._master_lost_ts = 0.0
    inst._durability_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    inst._failover_coordinator = FailoverCoordinatorMachine()
    inst._failover_participant = FailoverParticipantMachine()
    inst._executor = MagicMock()
    inst._executor.plans = []

    def _run(machine, obs):
        plan = _plan(machine, obs)
        inst._executor.plans.append((machine, obs, plan))
        return Decision(plan, True)

    inst._executor.run.side_effect = _run
    inst._executor.set_iteration_state = MagicMock()
    return inst


class TestWinnerIsCoordinatorPromotes:
    """A winner-coordinator waits for the top-level ownership reconciler."""

    def test_winner_coordinator_has_no_leader_lock_command(self):
        """Winner-is-coordinator leaves lock acquisition to the main loop."""
        inst = _make_instance()
        my_host = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'

        # ZK state: failover is active, winner_selected.
        zk_state = {
            inst.zk.FAILOVER_STATE_PATH: FailoverPhase.WINNER_SELECTED,
        }
        inst.zk.FAILOVER_STATE_PATH = 'failover_state'
        inst.zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
        inst.zk.PRIMARY_LOCK_PATH = 'leader'
        inst.zk.ELECTION_WINNER_PATH = 'election_winner'

        observation = FailoverObservation(
            phase=FailoverPhase.WINNER_SELECTED,
            my_hostname=my_host,
            role='replica',
            lock_holder=None,
            is_coordinator=True,
            election_winner=my_host,
            votes={my_host: 100},
            replics_info=[],
            last_failover_ts=None,
            last_primary_availability_ts=None,
            is_primary_unreachable=True,
            failover_started_ts=1.0,
            downtime_started_ts=1.0,
            zk_timeline=1,
            local_timeline=1,
            quorum_size=2,
            failover_version='version-1',
            current_time=2.0,
        )

        # This node holds the election manager lock → is_coordinator=True.
        inst.zk.get_current_lock_holder.return_value = my_host
        inst.zk.get_election_winner.return_value = my_host
        inst._build_failover_observation = MagicMock(return_value=observation)

        inst._run_failover_coordinator(observation)
        inst._run_failover_participant(observation, {'role': 'replica', 'timeline': 1})

        assert [machine for machine, _, _ in inst._executor.plans] == [
            inst._failover_coordinator,
            inst._failover_participant,
        ]
        assert all(snapshot is observation for _, snapshot, _ in inst._executor.plans)

    @pytest.mark.parametrize(
        ('role', 'expected_command'),
        [('primary', Promote), ('replica', ClearFailoverDesiredPrimary)],
    )
    def test_failed_winner_coordinator_resolves_its_primary_lock(
        self,
        role,
        expected_command,
    ):
        my_host = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        observation = FailoverObservation(
            phase=FailoverPhase.RESOLVING_WINNER,
            my_hostname=my_host,
            role=role,
            lock_holder=my_host,
            is_coordinator=True,
            election_winner=my_host,
            votes={},
            replics_info=[],
            last_failover_ts=None,
            last_primary_availability_ts=None,
            is_primary_unreachable=True,
            failover_started_ts=1.0,
            downtime_started_ts=1.0,
            zk_timeline=1,
            local_timeline=1,
            quorum_size=1,
            failover_version='version-1',
            current_time=2.0,
        )

        plan = _plan(FailoverParticipantMachine(), observation)

        assert isinstance(plan[0], expected_command)
