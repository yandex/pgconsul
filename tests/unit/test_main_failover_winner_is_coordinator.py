# coding: utf8
"""Red test: winner-is-coordinator deadlock in _run_failover_step.

Reproduces MDB-41951 behave failure: failover_with_network_inconsistency
feature, scenario "Failover will happen". When the failover winner is also
the coordinator (holds ELECTION_MANAGER_LOCK_PATH), _run_failover_step
routes the node to FailoverCoordinatorMachine. In phase winner_selected the
coordinator only waits for the primary lock holder (empty Plan), so the
winner never acquires the primary lock and never promotes — failover stalls
forever.

The fix: when the coordinator IS the winner, it must run the participant
plan (AcquireLock + transition to PROMOTING) instead of the coordinator
wait-for-lock-holder plan.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.commands import AcquireLock, FailoverTransitionTo, Promote, ReleaseLock
from src.failover import FailoverMachine, FailoverObservation, FailoverPhase


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
        use_lwaldump=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='2',
        stream_from=None,
        autofailover=True,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        do_consecutive_primary_switch=False,
        max_allowed_switchover_lag_ms=0,
        allow_potential_data_loss=False,
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
    inst._replication_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    inst._failover_machine = FailoverMachine()
    inst._executor = MagicMock()
    # Capture which machine + observation the executor was called with.
    inst._executor.last_plan = None

    def _run(machine, obs):
        plan = machine.plan(obs)
        inst._executor.last_plan = plan
        return bool(plan)

    inst._executor.run.side_effect = _run
    inst._executor.set_iteration_state = MagicMock()
    return inst


class TestWinnerIsCoordinatorPromotes:
    """When the winner is also the coordinator, the node must promote itself.

    The coordinator's plan_winner_selected only waits for the primary lock
    holder (empty Plan). If the winner == coordinator, the participant plan
    (AcquireLock + TransitionTo(PROMOTING)) must run instead — otherwise the
    winner never acquires the primary lock and failover stalls forever.
    """

    def test_winner_coordinator_runs_participant_plan(self):
        """Winner-is-coordinator must produce AcquireLock, not empty Plan."""
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
            votes={my_host: (100, 2)},
            alive_hosts=[my_host, 'host3'],
            replics_info=[],
            host_lsn=100,
            host_priority=2,
            last_failover_ts=None,
            last_primary_availability_ts=None,
            is_primary_unreachable=True,
            is_replaying_wal=False,
            failover_started_ts=1.0,
            downtime_started_ts=1.0,
            zk_timeline=1,
            local_timeline=1,
            allow_data_loss=False,
            quorum_size=2,
            current_time=2.0,
        )

        # This node holds the election manager lock → is_coordinator=True.
        inst.zk.get_current_lock_holder.return_value = my_host
        inst.zk.get_election_winner.return_value = my_host
        inst._build_failover_observation = MagicMock(return_value=observation)

        inst._run_failover_step(
            FailoverPhase.WINNER_SELECTED,
            {'role': 'replica', 'timeline': 1},
            zk_state,
            must_reset=False,
        )

        # The produced plan must contain AcquireLock + TransitionTo(PROMOTING).
        plan = inst._executor.last_plan
        cmd_types = [type(c).__name__ for c in plan]
        assert 'AcquireLock' in cmd_types, (
            f'Winner must acquire the primary lock; got plan={cmd_types}'
        )
        assert 'FailoverTransitionTo' in cmd_types
        acquire = [c for c in plan if isinstance(c, AcquireLock)][0]
        assert acquire.timeout == 0
        transition = [c for c in plan if isinstance(c, FailoverTransitionTo)][0]
        assert transition.phase == FailoverPhase.PROMOTING

    @pytest.mark.parametrize(
        ('role', 'expected_command'),
        [('primary', Promote), ('replica', ReleaseLock)],
    )
    def test_failed_winner_coordinator_resolves_its_primary_lock(
        self,
        role,
        expected_command,
    ):
        my_host = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        observation = FailoverObservation(
            phase=FailoverPhase.FAILED,
            my_hostname=my_host,
            role=role,
            lock_holder=my_host,
            is_coordinator=True,
            election_winner=my_host,
            votes={},
            alive_hosts=[my_host],
            replics_info=[],
            host_lsn=100,
            host_priority=2,
            last_failover_ts=None,
            last_primary_availability_ts=None,
            is_primary_unreachable=True,
            is_replaying_wal=False,
            failover_started_ts=1.0,
            downtime_started_ts=1.0,
            zk_timeline=1,
            local_timeline=1,
            allow_data_loss=False,
            quorum_size=1,
            current_time=2.0,
        )

        plan = FailoverMachine().plan(observation)

        assert isinstance(plan[0], expected_command)
