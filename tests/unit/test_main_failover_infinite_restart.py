# coding: utf8
"""Red test: infinite failover restart loop in _run_failover_step.

Reproduces MDB-41951 behave hang: async.feature:46 scenario
"No failover in allow_potential_data_loss=no mode -- @1.1 without
replication slots".

When sync_quorum is empty (async mode) and allow_potential_data_loss=no,
the promote-safe gate permanently fails. The cycle is:

  1. _run_failover_step: phase=None/FINISHED → become coordinator →
     write 'detected' to ZK.
  2. plan_detected: _is_promote_safe=False → TransitionTo(FAILED).
  3. plan_failed: ReleaseLock + ResetFailoverNode → writes 'finished'.
  4. Next iteration: primary still dead → phase=FINISHED → goto 1.

This creates an infinite loop: detected → failed → finished → detected → ...
In behave, the test hangs for 360s until timeout.

In main, _can_do_failover checks is_promote_safe BEFORE writing 'detected',
so failover never starts and failover_state stays None.

The fix: check promote-safe gate before writing 'detected' in
_run_failover_step (analog of main's _can_do_failover).
"""
from unittest.mock import MagicMock, patch

from src.commands import FailoverTransitionTo, ResetFailoverNode
from src.failover import (
    FailoverObservation,
    FailoverPhase,
    FailoverRecord,
)


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
        switchover_replica_turn_timeout=0.0,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        election_timeout=0,
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
    # Failover machines (constructed in Pgconsul.__init__, absent in __new__).
    from src.failover import FailoverCoordinatorMachine, FailoverParticipantMachine
    inst._failover_coord_machine = FailoverCoordinatorMachine()
    inst._failover_part_machine = FailoverParticipantMachine()
    inst._executor = MagicMock()
    inst._executor.set_iteration_state = MagicMock()
    return inst


class TestFailoverInfiniteRestart:
    """async mode + no data loss → failover must not restart infinitely.

    Reproduces the cycle: detected → FAILED → finished → detected → ...
    Each call to _run_failover_step represents one pgconsul iteration.
    The test calls _run_failover_step 3 times and asserts that 'detected'
    is written at most once (no restart after permanent failure).
    """

    def test_detected_not_rewritten_after_permanent_failure(self):
        """After detected→FAILED→finished, 'detected' must not be written again.

        Iteration 1: phase=None → become coordinator → write 'detected' →
                     plan_detected → _is_promote_safe=False → TransitionTo(FAILED).
        Iteration 2: phase=FAILED → plan_failed → ResetFailoverNode → 'finished'.
        Iteration 3: phase=FINISHED → become coordinator → write 'detected' again → BUG!

        With the bug, 'detected' is written twice (iterations 1 and 3),
        creating an infinite restart loop. After the fix, iteration 3 checks
        the promote-safe gate before writing 'detected' and bails out.
        """
        inst = _make_instance()
        my_host = 'host1'

        # ZK paths (MagicMock auto-creates attributes, but we need strings).
        inst.zk.FAILOVER_STATE_PATH = 'failover_state'
        inst.zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
        inst.zk.PRIMARY_LOCK_PATH = 'leader'

        # ZK state dict — persists across _run_failover_step calls.
        zk_state = {}

        # Track write_failover_state(DETECTED) calls.
        detected_writes = []

        def _write_failover_state(phase):
            zk_state['failover_state'] = str(phase)
            if phase == FailoverPhase.DETECTED:
                detected_writes.append(True)

        inst.zk.write_failover_state.side_effect = _write_failover_state

        # Always succeed at becoming coordinator (simulates acquiring
        # ELECTION_MANAGER_LOCK_PATH after previous coordinator released it).
        inst._try_become_failover_coordinator = MagicMock(return_value=True)

        # Patch FailoverRecord.from_zk_state to build from zk_state.
        def _from_zk_state(failover_state, zk):
            return FailoverRecord(phase=FailoverPhase.from_str(failover_state))

        # Patch _build_failover_observation to build from zk_state.
        # All gates pass except _is_promote_safe (empty sync_quorum = async mode).
        def _build_obs(db_state, zk_s, **kwargs):
            phase = FailoverPhase.from_str(zk_s.get('failover_state'))
            record = FailoverRecord(phase=phase)
            return FailoverObservation(
                record=record,
                my_hostname=my_host,
                role='replica',
                fallback_role='replica',
                lock_holder=None,
                is_coordinator=True,
                election_status=None,
                election_winner=None,
                votes={},
                ha_replics=frozenset({'host2', 'host3'}),
                alive_hosts=['host2', 'host3'],
                replics_info=[{'application_name': 'host2', 'state': 'streaming'}],
                host_lsn=100,
                host_priority=2,
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
                sync_quorum=[],  # async mode — promote permanently impossible
            )

        # Executor: simulate command execution by updating zk_state.
        def _run(machine, obs):
            plan = machine.plan(obs)
            for cmd in plan:
                if isinstance(cmd, FailoverTransitionTo):
                    zk_state['failover_state'] = str(cmd.phase)
                elif isinstance(cmd, ResetFailoverNode):
                    zk_state['failover_state'] = str(FailoverPhase.FINISHED)
            return bool(plan)

        inst._executor.run.side_effect = _run

        db_state = {'role': 'replica', 'timeline': 5}

        with patch('src.main.FailoverRecord.from_zk_state', side_effect=_from_zk_state):
            with patch('src.main.helpers.get_hostname', return_value=my_host):
                with patch.object(
                    inst, '_build_failover_observation', side_effect=_build_obs,
                ):
                    # Iteration 1: phase=None → write 'detected' →
                    #              plan_detected → FAILED
                    inst._run_failover_step(db_state, zk_state)
                    # Iteration 2: phase=FAILED → plan_failed →
                    #              ResetFailoverNode → 'finished'
                    inst._run_failover_step(db_state, zk_state)
                    # Iteration 3: phase=FINISHED → write 'detected' again → BUG!
                    inst._run_failover_step(db_state, zk_state)

        # After 3 iterations, 'detected' must have been written at most once.
        # With the bug: detected_writes = [True, True] (iterations 1 and 3).
        # After fix: detected_writes = [True] (iteration 1 only; iteration 3
        # checks gates before writing 'detected' and bails out).
        assert len(detected_writes) <= 1, (
            f"'detected' written {len(detected_writes)} times — infinite "
            f'restart loop. After detected→FAILED→finished, failover must '
            f'not restart when promote-safe gate permanently fails (async '
            f'mode, allow_potential_data_loss=no).'
        )
