# coding: utf8
"""
Red unit test for MDB-41951: dead_iter prematurely releases the leader lock
during an active switchover.

Reproduces the bug from anywhere_switchover.feature:132 (@switchover_failed_promote):
  - Old primary reaches pg_stopped (PG stopped, lock still held).
  - On the next iteration PG is dead → run_iteration dispatches to dead_iter.
  - dead_iter unconditionally calls release_if_hold(PRIMARY_LOCK_PATH).
  - The candidate acquires the lock and promotes (or fails to promote).
  - The old primary restarts PG and races with the candidate.

The fix: dead_iter must NOT release the leader lock while a switchover is
in progress (switchover state is one of the active phases). The switchover
state machine (PrimarySwitchoverMachine) owns lock release in plan_pg_stopped
/ plan_primary_shut.
"""
from unittest.mock import MagicMock, patch

import pytest


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
        priority='100',
        stream_from=None,
        autofailover=False,
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
    inst._master_lost_ts = None
    inst._is_single_node = False
    inst._slot_manager = MagicMock()
    inst._replication_manager = MagicMock()
    inst._timings = MagicMock()
    inst._maintenance = MagicMock()
    inst._maintenance.is_in_maintenance = False
    inst._local_states = {'switchover_primary': MagicMock()}
    inst._local_states['switchover_primary'].read.return_value = None
    inst.last_zk_host_stat_write = 0.0
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    # Switchover state machine (dead_iter may call it when PG is dead)
    inst._executor = MagicMock()
    inst._sw_machine = MagicMock()
    inst._build_switchover_observation = MagicMock(return_value=MagicMock())
    # ZK path constants
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover_state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover_root'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'switchover_side_replicas'
    inst.zk.SWITCHOVER_CANDIDATE = 'switchover_candidate'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.CURRENT_PROMOTING_HOST = 'current_promoting_host'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    return inst


def _dead_zk_state(switchover_state='pg_stopped'):
    """ZK state where PG is dead but switchover is active."""
    return {
        'alive': True,
        'lock_holder': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
        'switchover_state': switchover_state,
        'switchover_root': {'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'},
        'timeline_info': 1,
        'failover_state': None,
        'current_promoting_host': None,
        'failover_must_be_reset': False,
    }


def _dead_db_state():
    """DB state where local PostgreSQL is dead."""
    return {
        'alive': False,
        'running': False,
        'role': 'primary',
        'timeline': 1,
    }


class TestDeadIterDoesNotReleaseLockDuringSwitchover:
    """dead_iter must not release the leader lock while a switchover is active.

    Reproduces: anywhere_switchover.feature:132 (@switchover_failed_promote)
    """

    @pytest.mark.parametrize('phase', ['sync_set', 'initiated', 'candidate_found',
                                        'pooler_stopped', 'pg_stopped', 'primary_shut'])
    def test_does_not_release_lock_during_active_switchover(self, phase):
        """dead_iter must not call release_if_hold when switchover is in progress.

        The old primary stopped PG in pg_stopped phase. On the next iteration
        PG is dead → dead_iter. The switchover state machine owns lock release
        (in plan_pg_stopped / plan_primary_shut), not dead_iter.
        """
        inst = _make_instance()
        inst.db.role = 'primary'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
        inst.zk.get_host_op.return_value = None

        db_state = _dead_db_state()
        zk_state = _dead_zk_state(switchover_state=phase)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql1_1.pgconsul_pgconsul_net'):
            inst.dead_iter(db_state, zk_state, is_in_terminal_state=True)

        # The fix: release_if_hold must NOT be called during active switchover.
        inst.zk.release_if_hold.assert_not_called()

    def test_releases_lock_when_no_switchover(self):
        """dead_iter still releases the lock when there is no switchover (normal failover)."""
        inst = _make_instance()
        inst.db.role = 'primary'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = None
        inst.zk.get_host_op.return_value = None

        db_state = _dead_db_state()
        zk_state = _dead_zk_state(switchover_state=None)
        zk_state['switchover_root'] = None
        zk_state['lock_holder'] = None

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql1_1.pgconsul_pgconsul_net'):
            inst.dead_iter(db_state, zk_state, is_in_terminal_state=True)

        # Without switchover, release_if_hold should be called (normal behavior).
        inst.zk.release_if_hold.assert_called_once_with(inst.zk.PRIMARY_LOCK_PATH)


class TestDeadIterRunsSwitchoverMachineWhenPGLost:
    """dead_iter must run the switchover state machine when PG is dead during
    an active switchover (pg_stopped → primary_shut transition).

    Reproduces: anywhere_switchover.feature:132 (@switchover_failed_promote)
    Bug: dead_iter's switchover guard prevents lock release but never calls
    PrimarySwitchoverMachine.plan() to advance pg_stopped → primary_shut.
    The old primary gets stuck in an infinite loop: PG dead → dead_iter →
    guard → return None → next iteration → dead_iter → guard → ...
    """

    @pytest.mark.parametrize('phase', ['pg_stopped', 'pooler_stopped'])
    def test_runs_switchover_machine_when_pg_dead(self, phase):
        """dead_iter must call executor.run(sw_machine) when switchover is
        active and PG is dead, so the state machine can advance to primary_shut.
        """
        inst = _make_instance()
        inst.db.role = 'primary'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
        inst.zk.get_host_op.return_value = None

        db_state = _dead_db_state()
        zk_state = _dead_zk_state(switchover_state=phase)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql1_1.pgconsul_pgconsul_net'):
            inst.dead_iter(db_state, zk_state, is_in_terminal_state=True)

        # The fix: dead_iter must call executor.run(sw_machine, obs) to
        # advance the switchover state machine (pg_stopped → primary_shut).
        inst._executor.run.assert_called_once()
        args = inst._executor.run.call_args[0]
        assert args[0] is inst._sw_machine
        # Lock must NOT be released by dead_iter directly.
        inst.zk.release_if_hold.assert_not_called()


class TestDeadIterObservationBuilderSurvivesDeadPG:
    """dead_iter must build the switchover observation without PG reads when
    PG is dead.

    Reproduces: anywhere_switchover.feature:132 (@switchover_failed_promote)
    Bug: _build_switchover_observation calls _get_streaming_replicas() (which
    queries db.get_replics_info) and SwitchoverObservation.build() calls
    db.get_role(). When PG is dead, both raise PostgresConnectionError, which
    propagates to run_iteration and restarts the iteration — the state machine
    never advances (pg_stopped → primary_shut), causing an infinite loop.
    """

    def test_observation_builder_does_not_raise_when_pg_dead(self):
        """dead_iter must not raise PostgresConnectionError from the observation
        builder when PG is dead — the state machine must still be called.
        """
        from src.exceptions import PostgresConnectionError
        from src.main import Pgconsul

        inst = _make_instance()
        inst.db.role = 'primary'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
        inst.zk.get_host_op.return_value = None
        # Do NOT mock _build_switchover_observation — use the real one.
        # Restore the real method (overridden by _make_instance MagicMock).
        inst._build_switchover_observation = Pgconsul._build_switchover_observation.__get__(inst, Pgconsul)

        # PG is dead: db.get_replics_info and db.get_role must raise.
        inst.db.get_replics_info.side_effect = PostgresConnectionError('Local conn is dead')
        inst.db.get_role.side_effect = PostgresConnectionError('Local conn is dead')
        # ZK reads used by SwitchoverObservation.build().
        inst.zk.get_failover_state.return_value = None
        inst.zk.get_last_failover_time.return_value = None
        inst.zk.get_last_switchover_time.return_value = None
        inst.zk.get_ha_replics.return_value = []
        inst.zk.get_switchover_state.return_value = 'pg_stopped'
        inst.zk.get_members.return_value = []
        inst.zk.is_host_alive.return_value = True
        inst._timings.get_start.return_value = None

        db_state = _dead_db_state()
        zk_state = _dead_zk_state(switchover_state='pg_stopped')

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql1_1.pgconsul_pgconsul_net'):
            # Must not raise PostgresConnectionError.
            inst.dead_iter(db_state, zk_state, is_in_terminal_state=True)

        # The state machine must be called despite PG being dead.
        inst._executor.run.assert_called_once()
        args = inst._executor.run.call_args[0]
        assert args[0] is inst._sw_machine
        # Lock must NOT be released by dead_iter directly.
        inst.zk.release_if_hold.assert_not_called()
