# coding: utf-8
"""
Red test: primary_iter must not restart the pooler during switchover shutdown
phases (pooler_stopped, pg_stopped, primary_shut).

Root cause (MDB-41951, pgconsul_util.feature:402):
  The state machine executes StopPooler and transitions to pooler_stopped,
  but primary_iter() then unconditionally calls ensure_pooler_started() in
  the "Repairs" section (main.py:579).  This restarts the pooler, letting
  new writes in through pgbouncer.  The candidate cannot catch up, and the
  switchover takes ~72 seconds — exceeding the 60-second test timeout.

Fix: skip ensure_pooler_started() when the switchover phase is pooler_stopped
or later (the state machine intentionally stopped the pooler).
"""
from unittest.mock import MagicMock, patch

import pytest

from src.switchover.types import SwitchoverPhase

_HOSTNAME = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
_CANDIDATE = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'


def _make_instance():
    """Create a Pgconsul instance with all dependencies mocked."""
    from src.main import PgconsulConfig, Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.db.role = 'primary'
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
    inst.last_zk_host_stat_write = 0.0
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    inst._timings = MagicMock()
    inst._executor = MagicMock()
    inst._sw_machine = MagicMock()
    # ZK path constants (must match zk_state dict keys).
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover_state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover_root'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'switchover_side_replicas'
    inst.zk.SWITCHOVER_CANDIDATE = 'switchover_candidate'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.CURRENT_PROMOTING_HOST = 'current_promoting_host'
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    inst.zk.SWITCHOVER_LOCK_PATH = 'switchover_lock'
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    return inst


def _zk_state(phase: str) -> dict:
    """Build zk_state dict for a primary with active switchover in *phase*."""
    return {
        'timeline_info': 1,
        'failover_must_be_reset': False,
        'failover_state': 'finished',
        'current_promoting_host': None,
        'switchover_root': {'hostname': _HOSTNAME, 'timeline': 1},
        'switchover_state': phase,
        'switchover_side_replicas': [],
        'switchover_candidate': _CANDIDATE,
    }


def _setup_guards(inst, phase: SwitchoverPhase):
    """Mock all early-return guards so primary_iter reaches the repair section."""
    inst.zk.get_host_op.return_value = None
    inst.zk.get_current_lock_holder.return_value = _HOSTNAME
    inst.zk.try_acquire_lock.return_value = True
    inst.zk.write_last_primary_availability_time.return_value = True
    inst.zk.get_switchover_state.return_value = phase.value
    inst.zk.get_ha_replics.return_value = None  # early return after repairs
    inst._store_replics_info = MagicMock(return_value=True)
    inst._verify_timeline = MagicMock(return_value=True)
    inst._build_switchover_observation = MagicMock(return_value=MagicMock())


class TestPrimaryIterPoolerRestart:
    """primary_iter must not restart the pooler during switchover shutdown.

    Reproduces pgconsul_util.feature:402 timeout (MDB-41951):
    The state machine stops the pooler (StopPooler command) and transitions
    to pooler_stopped, but primary_iter's repair section unconditionally
    calls ensure_pooler_started(), restarting the pooler.  New writes flow
    in, the candidate cannot catch up, and switchover exceeds the timeout.
    """

    @pytest.mark.parametrize('phase', [
        SwitchoverPhase.POOLER_STOPPED,
        SwitchoverPhase.PG_STOPPED,
        SwitchoverPhase.PRIMARY_SHUT,
    ])
    def test_does_not_restart_pooler_during_shutdown_phases(self, phase):
        """ensure_pooler_started must NOT be called when phase >= pooler_stopped."""
        inst = _make_instance()
        _setup_guards(inst, phase)

        with patch('src.main.helpers.get_hostname', return_value=_HOSTNAME):
            inst.primary_iter({'timeline': 1}, _zk_state(phase.value))

        # RED: this fails because ensure_pooler_started is called unconditionally.
        inst.db.ensure_pooler_started.assert_not_called()

    def test_restarts_pooler_when_no_switchover(self):
        """ensure_pooler_started IS called when there is no active switchover."""
        inst = _make_instance()
        _setup_guards(inst, SwitchoverPhase.SCHEDULED)
        # No active switchover — switchover_root is None.
        zk_state = {
            'timeline_info': 1,
            'failover_must_be_reset': False,
            'failover_state': 'finished',
            'current_promoting_host': None,
            'switchover_root': None,
        }
        with patch('src.main.helpers.get_hostname', return_value=_HOSTNAME):
            inst.primary_iter({'timeline': 1}, zk_state)

        inst.db.ensure_pooler_started.assert_called_once()

    def test_restarts_pooler_during_pre_shutdown_phases(self):
        """ensure_pooler_started IS called for phases before pooler_stopped."""
        inst = _make_instance()
        _setup_guards(inst, SwitchoverPhase.INITIATED)

        with patch('src.main.helpers.get_hostname', return_value=_HOSTNAME):
            inst.primary_iter({'timeline': 1}, _zk_state(SwitchoverPhase.INITIATED.value))

        inst.db.ensure_pooler_started.assert_called_once()
