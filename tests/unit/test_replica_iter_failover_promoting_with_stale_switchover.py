# coding: utf8
"""Red unit test: replica_iter stuck in failover 'promoting' when stale switchover record exists.

Reproduces dead_primary_switchover.feature:53 ("dead primary switchover will happen"):

Sequence:
  1. A switchover was initiated (record written to ZK, phase=scheduled).
  2. Primary (pg1) dies — both switchover and failover are now active.
  3. Failover election runs: pg3 wins, acquires PRIMARY lock, writes 'promoting'.
  4. The stale switchover record (phase=scheduled, timeline matching) remains in ZK.
  5. In replica_iter for pg3:
     a. _check_replica_switchover() returns True (switchover record found, timelines match).
     b. Inside the switchover block: phase=scheduled, no candidate → return False ("waiting").
     c. The failover guard at line 1006 is NEVER reached.
     d. _run_failover_step() is never called → pg3 never promotes → stuck forever.

Fix: add an early failover-winner guard BEFORE the switchover block:
if failover state is active (promoting/checkpointing/creating_slots) AND this node
holds the primary lock → call _run_failover_step() immediately, bypassing switchover logic.
"""
from unittest.mock import MagicMock, patch

from src.failover import FailoverPhase


_MY_HOST = 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'
_OLD_PRIMARY = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
_TIMELINE = 1


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
        do_consecutive_primary_switch=False,
        max_allowed_switchover_lag_ms=0,
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
    inst.last_zk_host_stat_write = 0.0
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    inst._executor = MagicMock()
    # ZK path constants
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    inst.zk.SWITCHOVER_RECORD_PATH = 'switchover_record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    inst.zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    return inst


def _zk_state_with_stale_switchover():
    """ZK state where:
    - pg3 holds the primary lock (winner of failover election)
    - failover_state = 'promoting'
    - stale switchover record (phase=scheduled, timeline=1, no candidate)
    """
    return {
        'alive': True,
        # pg3 won failover and now holds the primary lock
        'lock_holder': _MY_HOST,
        # Stale switchover record from before the primary died
        'switchover_record': {
            'hostname': _OLD_PRIMARY,
            'timeline_info': _TIMELINE,
            'phase': 'scheduled',
            'candidate': None,
            'side_replicas': [],
        },
        'switchover_version': 1,
        'timeline_info': _TIMELINE,
        # Failover state: pg3 won election, must promote
        'failover_state': 'promoting',
        'failover_must_be_reset': False,
        'replics_info': [],
        'epoch_manager': None,
        'election_winner': _MY_HOST,
    }


def _replica_db_state():
    """DB state for pg3 — still a replica (promote not run yet)."""
    return {
        'alive': True,
        'running': True,
        'role': 'replica',
        'timeline': _TIMELINE,
        # primary_fqdn matches switchover hostname — causes _check_replica_switchover to return True
        'primary_fqdn': _OLD_PRIMARY,
        'replics_info': [],
    }


class TestReplicaIterPromotingWithStaleSwitchover:
    """Failover winner (holding primary lock, failover_state=promoting) must promote
    even when a stale switchover record (phase=scheduled) exists in ZK.

    Without the fix: _check_replica_switchover() returns True, the switchover
    block issues 'return False' (waiting), and _run_failover_step() is never
    called → failover stuck in 'promoting' forever (dead_primary_switchover.feature:53).

    With the fix: an early guard before the switchover block detects that
    failover is active and this node holds the lock → calls _run_failover_step()
    immediately, bypassing the stale switchover record.
    """

    def test_failover_winner_promotes_despite_stale_scheduled_switchover(self):
        """Failover winner + promoting state + stale scheduled switchover → _run_failover_step called.

        This is the core regression from dead_primary_switchover.feature:53:
        pg3 (winner) holds the primary lock and failover_state='promoting', but
        a stale switchover record (phase=scheduled) causes _check_replica_switchover()
        to return True and the switchover block returns False before the failover
        guard can trigger.
        """
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = _TIMELINE
        # pg3 holds the primary lock (winner of failover)
        inst.zk.get_current_lock_holder.return_value = _MY_HOST
        # ZK timeline matches (so _check_my_timeline_sync passes)
        inst.zk.get_timeline.return_value = _TIMELINE

        db_state = _replica_db_state()
        zk_state = _zk_state_with_stale_switchover()

        inst._run_failover_step = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname', return_value=_MY_HOST), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql3_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            assert inst.handle_failover(db_state, zk_state) is True

        # MUST call _run_failover_step: pg3 is the winner, holds the primary lock,
        # and failover_state='promoting' → must execute DoFailover (promote).
        # Without the fix, _check_replica_switchover() intercepts and returns False
        # before reaching the failover guard, so _run_failover_step is never called.
        inst._run_failover_step.assert_called_once_with(
            FailoverPhase.PROMOTING,
            db_state,
            zk_state,
            must_reset=False,
        )

    def test_non_winner_replica_with_stale_switchover_waits_in_failover_handler(self):
        _OTHER_HOST = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = _TIMELINE
        # pg3 holds the lock, but THIS node is pg2 (loser)
        inst.zk.get_current_lock_holder.return_value = _MY_HOST
        inst.zk.get_timeline.return_value = _TIMELINE

        db_state = _replica_db_state()
        zk_state = _zk_state_with_stale_switchover()

        inst._run_failover_step = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname', return_value=_OTHER_HOST), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql2_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            assert inst.handle_failover(db_state, zk_state) is True

        inst._run_failover_step.assert_called_once_with(
            FailoverPhase.PROMOTING,
            db_state,
            zk_state,
            must_reset=False,
        )
