# coding: utf8
"""
Red unit tests for MDB-41951: two new bugs discovered in the failed-promote
switchover scenario (anywhere_switchover.feature:132 @switchover_failed_promote).

Bug #8 — replica_iter: FQDN mismatch bypasses is_failed() check
  After a failed switchover the old primary (postgresql1) comes back as a
  replica streaming from the ex-candidate (postgresql3).  At that point
  db_state['primary_fqdn'] == 'postgresql3' but switchover.hostname == 'postgresql1'.
  _check_replica_switchover() returns False (FQDN mismatch) so the is_failed()
  guard added in report-37 is *never reached*.  The code falls through to the
  ordinary failover path which also fails — see Bug #9.

Bug #9 — _can_do_failover: is_host_unreachable(check_primary=False) blocks failover
  When replica_iter falls through to holder-is-None it calls
  _accept_failover() without switchover_in_progress=True.
  _can_do_failover() then calls is_host_unreachable(check_primary=False).
  Without target_session_attrs=primary the check connects to postgresql3 (alive
  as a replica after the failed promote) and SELECT 42 succeeds → the method
  returns False (host reachable) → "primary still accessible" → failover aborted.

Fixes:
  Fix #8 — replica_iter: add an early FAILED-switchover guard *before*
    _check_replica_switchover(), so that FQDN mismatch cannot hide it.
  Fix #9 — _can_do_failover: skip is_host_unreachable when
    switchover_in_progress=True (we already know the old primary shut down).
"""
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Shared helper — reuse factory from the earlier test file if importable,
# otherwise define it inline.
# ---------------------------------------------------------------------------

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
        autofailover=True,   # enabled, as in the BDD test


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



        change_replication_type=False,
        sync_replication_in_maintenance=False,
        promote_checkpoint_sql=None,



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
    inst._cand_machine = MagicMock()
    inst._sw_machine = MagicMock()
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
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    return inst


# ZK state: switchover FAILED, hostname=postgresql1 (old primary), no lock holder.
def _failed_switchover_zk_state_fqdn_mismatch():
    """
    Simulates the state seen in the real cluster after a failed promote:
    - switchover.hostname = 'postgresql1'  (the original primary)
    - No lock holder
    - The calling replica's db_state['primary_fqdn'] = 'postgresql3'
      (postgresql1 restarted as replica streaming from postgresql3).
    """
    return {
        'alive': True,
        'lock_holder': None,
        'switchover_state': 'failed',
        'switchover_root': {
            'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',  # old primary
            'timeline_info': 1,
            'destination': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
        },
        'switchover_candidate': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
        'timeline_info': 1,
        'failover_state': None,
        'current_promoting_host': None,
        'failover_must_be_reset': False,
        'replics_info': [],
    }


def _replica_db_state_after_rewind():
    """
    DB state for postgresql2 (or any surviving replica) after the failed
    switchover.  primary_fqdn points to postgresql3 (the ex-candidate that
    is now a replica again), NOT to postgresql1.
    This is the FQDN mismatch that defeats _check_replica_switchover.
    """
    return {
        'alive': True,
        'running': True,
        'role': 'replica',
        'timeline': 1,
        'primary_fqdn': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net',  # NOT postgresql1
    }


# ---------------------------------------------------------------------------
# Bug #8 — FQDN mismatch hides is_failed() guard
# ---------------------------------------------------------------------------

class TestReplicaIterFqdnMismatch:
    """replica_iter must fall back to failover even when db_state['primary_fqdn']
    does not match switchover.hostname (FQDN mismatch after rewind).

    Before the fix: _check_replica_switchover() returns False due to FQDN
    mismatch, so the is_failed() guard at line 916 is never reached; the code
    falls through to the ordinary failover path which also fails (Bug #9).

    After the fix: an early check outside _check_replica_switchover() catches
    FAILED + no-lock-holder and calls _accept_failover(switchover_in_progress=True).
    """

    def test_replica_iter_falls_over_on_fqdn_mismatch_failed_switchover(self):
        """With FQDN mismatch, replica_iter must still call _accept_failover
        when switchover phase is FAILED and no one holds the lock.
        """
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = None
        inst.zk.get_host_op.return_value = None

        db_state = _replica_db_state_after_rewind()    # primary_fqdn=postgresql3
        zk_state = _failed_switchover_zk_state_fqdn_mismatch()  # hostname=postgresql1

        inst._run_failover_step = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql2_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql2_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.replica_iter(db_state, zk_state)

        # Fix #8: _run_failover_step must be called regardless of FQDN mismatch.
        inst._run_failover_step.assert_called_once_with(
            db_state, zk_state, switchover_in_progress=True,
        )


# TestCanDoFailoverSwitchoverInProgress removed — _can_do_failover is deprecated (ADR-0007 §7).
# The switchover_in_progress gate now lives in FailoverCoordinatorMachine._gates_pass().
