# encoding: utf-8
"""
Red test for MDB-41951 — spurious pg_rewind after primary change.

Reproduces kill_primary.feature:184 (@failover_archive):
  "Destroy primary with one replica in archive recovery"

Root cause: _return_to_cluster() calls _set_simple_primary_switch_try()
unconditionally after a single failed simple switch (main.py:1481).
The flag should only be set inside _simple_primary_switch() when
checks['primary_switch'] >= primary_switch_checks (after N failures),
as in the pre-refactoring code (commit 619062f^).

Consequence: after one failed simple switch to primary A, the flag is
set in ZK. When the primary changes to B (timeline diverges), the next
iteration sees simple_switch_tried=True + timelines diverge → REWIND,
even though simple switch to B was never attempted.
"""
from unittest.mock import MagicMock, patch


def _make_pgconsul():
    """Create a pgconsul instance bypassing __init__ entirely."""
    from src.main import PgconsulConfig
    with patch('src.main.pgconsul.__init__', return_value=None):
        from src.main import Pgconsul
        inst = Pgconsul.__new__(Pgconsul)

    inst.db = MagicMock()
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


        max_rewind_retries=3,

        do_consecutive_primary_switch=False,
        max_allowed_switchover_lag_ms=0,
        allow_potential_data_loss=False,
        close_detached_after=0.0,
        start_pooler=False,
        recovery_timeout=60.0,
        can_delayed=False,
        primary_switch_disable_archive_restore=True,
        primary_switch_checks=3,
        primary_switch_restart=False,



        change_replication_type=False,
        sync_replication_in_maintenance=False,
        promote_checkpoint_sql=None,



    )
    inst._timings = MagicMock()
    inst._maintenance = MagicMock()
    inst.zk = MagicMock()
    inst.checks = {'primary_switch': 0, 'rewind': 0}

    # Return-to-cluster callbacks (direct calls, no executor delegation).
    inst._simple_primary_switch = MagicMock(return_value=False)
    inst._ensure_restoring_wal = MagicMock()
    inst._rewind_from_source = MagicMock(return_value=None)
    inst._set_simple_primary_switch_try = MagicMock()
    inst._is_simple_primary_switch_tried = MagicMock(return_value=False)

    return inst


class TestSpuriousRewindOnPrimaryChange:
    """
    Reproduces kill_primary.feature:184 (@failover_archive) — postgresql3
    must NOT invoke pg_rewind when the simple_switch_tried flag was set
    by a single transient failure (not after N retries).
    """

    def test_single_simple_switch_failure_does_not_set_tried_flag(self):
        """
        Simple switch fails once (checks['primary_switch']=1 < threshold=3).
        _set_simple_primary_switch_try must NOT be called — the flag should
        only be set after primary_switch_checks consecutive failures, inside
        _simple_primary_switch().

        On the buggy code (main.py:1481) the flag IS set unconditionally → RED.
        After removing line 1481, the flag is NOT set → GREEN.
        """
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'

        # Replica state, timeline=1.
        inst._get_db_state = MagicMock(return_value={
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        })
        inst.db.get_state.return_value = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }

        inst.zk.noexcept_get.return_value = None
        inst.zk.get_failover_state.return_value = 'finished'
        inst.zk.get_timeline.return_value = 1  # matches local
        inst._is_simple_primary_switch_tried = MagicMock(return_value=False)
        inst._acquire_replication_source_slot_lock = MagicMock()

        # Simple switch fails (transient — e.g. archive recovery).
        inst._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False, skip_check=True)

        # After a single failure below the threshold, the flag must NOT be set.
        inst._set_simple_primary_switch_try.assert_not_called()

    def test_spurious_rewind_after_primary_change(self):
        """
        Two-iteration scenario reproducing the full @failover_archive bug:

        Iteration 1: simple switch to primary A fails once → flag must NOT
        be set (but on buggy code it IS set via line 1481).

        Iteration 2: primary changed to B, timelines diverge (local=1, zk=2).
        With the flag incorrectly set, decide_return_action returns REWIND
        instead of trying SIMPLE_SWITCH to the new primary.

        On the buggy code: _rewind_from_source IS called → RED.
        After fix: _simple_primary_switch is called (retry), _rewind_from_source is NOT → GREEN.
        """
        inst = _make_pgconsul()
        primary_a = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
        primary_b = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'

        # --- Iteration 1: simple switch to A fails ---
        inst._get_db_state = MagicMock(return_value={
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        })
        inst.db.get_state.return_value = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }
        inst.zk.noexcept_get.return_value = None
        inst.zk.get_failover_state.return_value = 'finished'
        inst.zk.get_timeline.return_value = 1  # matches local
        inst._is_simple_primary_switch_tried.return_value = False
        inst._acquire_replication_source_slot_lock = MagicMock()
        inst._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(primary_a, 'replica', is_dead=False, skip_check=True)

        # The flag must NOT be set after a single failure below threshold.
        # On buggy code, _set_simple_primary_switch_try was called (line 1481),
        # so we simulate the flag being set for iteration 2.
        flag_was_set = inst._set_simple_primary_switch_try.called
        inst._is_simple_primary_switch_tried.return_value = flag_was_set

        # --- Iteration 2: primary changed to B, timelines diverge ---
        inst.zk.get_timeline.return_value = 2  # new primary, diverges
        inst._simple_primary_switch.reset_mock()
        inst._rewind_from_source.reset_mock()

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(primary_b, 'replica', is_dead=False, skip_check=True)

        # After the fix: flag was NOT set → SIMPLE_SWITCH to B (not REWIND).
        # On buggy code: flag WAS set → REWIND (spurious).
        inst._rewind_from_source.assert_not_called()
