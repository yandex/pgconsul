# coding: utf8
"""
Tests for _drop_stale_switchover stale criterion (ADR-0005 §4).

A switchover record is stale ONLY if it cannot belong to a resumable process:
  - record timeline < current PG timeline, OR
  - state is 'failed', OR
  - timeline info is missing (None).

States 'initiated' / 'candidate_found' with matching timeline are NOT stale
and must not be cleaned up.
"""
from unittest.mock import MagicMock, patch

from src.main import Pgconsul, PgconsulConfig


def _make_instance():
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
    inst._timings = MagicMock()
    inst.zk.SWITCHOVER_LOCK_PATH = 'switchover/lock'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    return inst


class TestDropStaleSwitchover:
    """_drop_stale_switchover follows ADR-0005 §4 stale criterion."""

    def test_initiated_matching_timeline_not_stale(self):
        """initiated with matching timeline must NOT be cleaned up."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': 5,
        }
        inst.zk.get_switchover_state.return_value = 'initiated'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_not_called()

    def test_candidate_found_matching_timeline_not_stale(self):
        """candidate_found with matching timeline must NOT be cleaned up."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': 5,
        }
        inst.zk.get_switchover_state.return_value = 'candidate_found'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_not_called()

    def test_scheduled_matching_timeline_not_stale(self):
        """scheduled with matching timeline must NOT be cleaned up."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': 5,
        }
        inst.zk.get_switchover_state.return_value = 'scheduled'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_not_called()

    def test_failed_state_is_stale(self):
        """failed state must be cleaned up regardless of timeline."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': 5,
        }
        inst.zk.get_switchover_state.return_value = 'failed'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_called_once()

    def test_older_timeline_is_stale(self):
        """timeline < current PG timeline must be cleaned up."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': 3,
        }
        inst.zk.get_switchover_state.return_value = 'initiated'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_called_once()

    def test_missing_timeline_info_is_stale(self):
        """timeline_info is None must be cleaned up."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': None,
        }
        inst.zk.get_switchover_state.return_value = 'scheduled'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_called_once()

    def test_no_switchover_info_does_nothing(self):
        """No switchover record → no cleanup."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = None

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.cleanup_switchover.assert_not_called()

    def test_lock_not_acquired_does_nothing(self):
        """If switchover lock cannot be acquired, do nothing."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = False

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.get_switchover_primary_info.assert_not_called()
        inst.zk.cleanup_switchover.assert_not_called()

    def test_lock_always_released(self):
        """Switchover lock is released even on success path."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = None

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.release_lock.assert_called_once_with('switchover/lock')

    def test_lock_released_on_stale_cleanup(self):
        """Switchover lock is released after cleanup of stale record."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_switchover_primary_info.return_value = {
            'hostname': 'host1',
            'timeline_info': 3,
        }
        inst.zk.get_switchover_state.return_value = 'initiated'

        inst._drop_stale_switchover({'timeline': 5})

        inst.zk.release_lock.assert_called_once_with('switchover/lock')
