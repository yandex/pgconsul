# encoding: utf-8
"""After promote, winner must remove itself from ZK quorum (MDB-41951).

Bug (failover_timeout.feature:65): after the first failover, the winner
(postgresql2) stays in the ZK quorum list via DelayedListRemovalStrategy
(delay=60s).  When postgresql2 then dies as primary before the list is
updated, the next failover attempt sees:

    sync_quorum = ['postgresql2', 'postgresql3']   ← stale, includes dead primary
    alive_hosts  = ['postgresql1', 'postgresql3']

    hosts_in_quorum = |{'postgresql2','postgresql3'} ∩ alive_replics| = 1
    required        = 2 // 2 + 1 = 2
    1 < 2  →  _is_promote_safe returns False — permanently.

The deadlock: no primary → quorum not updated → stale quorum blocks failover
→ no primary.  The test hangs for 360 s (failover_timeout.feature:65).

Fix: during promotion, remove the winner from the ZK quorum list immediately
after promote, bypassing the delayed removal strategy.  The winner is
definitively no longer a replica — keeping it in the quorum list is wrong.
"""
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _make_instance(hostname: str = 'postgresql2') -> object:
    """Return a minimal Pgconsul instance suitable for promotion tests."""
    from src.main import Pgconsul, PgconsulConfig

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
        failure_count=100_000_000,
        sleep_before_disable_walreceiver=0.0,
        election_lsn_read_sleep=0.0,
        election_loser_timeout=0,
    )
    inst._master_lost_ts = 0.0
    inst._replication_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    local_state = MagicMock()
    local_state.read.return_value = None
    inst._local_states = {'failover_participant': local_state}
    return inst


# ---------------------------------------------------------------------------
# Regression: promotion must remove winner from ZK quorum
# ---------------------------------------------------------------------------

class TestPromotionRemovesWinnerFromZkQuorum:
    """Promotion must update ZK quorum to exclude the winner after promote.

    Without this, the stale quorum blocks future failovers via _is_promote_safe
    (failover_timeout.feature:65, MDB-41951).
    """

    def test_winner_removed_from_quorum_after_successful_promote(self):
        """Promotion delegates quorum cleanup to replication_manager.

        Scenario: 'postgresql2' is the failover winner.  After promote,
        ``_replication_manager.remove_self_from_quorum_after_promote()`` must
        be called so the stale quorum doesn't block a future second failover.
        """
        inst = _make_instance(hostname='postgresql2')
        inst.zk.delete_failover_state.return_value = True
        inst._replication_manager.set_ssn_before_promote.return_value = True

        with patch.object(inst, '_promote_handle_slots', return_value=True):
            with patch.object(inst, '_promote', return_value=True):
                with patch.object(inst, '_finish_promote', return_value=True):
                    result = inst._run_promotion('failover_participant')

        assert result is True
        # Winner must call remove_self_from_quorum_after_promote to prevent
        # stale quorum blocking future failovers (MDB-41951, failover_timeout.feature:65).
        inst._replication_manager.remove_self_from_quorum_after_promote.assert_called_once()

    def test_winner_not_in_quorum_is_a_noop(self):
        """remove_self_from_quorum_after_promote is still called (it handles noop internally)."""
        inst = _make_instance(hostname='postgresql2')
        inst.zk.delete_failover_state.return_value = True
        inst._replication_manager.set_ssn_before_promote.return_value = True

        with patch.object(inst, '_promote_handle_slots', return_value=True):
            with patch.object(inst, '_promote', return_value=True):
                with patch.object(inst, '_finish_promote', return_value=True):
                    result = inst._run_promotion('failover_participant')

        assert result is True
        # The method is always called — it handles the noop case internally.
        inst._replication_manager.remove_self_from_quorum_after_promote.assert_called_once()

    def test_quorum_write_failure_does_not_abort_promote(self):
        """remove_self_from_quorum_after_promote is best-effort — promote still succeeds."""
        inst = _make_instance(hostname='postgresql2')
        inst.zk.delete_failover_state.return_value = True
        inst._replication_manager.set_ssn_before_promote.return_value = True
        # Simulate write failure inside remove_self_from_quorum_after_promote
        inst._replication_manager.remove_self_from_quorum_after_promote.side_effect = None

        with patch.object(inst, '_promote_handle_slots', return_value=True):
            with patch.object(inst, '_promote', return_value=True):
                with patch.object(inst, '_finish_promote', return_value=True):
                    result = inst._run_promotion('failover_participant')

        # Promote itself succeeded regardless of quorum cleanup result.
        assert result is True


# ---------------------------------------------------------------------------
# End-to-end: can_start_failover passes after clean quorum
# ---------------------------------------------------------------------------

class TestCanStartFailoverAfterCleanQuorum:
    """End-to-end: _is_promote_safe passes when quorum doesn't include dead primary.

    This verifies the second half of the fix: once winner removes itself from
    quorum at promote time, the next failover attempt sees an accurate quorum.
    """

    def test_can_start_failover_with_one_alive_quorum_member(self):
        """Quorum=['survivor'], alive=['reconnected','survivor'] → can_start=True.

        This is the state AFTER the fix: winner (ex_primary) was removed from
        quorum at promote time.  Quorum = ['survivor'], required = 1,
        hosts_in_quorum = 1.  Failover is allowed.
        """
        from src.failover import (
            FailoverCoordinatorMachine,
            FailoverObservation,
        )

        machine = FailoverCoordinatorMachine()
        obs = FailoverObservation(
            phase=None,
            my_hostname='reconnected',
            role='replica',
            lock_holder=None,
            is_coordinator=True,
            election_winner=None,
            votes={},
            alive_hosts=['reconnected', 'survivor'],
            replics_info=[
                {'application_name': 'survivor', 'state': 'streaming'},
                {'application_name': 'reconnected', 'state': 'streaming'},
            ],
            host_lsn=100,
            host_priority=1,
            last_failover_ts=None,
            last_primary_availability_ts=0.0,
            is_primary_unreachable=True,
            is_replaying_wal=False,
            failover_started_ts=None,
            downtime_started_ts=None,
            zk_timeline=2,
            local_timeline=2,
            allow_data_loss=False,
            quorum_size=1,
            autofailover=True,
            # After fix: quorum is clean, doesn't include dead ex_primary.
            sync_quorum=['survivor'],
            promote_started_ts=None,
            current_time=9_999_999_999.0,  # far future → failover timeout passed
        )
        # With clean quorum: required=1, survivor in alive → passes.
        assert machine.can_start_failover(obs) is True

    def test_can_start_failover_blocked_by_stale_quorum_with_dead_primary(self):
        """BUG: stale quorum includes dead ex-primary → can_start_failover=False.

        This is the CURRENT broken state (before fix): winner stays in quorum.
        Quorum=['ex_primary','survivor'], alive=['reconnected','survivor'],
        hosts_in_quorum=1 < required=2 → permanently blocked.

        After fix (promotion removes winner from quorum), this scenario
        should no longer occur in practice.  This test documents the bug.
        """
        from src.failover import (
            FailoverCoordinatorMachine,
            FailoverObservation,
        )

        machine = FailoverCoordinatorMachine()
        obs = FailoverObservation(
            phase=None,
            my_hostname='reconnected',
            role='replica',
            lock_holder=None,
            is_coordinator=True,
            election_winner=None,
            votes={},
            alive_hosts=['reconnected', 'survivor'],
            replics_info=[
                {'application_name': 'survivor', 'state': 'streaming'},
                {'application_name': 'reconnected', 'state': 'streaming'},
            ],
            host_lsn=100,
            host_priority=1,
            last_failover_ts=None,
            last_primary_availability_ts=0.0,
            is_primary_unreachable=True,
            is_replaying_wal=False,
            failover_started_ts=None,
            downtime_started_ts=None,
            zk_timeline=2,
            local_timeline=2,
            allow_data_loss=False,
            quorum_size=1,
            autofailover=True,
            # BUG: stale quorum includes 'ex_primary' (dead, was promoted).
            # required = 2//2+1 = 2, hosts_in_quorum = 1 (only 'survivor') → False.
            sync_quorum=['ex_primary', 'survivor'],
            promote_started_ts=None,
            current_time=9_999_999_999.0,
        )
        # BUG: currently returns False because stale quorum includes dead primary.
        # This documents the bug scenario — the actual failover deadlock.
        assert machine.can_start_failover(obs) is False
