# encoding: utf-8
"""Unit tests for failover command dispatch in CommandExecutor (ADR-0007)."""

import logging
from unittest.mock import MagicMock, patch

from src.command_executor import CommandExecutor
from src.commands import (
    CleanupFailover,
    FailoverTransitionTo,
    ForceReleasePrimaryLock,
    PrepareFailoverVote,
    Promote,
    PromotionResult,
    WriteElectionWinner,
    WriteFailoverParticipantState,
    WriteLastFailoverTime,
)
from src.failover import FailoverPhase
from src.zk import ZookeeperException


def _make_executor():
    zk = MagicMock()
    promote = MagicMock(return_value=PromotionResult.SUCCESS)
    local_states = {'failover_participant': MagicMock()}
    executor = CommandExecutor(
        zk=zk,
        db=MagicMock(),
        timings=MagicMock(),
        promote=promote,
        return_to_cluster=MagicMock(),
        local_states=local_states,
    )
    executor._local_operation_id = 'operation-1'
    return executor, zk, promote


class TestWriteLastFailoverTime:
    def test_dispatches_to_zk(self):
        executor, zk, _ = _make_executor()
        zk.write_last_failover_time.return_value = True

        assert executor._dispatch(WriteLastFailoverTime()) is True

        zk.write_last_failover_time.assert_called_once_with()

    def test_returns_false_on_zk_failure(self):
        executor, zk, _ = _make_executor()
        zk.write_last_failover_time.return_value = False

        assert executor._dispatch(WriteLastFailoverTime()) is False


class TestPrepareFailoverVote:
    def test_fences_sources_before_reading_and_publishing_flush_lsn(self):
        executor, zk, _ = _make_executor()
        db = executor._db
        events = []
        db.stop_restoring_wal.side_effect = lambda: events.append('restore') or True
        db.disable_wal_receiver.side_effect = lambda _: events.append('receiver') or True
        db.get_timeline.side_effect = lambda: events.append('timeline') or 5
        db.get_wal_flush_lsn.side_effect = lambda: events.append('lsn') or 123
        zk.write_election_vote.side_effect = lambda *_, **__: events.append('vote') or True

        assert executor._dispatch(PrepareFailoverVote(7, 5.0, 'version-1', 5)) is True

        db.stop_restoring_wal.assert_called_once_with()
        db.disable_wal_receiver.assert_called_once_with(5.0)
        db.get_wal_flush_lsn.assert_called_once_with()
        zk.write_election_vote.assert_called_once_with(
            123,
            7,
            failover_version='version-1',
            timeline=5,
        )
        assert events == ['restore', 'receiver', 'timeline', 'lsn', 'vote']

    def test_unfenced_data_loss_vote_keeps_wal_sources_running(self, caplog):
        executor, zk, _ = _make_executor()
        db = executor._db
        db.get_timeline.return_value = 5
        db.get_wal_flush_lsn.return_value = 123
        zk.write_election_vote.return_value = True

        with caplog.at_level(logging.WARNING):
            assert executor._dispatch(PrepareFailoverVote(
                7, 5.0, 'version-1', 5, fence_wal_sources=False,
            )) is True

        db.stop_restoring_wal.assert_not_called()
        db.disable_wal_receiver.assert_not_called()
        assert 'unfenced failover vote' in caplog.text

    def test_debug_sleep_happens_after_lsn_read(self):
        executor, zk, _ = _make_executor()
        db = executor._db
        events = []
        db.stop_restoring_wal.return_value = True
        db.disable_wal_receiver.return_value = True
        db.get_timeline.return_value = 5
        db.get_wal_flush_lsn.side_effect = lambda: events.append('lsn') or 123
        zk.write_election_vote.side_effect = lambda *_, **__: events.append('vote') or True

        with patch('src.command_executor.time.sleep', side_effect=lambda _: events.append('sleep')) as sleep:
            assert executor._dispatch(PrepareFailoverVote(7, 5.0, 'version-1', 5, 3.0)) is True

        sleep.assert_called_once_with(3.0)
        assert events == ['lsn', 'sleep', 'vote']

    def test_stopped_old_primary_publishes_timeline_without_reading_lsn(self):
        executor, zk, _ = _make_executor()
        db = executor._db
        db.get_timeline.return_value = 6
        zk.write_election_vote.return_value = True

        assert executor._dispatch(
            PrepareFailoverVote(7, 5.0, 'version-1', 6, timeline_only=True)
        ) is True

        db.stop_restoring_wal.assert_not_called()
        db.disable_wal_receiver.assert_not_called()
        db.get_timeline.assert_called_once_with()
        db.get_wal_flush_lsn.assert_not_called()
        zk.write_election_vote.assert_called_once_with(
            0, 7, failover_version='version-1', timeline=6,
        )


class TestWriteFailoverParticipantState:
    def test_publishes_local_progress(self):
        executor, zk, _ = _make_executor()
        zk.write_failover_participant_state.return_value = True

        assert executor._dispatch(WriteFailoverParticipantState('promoted', 'version-1')) is True

        zk.write_failover_participant_state.assert_called_once_with('promoted', 'version-1')


class TestWriteElectionWinner:
    def test_dispatches_to_zk(self):
        executor, zk, _ = _make_executor()
        zk.write_election_winner.return_value = True

        assert executor._dispatch(WriteElectionWinner(winner='host2')) is True

        zk.write_election_winner.assert_called_once_with('host2')

    def test_returns_false_on_zk_failure(self):
        executor, zk, _ = _make_executor()
        zk.write_election_winner.return_value = False

        assert executor._dispatch(WriteElectionWinner(winner='host2')) is False


class TestForceReleasePrimaryLock:
    def test_coordinator_deletes_only_expected_holder(self):
        executor, zk, _ = _make_executor()
        zk.is_lock_holder.return_value = True
        zk.force_release_primary_lock.return_value = True

        assert executor._dispatch(
            ForceReleasePrimaryLock(expected_holder='old-primary')
        ) is True

        zk.is_lock_holder.assert_called_once_with(zk.ELECTION_MANAGER_LOCK_PATH)
        zk.force_release_primary_lock.assert_called_once_with('old-primary')

    def test_participant_cannot_force_release_primary_lock(self):
        executor, zk, _ = _make_executor()
        zk.is_lock_holder.return_value = False

        assert executor._dispatch(
            ForceReleasePrimaryLock(expected_holder='old-primary')
        ) is False

        zk.force_release_primary_lock.assert_not_called()

class TestFailoverTransitionTo:
    def test_writes_failover_state(self):
        executor, zk, _ = _make_executor()
        zk.write_failover_state.return_value = True

        result = executor._dispatch(FailoverTransitionTo(FailoverPhase.GATES_PASSED))

        assert result is True
        zk.write_failover_state.assert_called_once_with(FailoverPhase.GATES_PASSED)

    def test_returns_false_on_zk_failure(self):
        executor, zk, _ = _make_executor()
        zk.write_failover_state.return_value = False

        result = executor._dispatch(FailoverTransitionTo(FailoverPhase.VOTING))

        assert result is False

    def test_participant_cannot_change_global_phase(self):
        executor, zk, _ = _make_executor()
        zk.is_lock_holder.return_value = False

        result = executor._dispatch(FailoverTransitionTo(FailoverPhase.VOTING))

        assert result is False
        zk.write_failover_state.assert_not_called()


class TestPromote:
    def test_dispatches_failover_promotion(self):
        executor, _, promote = _make_executor()

        result = executor._dispatch(
            Promote(scope='failover_participant', old_primary='host1')
        )

        assert result is True
        promote.assert_called_once_with(
            scope='failover_participant',
            operation_id='operation-1',
            old_primary='host1',
            start_postgresql=False,
        )

    def test_returns_false_when_promotion_fails(self):
        executor, _, promote = _make_executor()
        promote.return_value = PromotionResult.RETRY

        assert executor._dispatch(Promote(scope='failover_participant')) is False

    def test_rejected_failover_promotion_publishes_failure_and_releases_lock(self):
        executor, zk, promote = _make_executor()
        promote.return_value = PromotionResult.REJECTED

        assert executor._dispatch(Promote(
            scope='failover_participant',
            failover_version='version-1',
        )) is False

        zk.write_switchover_record.assert_not_called()
        zk.write_failover_participant_state.assert_called_once_with('failed', 'version-1')
        zk.release_lock.assert_called_once_with()


class TestCleanupFailover:
    def test_cleans_metadata_and_releases_coordinator_lock(self):
        executor, zk, _ = _make_executor()
        zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
        zk.ensure_failover_must_be_reset.return_value = True
        zk.cleanup_failover.return_value = True
        zk.release_lock.return_value = True
        zk.delete_failover_must_be_reset.return_value = True

        assert executor._dispatch(CleanupFailover()) is True

        zk.ensure_failover_must_be_reset.assert_called_once_with()
        zk.cleanup_failover.assert_called_once_with()
        zk.release_lock.assert_called_once_with('epoch_manager')
        zk.delete_failover_must_be_reset.assert_called_once_with()

    def test_stops_when_reset_marker_cannot_be_ensured(self):
        executor, zk, _ = _make_executor()
        zk.ensure_failover_must_be_reset.return_value = False

        assert executor._dispatch(CleanupFailover()) is False

        zk.cleanup_failover.assert_not_called()

    def test_stops_when_metadata_cleanup_fails(self):
        executor, zk, _ = _make_executor()
        zk.ensure_failover_must_be_reset.return_value = True
        zk.cleanup_failover.return_value = False

        assert executor._dispatch(CleanupFailover()) is False

        zk.release_lock.assert_not_called()
        zk.delete_failover_must_be_reset.assert_not_called()

    def test_keeps_reset_marker_when_coordinator_unlock_fails(self):
        executor, zk, _ = _make_executor()
        zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
        zk.ensure_failover_must_be_reset.return_value = True
        zk.cleanup_failover.return_value = True
        zk.release_lock.return_value = False

        assert executor._dispatch(CleanupFailover()) is False

        zk.delete_failover_must_be_reset.assert_not_called()

    def test_returns_false_when_reset_marker_removal_fails(self):
        executor, zk, _ = _make_executor()
        zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
        zk.ensure_failover_must_be_reset.return_value = True
        zk.cleanup_failover.return_value = True
        zk.release_lock.return_value = True
        zk.delete_failover_must_be_reset.return_value = False

        assert executor._dispatch(CleanupFailover()) is False


class TestFailoverExceptionHandling:
    def test_zookeeper_exception_on_vote_is_caught(self):
        executor, zk, _ = _make_executor()
        zk.write_election_vote.side_effect = ZookeeperException('zk down')
        db = executor._db
        db.stop_restoring_wal.return_value = True
        db.disable_wal_receiver.return_value = True
        db.get_timeline.return_value = 5
        db.get_wal_flush_lsn.return_value = 100

        result = executor._dispatch(PrepareFailoverVote(1, 5.0, 'version-1', 5))

        assert result is False

    def test_zookeeper_exception_on_transition_is_caught(self):
        executor, zk, _ = _make_executor()
        zk.write_failover_state.side_effect = ZookeeperException('zk down')

        result = executor._dispatch(FailoverTransitionTo(FailoverPhase.GATES_PASSED))

        assert result is False
