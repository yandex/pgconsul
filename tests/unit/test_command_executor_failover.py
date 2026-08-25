# encoding: utf-8
"""Unit tests for failover command dispatch in CommandExecutor (ADR-0007)."""

from unittest.mock import MagicMock

from src.command_executor import CommandExecutor
from src.commands import (
    CleanupFailover,
    CleanupVotes,
    FailoverTransitionTo,
    Promote,
    WriteElectionVote,
    WriteElectionWinner,
    WriteLastFailoverTime,
)
from src.failover import FailoverPhase
from src.zk import ZookeeperException


def _make_executor():
    zk = MagicMock()
    promote = MagicMock(return_value=True)
    local_states = {'failover_participant': MagicMock()}
    executor = CommandExecutor(
        zk=zk,
        db=MagicMock(),
        replication_manager=MagicMock(),
        timings=MagicMock(),
        stop_postgresql=MagicMock(return_value=0),
        store_replics_info=MagicMock(return_value=True),
        rewind_from_source=MagicMock(return_value=True),
        promote=promote,
        return_to_cluster=MagicMock(),
        set_simple_primary_switch_try=MagicMock(),
        create_slots_for_hosts=MagicMock(return_value=True),
        initialize_failover=MagicMock(return_value=True),
        local_states=local_states,
    )
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


class TestWriteElectionVote:
    def test_dispatches_to_zk(self):
        executor, zk, _ = _make_executor()
        zk.write_election_vote.return_value = True

        assert executor._dispatch(WriteElectionVote(lsn=100, priority=1)) is True

        zk.write_election_vote.assert_called_once_with(100, 1)

    def test_returns_false_on_zk_failure(self):
        executor, zk, _ = _make_executor()
        zk.write_election_vote.return_value = False

        assert executor._dispatch(WriteElectionVote(lsn=100, priority=1)) is False


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


class TestCleanupVotes:
    def test_deletes_vote_tree(self):
        executor, zk, _ = _make_executor()
        zk.ELECTION_VOTES_PATH = '/election/votes'
        zk.delete.return_value = True

        assert executor._dispatch(CleanupVotes()) is True

        zk.delete.assert_called_once_with('/election/votes', recursive=True)

    def test_returns_false_when_delete_fails(self):
        executor, zk, _ = _make_executor()
        zk.ELECTION_VOTES_PATH = '/election/votes'
        zk.delete.return_value = False

        assert executor._dispatch(CleanupVotes()) is False


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


class TestPromote:
    def test_dispatches_failover_promotion(self):
        executor, _, promote = _make_executor()

        result = executor._dispatch(
            Promote(scope='failover_participant', old_primary='host1')
        )

        assert result is True
        promote.assert_called_once_with(
            scope='failover_participant',
            old_primary='host1',
            start_postgresql=False,
        )

    def test_returns_false_when_promotion_fails(self):
        executor, _, promote = _make_executor()
        promote.return_value = False

        assert executor._dispatch(Promote(scope='failover_participant')) is False


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

        result = executor._dispatch(WriteElectionVote(lsn=100, priority=1))

        assert result is False

    def test_zookeeper_exception_on_transition_is_caught(self):
        executor, zk, _ = _make_executor()
        zk.write_failover_state.side_effect = ZookeeperException('zk down')

        result = executor._dispatch(FailoverTransitionTo(FailoverPhase.GATES_PASSED))

        assert result is False
