# encoding: utf-8
"""Unit tests for failover command dispatch in CommandExecutor (ADR-0007).

Verifies each failover-specific command is dispatched to the correct infra call
with the right arguments. Opaque commands (SetSSNBeforePromote,
ResetFailoverNode) delegate to callbacks; ZK commands delegate to zk methods.
"""

from unittest.mock import MagicMock

import pytest

from src.command_executor import CommandExecutor
from src.commands import (
    CleanupVotes,
    FailoverTransitionTo,
    ResetFailoverNode,
    SetSSNBeforePromote,
    WriteCurrentPromotingHost,
    WriteElectionStatus,
    WriteElectionVote,
    WriteElectionWinner,
    WriteLastFailoverTime,
)
from src.failover import FailoverPhase
from src.zk import ZookeeperException


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_executor():
    """Build a CommandExecutor with all infra objects and callbacks mocked."""
    zk = MagicMock()
    db = MagicMock()
    replication_manager = MagicMock()
    timings = MagicMock()
    slot_manager = MagicMock()

    executor = CommandExecutor(
        zk=zk,
        db=db,
        replication_manager=replication_manager,
        timings=timings,
        slot_manager=slot_manager,
        rewind_from_source=MagicMock(return_value=True),
        debug_failure=MagicMock(),
        promote_checkpoint_sql=None,
    )
    return executor, zk


# ---------------------------------------------------------------------------
# ZK-direct commands
# ---------------------------------------------------------------------------


class TestWriteCurrentPromotingHost:
    def test_dispatches_to_zk(self):
        executor, zk = _make_executor()
        zk.write_current_promoting_host.return_value = True

        result = executor._dispatch(WriteCurrentPromotingHost())

        assert result is True
        zk.write_current_promoting_host.assert_called_once()

    def test_returns_false_on_zk_failure(self):
        executor, zk = _make_executor()
        zk.write_current_promoting_host.return_value = False

        result = executor._dispatch(WriteCurrentPromotingHost())

        assert result is False


class TestWriteLastFailoverTime:
    def test_dispatches_to_zk(self):
        executor, zk = _make_executor()
        zk.write_last_failover_time.return_value = True

        result = executor._dispatch(WriteLastFailoverTime())

        assert result is True
        zk.write_last_failover_time.assert_called_once()

    def test_returns_false_on_zk_failure(self):
        executor, zk = _make_executor()
        zk.write_last_failover_time.return_value = False

        result = executor._dispatch(WriteLastFailoverTime())

        assert result is False


class TestWriteElectionStatus:
    def test_dispatches_to_zk(self):
        executor, zk = _make_executor()
        zk.write_election_status.return_value = True

        result = executor._dispatch(WriteElectionStatus(status='registration'))

        assert result is True
        zk.write_election_status.assert_called_once_with('registration')

    def test_returns_false_on_zk_failure(self):
        executor, zk = _make_executor()
        zk.write_election_status.return_value = False

        result = executor._dispatch(WriteElectionStatus(status='failed'))

        assert result is False


class TestWriteElectionVote:
    def test_dispatches_to_zk(self):
        executor, zk = _make_executor()
        zk.write_election_vote.return_value = True

        result = executor._dispatch(WriteElectionVote(lsn=100, priority=1))

        assert result is True
        zk.write_election_vote.assert_called_once_with(100, 1)

    def test_returns_false_on_zk_failure(self):
        executor, zk = _make_executor()
        zk.write_election_vote.return_value = False

        result = executor._dispatch(WriteElectionVote(lsn=100, priority=1))

        assert result is False


class TestWriteElectionWinner:
    def test_dispatches_to_zk(self):
        executor, zk = _make_executor()
        zk.write_election_winner.return_value = True

        result = executor._dispatch(WriteElectionWinner(winner='host2'))

        assert result is True
        zk.write_election_winner.assert_called_once_with('host2')

    def test_returns_false_on_zk_failure(self):
        executor, zk = _make_executor()
        zk.write_election_winner.return_value = False

        result = executor._dispatch(WriteElectionWinner(winner='host2'))

        assert result is False


class TestCleanupVotes:
    def test_deletes_vote_for_each_ha_host(self):
        executor, zk = _make_executor()
        zk.get_ha_hosts.return_value = ['host1', 'host2', 'host3']
        zk.delete_election_vote.return_value = True

        result = executor._dispatch(CleanupVotes())

        assert result is True
        assert zk.delete_election_vote.call_count == 3
        zk.delete_election_vote.assert_any_call('host1')
        zk.delete_election_vote.assert_any_call('host2')
        zk.delete_election_vote.assert_any_call('host3')

    def test_returns_false_if_any_delete_fails(self):
        executor, zk = _make_executor()
        zk.get_ha_hosts.return_value = ['host1', 'host2']
        zk.delete_election_vote.side_effect = [True, False]

        result = executor._dispatch(CleanupVotes())

        assert result is False

    def test_returns_true_when_no_ha_hosts(self):
        executor, zk = _make_executor()
        zk.get_ha_hosts.return_value = []

        result = executor._dispatch(CleanupVotes())

        assert result is True

    def test_returns_true_when_ha_hosts_none(self):
        executor, zk = _make_executor()
        zk.get_ha_hosts.return_value = None

        result = executor._dispatch(CleanupVotes())

        assert result is True


class TestFailoverTransitionTo:
    def test_writes_failover_state_and_logs(self):
        executor, zk = _make_executor()
        zk.write_failover_state.return_value = True

        result = executor._dispatch(FailoverTransitionTo(phase=FailoverPhase.DETECTED))

        assert result is True
        zk.write_failover_state.assert_called_once_with(FailoverPhase.DETECTED)

    def test_returns_false_on_zk_failure(self):
        executor, zk = _make_executor()
        zk.write_failover_state.return_value = False

        result = executor._dispatch(FailoverTransitionTo(phase=FailoverPhase.VOTING))

        assert result is False


# ---------------------------------------------------------------------------
# Opaque commands (delegated to callbacks)
# ---------------------------------------------------------------------------


class TestSetSSNBeforePromote:
    def test_dispatches_to_replication_manager_with_ha_replicas_and_old_primary(self):
        executor, zk = _make_executor()
        zk.get_quorum_replics_for_promote.return_value = ['host2', 'host3']
        executor._replication_manager.set_ssn_before_promote.return_value = True

        result = executor._dispatch(SetSSNBeforePromote(old_primary='host1'))

        assert result is True
        zk.get_quorum_replics_for_promote.assert_called_once()
        executor._replication_manager.set_ssn_before_promote.assert_called_once_with(
            ['host2', 'host3'], old_primary='host1'
        )

    def test_returns_false_when_replication_manager_returns_false(self):
        executor, zk = _make_executor()
        zk.get_quorum_replics_for_promote.return_value = ['host2']
        executor._replication_manager.set_ssn_before_promote.return_value = False

        result = executor._dispatch(SetSSNBeforePromote(old_primary=None))

        assert result is False


class TestResetFailoverNode:
    def test_resets_when_failover_state_already_finished(self):
        executor, zk = _make_executor()
        zk.get_failover_state.return_value = 'finished'
        zk.delete_current_promoting_host.return_value = True

        result = executor._dispatch(ResetFailoverNode())

        assert result is True
        zk.delete_failover_must_be_reset.assert_called_once()
        zk.write_failover_state.assert_not_called()

    def test_writes_finished_then_resets(self):
        executor, zk = _make_executor()
        zk.get_failover_state.return_value = 'detected'
        zk.write_failover_state.return_value = True
        zk.delete_current_promoting_host.return_value = True

        result = executor._dispatch(ResetFailoverNode())

        assert result is True
        zk.write_failover_state.assert_called_once_with('finished')
        zk.delete_failover_must_be_reset.assert_called_once()

    def test_returns_false_when_write_fails(self):
        executor, zk = _make_executor()
        zk.get_failover_state.return_value = 'detected'
        zk.write_failover_state.return_value = False

        result = executor._dispatch(ResetFailoverNode())

        assert result is False
        zk.ensure_failover_must_be_reset.assert_called_once()

    def test_returns_false_when_delete_promoting_host_fails(self):
        executor, zk = _make_executor()
        zk.get_failover_state.return_value = 'finished'
        zk.delete_current_promoting_host.return_value = False

        result = executor._dispatch(ResetFailoverNode())

        assert result is False
        zk.ensure_failover_must_be_reset.assert_called_once()


# ---------------------------------------------------------------------------
# Exception handling (ADR-0002)
# ---------------------------------------------------------------------------


class TestFailoverExceptionHandling:
    def test_zookeeper_exception_on_write_election_status_caught(self):
        executor, zk = _make_executor()
        zk.write_election_status.side_effect = ZookeeperException('zk down')

        result = executor._dispatch(WriteElectionStatus(status='registration'))

        assert result is False

    def test_zookeeper_exception_on_failover_transition_to_caught(self):
        executor, zk = _make_executor()
        zk.write_failover_state.side_effect = ZookeeperException('zk down')

        result = executor._dispatch(FailoverTransitionTo(phase=FailoverPhase.DETECTED))

        assert result is False
