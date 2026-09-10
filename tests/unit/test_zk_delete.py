# encoding: utf-8
"""
Regression tests for Zookeeper.delete() error-handling contract.

Before the fix, ZkClient.delete() raised ZkClientError on failure but
Zookeeper.delete() did not catch it, so callers expecting a bool would
receive an uncaught exception. After the fix Zookeeper.delete() catches
ZkClientError and returns False, so callers in utils.py / cli.py behave
correctly.
"""

from unittest.mock import MagicMock, patch


class TestZookeeperDelete:
    """Unit tests for Zookeeper.delete() error-handling."""

    def test_delete_returns_true_on_success(self, zk):
        """delete() returns True when ZkClient.delete succeeds."""
        zk._zk_client.delete = MagicMock(return_value=True)
        assert zk.delete('some/path') is True

    def test_delete_returns_false_on_zk_client_error(self, zk):
        """delete() returns False (not raises) when ZkClientError is raised."""
        from src.zk_client import ZkClientError
        zk._zk_client.delete = MagicMock(side_effect=ZkClientError('connection lost'))
        result = zk.delete('some/path')
        assert result is False

    def test_delete_recursive_passed_to_client(self, zk):
        """delete() forwards recursive=True to ZkClient."""
        zk._zk_client.delete = MagicMock(return_value=True)
        zk.delete('some/path', recursive=True)
        zk._zk_client.delete.assert_called_once_with('some/path', recursive=True)

    def test_delete_logs_exception_on_error(self, zk):
        """delete() logs the exception when ZkClientError occurs."""
        from src.zk_client import ZkClientError
        zk._zk_client.delete = MagicMock(side_effect=ZkClientError('timeout'))
        with patch('src.zk.logging') as mock_log:
            zk.delete('some/path')
            mock_log.exception.assert_called_once()


class TestZookeeperDeleteMethods:
    """Tests that delete_*() -> bool methods return bool even on ZkClientError."""

    def _make_delete_error(self, zk):
        from src.zk_client import ZkClientError
        zk.delete = MagicMock(return_value=False)

    def test_delete_failover_state_returns_false_on_error(self, zk):
        """delete_failover_state() returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_failover_state()
        assert result is False

    def test_delete_current_promoting_host_returns_false_on_error(self, zk):
        """delete_current_promoting_host() returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_current_promoting_host()
        assert result is False

    def test_delete_failover_must_be_reset_returns_false_on_error(self, zk):
        """delete_failover_must_be_reset() returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_failover_must_be_reset()
        assert result is False

    def test_delete_maintenance_returns_false_on_error(self, zk):
        """delete_maintenance() returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_maintenance()
        assert result is False

    def test_delete_host_op_returns_false_on_error(self, zk):
        """delete_host_op() returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_host_op('host1')
        assert result is False

    def test_delete_election_vote_returns_false_on_error(self, zk):
        """delete_election_vote() returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        with patch('src.zk.helpers.get_hostname', return_value='host1'):
            result = zk.delete_election_vote('host1')
        assert result is False


