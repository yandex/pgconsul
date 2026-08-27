# encoding: utf-8
"""
Unit tests for Zookeeper failover state business methods.
"""

import json
from unittest.mock import MagicMock, call, patch


class TestZookeeperFailoverState:
    """Tests for failover state methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    # === write_failover_state tests ===

    def test_write_failover_state_calls_write(self, zk):
        """Test write_failover_state writes state string (need_lock=False)."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_failover_state('promoting')
        assert result is True
        zk.write.assert_called_once_with('failover_state', 'promoting', need_lock=False)

    def test_write_failover_state_with_finished(self, zk):
        """Test write_failover_state with 'finished' state (need_lock=False)."""
        zk.write = MagicMock(return_value=True)
        zk.write_failover_state('finished')
        zk.write.assert_called_once_with('failover_state', 'finished', need_lock=False)

    def test_write_failover_state_failure_returns_false(self, zk):
        """Test write_failover_state returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_failover_state('promoting')
        assert result is False

    def test_write_failover_state_no_lock_returns_false(self, zk):
        """Test write_failover_state returns False when write() returns False."""
        zk.write = MagicMock(return_value=False)
        result = zk.write_failover_state('promoting')
        assert result is False

    def test_write_failover_state_works_without_primary_lock(self, zk):
        """Coordinator writes failover_state without holding PRIMARY_LOCK_PATH.

        Reproduces MDB-41951: the failover coordinator (which holds
        ELECTION_MANAGER_LOCK_PATH, not PRIMARY_LOCK_PATH) must be able to
        write failover_state. With need_lock=True the _write guard rejects
        the write because get_current_lock_holder() != contender.
        """
        zk.write = MagicMock(return_value=True)
        # Simulate: no primary lock holder (coordinator is not the primary)
        zk.get_current_lock_holder = MagicMock(return_value=None)
        result = zk.write_failover_state('gates_passed')
        assert result is True
        zk.write.assert_called_once_with('failover_state', 'gates_passed', need_lock=False)

    # === delete_failover_state tests ===

    def test_delete_failover_state_calls_delete(self, zk):
        """Test delete_failover_state calls delete."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_failover_state()
        assert result is True
        zk.delete.assert_called_once_with('failover_state')

    def test_delete_failover_state_failure_returns_false(self, zk):
        """Test delete_failover_state returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_failover_state()
        assert result is False

    def test_cleanup_failover_deletes_state_last(self, zk):
        zk.delete = MagicMock(return_value=True)

        assert zk.cleanup_failover() is True

        assert zk.delete.call_args_list == [
            call('election_vote', recursive=True),
            call('election_winner', recursive=False),
            call('failover_members', recursive=False),
            call('failover_version', recursive=False),
            call('failover_participant', recursive=True),
            call('failover_state'),
        ]

    def test_election_vote_is_one_atomic_versioned_json_value(self, zk):
        zk.write = MagicMock(return_value=True)

        with patch('src.zk.helpers.get_hostname', return_value='host1'):
            assert zk.write_election_vote(123, 7, 'version-1', 5) is True

        zk.write.assert_called_once_with(
            'election_vote/host1',
            {
                'failover_version': 'version-1',
                'timeline': 5,
                'flush_lsn': 123,
                'priority': 7,
            },
            preproc=json.dumps,
            need_lock=False,
        )

    def test_vote_from_another_failover_version_is_ignored(self, zk):
        zk.get = MagicMock(return_value={
            'failover_version': 'version-old',
            'timeline': 5,
            'flush_lsn': 123,
            'priority': 7,
        })

        assert zk.get_election_host_vote('host1', 'version-new', 5) is None

    def test_cleanup_failover_keeps_state_when_metadata_cleanup_fails(self, zk):
        zk.delete = MagicMock(side_effect=[True, False])

        assert zk.cleanup_failover() is False

        assert call('failover_state') not in zk.delete.call_args_list

    # === ensure_failover_must_be_reset tests ===

    def test_ensure_failover_must_be_reset_success(self, zk):
        """Test ensure_failover_must_be_reset returns True on success."""
        zk.ensure_path = MagicMock(return_value='result')
        result = zk.ensure_failover_must_be_reset()
        assert result is True
        zk.ensure_path.assert_called_once_with('failover_must_be_reset')

    def test_ensure_failover_must_be_reset_failure_returns_false(self, zk):
        """Test ensure_failover_must_be_reset returns False when ensure_path returns None."""
        zk.ensure_path = MagicMock(return_value=None)
        result = zk.ensure_failover_must_be_reset()
        assert result is False

    # === delete_failover_must_be_reset tests ===

    def test_delete_failover_must_be_reset_calls_delete(self, zk):
        """Test delete_failover_must_be_reset calls delete."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_failover_must_be_reset()
        assert result is True
        zk.delete.assert_called_once_with('failover_must_be_reset')

    def test_delete_failover_must_be_reset_failure_returns_false(self, zk):
        """Test delete_failover_must_be_reset returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_failover_must_be_reset()
        assert result is False

    # === get_last_failover_time tests ===

    def test_get_last_failover_time_returns_float(self, zk):
        """Test get_last_failover_time returns float timestamp."""
        expected = 1234567890.123
        zk.noexcept_get = MagicMock(return_value=expected)
        result = zk.get_last_failover_time()
        assert result == expected
        zk.noexcept_get.assert_called_once_with('last_failover_time', preproc=float)

    def test_get_last_failover_time_returns_none(self, zk):
        """Test get_last_failover_time returns None when not set."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_last_failover_time()
        assert result is None

    # === write_last_failover_time tests ===

    def test_write_last_failover_time_calls_write(self, zk):
        """Test write_last_failover_time writes current time as float."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_last_failover_time()
        assert result is True
        call_args = zk.write.call_args
        assert call_args[0][0] == 'last_failover_time'
        assert isinstance(call_args[0][1], float)
        assert call_args[1]['need_lock'] is False

    def test_write_last_failover_time_failure_returns_false(self, zk):
        """Test write_last_failover_time returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_last_failover_time()
        assert result is False

    # === get_last_primary_availability_time tests ===

    def test_get_last_primary_availability_time_returns_float(self, zk):
        """Test get_last_primary_availability_time returns float timestamp."""
        expected = 1234567890.123
        zk.noexcept_get = MagicMock(return_value=expected)
        result = zk.get_last_primary_availability_time()
        assert result == expected
        zk.noexcept_get.assert_called_once_with('last_master_activity_time', preproc=float)

    def test_get_last_primary_availability_time_returns_none(self, zk):
        """Test get_last_primary_availability_time returns None on error."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_last_primary_availability_time()
        assert result is None

    # === write_last_primary_availability_time tests ===

    def test_write_last_primary_availability_time_calls_write(self, zk):
        """Test write_last_primary_availability_time writes current time."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_last_primary_availability_time()
        assert result is True
        call_args = zk.write.call_args
        assert call_args[0][0] == 'last_master_activity_time'
        assert isinstance(call_args[0][1], float)

    def test_write_last_primary_availability_time_failure_returns_false(self, zk):
        """Test write_last_primary_availability_time returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_last_primary_availability_time()
        assert result is False
