# encoding: utf-8
"""
Unit tests for src/slot_manager.py — ReplicationSlotManager.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import PostgresConnectionError
from src.slot_manager import ReplicationSlotManager, ReplicationSlotManagerConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(
    replication_slots_polling=True, use_replication_slots=True, drop_slot_countdown=5
):
    return ReplicationSlotManagerConfig(
        replication_slots_polling=replication_slots_polling,
        use_replication_slots=use_replication_slots,
        drop_slot_countdown=drop_slot_countdown,
    )


def _make_manager(config=None, db=None, zk=None):
    if config is None:
        config = _make_config()
    if db is None:
        db = MagicMock()
    if zk is None:
        zk = MagicMock()
    return ReplicationSlotManager(db, zk, config)


# ---------------------------------------------------------------------------
# Tests: handle_slots
# ---------------------------------------------------------------------------

class TestHandleSlots:
    """handle_slots orchestrates slot creation and dropping."""

    def test_skips_when_polling_disabled(self):
        """Does nothing when replication_slots_polling is off."""
        config = _make_config(replication_slots_polling=False)
        manager = _make_manager(config=config)
        manager.handle_slots()
        manager._zk.get_lock_contenders.assert_not_called()

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_skips_when_lock_contenders_fail(self, _mock_hostname):
        """Returns early when ZK get_lock_contenders raises."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.side_effect = Exception('zk error')
        manager.handle_slots()
        manager._zk.get_members.assert_not_called()

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_skips_when_no_members(self, _mock_hostname):
        """Returns early when ZK members list is empty."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.return_value = ['host2']
        manager._zk.get_members.return_value = []
        manager.handle_slots()
        manager._db.get_replication_slots.assert_not_called()

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_calls_sync_slots_with_correct_names(self, _mock_hostname):
        """Creates slots for lock holders, drops none."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.return_value = ['host2', 'host3']
        manager._zk.get_members.return_value = ['host1', 'host2', 'host3']
        manager._db.get_replication_slots.return_value = []

        manager.handle_slots()

        created = [c.args[0] for c in manager._db._create_replication_slot.call_args_list]
        assert 'host2' in created
        assert 'host3' in created
        # host1 is the current host, should not be in drop list
        manager._db._drop_replication_slot.assert_not_called()

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_excludes_self_from_drop_list(self, _mock_hostname):
        """Current host is excluded from the drop list."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.return_value = []
        manager._zk.get_members.return_value = ['host1', 'host2']
        manager._db.get_replication_slots.return_value = ['host1', 'host2']
        # Pre-set countdown so host2 is expired (will go to -1)
        manager._drop_countdown = {'host1': 0, 'host2': 0}

        manager.handle_slots()

        manager._db._create_replication_slot.assert_not_called()
        dropped = [c.args[0] for c in manager._db._drop_replication_slot.call_args_list]
        assert 'host1' not in dropped  # self excluded
        assert 'host2' in dropped

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_catches_postgres_connection_error(self, _mock_hostname):
        """Catches PostgresConnectionError from get_replication_slots and logs warning."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.return_value = ['host2']
        manager._zk.get_members.return_value = ['host1', 'host2']
        manager._db.get_replication_slots.side_effect = PostgresConnectionError('db down')

        # Should not raise
        manager.handle_slots()

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_logs_warning_on_create_failure(self, _mock_hostname):
        """Logs warning when slot creation raises PostgresConnectionError."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.return_value = ['host2']
        manager._zk.get_members.return_value = ['host1', 'host2']
        manager._db.get_replication_slots.return_value = []
        manager._db._create_replication_slot.side_effect = PostgresConnectionError('db error')

        manager.handle_slots()

    @patch('src.slot_manager.helpers.get_hostname', return_value='host1')
    def test_drop_failure_sets_drop_ok_false(self, _mock_hostname):
        """drop_ok=False when _drop_replication_slot raises PostgresConnectionError."""
        manager = _make_manager()
        manager._zk.get_lock_contenders.return_value = []
        manager._zk.get_members.return_value = ['host1', 'host2']
        manager._db.get_replication_slots.return_value = ['host2']
        manager._drop_countdown = {'host1': 0, 'host2': 0}
        manager._db._drop_replication_slot.side_effect = PostgresConnectionError('db error')

        manager.handle_slots()

        manager._db._drop_replication_slot.assert_called_once_with('host2')


# ---------------------------------------------------------------------------
# Tests: _compute_non_holders
# ---------------------------------------------------------------------------

class TestComputeNonHolders:
    """_compute_non_holders tracks countdown and returns expired hosts."""

    def test_holder_resets_countdown(self):
        """Host holding the lock has countdown reset to default."""
        manager = _make_manager()
        manager._drop_countdown['host2'] = 0  # was about to expire
        result = manager._compute_non_holders(['host2'], {'host2'})

        assert result == []
        assert manager._drop_countdown['host2'] == 5

    def test_non_holder_decrements_countdown(self):
        """Host not holding the lock has countdown decremented."""
        manager = _make_manager()
        manager._drop_countdown['host2'] = 3
        result = manager._compute_non_holders(['host2'], set())

        assert result == []
        assert manager._drop_countdown['host2'] == 2

    def test_non_holder_added_when_countdown_expired(self):
        """Host with countdown below zero is added to non_holders."""
        manager = _make_manager()
        manager._drop_countdown['host2'] = 0
        result = manager._compute_non_holders(['host2'], set())

        assert result == ['host2']
        assert manager._drop_countdown['host2'] == -1

    def test_new_host_gets_default_countdown(self):
        """Host not in countdown dict gets default value."""
        manager = _make_manager()
        result = manager._compute_non_holders(['host2'], set())

        assert result == []
        assert manager._drop_countdown['host2'] == 4  # 5 - 1


# ---------------------------------------------------------------------------
# Tests: create_slots_for_hosts
# ---------------------------------------------------------------------------

class TestCreateSlotsForHosts:
    """create_slots_for_hosts creates missing slots via db primitives."""

    def test_skips_when_use_replication_slots_disabled(self):
        """Returns True when use_replication_slots is off."""
        config = _make_config(use_replication_slots=False)
        manager = _make_manager(config=config)
        assert manager.create_slots_for_hosts(['host2']) is True
        manager._db.get_replication_slots.assert_not_called()

    def test_skips_when_hosts_empty(self):
        """Returns True when hosts list is empty."""
        manager = _make_manager()
        assert manager.create_slots_for_hosts([]) is True
        manager._db.get_replication_slots.assert_not_called()

    def test_returns_true_on_success(self):
        """Returns True when all slots are created."""
        manager = _make_manager()
        manager._db.get_replication_slots.return_value = []
        manager._db._create_replication_slot.return_value = True
        assert manager.create_slots_for_hosts(['host2', 'host3']) is True
        assert manager._db._create_replication_slot.call_count == 2

    def test_returns_false_on_failure(self):
        """Returns False when slot creation raises PostgresConnectionError."""
        manager = _make_manager()
        manager._db.get_replication_slots.return_value = []
        manager._db._create_replication_slot.side_effect = PostgresConnectionError('db error')
        assert manager.create_slots_for_hosts(['host2']) is False

    def test_propagates_connection_error_from_get_slots(self):
        """PostgresConnectionError from get_replication_slots propagates to caller."""
        manager = _make_manager()
        manager._db.get_replication_slots.side_effect = PostgresConnectionError('db error')
        with pytest.raises(PostgresConnectionError):
            manager.create_slots_for_hosts(['host2'])


# ---------------------------------------------------------------------------
# Tests: reset_on_promote
# ---------------------------------------------------------------------------

class TestResetOnPromote:
    """reset_on_promote clears the countdown dict."""

    def test_clears_countdown(self):
        """Empties the _drop_countdown dict."""
        manager = _make_manager()
        manager._drop_countdown = {'host2': 3, 'host3': 1}
        manager.reset_on_promote()
        assert manager._drop_countdown == {}

    def test_clears_empty_countdown(self):
        """Works correctly when countdown is already empty."""
        manager = _make_manager()
        manager.reset_on_promote()
        assert manager._drop_countdown == {}
