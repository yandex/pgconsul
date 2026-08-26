# encoding: utf-8
"""
Unit tests for SSN-before-promote methods in ReplicationManager subclasses.

Note: tests for calculate_quorum_ssn() have moved to test_ssn_manager.py.
Here we test only the ReplicationManager-level behaviour of set_ssn_before_promote().
"""

import importlib
from unittest.mock import MagicMock, patch
from configparser import RawConfigParser
from src.types import DurabilityConfig

# Bootstrap (sys.path, sys.modules stubs) is handled by conftest.py
_rm = importlib.import_module('src.replication_manager')
_ssn_mod = importlib.import_module('src.ssn_manager')

ReplicationManager = _rm.ReplicationManager
build_replication_manager_config = _rm.build_replication_manager_config
SsnManager = _ssn_mod.SsnManager


def _make_config():
    config = RawConfigParser()
    config.add_section('global')
    config.set('global', 'priority', '100')
    config.add_section('replica')
    config.set('replica', 'primary_unavailability_timeout', '60.0')
    config.add_section('primary')
    config.set('primary', 'change_replication_metric', 'count')
    config.set('primary', 'weekday_change_hours', '9-18')
    config.set('primary', 'weekend_change_hours', '0-0')
    config.set('primary', 'overload_sessions_ratio', '0.8')
    config.set('primary', 'before_async_unavailability_timeout', '10.0')
    config.set('primary', 'quorum_removal_delay', '0.0')
    return build_replication_manager_config(config)


def _make_manager():
    db = MagicMock()
    zk = MagicMock()
    ssn_manager = MagicMock(spec=SsnManager)
    manager = ReplicationManager(_make_config(), db, zk)
    manager._ssn = ssn_manager
    return manager, db, zk, ssn_manager


class TestSetSsnBeforePromote:

    def test_success_delegates_to_ssn_manager(self):
        """Promotion derives SSN from the persisted durability config."""
        manager, db, zk, ssn = _make_manager()
        config = DurabilityConfig.build(['candidate', 'host1', 'host2'])
        ssn.calculate_ssn_for_host.return_value = 'ANY 1(host1,host2)'
        ssn.apply_and_persist.return_value = True

        with patch('src.replication_manager.helpers.get_hostname', return_value='candidate'):
            result = manager.set_ssn_before_promote(config)

        assert result is True
        ssn.calculate_ssn_for_host.assert_called_once_with(config, 'candidate')
        ssn.apply_and_persist.assert_called_once_with(
            'ANY 1(host1,host2)',
            'Setting SSN before promote: ANY 1(host1,host2).',
            'Set SSN before promote.',
        )

    def test_failure_propagates_from_ssn_manager(self):
        manager, db, zk, ssn = _make_manager()
        config = DurabilityConfig.build(['candidate', 'host1'])
        ssn.calculate_ssn_for_host.return_value = 'ANY 1(host1)'
        ssn.apply_and_persist.return_value = False

        result = manager.set_ssn_before_promote(config)

        assert result is False

    def test_missing_config_sets_async(self):
        manager, db, zk, ssn = _make_manager()
        ssn.apply_and_persist.return_value = True

        result = manager.set_ssn_before_promote(None)

        assert result is True
        ssn.calculate_ssn_for_host.assert_not_called()
        ssn.apply_and_persist.assert_called_once_with(
            '',
            'Setting SSN before promote: (async).',
            'Set SSN before promote.',
        )


class TestDurabilityMembers:

    def test_sync_host_reconciles_primary_and_replica(self):
        manager, _, zk, ssn = _make_manager()
        ssn.reconcile_durability.return_value = True

        with patch('src.replication_manager.helpers.get_hostname', return_value='primary'):
            assert manager.change_replication_to_sync_host('candidate') is True

        config = ssn.reconcile_durability.call_args.args[0]
        assert config.to_dict() == {'members': ['candidate', 'primary']}
        ssn.reconcile_durability.assert_called_once_with(config, 'primary')

    def test_regular_update_persists_primary_and_all_sync_replicas(self):
        manager, db, zk, ssn = _make_manager()
        db.get_replication_state.return_value = ('sync', 'ANY 1(replica1,replica2)')
        zk.get_durability_config.return_value = None
        zk.get_sync_quorum_hosts.return_value = ['replica1', 'replica2']
        manager._removal_strategy = MagicMock()
        manager._removal_strategy.get_hosts_to_keep.return_value = ['replica1', 'replica2']
        ssn.reconcile_durability.return_value = True

        with patch.object(manager, '_get_needed_replication_type', return_value='sync'), \
             patch('src.replication_manager.helpers.get_hostname', return_value='primary'):
            manager.update_replication_type({'replics_info': []}, {'replica1', 'replica2'})

        config = ssn.reconcile_durability.call_args.args[0]
        assert config.to_dict() == {'members': ['primary', 'replica1', 'replica2']}

    def test_async_config_contains_only_primary(self):
        manager, _, zk, ssn = _make_manager()
        ssn.reconcile_durability.return_value = True

        with patch('src.replication_manager.helpers.get_hostname', return_value='primary'):
            assert manager.change_replication_to_async() is True

        config = ssn.reconcile_durability.call_args.args[0]
        assert config.to_dict() == {'members': ['primary']}

class TestShouldClose:

    def _make_manager(self):
        from src.replication_manager import ReplicationManager
        manager, db, zk, _ = _make_manager()
        manager._zk_fail_timestamp = None
        return manager, db

    def test_raises_on_connection_error(self):
        # ADR-0002 §1: PostgresConnectionError must propagate, not be swallowed.
        import pytest
        from src.exceptions import PostgresConnectionError
        manager, db = self._make_manager()
        db.get_replics_info.side_effect = PostgresConnectionError("db down")
        with patch('src.replication_manager.time.time', return_value=1000.0):
            with pytest.raises(PostgresConnectionError):
                manager.should_close()

    def test_returns_false_for_async_replication(self):
        manager, db = self._make_manager()
        db.get_replics_info.return_value = []
        db.get_replication_state.return_value = ('async', None)
        with patch('src.replication_manager.time.time', return_value=1000.0):
            result = manager.should_close()
        assert result is False
