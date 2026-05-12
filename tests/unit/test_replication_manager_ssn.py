# encoding: utf-8
"""
Unit tests for SSN-before-promote methods in ReplicationManager subclasses.

Note: tests for calculate_quorum_ssn() have moved to test_ssn_manager.py.
Here we test only the ReplicationManager-level behaviour of set_ssn_before_promote().
"""

import importlib
from unittest.mock import MagicMock
from configparser import RawConfigParser

# Bootstrap (sys.path, sys.modules stubs) is handled by conftest.py
_rmf = importlib.import_module('src.replication_manager_factory')
_rm = importlib.import_module('src.replication_manager')
_ssn_mod = importlib.import_module('src.ssn_manager')

ReplicationManager = _rm.ReplicationManager
build_replication_manager_config = _rmf.build_replication_manager_config
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
        """Switchover: side_replicas + extra_host are assembled and SSN is applied."""
        manager, db, zk, ssn = _make_manager()
        ssn.calculate_quorum_ssn.return_value = 'ANY 1(host1,host2)'
        ssn.apply_and_persist.return_value = True

        result = manager.set_ssn_before_promote(
            known_replicas=['host1'],
            extra_host='host2',
        )

        assert result is True
        ssn.calculate_quorum_ssn.assert_called_once_with(['host1', 'host2'])
        ssn.apply_and_persist.assert_called_once_with(
            'ANY 1(host1,host2)',
            'Setting SSN before promote: ANY 1(host1,host2).',
            'Set SSN before promote.',
        )

    def test_failover_no_extra_host(self):
        """Failover: only known_replicas, no extra_host."""
        manager, db, zk, ssn = _make_manager()
        ssn.calculate_quorum_ssn.return_value = 'ANY 1(host1,host2)'
        ssn.apply_and_persist.return_value = True

        result = manager.set_ssn_before_promote(
            known_replicas=['host1', 'host2'],
        )

        assert result is True
        ssn.calculate_quorum_ssn.assert_called_once_with(['host1', 'host2'])

    def test_failure_propagates_from_ssn_manager(self):
        manager, db, zk, ssn = _make_manager()
        ssn.calculate_quorum_ssn.return_value = 'ANY 1(host1)'
        ssn.apply_and_persist.return_value = False

        result = manager.set_ssn_before_promote(known_replicas=['host1'])

        assert result is False

    def test_empty_replicas_sets_async(self):
        """Empty replica list → SSN = '' (async mode)."""
        manager, db, zk, ssn = _make_manager()
        ssn.calculate_quorum_ssn.return_value = ''
        ssn.apply_and_persist.return_value = True

        result = manager.set_ssn_before_promote(known_replicas=[])

        assert result is True
        ssn.calculate_quorum_ssn.assert_called_once_with([])
        ssn.apply_and_persist.assert_called_once_with(
            '',
            'Setting SSN before promote: (async).',
            'Set SSN before promote.',
        )

    def test_none_known_replicas_sets_async(self):
        """None known_replicas (ZK returned None) → SSN = '' (async mode)."""
        manager, db, zk, ssn = _make_manager()
        ssn.calculate_quorum_ssn.return_value = ''
        ssn.apply_and_persist.return_value = True

        result = manager.set_ssn_before_promote(known_replicas=None)

        assert result is True
        ssn.calculate_quorum_ssn.assert_called_once_with([])

    def test_three_replicas_correct_quorum_size(self):
        manager, db, zk, ssn = _make_manager()
        ssn.calculate_quorum_ssn.return_value = 'ANY 2(h1,h2,h3)'
        ssn.apply_and_persist.return_value = True

        result = manager.set_ssn_before_promote(known_replicas=['h1', 'h2'], extra_host='h3')

        assert result is True
        ssn.apply_and_persist.assert_called_once_with(
            'ANY 2(h1,h2,h3)',
            'Setting SSN before promote: ANY 2(h1,h2,h3).',
            'Set SSN before promote.',
        )
