# encoding: utf-8
"""
Unit tests for the Best-Effort sessions-ratio path in ReplicationManager
(ADR-0002 §3).

On DB loss while reading the sessions ratio, the operation must catch only
PostgresConnectionError, log a warning with exc_info=True, and return the
conservative 'sync' default.
"""

import importlib
import logging
from unittest.mock import MagicMock
from configparser import RawConfigParser

import pytest

# Bootstrap (sys.path, sys.modules stubs) is handled by conftest.py
_rm = importlib.import_module('src.replication_manager')
_exc = importlib.import_module('src.exceptions')

ReplicationManager = _rm.ReplicationManager
build_replication_manager_config = _rm.build_replication_manager_config
PostgresConnectionError = _exc.PostgresConnectionError


def _make_config(metric='load'):
    config = RawConfigParser()
    config.add_section('global')
    config.set('global', 'priority', '100')
    config.add_section('replica')
    config.set('replica', 'primary_unavailability_timeout', '60.0')
    config.add_section('primary')
    config.set('primary', 'change_replication_metric', metric)
    config.set('primary', 'weekday_change_hours', '9-18')
    config.set('primary', 'weekend_change_hours', '0-0')
    config.set('primary', 'overload_sessions_ratio', '0.8')
    config.set('primary', 'before_async_unavailability_timeout', '10.0')
    config.set('primary', 'quorum_removal_delay', '0.0')
    return build_replication_manager_config(config)


def _make_manager(metric='load'):
    db = MagicMock()
    zk = MagicMock()
    manager = ReplicationManager(_make_config(metric), db, zk)
    return manager, db, zk


# Empty replica set: forces the code past the 'count' short-circuit into the
# 'load' branch where get_sessions_ratio() is consulted.
_EMPTY_DB_STATE = {'replics_info': []}


class TestSessionsRatioBestEffort:

    def test_connection_error_falls_back_to_sync(self, caplog):
        """PostgresConnectionError → early return with conservative 'sync'."""
        manager, db, _ = _make_manager(metric='load')
        db.get_sessions_ratio.side_effect = PostgresConnectionError('db down')

        with caplog.at_level(logging.WARNING):
            result = manager._get_needed_replication_type_without_await_before_async(
                _EMPTY_DB_STATE, ha_replics=[]
            )

        assert result == 'sync'
        db.get_sessions_ratio.assert_called_once()

    def test_connection_error_logs_warning_with_exc_info(self, caplog):
        """Rule §3.2: warning level + exc_info=True (traceback preserved)."""
        manager, db, _ = _make_manager(metric='load')
        db.get_sessions_ratio.side_effect = PostgresConnectionError('db down')

        with caplog.at_level(logging.WARNING):
            manager._get_needed_replication_type_without_await_before_async(
                _EMPTY_DB_STATE, ha_replics=[]
            )

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        # exc_info must be populated so the traceback is preserved in logs.
        assert warnings[0].exc_info is not None
        assert warnings[0].exc_info[0] is PostgresConnectionError

    def test_only_connection_error_is_caught(self):
        """Rule §3.1: other exceptions must NOT be swallowed."""
        manager, db, _ = _make_manager(metric='load')
        db.get_sessions_ratio.side_effect = RuntimeError('unexpected')

        with pytest.raises(RuntimeError):
            manager._get_needed_replication_type_without_await_before_async(
                _EMPTY_DB_STATE, ha_replics=[]
            )

    def test_healthy_ratio_below_threshold_returns_sync(self):
        """Sanity: a healthy low ratio yields the default 'sync'."""
        manager, db, _ = _make_manager(metric='load')
        db.get_sessions_ratio.return_value = 0.1

        result = manager._get_needed_replication_type_without_await_before_async(
            _EMPTY_DB_STATE, ha_replics=[]
        )

        assert result == 'sync'

    def test_overloaded_ratio_returns_async(self):
        """Sanity: ratio above overload_sessions_ratio yields 'async'."""
        manager, db, _ = _make_manager(metric='load')
        db.get_sessions_ratio.return_value = 0.9

        result = manager._get_needed_replication_type_without_await_before_async(
            _EMPTY_DB_STATE, ha_replics=[]
        )

        assert result == 'async'
