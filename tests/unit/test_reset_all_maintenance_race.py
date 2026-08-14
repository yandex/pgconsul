# encoding: utf-8
"""
Unit test for reset_all maintenance node race condition.

Reproduces pgconsul_util.feature:877 — `reset-all --force` fails with
ResetException when pgconsul instances recreate maintenance child nodes
(ts, master, <host>) during recursive delete (NotEmptyError race).

Root cause: reset_all enables maintenance but never writes 'disable'
before deleting the maintenance subtree. pgconsul instances keep writing
child nodes on every iteration, causing NotEmptyError during recursive
delete.
"""
from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.cli import reset_all
from src.exceptions import ResetException


def _make_opts(force: bool = True, timeout: int = 300):
    """Create mock opts for reset_all."""
    opts = MagicMock()
    opts.force = force
    opts.timeout = timeout
    return opts


def _make_conf() -> RawConfigParser:
    """Create a minimal config with 'global' section."""
    conf = RawConfigParser()
    conf.add_section('global')
    return conf


def _make_zk(children: list[str] | None = None) -> MagicMock:
    """Create a mock Zookeeper with real path constants."""
    zk = MagicMock()
    zk.MEMBERS_PATH = 'members'
    zk.MAINTENANCE_PATH = 'maintenance'
    zk.get_root_children.return_value = children if children is not None else [
        'alive', 'leader', 'maintenance',
    ]
    # get_maintenance_status returns None so maintenance_disabled() is True
    zk.get_maintenance_status.return_value = None
    return zk


class TestResetAllWritesDisableBeforeDelete:
    """reset_all must write 'disable' before deleting maintenance node."""

    @patch('src.cli._wait_maintenance_disabled')
    @patch('src.cli.enable_maintenance')
    @patch('src.cli.create_zk')
    def test_writes_disable_before_deletion(self, mock_create_zk, _mock_enable, _mock_wait):
        """reset_all must call write_maintenance_status('disable') before delete.

        Without 'disable', pgconsul instances keep writing child nodes
        (ts, master, <host>) on every iteration, causing NotEmptyError
        during recursive delete (pgconsul_util.feature:877).
        """
        zk = _make_zk()
        zk.delete.return_value = True
        mock_create_zk.return_value.__enter__.return_value = zk

        reset_all(_make_opts(), _make_conf())

        # Must write 'disable' before deleting maintenance
        zk.write_maintenance_status.assert_any_call('disable')
        # Must have deleted maintenance
        zk.delete.assert_any_call('maintenance', recursive=True)


class TestResetAllRetriesMaintenanceDelete:
    """reset_all must retry maintenance deletion on NotEmptyError race."""

    @patch('time.sleep')
    @patch('src.cli._wait_maintenance_disabled')
    @patch('src.cli.enable_maintenance')
    @patch('src.cli.create_zk')
    def test_retries_maintenance_on_transient_failure(self, mock_create_zk, _mock_enable, _mock_wait, _mock_sleep):
        """reset_all must retry maintenance deletion when first attempt fails.

        pgconsul instances may still be writing child nodes when we try
        to delete maintenance. The delete must be retried.
        """
        zk = _make_zk(children=['alive', 'maintenance'])

        # Track delete calls per node
        call_counts: dict[str, int] = {}

        def mock_delete(node, recursive=False):
            call_counts[node] = call_counts.get(node, 0) + 1
            # 'alive' succeeds on first try
            if node == 'alive':
                return True
            # 'maintenance' fails first, succeeds on retry
            if node == 'maintenance':
                return call_counts[node] >= 2
            return True

        zk.delete.side_effect = mock_delete
        mock_create_zk.return_value.__enter__.return_value = zk

        reset_all(_make_opts(), _make_conf())

        # Must have retried maintenance deletion
        assert call_counts.get('maintenance', 0) >= 2

    @patch('time.sleep')
    @patch('src.cli._wait_maintenance_disabled')
    @patch('src.cli.enable_maintenance')
    @patch('src.cli.create_zk')
    def test_raises_after_max_retries(self, mock_create_zk, _mock_enable, _mock_wait, _mock_sleep):
        """reset_all must raise ResetException when maintenance delete fails after retries."""
        zk = _make_zk(children=['maintenance'])
        zk.delete.return_value = False
        mock_create_zk.return_value.__enter__.return_value = zk

        with pytest.raises(ResetException):
            reset_all(_make_opts(), _make_conf())

        # Must have tried multiple times
        assert zk.delete.call_count >= 3


class TestResetAllNonMaintenanceNodesNoRetry:
    """Non-maintenance nodes must fail immediately (no retry)."""

    @patch('src.cli._wait_maintenance_disabled')
    @patch('src.cli.enable_maintenance')
    @patch('src.cli.create_zk')
    def test_non_maintenance_failure_raises_immediately(self, mock_create_zk, _mock_enable, _mock_wait):
        """Non-maintenance node deletion failure must raise ResetException without retry."""
        zk = _make_zk(children=['alive', 'maintenance'])
        zk.delete.return_value = False
        mock_create_zk.return_value.__enter__.return_value = zk

        with pytest.raises(ResetException):
            reset_all(_make_opts(), _make_conf())

        # Must not retry non-maintenance nodes
        assert zk.delete.call_count == 1
