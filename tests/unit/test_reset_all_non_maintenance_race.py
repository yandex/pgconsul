# encoding: utf-8
"""
Unit test for reset_all non-maintenance node race condition.

Reproduces pgconsul_util.feature:877 — `reset-all --force` fails with
ResetException when pgconsul instances recreate child nodes under
non-maintenance ZK paths (alive, leader) during recursive delete.

Root cause: reset_all only retries deletion of the maintenance node.
After maintenance is disabled, the primary immediately starts recreating
child nodes under `alive/` and `leader/`. When reset_all tries to
recursively delete `alive`, the primary is concurrently creating alive
locks, causing NotEmptyError → zk.delete() returns False → ResetException.

The fix: retry deletion of ALL nodes, not just maintenance.
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


class TestResetAllRetriesNonMaintenanceNodes:
    """reset_all must retry deletion of non-maintenance nodes on transient failure.

    After maintenance is disabled, pgconsul instances immediately start
    recreating child nodes under `alive/`, `leader/`, etc. When reset_all
    tries to recursively delete these paths, the concurrent child creation
    causes NotEmptyError (zk.delete returns False). The deletion must be
    retried, not fail immediately.

    This test FAILS with the current code (non-maintenance nodes are not
    retried) — it is the red test for the race condition at
    pgconsul_util.feature:877.
    """

    @patch('time.sleep')
    @patch('src.cli._wait_maintenance_disabled')
    @patch('src.cli.enable_maintenance')
    @patch('src.cli.create_zk')
    def test_retries_non_maintenance_node_on_transient_failure(
        self, mock_create_zk, _mock_enable, _mock_wait, _mock_sleep,
    ):
        """reset_all must retry non-maintenance node deletion when first attempt fails.

        Simulates: pgconsul primary recreates a child under `alive/` while
        reset_all is recursively deleting it → NotEmptyError → zk.delete()
        returns False on first attempt, succeeds on retry.
        """
        zk = _make_zk(children=['alive', 'leader', 'maintenance'])

        call_counts: dict[str, int] = {}

        def mock_delete(node, recursive=False):
            call_counts[node] = call_counts.get(node, 0) + 1
            # 'alive' fails first (NotEmptyError race), succeeds on retry
            if node == 'alive':
                return call_counts[node] >= 2
            # 'leader' succeeds on first try
            if node == 'leader':
                return True
            # 'maintenance' succeeds on first try
            if node == 'maintenance':
                return True
            return True

        zk.delete.side_effect = mock_delete
        mock_create_zk.return_value.__enter__.return_value = zk

        # Should NOT raise — transient failure on 'alive' must be retried
        reset_all(_make_opts(), _make_conf())

        # Must have retried 'alive' deletion
        assert call_counts.get('alive', 0) >= 2, (
            f"Expected 'alive' to be retried, but delete was called "
            f"{call_counts.get('alive', 0)} times"
        )
