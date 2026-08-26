# encoding: utf-8
"""
Tests for get_members() / get_children() semantics contract.

Contract:
  - None   → ZK error (must NOT be interpreted as "empty cluster")
  - []     → node absent or has no children (empty, but not an error)
  - [...]  → normal non-empty list

This distinction is critical for slot/quorum logic: treating a ZK error as an
empty member list could silently destroy replication topology.
"""

from unittest.mock import MagicMock

import pytest

from src.types import DurabilityConfig


class TestGetMembersSemantics:
    """Verify that get_members() correctly distinguishes error from empty."""

    def test_get_members_returns_list_on_success(self, zk):
        """get_members() returns the member list when ZK responds normally."""
        zk.get_children = MagicMock(return_value=['host1', 'host2'])
        result = zk.get_members()
        assert result == ['host1', 'host2']

    def test_get_members_returns_empty_list_when_node_absent(self, zk):
        """get_members() returns [] when the members node has no children (or is absent)."""
        zk.get_children = MagicMock(return_value=[])
        result = zk.get_members()
        assert result == []

    def test_get_members_returns_none_on_zk_error(self, zk):
        """get_members() returns None on ZK error (catch_except=True).

        Callers MUST check ``is None`` before using the result — returning None
        is fundamentally different from returning [] and must not be treated as
        "empty cluster".
        """
        zk.get_children = MagicMock(return_value=None)
        result = zk.get_members()
        assert result is None

    def test_get_members_none_is_not_empty_list(self, zk):
        """None (error) and [] (empty) must be distinguishable, not equal."""
        zk.get_children = MagicMock(return_value=None)
        result_error = zk.get_members()

        zk.get_children = MagicMock(return_value=[])
        result_empty = zk.get_members()

        assert result_error is None
        assert result_empty == []
        assert result_error != result_empty

    def test_get_members_raises_on_zk_error_when_catch_except_false(self, zk):
        """get_members(catch_except=False) re-raises ZookeeperException on error."""
        from src.zk import ZookeeperException

        zk.get_children = MagicMock(side_effect=ZookeeperException('zk error'))
        with pytest.raises(ZookeeperException):
            zk.get_members(catch_except=False)

    def test_get_members_delegates_to_get_children_with_members_path(self, zk):
        """get_members() queries the correct ZK path (MEMBERS_PATH)."""
        zk.get_children = MagicMock(return_value=[])
        zk.get_members()
        zk.get_children.assert_called_once_with(zk.MEMBERS_PATH, catch_except=True)


class TestDurabilityConfig:

    def test_reads_members_and_required_from_one_node(self, zk):
        zk.get = MagicMock(return_value=DurabilityConfig.build(['p', 'r1', 'r2'], required=1))

        result = zk.get_durability_config()

        assert result == DurabilityConfig.build(['p', 'r1', 'r2'], required=1)
        assert zk.get.call_args.args[0] == zk.DURABILITY_MEMBERS_PATH

    def test_writes_members_and_required_to_one_node(self, zk):
        zk.write = MagicMock(return_value=True)
        config = DurabilityConfig.build(['p', 'r1', 'r2'], required=1)

        assert zk.write_durability_config(config) is True

        assert zk.write.call_args.args[:2] == (
            zk.DURABILITY_MEMBERS_PATH,
            {'members': ['p', 'r1', 'r2'], 'required': 1},
        )
