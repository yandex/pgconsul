# encoding: utf-8
"""
Unit tests for SsnManager.
"""

import importlib
from unittest.mock import MagicMock

# Bootstrap (sys.path, sys.modules stubs) is handled by conftest.py
_ssn_mod = importlib.import_module('src.ssn_manager')
SsnManager = _ssn_mod.SsnManager
from src.types import DurabilityConfig


def _make_manager():
    db = MagicMock()
    zk = MagicMock()
    return SsnManager(db, zk), db, zk


class TestCalculateQuorumSsn:

    def test_three_replicas(self):
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['host1', 'host2', 'host3'])
        # quorum_size = (3 + 1) // 2 = 2
        assert result == 'ANY 2(host1,host2,host3)'

    def test_two_replicas(self):
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['host1', 'host2'])
        # quorum_size = (2 + 1) // 2 = 1
        assert result == 'ANY 1(host1,host2)'

    def test_one_replica(self):
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['host1'])
        # quorum_size = (1 + 1) // 2 = 1
        assert result == 'ANY 1(host1)'

    def test_empty_list_returns_empty_string(self):
        mgr, _, _ = _make_manager()
        assert mgr.calculate_quorum_ssn([]) == ''

    def test_four_replicas_quorum_size_two(self):
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['h1', 'h2', 'h3', 'h4'])
        # quorum_size = (4 + 1) // 2 = 2
        assert result.startswith('ANY 2(')

    def test_dashes_replaced_with_underscores(self):
        """app_name_from_fqdn replaces dashes with underscores."""
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['sas-abc', 'vla-xyz'])
        assert 'sas_abc' in result
        assert 'vla_xyz' in result

    def test_hosts_are_sorted(self):
        """Hosts in the SSN string must be sorted for deterministic output."""
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['host3', 'host1', 'host2'])
        assert result == 'ANY 2(host1,host2,host3)'

    def test_reverse_order_is_sorted(self):
        """Even reverse-ordered input produces sorted output."""
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['z-host', 'a-host'])
        assert result == 'ANY 1(a_host,z_host)'

    def test_duplicates_are_removed(self):
        """Duplicate hosts must be deduplicated before quorum calculation."""
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['host1', 'host2', 'host1'])
        # Only 2 unique hosts → quorum_size = (2 + 1) // 2 = 1
        assert result == 'ANY 1(host1,host2)'

    def test_all_duplicates_single_host(self):
        """All entries are the same host → treated as single replica."""
        mgr, _, _ = _make_manager()
        result = mgr.calculate_quorum_ssn(['host1', 'host1', 'host1'])
        assert result == 'ANY 1(host1)'

    def test_uses_stored_required_value(self):
        mgr, _, _ = _make_manager()

        result = mgr.calculate_quorum_ssn(['host1', 'host2', 'host3'], required=1)

        assert result == 'ANY 1(host1,host2,host3)'

    def test_builds_ssn_from_all_durability_members(self):
        mgr, _, _ = _make_manager()
        config = DurabilityConfig(('primary', 'replica1', 'replica2'), required=1)

        result = mgr.calculate_ssn_for_host(config, 'primary')

        assert result == 'ANY 1(replica1,replica2)'

    def test_two_host_switchover_keeps_one_sync_replica(self):
        mgr, _, _ = _make_manager()
        config = DurabilityConfig(('old-primary', 'candidate'), required=1)

        assert mgr.calculate_ssn_for_host(config, 'old-primary') == 'ANY 1(candidate)'
        assert mgr.calculate_ssn_for_host(config, 'candidate') == 'ANY 1(old_primary)'


class TestApplyAndPersist:

    def test_success_calls_db_and_zk(self):
        mgr, db, zk = _make_manager()
        db.change_replication_type.return_value = True

        result = mgr.apply_and_persist('ANY 1(h1)', 'action', 'success')

        assert result is True
        db.change_replication_type.assert_called_once_with('ANY 1(h1)')
        zk.write_ssn_on_changes.assert_called_once_with('ANY 1(h1)')

    def test_db_failure_returns_false_no_zk_write(self):
        """DB fails → False, ZK never written."""
        mgr, db, zk = _make_manager()
        db.change_replication_type.return_value = False

        result = mgr.apply_and_persist('ANY 1(h1)', 'action', 'success')

        assert result is False
        zk.write_ssn_on_changes.assert_not_called()

    def test_empty_ssn_async_mode(self):
        """Empty SSN string (async) is applied correctly."""
        mgr, db, zk = _make_manager()
        db.change_replication_type.return_value = True

        result = mgr.apply_and_persist('', 'turning off sync', 'turned off sync')

        assert result is True
        db.change_replication_type.assert_called_once_with('')
        zk.write_ssn_on_changes.assert_called_once_with('')

    def test_zk_write_called_on_db_success(self):
        """write_ssn_on_changes is called once when DB call succeeds."""
        mgr, db, zk = _make_manager()
        db.change_replication_type.return_value = True

        mgr.apply_and_persist('ANY 1(h1)', 'action', 'success')

        zk.write_ssn_on_changes.assert_called_once_with('ANY 1(h1)')

    def test_zk_write_not_called_on_db_failure(self):
        """write_ssn_on_changes is not called when DB call fails."""
        mgr, db, zk = _make_manager()
        db.change_replication_type.return_value = False

        mgr.apply_and_persist('ANY 1(h1)', 'action', 'success')

        zk.write_ssn_on_changes.assert_not_called()
