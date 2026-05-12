# encoding: utf-8
"""
Unit tests for SsnManager.
"""

import importlib
from unittest.mock import MagicMock

# Bootstrap (sys.path, sys.modules stubs) is handled by conftest.py
_ssn_mod = importlib.import_module('src.ssn_manager')
SsnManager = _ssn_mod.SsnManager


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


class TestBuildReplicaHostsForPromote:

    def test_known_replicas_and_extra_host(self):
        """Switchover: side replicas + current primary, sorted."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['replica1', 'replica2'],
            old_primary='primary-host',
        )
        assert result == ['primary-host', 'replica1', 'replica2']


    def test_no_known_replicas_extra_host_ignored(self):
        """Two-host cluster: old master not among replicas → old_primary is
        ignored and we fall back to async (empty list)."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=[],
            old_primary='primary-host',
        )
        assert result == []

    def test_known_replicas_no_extra_host(self):
        """Failover: ha_replics only, no old_primary."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['replica1', 'replica2'],
            old_primary=None,
        )
        assert result == ['replica1', 'replica2']

    def test_known_replicas_none_extra_host_ignored(self):
        """ZK may return None for SWITCHOVER_SIDE_REPLICAS or ha_replics.
        With no known replicas, old_primary is ignored (reduced guarantees)."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=None,
            old_primary='primary-host',
        )
        assert result == []

    def test_both_none_returns_empty_list(self):
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=None,
            old_primary=None,
        )
        assert result == []

    def test_result_is_sorted(self):
        """Result list must be sorted lexicographically."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['sas-replica', 'vla-replica'],
            old_primary='msk-primary',
        )
        assert result == ['msk-primary', 'sas-replica', 'vla-replica']

    def test_single_known_replica_and_extra_host(self):
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['replica1'],
            old_primary='primary1',
        )
        assert result == ['primary1', 'replica1']

    # --- failover-style calls (no extra_host) ---

    def test_set_converted_to_list(self):
        """Failover: ha_replics as a set is converted to list."""
        result = SsnManager.build_replica_hosts_for_promote({'replica1', 'replica2'})
        assert sorted(result) == ['replica1', 'replica2']

    def test_none_returns_empty_list(self):
        """Failover: None ha_replics → empty list."""
        result = SsnManager.build_replica_hosts_for_promote(None)
        assert result == []

    def test_empty_set_returns_empty_list(self):
        result = SsnManager.build_replica_hosts_for_promote(set())
        assert result == []

    def test_single_host(self):
        result = SsnManager.build_replica_hosts_for_promote({'only-replica'})
        assert result == ['only-replica']

    def test_result_is_list_not_set(self):
        result = SsnManager.build_replica_hosts_for_promote({'h1', 'h2', 'h3'})
        assert isinstance(result, list)

    def test_duplicate_known_replicas_are_deduplicated(self):
        """Duplicate entries in ha_replicas must appear only once."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['replica1', 'replica2', 'replica1'],
        )
        assert result == ['replica1', 'replica2']

    def test_extra_host_same_as_known_replica_deduplicated(self):
        """old_primary that duplicates a ha_replica must not appear twice."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['replica1', 'replica2'],
            old_primary='replica1',
        )
        assert result == ['replica1', 'replica2']

    def test_extra_host_unique_is_added(self):
        """old_primary that is not in ha_replicas is added normally."""
        result = SsnManager.build_replica_hosts_for_promote(
            ha_replicas=['replica1'],
            old_primary='primary1',
        )
        assert result == ['primary1', 'replica1']
