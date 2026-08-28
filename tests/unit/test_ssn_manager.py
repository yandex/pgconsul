# encoding: utf-8
"""
Unit tests for SsnManager.
"""

import importlib
from unittest.mock import MagicMock

import pytest

# Bootstrap (sys.path, sys.modules stubs) is handled by conftest.py
_ssn_mod = importlib.import_module('src.ssn_manager')
SsnManager = _ssn_mod.SsnManager
from src.types import (
    DurabilityConfig,
    DurabilityState,
    DurabilityTransition,
    DurabilityTransitionOrder,
)


def _make_manager():
    db = MagicMock()
    db.get_current_wal_flush_lsn.return_value = 100
    db.get_replica_flush_lsns.return_value = {
        'a': 100,
        'b': 100,
        'c': 100,
        'host1': 100,
        'host2': 100,
        'host3': 100,
    }
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

    def test_required_is_derived_from_replica_count(self):
        mgr, _, _ = _make_manager()

        result = mgr.calculate_quorum_ssn(['host1', 'host2', 'host3'])

        assert result == 'ANY 2(host1,host2,host3)'

    def test_builds_ssn_from_all_durability_members(self):
        mgr, _, _ = _make_manager()
        config = DurabilityConfig(('primary', 'replica1', 'replica2'))

        result = mgr.calculate_ssn_for_host(config, 'primary')

        assert result == 'ANY 1(replica1,replica2)'

    def test_two_host_switchover_keeps_one_sync_replica(self):
        mgr, _, _ = _make_manager()
        config = DurabilityConfig(('old-primary', 'candidate'))

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


class TestDurabilityTransition:

    @pytest.mark.parametrize(
        ('stable_members', 'source_members', 'target_members', 'order'),
        [
            (
                ['p', 'a', 'b'],
                ['p', 'a', 'b'],
                ['p', 'a', 'b', 'c'],
                DurabilityTransitionOrder.SSN_FIRST,
            ),
            (
                ['p', 'a', 'b'],
                ['p', 'a'],
                ['p', 'a', 'b'],
                DurabilityTransitionOrder.ZK_FIRST,
            ),
        ],
    )
    def test_failover_discards_transition_and_preserves_stable(
        self, stable_members, source_members, target_members, order,
    ):
        mgr, db, zk = _make_manager()
        stable = DurabilityConfig.build(stable_members)
        transition = DurabilityTransition(
            DurabilityConfig.build(source_members),
            DurabilityConfig.build(target_members),
            order,
            lsn=100 if order == DurabilityTransitionOrder.SSN_FIRST else None,
        )
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (
            DurabilityState(stable, transition), 11,
        )
        zk.write_durability_state.return_value = 12

        assert mgr.discard_transition_after_failover()

        zk.write_durability_state.assert_called_once_with(
            DurabilityState(stable), 11,
        )
        db.change_replication_type.assert_not_called()
        db.get_current_wal_flush_lsn.assert_not_called()
        db.get_replica_flush_lsns.assert_not_called()

    def test_failover_retries_transition_discard_after_cas_conflict(self):
        mgr, _, zk = _make_manager()
        stable = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(
            stable,
            DurabilityConfig.build(['p', 'a', 'b', 'c']),
            DurabilityTransitionOrder.SSN_FIRST,
            lsn=100,
        )
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (
            DurabilityState(stable, transition), 11,
        )
        zk.write_durability_state.return_value = None

        assert not mgr.discard_transition_after_failover()

    def test_failover_transition_discard_is_idempotent(self):
        mgr, _, zk = _make_manager()
        stable = DurabilityConfig.build(['p', 'a', 'b'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(stable), 12)

        assert mgr.discard_transition_after_failover()

        zk.write_durability_state.assert_not_called()

    def test_order_covers_all_adjacent_majority_changes(self):
        mgr, _, _ = _make_manager()
        two = DurabilityConfig.build(['p', 'a'])
        three = DurabilityConfig.build(['p', 'a', 'b'])
        four = DurabilityConfig.build(['p', 'a', 'b', 'c'])

        assert mgr.transition_order(two, three) == DurabilityTransitionOrder.ZK_FIRST
        assert mgr.transition_order(three, two) == DurabilityTransitionOrder.SSN_FIRST
        assert mgr.transition_order(three, four) == DurabilityTransitionOrder.SSN_FIRST
        assert mgr.transition_order(four, three) == DurabilityTransitionOrder.ZK_FIRST

    @pytest.mark.parametrize(
        ('source_members', 'target_members', 'order'),
        [
            (['p', 'a'], ['p', 'a', 'b'], DurabilityTransitionOrder.ZK_FIRST),
            (['p', 'a', 'b'], ['p', 'a'], DurabilityTransitionOrder.SSN_FIRST),
            (['p', 'a', 'b'], ['p', 'a', 'b', 'c'], DurabilityTransitionOrder.SSN_FIRST),
            (['p', 'a', 'b', 'c'], ['p', 'a', 'b'], DurabilityTransitionOrder.ZK_FIRST),
        ],
    )
    def test_all_adjacent_transitions_reach_stable_target(self, source_members, target_members, order):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(source_members)
        target = DurabilityConfig.build(target_members)
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.side_effect = [8, 9, 10]
        db.change_replication_type.return_value = True

        assert mgr.reconcile_durability(target, 'p')

        prepared = zk.write_durability_state.call_args_list[0].args
        expected_stable = source if order == DurabilityTransitionOrder.SSN_FIRST else target
        assert prepared == (DurabilityState(expected_stable, DurabilityTransition(source, target, order)), 7)
        final_call = zk.write_durability_state.call_args_list[-1].args
        expected_version = 9 if order == DurabilityTransitionOrder.SSN_FIRST else 8
        assert final_call == (DurabilityState(target), expected_version)

    def test_replacement_adds_one_host_before_removal(self):
        mgr, _, _ = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b'])
        desired = DurabilityConfig.build(['p', 'b', 'c'])

        assert mgr.next_config(source, desired, 'p') == DurabilityConfig.build(['p', 'a', 'b', 'c'])

    def test_ssn_first_publishes_transition_before_applying_ssn(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b'])
        desired = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        events = []
        zk.write_durability_state.side_effect = lambda state, version: events.append(('zk', state, version)) or version + 1
        db.change_replication_type.side_effect = lambda ssn: events.append(('ssn', ssn)) or True

        assert mgr.reconcile_durability(desired, 'p')

        assert events[0][0] == 'zk'
        assert events[0][1].stable == source
        assert events[0][1].transition.order == DurabilityTransitionOrder.SSN_FIRST
        assert events[1] == ('ssn', 'ANY 2(a,b,c)')
        assert events[2][0] == 'zk'
        assert events[2][1].transition.lsn == 100
        assert events[3] == ('zk', DurabilityState(desired), 9)

    def test_ssn_first_does_not_publish_target_before_lsn_barrier(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b'])
        target = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.side_effect = [8, 9]
        db.change_replication_type.return_value = True
        db.get_current_wal_flush_lsn.return_value = 100
        db.get_replica_flush_lsns.return_value = {'a': 100, 'b': 99, 'c': 99}

        assert not mgr.reconcile_durability(target, 'p')

        assert all(
            call.args[0] != DurabilityState(target)
            for call in zk.write_durability_state.call_args_list
        )
        assert zk.write_durability_state.call_args_list[-1].args[0].transition.lsn == 100

    def test_initialization_does_not_publish_stable_before_lsn_barrier(self):
        mgr, db, zk = _make_manager()
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(None), None)
        zk.write_durability_state.side_effect = [0, 1]
        db.change_replication_type.return_value = True
        db.get_current_wal_flush_lsn.return_value = 100
        db.get_replica_flush_lsns.return_value = {'a': 99, 'b': 99}

        assert not mgr.reconcile_durability(target, 'p')

        assert all(
            call.args[0] != DurabilityState(target)
            for call in zk.write_durability_state.call_args_list
        )
        assert zk.write_durability_state.call_args_list[0].args[0].stable is None
        assert zk.write_durability_state.call_args_list[-1].args[0].transition.lsn == 100

    def test_initialization_publishes_stable_after_lsn_barrier(self):
        mgr, db, zk = _make_manager()
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(None), None)
        zk.write_durability_state.side_effect = [0, 1, 2]
        db.change_replication_type.return_value = True
        db.get_replica_flush_lsns.return_value = {'a': 100, 'b': 99}

        assert mgr.reconcile_durability(target, 'p')

        assert zk.write_durability_state.call_args_list[-1].args == (
            DurabilityState(target), 1,
        )

    def test_zk_first_publishes_target_before_applying_ssn(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        desired = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(source), 3)
        events = []
        zk.write_durability_state.side_effect = lambda state, version: events.append(('zk', state, version)) or version + 1
        db.change_replication_type.side_effect = lambda ssn: events.append(('ssn', ssn)) or True

        assert mgr.reconcile_durability(desired, 'p')

        assert events[0][0] == 'zk'
        assert events[0][1].stable == desired
        assert events[0][1].transition.order == DurabilityTransitionOrder.ZK_FIRST
        assert events[1] == ('ssn', 'ANY 1(a,b)')
        assert events[2] == ('zk', DurabilityState(desired), 4)
        db.get_current_wal_flush_lsn.assert_not_called()
        db.get_replica_flush_lsns.assert_not_called()

    def test_resumes_zk_first_transition_idempotently(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(source, target, DurabilityTransitionOrder.ZK_FIRST)
        zk.get_durability_state.return_value = (DurabilityState(target, transition), 11)
        zk.write_durability_state.return_value = 12
        db.change_replication_type.return_value = True

        assert mgr.reconcile_durability(target, 'p')

        db.change_replication_type.assert_called_once_with('ANY 1(a,b)')
        zk.write_durability_state.assert_called_once_with(DurabilityState(target), 11)

    def test_resumes_ssn_first_transition_idempotently(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b'])
        target = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        transition = DurabilityTransition(source, target, DurabilityTransitionOrder.SSN_FIRST)
        zk.get_durability_state.return_value = (DurabilityState(source, transition), 11)
        zk.write_durability_state.side_effect = [12, 13]
        db.change_replication_type.return_value = True

        assert mgr.reconcile_durability(target, 'p')

        db.change_replication_type.assert_called_once_with('ANY 2(a,b,c)')
        assert zk.write_durability_state.call_args_list[-1].args == (DurabilityState(target), 12)

    def test_resumes_persisted_lsn_barrier_without_reapplying_ssn(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b'])
        target = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        transition = DurabilityTransition(
            source, target, DurabilityTransitionOrder.SSN_FIRST, lsn=100,
        )
        zk.get_durability_state.return_value = (DurabilityState(source, transition), 11)
        zk.write_durability_state.return_value = 12

        assert mgr.reconcile_durability(target, 'p')

        db.change_replication_type.assert_not_called()
        db.get_current_wal_flush_lsn.assert_not_called()
        zk.write_durability_state.assert_called_once_with(DurabilityState(target), 11)

    def test_ssn_first_apply_failure_keeps_recorded_transition(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b'])
        target = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.return_value = 8
        db.change_replication_type.return_value = False

        assert not mgr.reconcile_durability(target, 'p')

        written = zk.write_durability_state.call_args.args
        assert written[0].stable == source
        assert written[0].transition == DurabilityTransition(source, target, DurabilityTransitionOrder.SSN_FIRST)
        assert written[1] == 7

    def test_zk_first_apply_failure_keeps_target_as_stable(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.return_value = 8
        db.change_replication_type.return_value = False

        assert not mgr.reconcile_durability(target, 'p')

        written = zk.write_durability_state.call_args.args
        assert written[0].stable == target
        assert written[0].transition == DurabilityTransition(source, target, DurabilityTransitionOrder.ZK_FIRST)
        assert written[1] == 7

    def test_transition_is_not_started_after_cas_conflict(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.return_value = None

        assert not mgr.reconcile_durability(target, 'p')

        db.change_replication_type.assert_not_called()

    def test_finalize_cas_conflict_leaves_transition_resumable(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.side_effect = [8, None]
        db.change_replication_type.return_value = True

        assert not mgr.reconcile_durability(target, 'p')

        assert zk.write_durability_state.call_args_list[-1].args == (DurabilityState(target), 8)

    def test_large_change_advances_only_one_host(self):
        mgr, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        desired = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        first_step = DurabilityConfig.build(['p', 'a', 'b'])
        zk.get_durability_state.return_value = (DurabilityState(source), 1)
        zk.write_durability_state.side_effect = [2, 3]
        db.change_replication_type.return_value = True

        assert not mgr.reconcile_durability(desired, 'p')

        assert zk.write_durability_state.call_args_list[-1].args[0] == DurabilityState(first_step)
