# encoding: utf-8
"""Unit tests for the unified SSN transition protocol."""

import importlib
from unittest.mock import MagicMock, patch

import pytest

_ssn_mod = importlib.import_module('src.ssn_manager')
SsnManager = _ssn_mod.SsnManager
from src.types import DurabilityConfig, DurabilityState, DurabilityTransition


def _make_manager():
    db = MagicMock()
    db.advance_wal_barrier.return_value = True
    zk = MagicMock()
    return SsnManager(db, zk), db, zk


class TestCalculateQuorumSsn:

    @pytest.mark.parametrize(
        ('hosts', 'expected'),
        [
            (['host1', 'host2', 'host3'], 'ANY 2(host1,host2,host3)'),
            (['host1', 'host2'], 'ANY 1(host1,host2)'),
            (['host1'], 'ANY 1(host1)'),
            ([], ''),
            (['z-host', 'a-host'], 'ANY 1(a_host,z_host)'),
            (['host1', 'host2', 'host1'], 'ANY 1(host1,host2)'),
        ],
    )
    def test_calculates_deterministic_quorum(self, hosts, expected):
        manager, _, _ = _make_manager()

        assert manager.calculate_quorum_ssn(hosts) == expected

    def test_builds_ssn_from_all_other_durability_members(self):
        manager, _, _ = _make_manager()
        config = DurabilityConfig.build(['primary', 'replica1', 'replica2'])

        assert manager.calculate_ssn_for_host(config, 'primary') == 'ANY 1(replica1,replica2)'

    def test_keeps_quorum_and_requires_candidate(self):
        manager, _, _ = _make_manager()
        config = DurabilityConfig.build([
            'primary.dc', 'candidate.dc', 'side1.dc', 'side2.dc',
        ])

        assert manager.calculate_ssn_with_mandatory(
            config, 'primary.dc', 'candidate.dc',
        ) == 'EVERY(candidate_dc), ANY 2(candidate_dc,side1_dc,side2_dc)'

    def test_mandatory_replica_must_belong_to_durability(self):
        manager, _, _ = _make_manager()
        config = DurabilityConfig.build(['primary', 'side'])

        with pytest.raises(ValueError, match='absent'):
            manager.calculate_ssn_with_mandatory(
                config, 'primary', 'candidate',
            )


class TestApplyAndPersist:

    def test_success_calls_db_and_monitoring_write(self):
        manager, db, zk = _make_manager()
        db.change_replication_type.return_value = True

        assert manager.apply_and_persist('ANY 1(h1)', 'action', 'success')

        db.change_replication_type.assert_called_once_with('ANY 1(h1)')
        zk.write_ssn_on_changes.assert_called_once_with('ANY 1(h1)')

    def test_db_failure_does_not_write_monitoring_value(self):
        manager, db, zk = _make_manager()
        db.change_replication_type.return_value = False

        assert not manager.apply_and_persist('ANY 1(h1)', 'action', 'success')

        zk.write_ssn_on_changes.assert_not_called()


class TestDurabilityTransition:

    def test_resume_only_completes_persisted_transition(self):
        manager, _, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(source, target, 'operation')
        state = DurabilityState(source, transition)
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (state, 11)
        manager._complete_transition = MagicMock(return_value=False)

        assert not manager.resume_durability_transition('p')
        manager._complete_transition.assert_called_once_with(state, 11, 'p')

    def test_resume_without_transition_is_noop(self):
        manager, db, zk = _make_manager()
        stable = DurabilityConfig.build(['p', 'a'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(stable), 11)

        assert manager.resume_durability_transition('p')

        db.change_replication_type.assert_not_called()
        zk.write_durability_state.assert_not_called()

    @pytest.mark.parametrize(
        ('source_members', 'target_members', 'target_ssn'),
        [
            (['p', 'a'], ['p', 'a', 'b'], 'ANY 1(a,b)'),
            (['p', 'a', 'b'], ['p', 'a'], 'ANY 1(a)'),
            (['p', 'a', 'b'], ['p', 'a', 'b', 'c'], 'ANY 2(a,b,c)'),
            (['p', 'a', 'b', 'c'], ['p', 'a', 'b'], 'ANY 1(a,b)'),
        ],
    )
    def test_every_adjacent_change_uses_same_order(
        self, source_members, target_members, target_ssn,
    ):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(source_members)
        target = DurabilityConfig.build(target_members)
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        events = []
        zk.write_durability_state.side_effect = (
            lambda state, version: events.append(('zk', state, version)) or version + 1
        )
        db.change_replication_type.side_effect = lambda ssn: events.append(('ssn', ssn)) or True
        db.advance_wal_barrier.side_effect = lambda op: events.append(('barrier', op)) or True

        with patch('src.ssn_manager.uuid.uuid4') as uuid4:
            uuid4.return_value.hex = 'operation'
            assert manager.reconcile_durability(target, 'p')

        transition = DurabilityTransition(source, target, 'operation')
        assert events == [
            ('zk', DurabilityState(source, transition), 7),
            ('ssn', target_ssn),
            ('barrier', 'operation'),
            ('zk', DurabilityState(target), 8),
        ]

    def test_barrier_in_progress_does_not_publish_target(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(source, target, 'operation')
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source, transition), 8)
        db.change_replication_type.return_value = True
        db.advance_wal_barrier.return_value = False

        assert not manager.reconcile_durability(target, 'p')

        zk.write_durability_state.assert_not_called()

    def test_restart_reapplies_ssn_and_repeats_barrier(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(source, target, 'operation')
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source, transition), 8)
        zk.write_durability_state.return_value = 9
        db.change_replication_type.return_value = True

        assert manager.reconcile_durability(target, 'p')

        db.change_replication_type.assert_called_once_with('ANY 1(a,b)')
        db.advance_wal_barrier.assert_called_once_with('operation')
        zk.write_durability_state.assert_called_once_with(DurabilityState(target), 8)

    def test_apply_failure_keeps_recorded_intent(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.return_value = 8
        db.change_replication_type.return_value = False

        with patch('src.ssn_manager.uuid.uuid4') as uuid4:
            uuid4.return_value.hex = 'operation'
            assert not manager.reconcile_durability(target, 'p')

        zk.write_durability_state.assert_called_once_with(
            DurabilityState(source, DurabilityTransition(source, target, 'operation')),
            7,
        )
        db.advance_wal_barrier.assert_not_called()

    def test_intent_cas_conflict_does_not_apply_ssn(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.return_value = None

        assert not manager.reconcile_durability(target, 'p')

        db.change_replication_type.assert_not_called()

    def test_finalize_cas_conflict_leaves_transition_resumable(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        zk.write_durability_state.side_effect = [8, None]
        db.change_replication_type.return_value = True

        assert not manager.reconcile_durability(target, 'p')

        assert zk.write_durability_state.call_args_list[-1].args == (
            DurabilityState(target), 8,
        )

    def test_large_change_advances_only_one_host(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        desired = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        first_step = DurabilityConfig.build(['p', 'a', 'b'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source), 1)
        zk.write_durability_state.side_effect = [2, 3]
        db.change_replication_type.return_value = True

        assert not manager.reconcile_durability(desired, 'p')

        assert zk.write_durability_state.call_args_list[-1].args == (
            DurabilityState(first_step), 2,
        )

    def test_large_contraction_removes_all_unavailable_members_in_one_transition(self):
        """A live primary must recover writes after several replicas fail."""
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a', 'b', 'c'])
        target = DurabilityConfig.build(['p'])
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source), 7)
        events = []
        zk.write_durability_state.side_effect = (
            lambda state, version: events.append(('zk', state, version)) or version + 1
        )
        db.change_replication_type.side_effect = lambda ssn: events.append(('ssn', ssn)) or True
        db.advance_wal_barrier.side_effect = lambda op: events.append(('barrier', op)) or True

        with patch('src.ssn_manager.uuid.uuid4') as uuid4:
            uuid4.return_value.hex = 'operation'
            assert manager.reconcile_durability(target, 'p')

        transition = DurabilityTransition(source, target, 'operation')
        assert events == [
            ('zk', DurabilityState(source, transition), 7),
            ('ssn', ''),
            ('barrier', 'operation'),
            ('zk', DurabilityState(target), 8),
        ]

    def test_rejects_multi_host_expansion_and_replacement_transition(self):
        source = DurabilityConfig.build(['p', 'a', 'b'])

        with pytest.raises(ValueError, match='add exactly one'):
            SsnManager.validate_transition(
                source, DurabilityConfig.build(['p', 'a', 'b', 'c', 'd']),
            )
        with pytest.raises(ValueError, match='only add or only remove'):
            SsnManager.validate_transition(
                source, DurabilityConfig.build(['p', 'a', 'c']),
            )

    def test_failover_discards_transition_and_keeps_source(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(source, target, 'operation')
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (DurabilityState(source, transition), 8)
        zk.write_durability_state.return_value = 9

        assert manager.discard_transition_after_failover('a')

        zk.write_durability_state.assert_called_once_with(DurabilityState(source), 8)
        db.change_replication_type.assert_not_called()

    def test_failover_target_only_winner_materializes_target(self):
        manager, db, zk = _make_manager()
        source = DurabilityConfig.build(['p', 'a'])
        target = DurabilityConfig.build(['p', 'a', 'b'])
        transition = DurabilityTransition(source, target, 'operation')
        zk.is_lock_holder.return_value = True
        zk.get_durability_state.return_value = (
            DurabilityState(source, transition), 8,
        )
        zk.write_durability_state.return_value = 9

        assert manager.discard_transition_after_failover('b')

        zk.write_durability_state.assert_called_once_with(
            DurabilityState(target), 8,
        )
