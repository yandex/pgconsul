from unittest.mock import MagicMock, call, patch

from src.commands import PromotionResult
from src.main import Pgconsul


def _make_instance(operation='switchover'):
    inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = MagicMock(promote_checkpoint_sql=None, use_pg_patches=False)
    inst._slot_manager = MagicMock()
    inst._replication_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    store = MagicMock()
    store.read.return_value = None
    scope = 'switchover_candidate' if operation == 'switchover' else 'failover_participant'
    inst._local_states = {scope: store}
    inst.zk.get_ha_replics.return_value = []
    inst.zk.get_durability_config.return_value = None
    inst.zk.write_timeline.return_value = True
    inst._slot_manager.create_slots_for_hosts.return_value = True
    inst._replication_manager.set_ssn_before_promote.return_value = True
    inst._replication_manager.discard_transition_after_failover.return_value = True
    inst.db.get_role.return_value = 'replica'
    inst.db.promote.return_value = True
    inst.db.checkpoint.return_value = True
    inst.db.get_timeline.return_value = 2
    return inst, store


def test_switchover_promotion_does_not_touch_failover_metadata():
    inst, store = _make_instance('switchover')

    assert inst._run_promotion(
        'switchover_candidate', 'operation-1', old_primary='old-primary'
    ) == PromotionResult.SUCCESS

    assert store.write.call_args_list == [
        call('operation-1', 'creating_slots'),
        call('operation-1', 'promoting'),
        call('operation-1', 'checkpointing'),
    ]
    store.clear.assert_not_called()
    inst.zk.write_failover_state.assert_not_called()
    inst.zk.delete_failover_state.assert_not_called()


def test_promoting_group_skips_completed_slot_group():
    inst, store = _make_instance('failover')
    store.read.return_value = 'promoting'

    with patch.object(inst, '_promote', return_value=True) as promote, \
         patch.object(inst, '_finish_promote', return_value=True) as finish:
        assert inst._run_promotion('failover_participant', 'operation-1') == PromotionResult.SUCCESS

    inst.db.pg_wal_replay_resume.assert_not_called()
    inst._replication_manager.set_ssn_before_promote.assert_not_called()
    promote.assert_called_once_with()
    finish.assert_called_once_with()
    store.write.assert_called_once_with('operation-1', 'checkpointing')


def test_promotion_passes_reserved_timeline_to_postgres():
    inst, store = _make_instance('switchover')
    store.read.return_value = 'promoting'

    with patch.object(inst, '_promote', return_value=True) as promote, \
         patch.object(inst, '_finish_promote', return_value=True):
        assert inst._run_promotion(
            'switchover_candidate', 'operation-1',
            prepared=True, target_timeline=21,
        ) == PromotionResult.SUCCESS

    promote.assert_called_once_with(target_timeline=21)


def test_patched_failover_reserves_and_uses_target_timeline():
    inst, store = _make_instance('failover')
    inst.config.use_pg_patches = True
    inst.db.get_timeline.return_value = 9
    inst.db.next_local_timeline.return_value = 12
    inst.zk.reserve_timeline.return_value = 21

    with patch.object(inst, '_promote', return_value=True) as promote, \
         patch.object(inst, '_finish_promote', return_value=True):
        assert inst._run_promotion(
            'failover_participant', 'operation-1',
        ) == PromotionResult.SUCCESS

    inst.zk.reserve_timeline.assert_called_once_with('failover:operation-1', 11)
    promote.assert_called_once_with(target_timeline=21)


def test_checkpointing_group_skips_promote():
    inst, store = _make_instance('switchover')
    store.read.return_value = 'checkpointing'

    with patch.object(inst, '_promote') as promote, \
         patch.object(inst, '_finish_promote', return_value=True) as finish:
        assert inst._run_promotion('switchover_candidate', 'operation-1') == PromotionResult.SUCCESS

    promote.assert_not_called()
    finish.assert_called_once_with()
    store.clear.assert_not_called()


def test_failover_discards_old_durability_transition_before_success():
    inst, store = _make_instance('failover')
    store.read.return_value = 'checkpointing'
    events = []
    inst._replication_manager.discard_transition_after_failover.side_effect = (
        lambda: events.append('discard') or True
    )
    inst._replication_manager.leave_sync_group.side_effect = (
        lambda: events.append('leave_sync_group')
    )

    with patch.object(
        inst, '_finish_promote', side_effect=lambda: events.append('finish') or True,
    ):
        assert inst._run_promotion('failover_participant', 'operation-1') == PromotionResult.SUCCESS

    assert events == ['finish', 'discard', 'leave_sync_group']


def test_failover_retries_before_success_when_transition_discard_conflicts():
    inst, store = _make_instance('failover')
    store.read.return_value = 'checkpointing'
    inst._replication_manager.discard_transition_after_failover.return_value = False

    with patch.object(inst, '_finish_promote', return_value=True):
        assert inst._run_promotion('failover_participant', 'operation-1') == PromotionResult.RETRY

    inst._replication_manager.leave_sync_group.assert_not_called()


def test_switchover_does_not_discard_durability_transition():
    inst, store = _make_instance('switchover')
    store.read.return_value = 'checkpointing'

    with patch.object(inst, '_finish_promote', return_value=True):
        assert inst._run_promotion('switchover_candidate', 'operation-1') == PromotionResult.SUCCESS

    inst._replication_manager.discard_transition_after_failover.assert_not_called()


def test_failed_promote_is_rejected_only_after_postgres_stays_replica():
    inst, store = _make_instance('switchover')
    store.read.return_value = 'promoting'

    with patch.object(inst, '_promote', return_value=False):
        assert inst._run_promotion('switchover_candidate', 'operation-1') == PromotionResult.REJECTED


def test_checkpoint_failure_remains_retryable():
    inst, store = _make_instance('switchover')
    store.read.return_value = 'checkpointing'

    with patch.object(inst, '_finish_promote', return_value=False):
        assert inst._run_promotion('switchover_candidate', 'operation-1') == PromotionResult.RETRY


def test_promote_command_is_skipped_when_postgres_is_already_primary():
    inst, _ = _make_instance('failover')
    inst.db.get_role.return_value = 'primary'

    assert inst._promote() is True

    inst.db.promote.assert_not_called()


def test_dead_postgres_is_started_before_resuming_persisted_promotion_phase():
    """Regression for failed_promote.feature:51."""
    inst, store = _make_instance('failover')
    store.read.return_value = 'checkpointing'
    inst.db.is_alive_and_in_terminal_state.return_value = (False, True)
    inst.db.start_postgresql.return_value = 0

    with patch.object(inst, '_finish_promote', return_value=True) as finish:
        assert inst._run_promotion(
            'failover_participant',
            'operation-1',
            start_postgresql=True,
        ) == PromotionResult.RETRY

        assert inst._run_promotion('failover_participant', 'operation-1') == PromotionResult.SUCCESS

    inst.db.start_postgresql.assert_called_once_with()
    finish.assert_called_once_with()
    store.write.assert_not_called()
