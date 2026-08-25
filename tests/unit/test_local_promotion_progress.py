from unittest.mock import MagicMock, call, patch

from src.main import Pgconsul


def _make_instance(operation='switchover'):
    inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = MagicMock(promote_checkpoint_sql=None)
    inst._slot_manager = MagicMock()
    inst._replication_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    store = MagicMock()
    store.read.return_value = None
    scope = 'switchover_candidate' if operation == 'switchover' else 'failover_participant'
    inst._local_states = {scope: store}
    inst.zk.get_ha_replics.return_value = []
    inst.zk.get_quorum_replics_for_promote.return_value = []
    inst.zk.write_timeline.return_value = True
    inst._slot_manager.create_slots_for_hosts.return_value = True
    inst._replication_manager.set_ssn_before_promote.return_value = True
    inst.db.get_role.return_value = 'replica'
    inst.db.promote.return_value = True
    inst.db.checkpoint.return_value = True
    inst.db.get_timeline.return_value = 2
    return inst, store


def test_switchover_promotion_does_not_touch_failover_metadata():
    inst, store = _make_instance('switchover')

    assert inst._run_promotion('switchover_candidate', old_primary='old-primary') is True

    assert store.write.call_args_list == [
        call('creating_slots'),
        call('promoting'),
        call('checkpointing'),
    ]
    store.clear.assert_not_called()
    inst.zk.write_failover_state.assert_not_called()
    inst.zk.delete_failover_state.assert_not_called()


def test_promoting_group_skips_completed_slot_group():
    inst, store = _make_instance('failover')
    store.read.return_value = 'promoting'

    with patch.object(inst, '_promote', return_value=True) as promote, \
         patch.object(inst, '_finish_promote', return_value=True) as finish:
        assert inst._run_promotion('failover_participant') is True

    inst.db.pg_wal_replay_resume.assert_not_called()
    inst._replication_manager.set_ssn_before_promote.assert_not_called()
    promote.assert_called_once_with()
    finish.assert_called_once_with()
    store.write.assert_called_once_with('checkpointing')


def test_checkpointing_group_skips_promote():
    inst, store = _make_instance('switchover')
    store.read.return_value = 'checkpointing'

    with patch.object(inst, '_promote') as promote, \
         patch.object(inst, '_finish_promote', return_value=True) as finish:
        assert inst._run_promotion('switchover_candidate') is True

    promote.assert_not_called()
    finish.assert_called_once_with()
    store.clear.assert_not_called()


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
            start_postgresql=True,
        ) is False

        assert inst._run_promotion('failover_participant') is True

    inst.db.start_postgresql.assert_called_once_with()
    finish.assert_called_once_with()
    store.write.assert_not_called()
