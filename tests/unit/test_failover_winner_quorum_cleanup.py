"""Promotion keeps the winner in the full durability-members config."""

from unittest.mock import MagicMock, patch

from src.commands import PromotionResult


def _make_instance() -> object:
    from src.main import Pgconsul, PgconsulConfig

    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)

    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='', working_dir='/tmp', iteration_timeout=0.0,
        quorum_commit=False, update_prio_in_zk=False,
        use_replication_slots=False, replication_slots_polling=False,
        priority='100', stream_from=None, autofailover=False,
        switchover_timeout=0.0, switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        close_detached_after=0.0, start_pooler=False, recovery_timeout=0.0,
        can_delayed=False, primary_switch_disable_archive_restore=False,
        primary_switch_checks=0, primary_switch_restart=False,
        primary_unavailability_timeout=0.0, walreceiver_disable_timeout=0.0,
        min_failover_timeout=0.0, change_replication_type=False,
        sync_replication_in_maintenance=False, promote_checkpoint_sql=None,
        failure_name=None, failure_count=100_000_000,
        sleep_before_disable_walreceiver=0.0, election_lsn_read_sleep=0.0,
        election_loser_timeout=0,
    )
    inst._durability_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    local_state = MagicMock()
    local_state.read.return_value = None
    inst._local_states = {'failover_participant': local_state}
    return inst


def test_successful_promote_keeps_full_durability_members():
    inst = _make_instance()
    inst._durability_manager.set_ssn_before_promote.return_value = True

    with patch.object(inst, '_promote_handle_slots', return_value=True), \
         patch.object(inst, '_promote', return_value=True), \
         patch.object(inst, '_finish_promote', return_value=True):
        result = inst._run_promotion('failover_participant', 'version-1')

    assert result == PromotionResult.RETRY
    inst._local_states['failover_participant'].write.assert_called_with(
        'version-1', 'waiting_durability',
    )
    inst._durability_manager.remove_self_from_quorum_after_promote.assert_not_called()
