"""Unit tests for the manager-owned bridge switchover protocol (ADR-0014)."""

from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul
from src.switchover import SwitchoverPhase, SwitchoverRecord
from src.commands import PromotionResult
from src.exceptions import PostgresConnectionError
from src.failover import FailoverPhase
from src.types import DurabilityConfig, DurabilityState


def _instance():
    instance = Pgconsul.__new__(Pgconsul)
    instance.zk = MagicMock()
    instance.zk.SWITCHOVER_MANAGER_LOCK_PATH = 'switchover/manager'
    instance.zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
    instance.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    instance.zk.FAILOVER_STATE_PATH = 'failover_state'
    instance.zk.PRIMARY_LOCK_PATH = 'leader'
    instance.zk.TIMELINE_INFO_PATH = 'timeline'
    instance.db = MagicMock()
    instance._replication_manager = MagicMock()
    instance._slot_manager = MagicMock()
    instance._timings = MagicMock()
    instance._return_to_cluster = MagicMock()
    instance.start_pooler = MagicMock()
    instance.stop_postgresql = MagicMock()
    instance.config = MagicMock(
        promote_checkpoint_sql='CHECKPOINT',
        switchover_catchup_timeout=60,
    )
    return instance


def test_bridge_resumes_persisted_durability_transition_before_protocol():
    instance = _instance()
    instance._handle_bridge_switchover = MagicMock(return_value=True)
    instance._replication_manager.resume_durability_transition.return_value = False
    record = SwitchoverRecord(
        hostname='primary', phase=SwitchoverPhase.SCHEDULED,
        protocol_version=2, operation_id='operation',
    )
    zk_state = {'lock_holder': 'primary'}

    with patch.object(SwitchoverRecord, 'from_zk_state', return_value=record), \
         patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance.handle_switchover({'role': 'primary'}, zk_state) is True

    instance._replication_manager.resume_durability_transition.assert_called_once_with()
    instance._handle_bridge_switchover.assert_not_called()


def test_restarted_old_primary_reacquires_lock_before_handoff_fallback():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', version=7,
    )
    instance.zk.get_switchover_record.return_value = (record.to_dict(), 7)
    instance.zk.get_current_lock_holder.return_value = None
    instance.zk.get_timeline.return_value = 1

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._recover_pre_handoff_switchover(
            record, {'role': 'primary'}, {instance.zk.FAILOVER_STATE_PATH: None},
        ) is True

    instance.zk.try_acquire_lock.assert_called_once_with(
        instance.zk.PRIMARY_LOCK_PATH, allow_queue=False, timeout=0,
    )
    instance._replication_manager.change_replication_to_durability_config.assert_not_called()
    instance.zk.write_switchover_record.assert_not_called()


def test_missing_old_primary_initializes_failover_before_persisting_fallback():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    instance.zk.is_lock_holder.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', version=7,
    )
    instance.zk.get_switchover_record.return_value = (record.to_dict(), 7)
    instance.zk.get_current_lock_holder.return_value = None
    instance.zk.get_timeline.return_value = 1
    instance.zk.is_host_alive.return_value = False
    instance.zk.write_switchover_record.return_value = 8
    events = []
    zk_state = {instance.zk.FAILOVER_STATE_PATH: None}

    def initialize(_db_state, state):
        events.append('failover')
        state[instance.zk.FAILOVER_STATE_PATH] = FailoverPhase.WALRECEIVER_DISABLING
        return True

    instance._initialize_failover_from_switchover = MagicMock(side_effect=initialize)
    instance.zk.write_switchover_record.side_effect = lambda *_: events.append('fallback') or 8

    with patch('src.main.helpers.get_hostname', return_value='replica'):
        assert instance._recover_pre_handoff_switchover(record, {'role': 'replica'}, zk_state) is True

    assert events == ['failover', 'fallback']
    written = instance.zk.write_switchover_record.call_args.args[0]
    assert written['phase'] == SwitchoverPhase.FALLBACK


def test_old_primary_daemon_initializes_failover_when_local_postgres_is_dead():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    instance.zk.is_lock_holder.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', version=7,
    )
    instance.zk.get_switchover_record.return_value = (record.to_dict(), 7)
    instance.zk.get_current_lock_holder.return_value = None
    instance.zk.get_timeline.return_value = 1
    instance.zk.write_switchover_record.return_value = 8
    zk_state = {instance.zk.FAILOVER_STATE_PATH: None}

    def initialize(_db_state, state):
        state[instance.zk.FAILOVER_STATE_PATH] = FailoverPhase.WALRECEIVER_DISABLING
        return True

    instance._initialize_failover_from_switchover = MagicMock(side_effect=initialize)

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._recover_pre_handoff_switchover(record, {'role': None}, zk_state) is True

    instance.zk.is_host_alive.assert_not_called()
    instance._initialize_failover_from_switchover.assert_called_once()
    written = instance.zk.write_switchover_record.call_args.args[0]
    assert written['phase'] == SwitchoverPhase.FALLBACK


def test_failed_failover_initialization_does_not_persist_fallback():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', version=7,
    )
    instance.zk.get_switchover_record.return_value = (record.to_dict(), 7)
    instance.zk.get_current_lock_holder.return_value = None
    instance.zk.get_timeline.return_value = 1
    instance.zk.is_host_alive.return_value = False
    instance._initialize_failover_from_switchover = MagicMock(return_value=False)

    with patch('src.main.helpers.get_hostname', return_value='replica'):
        assert instance._recover_pre_handoff_switchover(
            record, {'role': 'replica'}, {instance.zk.FAILOVER_STATE_PATH: None},
        ) is True

    instance.zk.write_switchover_record.assert_not_called()


def test_failover_remains_authoritative_when_fallback_cas_conflicts():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    instance.zk.is_lock_holder.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', version=7,
    )
    instance.zk.get_switchover_record.return_value = (record.to_dict(), 7)
    instance.zk.get_current_lock_holder.return_value = None
    instance.zk.get_timeline.return_value = 1
    instance.zk.is_host_alive.return_value = False
    instance.zk.write_switchover_record.return_value = None
    zk_state = {instance.zk.FAILOVER_STATE_PATH: None}

    def initialize(_db_state, state):
        state[instance.zk.FAILOVER_STATE_PATH] = FailoverPhase.WALRECEIVER_DISABLING
        return True

    instance._initialize_failover_from_switchover = MagicMock(side_effect=initialize)

    with patch('src.main.helpers.get_hostname', return_value='replica'):
        assert instance._recover_pre_handoff_switchover(record, {'role': 'replica'}, zk_state) is True

    assert zk_state[instance.zk.FAILOVER_STATE_PATH] == FailoverPhase.WALRECEIVER_DISABLING
    instance.zk.write_switchover_record.assert_called_once()


def test_fallback_record_is_cleaned_after_failover_selects_primary():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    instance.zk.cleanup_switchover.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.FALLBACK, protocol_version=2,
        operation_id='operation', version=8,
    )

    with patch('src.main.helpers.get_hostname', return_value='replica'):
        assert instance._handle_bridge_switchover(
            record, {'role': 'replica'}, {'lock_holder': 'winner', 'timeline': 1},
        ) is True

    instance.zk.cleanup_switchover.assert_called_once_with(8)
    instance.zk.release_if_hold.assert_called_once_with(instance.zk.SWITCHOVER_MANAGER_LOCK_PATH)


def test_prehandoff_record_is_cleaned_if_fallback_cas_was_lost():
    instance = _instance()
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    instance.zk.cleanup_switchover.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=1,
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', version=9,
    )

    with patch('src.main.helpers.get_hostname', return_value='winner'):
        assert instance._handle_bridge_switchover(
            record, {'role': 'primary'}, {'lock_holder': 'winner', 'timeline': 1},
        ) is True

    instance.zk.cleanup_switchover.assert_called_once_with(9)


def test_primary_freezes_stable_membership_in_manager_record():
    instance = _instance()
    instance.zk.get_durability_state.return_value = (
        DurabilityState(DurabilityConfig.build(['primary', 'candidate', 'side'])),
        11,
    )
    instance.zk.is_lock_holder.return_value = True
    instance.zk.write_switchover_record.return_value = 12
    instance._get_switchover_candidate = MagicMock(return_value='candidate')
    record = SwitchoverRecord(
        hostname='primary', timeline=1, phase=SwitchoverPhase.SCHEDULED,
        protocol_version=2, version=7,
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._bridge_begin(record, {'replics_info': []}) is True

    written = instance.zk.write_switchover_record.call_args.args[0]
    assert written['phase'] == SwitchoverPhase.PREPARING_DURABILITY
    assert written['candidate'] == 'candidate'
    assert written['side_replicas'] == ['side']
    assert written['original_durability_members'] == ['candidate', 'primary', 'side']
    assert written['durability_pin_owner'] == 'primary'


def test_primary_rejects_candidate_outside_stable_durability():
    instance = _instance()
    instance.zk.get_durability_state.return_value = (
        DurabilityState(DurabilityConfig.build(['primary', 'side'])), 11,
    )
    instance._get_switchover_candidate = MagicMock(return_value='other')
    record = SwitchoverRecord(
        hostname='primary', timeline=1, phase=SwitchoverPhase.SCHEDULED,
        protocol_version=2, version=7,
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._bridge_begin(record, {'replics_info': []}) is True

    instance.zk.write_switchover_record.assert_not_called()


def test_side_replica_only_acknowledges_after_it_streams_from_candidate():
    instance = _instance()
    instance.db.stop_restoring_wal.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side'],
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation',
    )

    with patch('src.main.helpers.get_hostname', return_value='side'):
        instance._run_bridge_side_replica(record, {'primary_fqdn': 'primary'})

    instance._return_to_cluster.assert_called_once_with('candidate', 'replica', is_dead=False)
    instance.zk.write_switchover_ack.assert_not_called()

    with patch('src.main.helpers.get_hostname', return_value='side'):
        instance._run_bridge_side_replica(record, {'primary_fqdn': 'candidate'})

    instance.zk.write_switchover_ack.assert_called_once_with(
        'side', 'operation', {'source': 'candidate', 'restore_disabled': True},
    )


def test_primary_keeps_serving_before_handoff_is_committed():
    """A missing candidate before the commit point must not fence old primary."""
    instance = _instance()
    instance.zk.is_lock_holder.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', phase=SwitchoverPhase.TURNING_SIDES,
        protocol_version=2, operation_id='operation', version=7,
    )
    instance._bridge_candidate_reached_handoff = MagicMock(return_value=False)

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._run_bridge_primary(record, {'role': 'primary'}, 'primary') is True

    instance.zk.release_lock.assert_not_called()
    instance.stop_postgresql.assert_not_called()
    instance.zk.write_switchover_record.assert_not_called()


def test_handoff_commit_records_expected_new_timeline_before_lock_release():
    instance = _instance()
    instance.zk.is_lock_holder.return_value = True
    instance.zk.write_switchover_record.return_value = 8
    instance._replication_manager.change_replication_to_durability_config.return_value = True
    instance.zk.get_switchover_ack.return_value = {
        'operation_id': 'operation',
        'prepared_ssn': DurabilityConfig.build(['primary', 'candidate']).to_dict(),
        'checkpointed': True,
    }
    record = SwitchoverRecord(
        hostname='primary', timeline=9, candidate='candidate',
        phase=SwitchoverPhase.PREPARING_BRIDGE, protocol_version=2,
        operation_id='operation', version=7,
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._run_bridge_primary(record, {'role': 'primary'}, 'primary') is True

    written = instance.zk.write_switchover_record.call_args.args[0]
    assert written['phase'] == SwitchoverPhase.HANDOFF_COMMITTED
    assert written['expected_timeline'] == 10
    instance.zk.release_lock.assert_not_called()


def test_primary_sends_pooler_stop_without_waiting_before_releasing_lock():
    instance = _instance()
    events = []
    instance.zk.write_timeline.side_effect = lambda _: events.append('timeline') or True
    instance.db.stop_pooler_async.side_effect = lambda: events.append('pooler-stop-sent') or True
    instance.zk.release_lock.side_effect = lambda _: events.append('lock-released') or True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._run_bridge_primary(record, {'role': 'primary'}, 'primary') is True

    instance.zk.release_lock.assert_called_once_with(instance.zk.PRIMARY_LOCK_PATH)
    instance.zk.write_timeline.assert_called_once_with(10)
    instance.db.stop_pooler_async.assert_called_once_with()
    instance.db.pgpooler.assert_not_called()
    instance.stop_postgresql.assert_called_once_with(wait=False, force_async=False)
    instance._replication_manager.change_replication_to_durability_config.assert_not_called()
    assert events == ['timeline', 'pooler-stop-sent', 'lock-released']


def test_candidate_promotes_from_committed_handoff_without_manager_wait():
    instance = _instance()
    instance.zk.is_lock_holder.side_effect = [False, True]
    instance.zk.try_acquire_lock.return_value = True
    instance._run_promotion = MagicMock(return_value=PromotionResult.SUCCESS)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, None, 10) is True

    instance._run_promotion.assert_called_once_with(
        'switchover_candidate', operation_id='operation', old_primary='primary',
        prepared=True, expected_timeline=10,
    )
    instance.zk.write_switchover_ack.assert_called_once_with(
        'candidate', 'operation', {'promoted_timeline': 10},
    )
    instance.db.get_timeline.assert_not_called()
    instance.zk.write_switchover_record.assert_not_called()
    assert all(
        call.args[0] != instance.zk.SWITCHOVER_MANAGER_LOCK_PATH
        for call in instance.zk.try_acquire_lock.call_args_list
    )


def test_failed_candidate_promote_keeps_committed_handoff_for_retry():
    instance = _instance()
    instance.zk.is_lock_holder.return_value = True
    instance._run_promotion = MagicMock(return_value=PromotionResult.REJECTED)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, 'candidate', 10) is True

    instance.zk.release_lock.assert_not_called()
    instance.zk.write_switchover_record.assert_not_called()
    instance.zk.write_switchover_ack.assert_not_called()


def test_manager_confirms_promotion_after_old_primary_is_stopped():
    """The old primary daemon remains manager; its database need not be alive."""
    instance = _instance()
    instance.zk.is_lock_holder.return_value = True
    instance.zk.write_switchover_record.return_value = 8
    instance.zk.get_switchover_ack.return_value = {'promoted_timeline': 10}
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10, version=7,
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._run_bridge_primary(record, {'role': None}, 'candidate') is True

    written = instance.zk.write_switchover_record.call_args.args[0]
    assert written['phase'] == SwitchoverPhase.WAITING_ARCHIVE
    assert written['promoted_timeline'] == 10


def test_candidate_checkpoints_before_releasing_old_primary_for_rewind():
    """A recently promoted pg_rewind source needs its new TLI in control data."""
    instance = _instance()
    instance.config = MagicMock(promote_checkpoint_sql='CHECKPOINT;')
    instance._try_acquire_switchover_manager = MagicMock(return_value=True)
    instance._bridge_expansion_durability = MagicMock(
        return_value=DurabilityConfig.build(['primary', 'candidate']),
    )
    instance._replication_manager.change_replication_to_durability_config.return_value = True
    instance._bridge_archive_ready = MagicMock(return_value=True)
    instance.db.checkpoint.return_value = True
    instance.zk.cleanup_switchover.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.WAITING_ARCHIVE, protocol_version=2,
        operation_id='operation', expected_timeline=10, version=7,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'primary'}, 'candidate') is True

    instance.db.checkpoint.assert_called_once_with(query='CHECKPOINT;')
    instance.zk.write_switchover_ack.assert_called_once_with(
        'candidate', 'operation', {'post_promote_checkpointed': True},
    )
    instance.zk.cleanup_switchover.assert_called_once_with(7)


@pytest.mark.parametrize('archived_name', [
    '000000090000000000000004',
    '000000090000000000000004.partial',
])
def test_bridge_archive_barrier_accepts_complete_or_partial_fork_wal(archived_name):
    instance = _instance()
    instance.db.fetch_timeline_history.return_value = '9\t0/4732390\tbranch\n'
    instance.db.get_wal_segment_size.return_value = 16 * 1024 * 1024
    instance.db.is_wal_archived.side_effect = lambda filename: filename == archived_name
    record = SwitchoverRecord(promoted_timeline=10)

    assert instance._bridge_archive_ready(record) is True


def test_primary_does_not_release_lock_until_new_timeline_is_in_zk():
    instance = _instance()
    instance.zk.get_timeline.return_value = 9
    instance.zk.write_timeline.return_value = False
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._run_bridge_primary(record, {'role': 'primary'}, 'primary') is True

    instance.zk.write_timeline.assert_called_once_with(10)
    instance.zk.release_lock.assert_not_called()
    instance.stop_postgresql.assert_not_called()


def test_candidate_never_promotes_before_new_timeline_is_committed():
    instance = _instance()
    instance._run_promotion = MagicMock()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, None, 9) is True

    instance.zk.try_acquire_lock.assert_not_called()
    instance._run_promotion.assert_not_called()


def test_manager_initializes_old_timeline_failover_before_branch_fence():
    instance = _instance()
    instance.zk.TIMELINE_INFO_PATH = 'timeline'
    instance._run_bridge_candidate = MagicMock()
    instance._recover_pre_handoff_switchover = MagicMock(return_value=True)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._handle_bridge_switchover(
            record, {'role': 'replica'}, {'lock_holder': None, 'timeline': 9},
        ) is True

    instance._run_bridge_candidate.assert_not_called()
    instance._recover_pre_handoff_switchover.assert_called_once()


def test_committed_handoff_releases_side_replica_for_fenced_failover():
    """After the branch fence, a dead C is handled by ordinary failover."""
    instance = _instance()
    instance.zk.TIMELINE_INFO_PATH = 'timeline'
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side'],
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='side'):
        assert instance._handle_bridge_switchover(
            record, {'role': 'replica'}, {'lock_holder': None, 'timeline': 10},
        ) is False

    instance._return_to_cluster.assert_not_called()


def test_side_replica_never_turns_back_to_old_primary_after_handoff():
    instance = _instance()
    instance.db.stop_restoring_wal.return_value = True
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side'],
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='side'):
        instance._run_bridge_side_replica(record, {'primary_fqdn': 'primary'})

    instance._return_to_cluster.assert_called_once_with('candidate', 'replica', is_dead=False)


def test_two_host_bridge_needs_no_side_replica_before_handoff():
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=[],
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', required_side_replicas=1,
    )
    instance = _instance()
    instance._bridge_ack = MagicMock(return_value={
        'slots_ready': True,
    })

    assert instance._bridge_sides_ready(record) is True
    assert instance._bridge_member(record) is None


def test_manager_selects_freshest_live_bridge_replica():
    instance = _instance()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side1', 'side2'],
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', required_side_replicas=1,
    )
    instance._bridge_ack = MagicMock(side_effect=lambda _record, host: {
        'candidate': {
            'slots_ready': True,
            'streaming_side_flush_lsns': {'side1': 100, 'side2': 200},
        },
        'side1': {'source': 'candidate', 'restore_disabled': True},
        'side2': {'source': 'candidate', 'restore_disabled': True},
    }.get(host))

    assert instance._bridge_sides_ready(record) is True
    assert instance._bridge_member(record) == 'side2'


def test_no_live_bridge_replica_is_allowed_after_catchup_timeout():
    instance = _instance()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side'],
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation', required_side_replicas=1,
        side_wait_started_at=100,
    )
    instance._bridge_ack = MagicMock(side_effect=lambda _record, host: {
        'candidate': {'slots_ready': True, 'streaming_side_flush_lsns': {}},
        'side': {'source': 'candidate', 'restore_disabled': True},
    }.get(host))

    with patch('src.main.time.time', return_value=161):
        assert instance._bridge_sides_ready(record) is True
    assert instance._bridge_member(record) is None


def test_candidate_refreshes_streaming_side_flush_lsns_until_bridge_selection():
    instance = _instance()
    instance._slot_manager.create_slots_for_hosts.return_value = True
    instance.zk.get_switchover_ack.return_value = {
        'operation_id': 'operation', 'slots_ready': True,
    }
    instance.db.get_replica_flush_lsns.return_value = {
        'side1': 100, 'side2': 200,
    }
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side1', 'side2'],
        phase=SwitchoverPhase.TURNING_SIDES, protocol_version=2,
        operation_id='operation',
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, 'primary') is True

    instance.zk.write_switchover_ack.assert_called_once_with(
        'candidate', 'operation', {
            'prepared_ssn': DurabilityConfig.build(['primary', 'candidate']).to_dict(),
            'slots_ready': True,
            'streaming_side_flush_lsns': {'side1': 100, 'side2': 200},
        },
    )


def test_two_host_candidate_prepares_ssn_without_a_bridge_replica():
    instance = _instance()
    instance._slot_manager.create_slots_for_hosts.return_value = True
    instance._replication_manager.set_ssn_before_promote.return_value = True
    instance.db.checkpoint.return_value = True
    instance.zk.get_switchover_ack.return_value = None
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate',
        phase=SwitchoverPhase.PREPARING_BRIDGE, protocol_version=2,
        operation_id='operation',
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, 'primary') is True

    instance._replication_manager.set_ssn_before_promote.assert_called_once_with(
        DurabilityConfig.build(['primary', 'candidate']),
    )
    instance.zk.write_switchover_ack.assert_called_once_with(
        'candidate', 'operation', {
            'prepared_ssn': DurabilityConfig.build(['primary', 'candidate']).to_dict(),
            'slots_ready': True,
            'checkpointed': True,
        },
    )


def test_expanding_pin_keeps_old_primary_without_a_turned_side_replica():
    instance = _instance()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=[],
        phase=SwitchoverPhase.WAITING_ARCHIVE, protocol_version=2,
        operation_id='operation', original_durability_members=['primary', 'candidate'],
    )

    assert instance._bridge_expansion_durability(record, {'replics_info': []}) == DurabilityConfig.build(
        ['primary', 'candidate'],
    )


def test_expansion_keeps_selected_bridge_when_it_is_no_longer_streaming():
    instance = _instance()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side1', 'side2'],
        phase=SwitchoverPhase.WAITING_ARCHIVE, protocol_version=2, bridge_member='side2',
        operation_id='operation', original_durability_members=['primary', 'candidate', 'side1', 'side2'],
    )
    instance._bridge_ack = MagicMock(side_effect=lambda _record, host: {
        'source': 'candidate', 'restore_disabled': True,
    } if host in ('side1', 'side2') else None)

    durability = instance._bridge_expansion_durability(
        record,
        {'replics_info': [{'application_name': 'side1', 'state': 'streaming'}]},
    )

    assert durability == DurabilityConfig.build(['primary', 'candidate', 'side1', 'side2'])


def test_prepared_bridge_promotion_does_not_repeat_slots_or_ssn_setup():
    instance = _instance()
    state = MagicMock()
    state.read.return_value = None
    instance._local_states = {'switchover_candidate': state}
    instance._debug_failure = MagicMock(return_value=False)
    instance._promote = MagicMock(return_value=True)
    instance._finish_promote = MagicMock(return_value=True)

    assert instance._run_promotion(
        'switchover_candidate', 'operation-1', prepared=True, expected_timeline=2,
    ) == PromotionResult.SUCCESS

    instance._slot_manager.create_slots_for_hosts.assert_not_called()
    instance._replication_manager.set_ssn_before_promote.assert_not_called()
    instance.zk.get_durability_config.assert_not_called()
    instance._finish_promote.assert_called_once_with(checkpoint=False, expected_timeline=2)


def test_bridge_promotion_checks_current_wal_timeline():
    instance = _instance()
    instance.db.get_live_timeline.return_value = 2
    instance.zk.write_timeline.return_value = True

    assert instance._finish_promote(checkpoint=False, expected_timeline=2) is True

    instance.db.get_live_timeline.assert_called_once_with()
    instance.db.get_timeline.assert_not_called()
    instance.zk.write_timeline.assert_called_once_with(2)


def test_bridge_promotion_checkpoints_when_current_wal_timeline_is_unavailable():
    instance = _instance()
    instance.db.get_live_timeline.side_effect = PostgresConnectionError('starting')
    instance.db.checkpoint.return_value = True
    instance.db.get_timeline.return_value = 2
    instance.zk.write_timeline.return_value = True

    assert instance._finish_promote(checkpoint=False, expected_timeline=2) is True

    instance.db.get_live_timeline.assert_called_once_with()
    instance.db.checkpoint.assert_called_once_with(query='CHECKPOINT')
    instance.db.get_timeline.assert_called_once_with()
    instance.zk.write_timeline.assert_called_once_with(2)


def test_candidate_acknowledges_restartpoint_before_handoff_commit():
    instance = _instance()
    instance._slot_manager.create_slots_for_hosts.return_value = True
    instance._replication_manager.set_ssn_before_promote.return_value = True
    instance.db.checkpoint.return_value = True
    instance.zk.get_switchover_ack.return_value = None
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', bridge_member='side',
        phase=SwitchoverPhase.PREPARING_BRIDGE, protocol_version=2,
        operation_id='operation',
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, 'primary') is True

    instance.db.checkpoint.assert_called_once_with()
    instance.zk.write_switchover_ack.assert_called_once_with(
        'candidate',
        'operation',
        {
            'prepared_ssn': DurabilityConfig.build(['primary', 'candidate', 'side']).to_dict(),
            'slots_ready': True,
            'checkpointed': True,
        },
    )
