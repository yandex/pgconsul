"""Unit tests for the manager-owned bridge switchover protocol (ADR-0014)."""

from unittest.mock import MagicMock, patch

from src.main import Pgconsul
from src.switchover import SwitchoverPhase, SwitchoverRecord
from src.commands import PromotionResult
from src.types import DurabilityConfig, DurabilityState


def _instance():
    instance = Pgconsul.__new__(Pgconsul)
    instance.zk = MagicMock()
    instance.zk.SWITCHOVER_MANAGER_LOCK_PATH = 'switchover/manager'
    instance.db = MagicMock()
    instance._replication_manager = MagicMock()
    instance._slot_manager = MagicMock()
    instance._timings = MagicMock()
    instance._return_to_cluster = MagicMock()
    instance.start_pooler = MagicMock()
    instance.stop_postgresql = MagicMock()
    return instance


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
    assert events == ['timeline', 'pooler-stop-sent', 'lock-released']


def test_candidate_promotes_from_committed_handoff_without_manager_wait():
    instance = _instance()
    instance.zk.is_lock_holder.side_effect = [False, True]
    instance.zk.try_acquire_lock.return_value = True
    instance._run_promotion = MagicMock(return_value=PromotionResult.SUCCESS)
    instance.db.get_timeline.return_value = 10
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._run_bridge_candidate(record, {'role': 'replica'}, None, 10) is True

    instance._run_promotion.assert_called_once_with(
        'switchover_candidate', old_primary='primary', prepared=True, expected_timeline=10,
    )
    instance.zk.write_switchover_ack.assert_called_once_with(
        'candidate', 'operation', {'promoted_timeline': 10},
    )
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


def test_candidate_allows_old_timeline_failover_if_primary_dies_before_branch_fence():
    instance = _instance()
    instance.zk.TIMELINE_INFO_PATH = 'timeline'
    instance._run_bridge_candidate = MagicMock()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', timeline=9,
        phase=SwitchoverPhase.HANDOFF_COMMITTED, protocol_version=2,
        operation_id='operation', expected_timeline=10,
    )

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert instance._handle_bridge_switchover(
            record, {'role': 'replica'}, {'lock_holder': None, 'timeline': 9},
        ) is False

    instance._run_bridge_candidate.assert_not_called()


def test_expansion_excludes_turned_replica_that_is_no_longer_streaming():
    instance = _instance()
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side1', 'side2'],
        phase=SwitchoverPhase.WAITING_ARCHIVE, protocol_version=2,
        operation_id='operation', original_durability_members=['primary', 'candidate', 'side1', 'side2'],
    )
    instance._bridge_ack = MagicMock(side_effect=lambda _record, host: {
        'source': 'candidate', 'restore_disabled': True,
    } if host in ('side1', 'side2') else None)

    durability = instance._bridge_expansion_durability(
        record,
        {'replics_info': [{'application_name': 'side1', 'state': 'streaming'}]},
    )

    assert durability == DurabilityConfig.build(['primary', 'candidate', 'side1'])


def test_prepared_bridge_promotion_does_not_repeat_slots_or_ssn_setup():
    instance = _instance()
    state = MagicMock()
    state.read.return_value = None
    instance._local_states = {'switchover_candidate': state}
    instance._debug_failure = MagicMock(return_value=False)
    instance._promote = MagicMock(return_value=True)
    instance._finish_promote = MagicMock(return_value=True)

    assert instance._run_promotion(
        'switchover_candidate', prepared=True, expected_timeline=2,
    ) == PromotionResult.SUCCESS

    instance._slot_manager.create_slots_for_hosts.assert_not_called()
    instance._replication_manager.set_ssn_before_promote.assert_not_called()
    instance.zk.get_durability_config.assert_not_called()
    instance._finish_promote.assert_called_once_with(checkpoint=False, expected_timeline=2)


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
