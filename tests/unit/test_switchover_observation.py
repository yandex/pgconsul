from unittest.mock import MagicMock, call, patch

from src.switchover import (
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _zk():
    zk = MagicMock()
    zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
    zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    zk.TIMELINE_INFO_PATH = 'timeline'
    zk.get_last_role_transition_time.return_value = 10.0
    zk.get_ha_replics.return_value = ['host2', 'host3']
    zk.get_current_lock_holder.return_value = 'host1'
    zk.is_host_alive.return_value = True
    return zk


def _record(phase=SwitchoverPhase.SCHEDULED, **kwargs):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination='host2',
        phase=phase,
        **kwargs,
    )


def _build(record=None, *, db_state=None, zk_state=None, **kwargs):
    zk = _zk()
    timings = MagicMock()
    timings.get_start.side_effect = lambda name: {
        'switchover': 20.0,
        'downtime': 30.0,
    }.get(name)
    observation = SwitchoverObservation.build(
        record=record or _record(),
        zk=zk,
        timings=timings,
        my_hostname='host1',
        db_state=db_state or {'role': 'primary', 'replics_info': []},
        zk_state=zk_state or {'timeline': 5},
        **kwargs,
    )
    return observation, zk, timings


def test_record_is_built_from_zk_snapshot():
    zk = _zk()
    state = {
        'switchover/record': {
            'hostname': 'host1',
            'timeline': 5,
            'destination': 'host2',
            'phase': 'initiated',
            'candidate': 'host3',
            'side_replicas': ['host4'],
        },
        'switchover_version': 7,
    }

    record = SwitchoverRecord.from_zk_state(state, zk)

    assert record == SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination='host2',
        phase=SwitchoverPhase.INITIATED,
        candidate='host3',
        side_replicas=['host4'],
        version=7,
    )
    assert record.selected_candidate == 'host3'


def test_build_collects_common_snapshot_fields():
    replics_info = [{'application_name': 'host2', 'state': 'streaming'}]
    observation, zk, timings = _build(
        db_state={'role': 'primary', 'replics_info': replics_info},
    )

    assert observation.role == 'primary'
    assert observation.zk_timeline == 5
    assert observation.last_role_transition_ts == 10.0
    assert observation.ha_replics == frozenset({'host2', 'host3'})
    assert observation.replics_info == replics_info
    assert observation.lock_holder == 'host1'
    assert observation.switchover_started_ts == 20.0
    assert observation.downtime_started_ts == 30.0
    assert timings.get_start.call_args_list == [call('switchover'), call('downtime')]
    zk.get_ha_replics.assert_called_once_with('host1')


def test_build_preserves_none_ha_replics():
    zk = _zk()
    zk.get_ha_replics.return_value = None
    timings = MagicMock()

    observation = SwitchoverObservation.build(
        record=_record(),
        zk=zk,
        timings=timings,
        my_hostname='host1',
        db_state={'role': 'primary'},
        zk_state={'timeline': 5},
    )

    assert observation.ha_replics is None
    assert observation.replics_info == []


def test_candidate_liveness_is_checked_only_while_initiated():
    initiated = _record(SwitchoverPhase.INITIATED, candidate='host3')
    observation, zk, _ = _build(record=initiated)

    assert observation.candidate_alive is True
    zk.is_host_alive.assert_called_once_with('host3', timeout=1)

    _, zk, _ = _build(record=_record(SwitchoverPhase.CANDIDATE_FOUND, candidate='host3'))
    zk.is_host_alive.assert_not_called()


def test_failed_switchover_checks_old_primary_liveness_before_fallback():
    zk = _zk()
    zk.get_current_lock_holder.return_value = None
    timings = MagicMock()

    observation = SwitchoverObservation.build(
        record=_record(SwitchoverPhase.FAILED, candidate='host2'),
        zk=zk,
        timings=timings,
        my_hostname='host3',
        db_state={'role': 'replica'},
        zk_state={'timeline': 5},
    )

    assert observation.primary_alive is True
    zk.is_host_alive.assert_called_once_with('host1', timeout=1)


def test_build_passes_shell_specific_values_through():
    observation, _, _ = _build(
        streaming_replicas=('host2', 'host3'),
        all_side_replicas_turned=True,
        switchover_candidate='host3',
        local_phase=SwitchoverPhase.POOLER_STOPPED,
    )

    assert observation.streaming_replicas == ('host2', 'host3')
    assert observation.all_side_replicas_turned is True
    assert observation.switchover_candidate == 'host3'
    assert observation.local_phase == SwitchoverPhase.POOLER_STOPPED


def test_build_snapshots_current_time_once():
    with patch('src.switchover.types.time.time', return_value=123.0) as now:
        observation, _, _ = _build()

    assert observation.current_time == 123.0
    now.assert_called_once_with()
