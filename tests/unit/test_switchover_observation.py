# encoding: utf-8
"""
Unit tests for SwitchoverObservation.build() and SwitchoverRecord.from_zk_state()
(ADR-0006, step H4).

Verifies that the observation builder — the sole I/O read point for a step —
correctly assembles all fields from db/zk/timings/record.
"""

from unittest.mock import MagicMock

from src.switchover import (
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


# ---------------------------------------------------------------------------
# SwitchoverRecord.from_zk_state
# ---------------------------------------------------------------------------


def _make_zk():
    """Create a MagicMock with ZK path constants set to real values."""
    zk = MagicMock()
    zk.SWITCHOVER_ROOT_PATH = 'switchover'
    zk.SWITCHOVER_STATE_PATH = 'switchover/state'
    zk.SWITCHOVER_SIDE_REPLICAS = 'switchover/side_replicas'
    zk.SWITCHOVER_CANDIDATE = 'switchover/candidate'
    zk.TIMELINE_INFO_PATH = 'timeline'
    zk.PRIMARY_LOCK_PATH = 'leader'
    return zk


class TestSwitchoverRecordFromZkState:
    def test_parses_full_zk_state(self):
        zk = _make_zk()
        zk_state = {
            zk.SWITCHOVER_ROOT_PATH: {
                'hostname': 'host1',
                'destination': 'host2',
                zk.TIMELINE_INFO_PATH: 5,
            },
            zk.SWITCHOVER_STATE_PATH: 'scheduled',
            zk.SWITCHOVER_SIDE_REPLICAS: ['host3', 'host4'],
            zk.SWITCHOVER_CANDIDATE: 'host2',
        }

        record = SwitchoverRecord.from_zk_state(zk_state, zk)

        assert record.hostname == 'host1'
        assert record.timeline == 5
        assert record.destination == 'host2'
        assert record.phase == SwitchoverPhase.SCHEDULED
        assert record.candidate == 'host2'
        assert record.side_replicas == ['host3', 'host4']

    def test_handles_missing_switchover_info(self):
        zk = _make_zk()
        zk_state = {}

        record = SwitchoverRecord.from_zk_state(zk_state, zk)

        assert record.hostname is None
        assert record.timeline is None
        assert record.destination is None
        assert record.phase is None
        assert record.candidate is None
        assert record.side_replicas == []

    def test_handles_unknown_phase_string(self):
        zk = _make_zk()
        zk_state = {
            zk.SWITCHOVER_STATE_PATH: 'unknown_phase',
        }

        record = SwitchoverRecord.from_zk_state(zk_state, zk)

        assert record.phase is None

    def test_handles_none_state_string(self):
        zk = _make_zk()
        zk_state = {
            zk.SWITCHOVER_STATE_PATH: None,
        }

        record = SwitchoverRecord.from_zk_state(zk_state, zk)

        assert record.phase is None

    def test_side_replicas_empty_when_none(self):
        zk = _make_zk()
        zk_state = {
            zk.SWITCHOVER_SIDE_REPLICAS: None,
        }

        record = SwitchoverRecord.from_zk_state(zk_state, zk)

        assert record.side_replicas == []

    def test_belongs_to(self):
        zk = _make_zk()
        zk_state = {
            zk.SWITCHOVER_ROOT_PATH: {'hostname': 'host1'},
        }
        record = SwitchoverRecord.from_zk_state(zk_state, zk)

        assert record.belongs_to('host1') is True
        assert record.belongs_to('host2') is False

    def test_is_active_for_in_progress_phases(self):
        for phase in (
            SwitchoverPhase.SCHEDULED,
            SwitchoverPhase.SYNC_SET,
            SwitchoverPhase.INITIATED,
            SwitchoverPhase.CANDIDATE_FOUND,
            SwitchoverPhase.POOLER_STOPPED,
            SwitchoverPhase.PG_STOPPED,
            SwitchoverPhase.PRIMARY_SHUT,
            SwitchoverPhase.PROMOTED,
        ):
            record = SwitchoverRecord(phase=phase)
            assert record.is_active() is True

    def test_is_active_false_for_failed(self):
        record = SwitchoverRecord(phase=SwitchoverPhase.FAILED)
        assert record.is_active() is False

    def test_is_failed(self):
        record = SwitchoverRecord(phase=SwitchoverPhase.FAILED)
        assert record.is_failed() is True

        record_ok = SwitchoverRecord(phase=SwitchoverPhase.SCHEDULED)
        assert record_ok.is_failed() is False


# ---------------------------------------------------------------------------
# SwitchoverObservation.build — common reads
# ---------------------------------------------------------------------------


def _make_record(
    phase=SwitchoverPhase.SCHEDULED,
    candidate='host2',
    side_replicas=None,
):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination='host2',
        phase=phase,
        candidate=candidate,
        side_replicas=side_replicas or ['host3'],
    )


def _make_db():
    db = MagicMock()
    db.get_role.return_value = 'primary'
    return db


def _make_timings():
    timings = MagicMock()
    timings.get_start.return_value = None
    return timings


def _make_zk_state(zk):
    return {
        zk.TIMELINE_INFO_PATH: 5,
        'replics_info': [{'application_name': 'host2', 'state': 'streaming'}],
    }


class TestObservationBuildCommon:
    def test_builds_role_from_db(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.role == 'primary'
        db.get_role.assert_called_once()

    def test_builds_zk_timeline_from_zk_state(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = {zk.TIMELINE_INFO_PATH: 7}

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.zk_timeline == 7

    def test_builds_failover_state_from_zk(self):
        zk = _make_zk()
        zk.get_failover_state.return_value = 'finished'
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.failover_state == 'finished'

    def test_builds_last_failover_ts_from_zk(self):
        zk = _make_zk()
        zk.get_last_failover_time.return_value = 1000.0
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.last_failover_ts == 1000.0

    def test_builds_last_switchover_ts_from_zk(self):
        zk = _make_zk()
        zk.get_last_switchover_time.return_value = 2000.0
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.last_switchover_ts == 2000.0

    def test_builds_ha_replics_as_frozenset(self):
        zk = _make_zk()
        zk.get_ha_replics.return_value = {'host2', 'host3'}
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.ha_replics == frozenset({'host2', 'host3'})

    def test_ha_replics_none_when_zk_returns_none(self):
        zk = _make_zk()
        zk.get_ha_replics.return_value = None
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.ha_replics is None

    def test_builds_replics_info_from_db_state(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        replics_info = [
            {'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 0},
            {'application_name': 'host3', 'state': 'streaming', 'replay_lag_msec': 5},
        ]
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1',
            db_state={'replics_info': replics_info},
            zk_state=zk_state,
        )

        assert obs.replics_info == replics_info

    def test_replics_info_defaults_to_empty_list(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.replics_info == []

    def test_switchover_timer_started_true(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        timings.get_start.side_effect = lambda name: 100.0 if name == 'switchover' else None
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.switchover_timer_started is True
        assert obs.downtime_timer_started is False

    def test_downtime_timer_started_true(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        timings.get_start.side_effect = lambda name: 200.0 if name == 'downtime' else None
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.downtime_timer_started is True
        assert obs.switchover_timer_started is False

    def test_builds_lock_holder_from_zk(self):
        zk = _make_zk()
        zk.get_current_lock_holder.return_value = 'host1'
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.lock_holder == 'host1'
        zk.get_current_lock_holder.assert_called_once_with(zk.PRIMARY_LOCK_PATH)


# ---------------------------------------------------------------------------
# SwitchoverObservation.build — phase-specific reads
# ---------------------------------------------------------------------------


class TestObservationBuildPhaseSpecific:
    def test_builds_live_switchover_state_from_zk(self):
        zk = _make_zk()
        zk.get_switchover_state.return_value = 'candidate_found'
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.live_switchover_state == SwitchoverPhase.CANDIDATE_FOUND

    def test_live_switchover_state_none_when_zk_returns_none(self):
        zk = _make_zk()
        zk.get_switchover_state.return_value = None
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.live_switchover_state is None

    def test_candidate_from_record_candidate(self):
        zk = _make_zk()
        zk.is_host_alive.return_value = True
        db = _make_db()
        timings = _make_timings()
        record = _make_record(candidate='host2')
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.candidate == 'host2'
        assert obs.candidate_alive is True
        zk.is_host_alive.assert_called_once_with('host2', timeout=1)

    def test_candidate_falls_back_to_destination(self):
        zk = _make_zk()
        zk.is_host_alive.return_value = True
        db = _make_db()
        timings = _make_timings()
        record = SwitchoverRecord(
            hostname='host1', timeline=5, destination='host3',
            phase=SwitchoverPhase.SCHEDULED, candidate=None,
        )
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.candidate == 'host3'

    def test_candidate_alive_none_when_no_candidate(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = SwitchoverRecord(
            hostname='host1', timeline=5, destination=None,
            phase=SwitchoverPhase.SCHEDULED, candidate=None,
        )
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.candidate is None
        assert obs.candidate_alive is None
        zk.is_host_alive.assert_not_called()

    def test_candidate_alive_false_when_host_dead(self):
        zk = _make_zk()
        zk.is_host_alive.return_value = False
        db = _make_db()
        timings = _make_timings()
        record = _make_record(candidate='host2')
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.candidate_alive is False


# ---------------------------------------------------------------------------
# SwitchoverObservation.build — candidate-side reads
# ---------------------------------------------------------------------------


class TestObservationBuildCandidateSide:
    def test_switchover_primary_info_not_read_by_default(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.switchover_primary_info is None
        zk.get_switchover_primary_info.assert_not_called()

    def test_switchover_primary_info_read_when_candidate_side(self):
        zk = _make_zk()
        zk.get_switchover_primary_info.return_value = {'hostname': 'host1'}
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
            is_candidate_side=True,
        )

        assert obs.switchover_primary_info == {'hostname': 'host1'}
        zk.get_switchover_primary_info.assert_called_once()


# ---------------------------------------------------------------------------
# SwitchoverObservation.build — pass-through fields
# ---------------------------------------------------------------------------


class TestObservationBuildPassThrough:
    def test_streaming_replicas_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
            streaming_replicas=('host2', 'host3'),
        )

        assert obs.streaming_replicas == ('host2', 'host3')

    def test_all_side_replicas_turned_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
            all_side_replicas_turned=True,
        )

        assert obs.all_side_replicas_turned is True

    def test_switchover_candidate_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
            switchover_candidate='host2',
        )

        assert obs.switchover_candidate == 'host2'

    def test_side_replicas_from_record(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record(side_replicas=['host3', 'host4'])
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={}, zk_state=zk_state,
        )

        assert obs.side_replicas == ('host3', 'host4')

    def test_my_hostname_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        record = _make_record()
        zk_state = _make_zk_state(zk)

        obs = SwitchoverObservation.build(
            record=record, zk=zk, db=db, timings=timings,
            my_hostname='myhost', db_state={}, zk_state=zk_state,
        )

        assert obs.my_hostname == 'myhost'
