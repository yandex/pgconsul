"""Unit tests for SwitchoverPhase and SwitchoverRecord (step 14a)."""

from unittest.mock import MagicMock

from src.switchover import (
    DurabilityPinMode,
    SwitchoverPhase,
    SwitchoverRecord,
    SwitchoverRoute,
    decide_switchover_route,
)


class TestSwitchoverPhase:
    def test_values_match_zk_strings(self):
        assert SwitchoverPhase.SCHEDULED == 'scheduled'
        assert SwitchoverPhase.SYNC_SET == 'sync_set'
        assert SwitchoverPhase.INITIATED == 'initiated'
        assert SwitchoverPhase.CANDIDATE_FOUND == 'candidate_found'
        assert SwitchoverPhase.POOLER_STOPPED == 'pooler_stopped'
        assert SwitchoverPhase.PG_STOPPED == 'pg_stopped'
        assert SwitchoverPhase.PRIMARY_SHUT == 'primary_shut'
        assert SwitchoverPhase.PROMOTED == 'promoted'
        assert SwitchoverPhase.FAILED == 'failed'

    def test_from_str_known(self):
        assert SwitchoverPhase.from_str('scheduled') == SwitchoverPhase.SCHEDULED
        assert SwitchoverPhase.from_str('sync_set') == SwitchoverPhase.SYNC_SET
        assert SwitchoverPhase.from_str('pooler_stopped') == SwitchoverPhase.POOLER_STOPPED
        assert SwitchoverPhase.from_str('pg_stopped') == SwitchoverPhase.PG_STOPPED
        assert SwitchoverPhase.from_str('primary_shut') == SwitchoverPhase.PRIMARY_SHUT

    def test_from_str_none(self):
        assert SwitchoverPhase.from_str(None) is None

    def test_from_str_unknown_returns_none(self):
        assert SwitchoverPhase.from_str('nonsense') is None

    def test_str_enum_is_str(self):
        # StrEnum members are strings — safe for ZK writes and comparisons
        assert isinstance(SwitchoverPhase.SCHEDULED, str)


class TestSwitchoverRecord:
    def _make_zk(self):
        zk = MagicMock()
        zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
        zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
        zk.TIMELINE_INFO_PATH = 'timeline'
        return zk

    def test_from_zk_state_full(self):
        zk = self._make_zk()
        zk_state = {
            'switchover/record': {
                'hostname': 'host1', 'timeline': 5, 'destination': 'host2',
                'phase': 'initiated', 'candidate': 'host2',
                'side_replicas': ['host3', 'host4'],
            },
            'switchover_version': 7,
        }
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.hostname == 'host1'
        assert rec.timeline == 5
        assert rec.destination == 'host2'
        assert rec.phase == SwitchoverPhase.INITIATED
        assert rec.candidate == 'host2'
        assert rec.side_replicas == ['host3', 'host4']
        assert rec.version == 7

    def test_from_zk_state_empty(self):
        zk = self._make_zk()
        rec = SwitchoverRecord.from_zk_state({}, zk)
        assert rec.hostname is None
        assert rec.timeline is None
        assert rec.destination is None
        assert rec.phase is None
        assert rec.candidate is None
        assert rec.side_replicas == []

    def test_from_zk_state_new_phase(self):
        zk = self._make_zk()
        zk_state = {
            'switchover/record': {
                'hostname': 'host1', 'timeline': 5, 'phase': 'primary_shut',
            },
            'switchover_version': 2,
        }
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.phase == SwitchoverPhase.PRIMARY_SHUT

    def test_from_zk_state_unknown_phase(self):
        zk = self._make_zk()
        zk_state = {
            'switchover/record': {'phase': 'bogus'},
            'switchover_version': 2,
        }
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.phase == SwitchoverPhase.FAILED

    def test_selected_candidate_prefers_explicit_candidate(self):
        rec = SwitchoverRecord(candidate='host2', destination='host3')
        assert rec.selected_candidate == 'host2'

    def test_selected_candidate_falls_back_to_destination(self):
        rec = SwitchoverRecord(destination='host3')
        assert rec.selected_candidate == 'host3'

    def test_bridge_pin_round_trips_through_zk_record(self):
        zk = self._make_zk()
        record = SwitchoverRecord(
            hostname='primary',
            timeline=7,
            phase=SwitchoverPhase.PREPARING_BRIDGE,
            candidate='candidate',
            protocol_version=2,
            operation_id='op-1',
            durability_pin_mode=DurabilityPinMode.CONTRACTING,
            durability_pin_owner='primary',
            bridge_member='side1',
            bridge_source='candidate',
            handoff_lsn=123,
            side_wait_started_at=456.0,
            required_side_replicas=2,
            expected_timeline=8,
            started_at=100.0,
            deadline_at=123.0,
            failure_reason='timeout',
        )

        parsed = SwitchoverRecord.from_zk_state(
            {'switchover/record': record.to_dict(), 'switchover_version': 4},
            zk,
        )

        assert parsed == SwitchoverRecord(**{**record.__dict__, 'version': 4})

    def test_requires_primary_lock(self):
        assert SwitchoverRecord(phase=SwitchoverPhase.SCHEDULED).requires_primary_lock()
        assert SwitchoverRecord(phase=SwitchoverPhase.PG_STOPPED).requires_primary_lock()
        assert not SwitchoverRecord(phase=SwitchoverPhase.PRIMARY_SHUT).requires_primary_lock()

    def test_can_follow_candidate(self):
        assert SwitchoverRecord(phase=SwitchoverPhase.INITIATED).can_follow_candidate()
        assert SwitchoverRecord(phase=SwitchoverPhase.PROMOTED).can_follow_candidate()
        assert not SwitchoverRecord(phase=SwitchoverPhase.SCHEDULED).can_follow_candidate()

    def test_handoff_committed_is_the_irrevocable_boundary(self):
        assert not SwitchoverRecord(phase=SwitchoverPhase.PREPARING_BRIDGE).handoff_is_committed()
        assert SwitchoverRecord(phase=SwitchoverPhase.HANDOFF_COMMITTED).handoff_is_committed()
        assert SwitchoverRecord(phase=SwitchoverPhase.WAITING_ARCHIVE).handoff_is_committed()


def test_side_replica_waits_for_candidate_when_old_primary_lock_is_lost():
    """switchover_kill9_survives.feature:125 must not start a competing failover."""
    record = SwitchoverRecord(
        hostname='old-primary',
        candidate='candidate',
        phase=SwitchoverPhase.CANDIDATE_FOUND,
    )

    assert decide_switchover_route(
        record,
        hostname='side-replica',
        role='replica',
        lock_holder=None,
    ) == SwitchoverRoute.REPLICA
