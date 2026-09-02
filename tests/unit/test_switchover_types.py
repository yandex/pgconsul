"""Unit tests for SwitchoverPhase and SwitchoverRecord (step 14a)."""

from unittest.mock import MagicMock

from src.switchover import (
    DurabilityPinMode,
    SwitchoverPhase,
    SwitchoverRecord,
)


class TestSwitchoverPhase:
    def test_values_match_zk_strings(self):
        assert SwitchoverPhase.SCHEDULED == 'scheduled'
        assert SwitchoverPhase.PREPARING_DURABILITY == 'preparing_durability'
        assert SwitchoverPhase.HANDOFF_COMMITTED == 'handoff_committed'
        assert SwitchoverPhase.FAILED == 'failed'

    def test_from_str_known(self):
        assert SwitchoverPhase.from_str('scheduled') == SwitchoverPhase.SCHEDULED
        assert SwitchoverPhase.from_str('turning_sides') == SwitchoverPhase.TURNING_SIDES

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
                'phase': 'turning_sides', 'candidate': 'host2',
                'side_replicas': ['host3', 'host4'],
                'protocol_version': 2,
                'operation_id': 'operation',
            },
            'switchover_version': 7,
        }
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.hostname == 'host1'
        assert rec.timeline == 5
        assert rec.destination == 'host2'
        assert rec.phase == SwitchoverPhase.TURNING_SIDES
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

    def test_durability_pin_round_trips_through_zk_record(self):
        zk = self._make_zk()
        record = SwitchoverRecord(
            hostname='primary',
            timeline=7,
            phase=SwitchoverPhase.TURNING_SIDES,
            candidate='candidate',
            protocol_version=2,
            operation_id='op-1',
            durability_pin_mode=DurabilityPinMode.CONTRACTING,
            durability_pin_owner='primary',
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

    def test_handoff_committed_is_the_irrevocable_boundary(self):
        assert not SwitchoverRecord(phase=SwitchoverPhase.TURNING_SIDES).handoff_is_committed()
        assert SwitchoverRecord(phase=SwitchoverPhase.HANDOFF_COMMITTED).handoff_is_committed()
        assert SwitchoverRecord(phase=SwitchoverPhase.WAITING_ARCHIVE).handoff_is_committed()
