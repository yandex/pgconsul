"""Unit tests for SwitchoverPhase and SwitchoverRecord (step 14a)."""

from unittest.mock import MagicMock

from src.switchover import SwitchoverPhase, SwitchoverRecord


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
        zk.SWITCHOVER_ROOT_PATH = 'switchover'
        zk.SWITCHOVER_STATE_PATH = 'switchover/state'
        zk.SWITCHOVER_SIDE_REPLICAS = 'switchover/side_replicas'
        zk.SWITCHOVER_CANDIDATE = 'switchover/candidate'
        zk.TIMELINE_INFO_PATH = 'timeline'
        return zk

    def test_from_zk_state_full(self):
        zk = self._make_zk()
        zk_state = {
            'switchover': {'hostname': 'host1', 'timeline': 5, 'destination': 'host2'},
            'switchover/state': 'initiated',
            'switchover/side_replicas': ['host3', 'host4'],
            'switchover/candidate': 'host2',
        }
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.hostname == 'host1'
        assert rec.timeline == 5
        assert rec.destination == 'host2'
        assert rec.phase == SwitchoverPhase.INITIATED
        assert rec.candidate == 'host2'
        assert rec.side_replicas == ['host3', 'host4']

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
            'switchover': {'hostname': 'host1', 'timeline': 5},
            'switchover/state': 'primary_shut',
        }
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.phase == SwitchoverPhase.PRIMARY_SHUT

    def test_from_zk_state_unknown_phase(self):
        zk = self._make_zk()
        zk_state = {'switchover': {}, 'switchover/state': 'bogus'}
        rec = SwitchoverRecord.from_zk_state(zk_state, zk)
        assert rec.phase is None

    def test_belongs_to(self):
        rec = SwitchoverRecord(hostname='host1')
        assert rec.belongs_to('host1')
        assert not rec.belongs_to('host2')

    def test_is_active(self):
        for phase in [
            SwitchoverPhase.SCHEDULED,
            SwitchoverPhase.SYNC_SET,
            SwitchoverPhase.INITIATED,
            SwitchoverPhase.CANDIDATE_FOUND,
            SwitchoverPhase.POOLER_STOPPED,
            SwitchoverPhase.PG_STOPPED,
            SwitchoverPhase.PRIMARY_SHUT,
            SwitchoverPhase.PROMOTED,
        ]:
            assert SwitchoverRecord(phase=phase).is_active()
        assert not SwitchoverRecord(phase=SwitchoverPhase.FAILED).is_active()
        assert not SwitchoverRecord(phase=None).is_active()

    def test_is_failed(self):
        assert SwitchoverRecord(phase=SwitchoverPhase.FAILED).is_failed()
        assert not SwitchoverRecord(phase=SwitchoverPhase.SCHEDULED).is_failed()
