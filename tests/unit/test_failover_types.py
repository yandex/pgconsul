# encoding: utf-8
"""Unit tests for FailoverPhase and FailoverRecord (ADR-0007, stage 1)."""

from unittest.mock import MagicMock

from src.failover import FailoverPhase, FailoverRecord


class TestFailoverPhase:
    def test_existing_values_match_zk_strings(self):
        # Existing values — written by _do_failover/_promote, read by old versions.
        assert FailoverPhase.PROMOTING == 'promoting'
        assert FailoverPhase.CHECKPOINTING == 'checkpointing'
        assert FailoverPhase.CREATING_SLOTS == 'creating_slots'
        assert FailoverPhase.FINISHED == 'finished'

    def test_new_values_match_zk_strings(self):
        # New values — unrecognized by old versions (two-phase rollout).
        assert FailoverPhase.DETECTED == 'detected'
        assert FailoverPhase.GATES_PASSED == 'gates_passed'
        assert FailoverPhase.REGISTRATION == 'registration'
        assert FailoverPhase.VOTING == 'voting'
        assert FailoverPhase.WINNER_SELECTED == 'winner_selected'
        assert FailoverPhase.FAILED == 'failed'

    def test_from_str_known(self):
        assert FailoverPhase.from_str('promoting') == FailoverPhase.PROMOTING
        assert FailoverPhase.from_str('finished') == FailoverPhase.FINISHED
        assert FailoverPhase.from_str('detected') == FailoverPhase.DETECTED
        assert FailoverPhase.from_str('winner_selected') == FailoverPhase.WINNER_SELECTED

    def test_from_str_none(self):
        assert FailoverPhase.from_str(None) is None

    def test_from_str_unknown_returns_none(self):
        assert FailoverPhase.from_str('nonsense') is None

    def test_str_enum_is_str(self):
        assert isinstance(FailoverPhase.PROMOTING, str)
        assert isinstance(FailoverPhase.DETECTED, str)


class TestFailoverRecord:
    def _make_zk(self):
        zk = MagicMock()
        zk.get_election_winner.return_value = None
        zk.get_election_status.return_value = None
        return zk

    def test_from_zk_state_full(self):
        zk = self._make_zk()
        zk.get_election_winner.return_value = 'host2'
        zk.get_election_status.return_value = 'registration'
        rec = FailoverRecord.from_zk_state('registration', zk)
        assert rec.phase == FailoverPhase.REGISTRATION
        assert rec.winner == 'host2'
        assert rec.election_status == 'registration'

    def test_from_zk_state_empty(self):
        zk = self._make_zk()
        rec = FailoverRecord.from_zk_state(None, zk)
        assert rec.phase is None
        assert rec.winner is None
        assert rec.election_status is None

    def test_from_zk_state_existing_phase(self):
        zk = self._make_zk()
        rec = FailoverRecord.from_zk_state('promoting', zk)
        assert rec.phase == FailoverPhase.PROMOTING

    def test_from_zk_state_unknown_phase(self):
        zk = self._make_zk()
        rec = FailoverRecord.from_zk_state('bogus', zk)
        assert rec.phase is None

    def test_is_active_for_in_progress_phases(self):
        for phase in (
            FailoverPhase.DETECTED,
            FailoverPhase.GATES_PASSED,
            FailoverPhase.REGISTRATION,
            FailoverPhase.VOTING,
            FailoverPhase.WINNER_SELECTED,
            FailoverPhase.PROMOTING,
            FailoverPhase.CHECKPOINTING,
            FailoverPhase.CREATING_SLOTS,
        ):
            assert FailoverRecord(phase=phase).is_active() is True

    def test_is_active_false_for_finished(self):
        assert FailoverRecord(phase=FailoverPhase.FINISHED).is_active() is False

    def test_is_active_false_for_failed(self):
        assert FailoverRecord(phase=FailoverPhase.FAILED).is_active() is False

    def test_is_active_false_for_none(self):
        assert FailoverRecord(phase=None).is_active() is False

    def test_is_failed(self):
        assert FailoverRecord(phase=FailoverPhase.FAILED).is_failed() is True
        assert FailoverRecord(phase=FailoverPhase.PROMOTING).is_failed() is False
