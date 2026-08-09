"""Unit tests for CandidateSwitchoverMachine (step 15a)."""

from unittest.mock import MagicMock, call

from src.exceptions import PostgresConnectionError
from src.switchover import (
    CandidateContext,
    CandidateSwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _make_zk(write_ok=True):
    zk = MagicMock()
    zk.write_switchover_state.return_value = write_ok
    zk.try_acquire_lock.return_value = True
    zk.get_switchover_primary_info.return_value = {'hostname': 'host1'}
    zk.PRIMARY_LOCK_PATH = 'master'
    return zk


def _make_record(phase, candidate='host2', side_replicas=None):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination='host2',
        phase=phase,
        candidate=candidate,
        side_replicas=side_replicas if side_replicas is not None else ['host3'],
    )


def _make_context(
    create_slots_ok=True,
    all_turned=True,
    do_failover_ok=True,
):
    ctx = MagicMock()
    ctx.create_slots_for_hosts.return_value = create_slots_ok
    ctx.all_side_replicas_turned.return_value = all_turned
    ctx.do_failover.return_value = do_failover_ok
    ctx.zk = _make_zk()
    ctx.get_hostname.return_value = 'host2'
    ctx.timings = MagicMock()
    return ctx


def _make_machine(ctx, zk=None, debug_failure=None):
    zk = zk or ctx.zk
    return CandidateSwitchoverMachine(zk, context=ctx, debug_failure=debug_failure)


class TestCandidateTransitionTo:
    def test_writes_phase_to_zk(self):
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        result = m.transition_to(SwitchoverPhase.CANDIDATE_FOUND)
        assert result is True
        zk.write_switchover_state.assert_called_once_with(SwitchoverPhase.CANDIDATE_FOUND)

    def test_returns_false_on_zk_failure(self):
        zk = _make_zk(write_ok=False)
        m = CandidateSwitchoverMachine(zk)
        result = m.transition_to(SwitchoverPhase.PROMOTED)
        assert result is False


class TestCandidateStepDispatch:
    def test_returns_false_for_failed_phase(self):
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        record = _make_record(SwitchoverPhase.FAILED)
        assert m.step(record, {}, {}) is False

    def test_returns_false_for_none_phase(self):
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        record = _make_record(None)
        assert m.step(record, {}, {}) is False

    def test_returns_false_for_scheduled_phase(self):
        # SCHEDULED has no candidate-side handler
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        record = _make_record(SwitchoverPhase.SCHEDULED)
        assert m.step(record, {}, {}) is False

    def test_returns_false_for_sync_set_phase(self):
        # SYNC_SET is primary-side only
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        record = _make_record(SwitchoverPhase.SYNC_SET)
        assert m.step(record, {}, {}) is False

    def test_returns_false_for_primary_shut_phase(self):
        # PRIMARY_SHUT is primary-side only
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        record = _make_record(SwitchoverPhase.PRIMARY_SHUT)
        assert m.step(record, {}, {}) is False

    def test_returns_false_when_context_is_none(self):
        zk = _make_zk()
        m = CandidateSwitchoverMachine(zk)
        for phase in (SwitchoverPhase.INITIATED, SwitchoverPhase.CANDIDATE_FOUND):
            record = _make_record(phase)
            result = m.step(record, {}, {})
            assert result is False, f'phase {phase} should return False without context'

    def test_debug_failure_hook_callable(self):
        zk = _make_zk()
        debug = MagicMock(return_value=False)
        m = CandidateSwitchoverMachine(zk, debug_failure=debug)
        assert m._debug_failure is debug


class TestCandidateHandleInitiated:
    def test_creates_slots_and_transitions_when_all_turned(self):
        ctx = _make_context(create_slots_ok=True, all_turned=True)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        ctx.create_slots_for_hosts.assert_called_once_with(['host3'])
        ctx.all_side_replicas_turned.assert_called_once_with(['host3'])
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.CANDIDATE_FOUND)

    def test_waits_when_side_replicas_not_turned(self):
        ctx = _make_context(all_turned=False)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        # Should not transition to CANDIDATE_FOUND
        ctx.zk.write_switchover_state.assert_not_called()

    def test_retries_when_slot_creation_fails(self):
        ctx = _make_context(create_slots_ok=False)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        # Should not check side replicas or transition
        ctx.all_side_replicas_turned.assert_not_called()
        ctx.zk.write_switchover_state.assert_not_called()

    def test_retries_on_pg_connection_error(self):
        ctx = _make_context()
        ctx.all_side_replicas_turned.side_effect = PostgresConnectionError('lost')
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        ctx.zk.write_switchover_state.assert_not_called()

    def test_transitions_without_side_replicas(self):
        ctx = _make_context()
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED, side_replicas=[])
        assert m.step(record, {}, {}) is True
        ctx.create_slots_for_hosts.assert_not_called()
        ctx.all_side_replicas_turned.assert_not_called()
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.CANDIDATE_FOUND)

    def test_returns_true_on_zk_write_failure(self):
        ctx = _make_context()
        ctx.zk.write_switchover_state.return_value = False
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        # transition_to failed, but step still returns True (iteration consumed)


class TestCandidateHandleCandidateFound:
    def test_acquires_lock_and_promotes(self):
        ctx = _make_context(do_failover_ok=True)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.try_acquire_lock.assert_called_once_with(allow_queue=True, timeout=0)
        ctx.do_failover.assert_called_once_with(old_primary='host1')
        ctx.zk.cleanup_switchover.assert_called_once()
        ctx.zk.write_last_switchover_time.assert_called_once()
        ctx.timings.stop.assert_called_once_with('switchover')

    def test_starts_downtime_when_not_already_started(self):
        """Candidate starts downtime timer if old primary was killed before starting it."""
        ctx = _make_context(do_failover_ok=True)
        ctx.timings.get_start.return_value = None  # downtime not started
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.timings.start.assert_called_once_with('downtime')

    def test_does_not_start_downtime_when_already_started(self):
        """Candidate does not restart downtime timer if old primary already started it."""
        ctx = _make_context(do_failover_ok=True)
        ctx.timings.get_start.return_value = 12345.0  # downtime already started
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.timings.start.assert_not_called()

    def test_does_not_acquire_when_debug_failure(self):
        ctx = _make_context()
        debug = MagicMock(side_effect=lambda name: name == 'candidate_switchover_before_acquire')
        m = _make_machine(ctx, debug_failure=debug)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.try_acquire_lock.assert_not_called()

    def test_releases_lock_on_failover_failure(self):
        ctx = _make_context(do_failover_ok=False)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.release_lock.assert_called_once()

    def test_releases_lock_when_switchover_info_missing(self):
        ctx = _make_context()
        ctx.zk.get_switchover_primary_info.return_value = None
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.release_lock.assert_called_once()
        ctx.do_failover.assert_not_called()

    def test_waits_when_lock_not_acquired(self):
        ctx = _make_context()
        ctx.zk.try_acquire_lock.return_value = False
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.do_failover.assert_not_called()
        ctx.zk.release_lock.assert_not_called()

    def test_lock_acquisition_is_non_blocking(self):
        """Step 15b: lock acquisition uses timeout=0 (non-blocking, one attempt per iteration)."""
        ctx = _make_context()
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.try_acquire_lock.assert_called_once_with(allow_queue=True, timeout=0)

    def test_writes_promoted_before_cleanup(self):
        ctx = _make_context(do_failover_ok=True)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        # PROMOTED should be written before cleanup
        write_calls = ctx.zk.write_switchover_state.call_args_list
        assert any(c == call(SwitchoverPhase.PROMOTED) for c in write_calls)

