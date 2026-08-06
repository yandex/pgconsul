"""Unit tests for PrimarySwitchoverMachine (step 14b-14d)."""

from unittest.mock import MagicMock, call

from src.exceptions import PostgresConnectionError
from src.switchover import (
    PrimaryContext,
    PrimarySwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _make_zk(write_ok=True):
    zk = MagicMock()
    zk.write_switchover_state.return_value = write_ok
    zk.write_switchover_candidate.return_value = write_ok
    zk.write_switchover_side_replicas.return_value = write_ok
    zk.write_failover_state.return_value = write_ok
    zk.PRIMARY_LOCK_PATH = 'master'
    return zk


def _make_record(phase, candidate='host2', destination='host2'):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination=destination,
        phase=phase,
        candidate=candidate,
        side_replicas=['host3'],
    )


def _make_context(
    streaming_replicas=('host2', 'host3'),
    candidate_is_sync=True,
    stop_pg_return=0,
    pg_status=0,
    lock_holder=None,
    new_primary=None,
):
    ctx = MagicMock()
    ctx.get_streaming_replicas.return_value = list(streaming_replicas)
    ctx.candidate_is_sync.return_value = candidate_is_sync
    ctx.stop_postgresql.return_value = stop_pg_return
    ctx.db.get_postgresql_status.return_value = pg_status
    ctx.db.get_replics_info.return_value = []
    ctx.db.checkpoint.return_value = None
    ctx.db.pgpooler.return_value = None
    ctx.zk = _make_zk()
    ctx.zk.get_current_lock_holder.side_effect = lambda *a, **k: lock_holder if a == () else new_primary
    ctx.zk.get_switchover_state.return_value = None
    ctx.timings.get_start.return_value = None
    ctx.get_hostname.return_value = 'host1'
    return ctx


def _make_machine(ctx, zk=None, debug_failure=None):
    """Create a machine sharing the same zk as the context."""
    zk = zk or ctx.zk
    return PrimarySwitchoverMachine(zk, context=ctx, debug_failure=debug_failure)


class TestTransitionTo:
    def test_writes_phase_to_zk(self):
        zk = _make_zk()
        m = PrimarySwitchoverMachine(zk)
        result = m.transition_to(SwitchoverPhase.SYNC_SET)
        assert result is True
        zk.write_switchover_state.assert_called_once_with(SwitchoverPhase.SYNC_SET)

    def test_returns_false_on_zk_failure(self):
        zk = _make_zk(write_ok=False)
        m = PrimarySwitchoverMachine(zk)
        result = m.transition_to(SwitchoverPhase.INITIATED)
        assert result is False


class TestStepDispatch:
    def test_returns_false_for_failed_phase(self):
        zk = _make_zk()
        m = PrimarySwitchoverMachine(zk)
        record = _make_record(SwitchoverPhase.FAILED)
        assert m.step(record, {}, {}) is False

    def test_returns_false_for_none_phase(self):
        zk = _make_zk()
        m = PrimarySwitchoverMachine(zk)
        record = _make_record(None)
        assert m.step(record, {}, {}) is False

    def test_returns_false_for_promoted_phase(self):
        # PROMOTED has no primary-side handler
        zk = _make_zk()
        m = PrimarySwitchoverMachine(zk)
        record = _make_record(SwitchoverPhase.PROMOTED)
        assert m.step(record, {}, {}) is False

    def test_returns_false_when_context_is_none(self):
        """Without context, step returns False (stub-only machine)."""
        zk = _make_zk()
        m = PrimarySwitchoverMachine(zk)
        for phase in (
            SwitchoverPhase.SYNC_SET,
            SwitchoverPhase.INITIATED,
            SwitchoverPhase.CANDIDATE_FOUND,
            SwitchoverPhase.PRIMARY_SHUT,
        ):
            record = _make_record(phase)
            result = m.step(record, {}, {})
            assert result is False, f'phase {phase} should return False without context'

    def test_debug_failure_hook_callable(self):
        """debug_failure hook can be injected and is callable."""
        zk = _make_zk()
        debug = MagicMock(return_value=False)
        m = PrimarySwitchoverMachine(zk, debug_failure=debug)
        assert m._debug_failure is debug


class TestHandleSyncSet:
    def test_transitions_to_initiated(self):
        ctx = _make_context()
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.SYNC_SET)
        assert m.step(record, {}, {}) is True
        ctx.zk.write_switchover_candidate.assert_called_once_with('host2')
        ctx.zk.write_switchover_side_replicas.assert_called_once_with(['host3'])
        # transition_to(INITIATED) + backwards-compat failover_state
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.INITIATED)
        ctx.zk.write_failover_state.assert_called_once_with('switchover_initiated')

    def test_aborts_when_candidate_is_none(self):
        ctx = _make_context()
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.SYNC_SET, candidate=None, destination=None)
        assert m.step(record, {}, {}) is True
        # FAILED is written
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.FAILED)

    def test_returns_true_on_zk_write_candidate_failure(self):
        ctx = _make_context()
        ctx.zk.write_switchover_candidate.return_value = False
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.SYNC_SET)
        assert m.step(record, {}, {}) is True
        # Should not proceed to INITIATED
        ctx.zk.write_switchover_state.assert_not_called()

    def test_idempotent_side_replicas_excludes_candidate(self):
        ctx = _make_context(streaming_replicas=('host2', 'host3', 'host4'))
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.SYNC_SET, candidate='host2')
        assert m.step(record, {}, {}) is True
        ctx.zk.write_switchover_side_replicas.assert_called_once_with(['host3', 'host4'])


class TestHandleInitiated:
    def test_waits_when_candidate_not_ready(self):
        ctx = _make_context()
        ctx.zk.get_switchover_state.return_value = SwitchoverPhase.INITIATED
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        # No checkpoint yet — candidate not ready
        ctx.db.checkpoint.assert_not_called()

    def test_proceeds_when_candidate_found(self):
        ctx = _make_context()
        ctx.zk.get_switchover_state.return_value = SwitchoverPhase.CANDIDATE_FOUND
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        ctx.db.get_replics_info.assert_called_once_with('primary')
        ctx.db.checkpoint.assert_called_once()

    def test_continues_on_pg_connection_error(self):
        ctx = _make_context()
        ctx.zk.get_switchover_state.return_value = SwitchoverPhase.CANDIDATE_FOUND
        ctx.db.get_replics_info.side_effect = PostgresConnectionError('lost')
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.INITIATED)
        assert m.step(record, {}, {}) is True
        # Should still attempt checkpoint despite replics_info failure
        ctx.db.checkpoint.assert_called_once()


class TestHandleCandidateFound:
    def test_starts_downtime_timer_once(self):
        ctx = _make_context(candidate_is_sync=True)
        ctx.timings.get_start.return_value = None
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.timings.start.assert_called_once_with('downtime')
        ctx.db.pgpooler.assert_called_once_with('stop')

    def test_does_not_restart_downtime_if_already_started(self):
        ctx = _make_context(candidate_is_sync=True)
        ctx.timings.get_start.return_value = 12345.0
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.timings.start.assert_not_called()

    def test_waits_when_candidate_not_in_sync(self):
        ctx = _make_context(candidate_is_sync=False)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        # Should not stop PG yet
        ctx.stop_postgresql.assert_not_called()
        # Should not transition to PRIMARY_SHUT
        ctx.zk.write_switchover_state.assert_not_called()

    def test_stops_pg_and_releases_lock_when_in_sync(self):
        ctx = _make_context(candidate_is_sync=True, stop_pg_return=0)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        # First stop: non-blocking (wait=False)
        ctx.stop_postgresql.assert_any_call(wait=False, force_async=False)
        # Lock released
        ctx.zk.release_lock.assert_called_once()
        # PRIMARY_SHUT persisted
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.PRIMARY_SHUT)
        # Return-to-cluster signaled
        ctx.set_simple_primary_switch_try.assert_called_once()

    def test_aborts_when_candidate_is_none(self):
        ctx = _make_context()
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND, candidate=None, destination=None)
        assert m.step(record, {}, {}) is True
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.FAILED)

    def test_debug_failure_before_catchup_aborts(self):
        ctx = _make_context(candidate_is_sync=True)
        debug = MagicMock(side_effect=lambda name: name == 'primary_switchover_before_catchup')
        m = _make_machine(ctx, debug_failure=debug)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.FAILED)

    def test_debug_failure_before_release_aborts(self):
        ctx = _make_context(candidate_is_sync=True, stop_pg_return=0)
        debug = MagicMock(side_effect=lambda name: name == 'primary_switchover_before_release')
        m = _make_machine(ctx, debug_failure=debug)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        ctx.zk.write_switchover_state.assert_called_with(SwitchoverPhase.FAILED)
        # Lock should not be released
        ctx.zk.release_lock.assert_not_called()

    def test_retries_on_pg_connection_error(self):
        ctx = _make_context(candidate_is_sync=True)
        ctx.db.get_replics_info.side_effect = PostgresConnectionError('lost')
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.CANDIDATE_FOUND)
        assert m.step(record, {}, {}) is True
        # Should not stop PG when sync check fails
        ctx.stop_postgresql.assert_not_called()


class TestHandlePrimaryShut:
    def test_releases_lock_if_unexpectedly_held(self):
        ctx = _make_context(lock_holder='host1')
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.PRIMARY_SHUT)
        assert m.step(record, {}, {}) is True
        ctx.db.pgpooler.assert_called_once_with('stop')
        ctx.zk.release_lock.assert_called_once()

    def test_rewinds_to_new_primary(self):
        ctx = _make_context(new_primary='host2')
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.PRIMARY_SHUT)
        assert m.step(record, {}, {}) is True
        ctx.zk.delete_host_op.assert_called_once()
        ctx.set_simple_primary_switch_try.assert_called_once()
        ctx.rewind_from_source.assert_called_once()

    def test_waits_when_no_new_primary(self):
        ctx = _make_context(new_primary=None)
        m = _make_machine(ctx)
        record = _make_record(SwitchoverPhase.PRIMARY_SHUT)
        assert m.step(record, {}, {}) is True
        ctx.rewind_from_source.assert_not_called()
