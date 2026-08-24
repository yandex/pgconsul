# encoding: utf-8
"""
Unit tests for CommandExecutor dispatch (ADR-0006, step H3).

Interaction tests: each command type is dispatched to the correct infra call
with the right arguments. Fail-fast semantics and per-command exception
handling (PostgresConnectionError / ZookeeperException) are verified.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.command_executor import CommandExecutor
from src.commands import (
    AcquireLock,
    Checkpoint,
    CleanupSwitchover,
    CreateSlots,
    DeleteHostOp,
    DoFailover,
    LeaveSyncGroup,
    Log,
    ReleaseLock,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    Sleep,
    StartTimer,
    StopPooler,
    StopPostgresql,
    StopTimer,
    StoreReplicsInfo,
    TransitionTo,
    WriteCandidate,
    WriteFailoverState,
    WriteLastSwitchoverTime,
    WriteSideReplicas,
    WriteTimeline,
    WriteHostStat,
)
from src.exceptions import PostgresConnectionError
from src.switchover import SwitchoverPhase
from src.zk import ZookeeperException


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_executor():
    """Build a CommandExecutor with all infra objects and callbacks mocked."""
    zk = MagicMock()
    db = MagicMock()
    replication_manager = MagicMock()
    timings = MagicMock()
    slot_manager = MagicMock()

    rewind_from_source = MagicMock(return_value=True)
    debug_failure = MagicMock(return_value=False)

    executor = CommandExecutor(
        zk=zk,
        db=db,
        replication_manager=replication_manager,
        timings=timings,
        slot_manager=slot_manager,
        rewind_from_source=rewind_from_source,
        debug_failure=debug_failure,
        promote_checkpoint_sql=None,
    )
    return executor, {
        'zk': zk,
        'db': db,
        'replication_manager': replication_manager,
        'timings': timings,
        'slot_manager': slot_manager,
        'rewind_from_source': rewind_from_source,
        'debug_failure': debug_failure,
    }


class _StubMachine:
    """Minimal PlanMachine returning a fixed Plan."""

    def __init__(self, plan):
        self._plan = plan

    def plan(self, observation):  # noqa: ANN001
        return self._plan


# ---------------------------------------------------------------------------
# Common commands
# ---------------------------------------------------------------------------


class TestAcquireLock:
    def test_dispatches_to_zk_try_acquire_lock(self):
        executor, deps = _make_executor()
        deps['zk'].try_acquire_lock.return_value = True
        cmd = AcquireLock(lock_type='primary', allow_queue=False, timeout=10)

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].try_acquire_lock.assert_called_once_with(
            lock_type='primary', allow_queue=False, timeout=10
        )

    def test_returns_false_when_lock_not_acquired(self):
        executor, deps = _make_executor()
        deps['zk'].try_acquire_lock.return_value = False
        cmd = AcquireLock()

        result = executor._dispatch(cmd)

        assert result is False


class TestReleaseLock:
    def test_dispatches_to_zk_release_lock(self):
        executor, deps = _make_executor()
        deps['zk'].release_lock.return_value = True
        cmd = ReleaseLock(lock_type='primary', wait=5)

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].release_lock.assert_called_once_with(lock_type='primary', wait=5)

    def test_returns_false_when_release_fails(self):
        executor, deps = _make_executor()
        deps['zk'].release_lock.return_value = False
        cmd = ReleaseLock(lock_type='primary', wait=5)

        result = executor._dispatch(cmd)

        assert result is False


class TestStartTimer:
    def test_starts_timer_when_not_started(self):
        executor, deps = _make_executor()
        deps['timings'].get_start.return_value = None
        cmd = StartTimer(name='switchover', ts=100.0)

        result = executor._dispatch(cmd)

        assert result is True
        deps['timings'].start.assert_called_once_with('switchover', 100.0)

    def test_skips_when_already_started(self):
        executor, deps = _make_executor()
        deps['timings'].get_start.return_value = 50.0
        cmd = StartTimer(name='switchover')

        result = executor._dispatch(cmd)

        assert result is True
        deps['timings'].start.assert_not_called()


class TestStopTimer:
    def test_dispatches_to_timings_stop(self):
        executor, deps = _make_executor()
        cmd = StopTimer(name='switchover', track_as='switchover_duration')

        result = executor._dispatch(cmd)

        assert result is True
        deps['timings'].stop.assert_called_once_with('switchover', 'switchover_duration')

    def test_track_as_defaults_to_none(self):
        executor, deps = _make_executor()
        cmd = StopTimer(name='downtime')

        executor._dispatch(cmd)

        deps['timings'].stop.assert_called_once_with('downtime', None)


class TestWriteFailoverState:
    def test_dispatches_to_zk_write_failover_state(self):
        executor, deps = _make_executor()
        deps['zk'].write_failover_state.return_value = True
        cmd = WriteFailoverState(value='switchover_initiated')

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_failover_state.assert_called_once_with('switchover_initiated')

    def test_returns_false_on_zk_failure(self):
        executor, deps = _make_executor()
        deps['zk'].write_failover_state.return_value = False
        cmd = WriteFailoverState(value='failed')

        result = executor._dispatch(cmd)

        assert result is False


class TestWriteTimeline:
    def test_dispatches_to_zk_write_timeline(self):
        executor, deps = _make_executor()
        deps['zk'].write_timeline.return_value = True
        cmd = WriteTimeline(timeline=7)

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_timeline.assert_called_once_with(7)


class TestWriteLastSwitchoverTime:
    def test_dispatches_to_zk_write_last_switchover_time(self):
        executor, deps = _make_executor()
        deps['zk'].write_last_switchover_time.return_value = True
        cmd = WriteLastSwitchoverTime()

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_last_switchover_time.assert_called_once()


class TestStopPooler:
    def test_dispatches_to_db_pgpooler_stop(self):
        executor, deps = _make_executor()
        deps['db'].pgpooler.return_value = True
        cmd = StopPooler()

        result = executor._dispatch(cmd)

        assert result is True
        deps['db'].pgpooler.assert_called_once_with('stop')

    def test_returns_false_when_pgpooler_fails(self):
        executor, deps = _make_executor()
        deps['db'].pgpooler.return_value = False
        cmd = StopPooler()

        result = executor._dispatch(cmd)

        assert result is False


class TestStopPostgresql:
    def test_dispatches_with_explicit_timeout(self):
        executor, deps = _make_executor()
        deps['db'].stop_postgresql.return_value = 0
        cmd = StopPostgresql(wait=True, force_async=False, timeout=30)

        result = executor._dispatch(cmd)

        assert result is True
        deps['db'].stop_postgresql.assert_called_once_with(timeout=30, wait=True)

    def test_defaults_timeout_to_60(self):
        executor, deps = _make_executor()
        deps['db'].stop_postgresql.return_value = 0
        cmd = StopPostgresql(wait=False, force_async=True)

        executor._dispatch(cmd)

        deps['db'].stop_postgresql.assert_called_once_with(timeout=60, wait=False)

    def test_force_async_switches_to_async_before_stop(self):
        executor, deps = _make_executor()
        deps['db'].stop_postgresql.return_value = 0
        cmd = StopPostgresql(wait=False, force_async=True)

        executor._dispatch(cmd)

        deps['replication_manager'].change_replication_to_async.assert_called_once_with(
            reset_sync_replication_in_zk=False
        )

    def test_force_async_swallows_pg_error_and_continues(self):
        executor, deps = _make_executor()
        deps['replication_manager'].change_replication_to_async.side_effect = \
            PostgresConnectionError('conn lost')
        deps['db'].stop_postgresql.return_value = 0
        cmd = StopPostgresql(wait=False, force_async=True)

        result = executor._dispatch(cmd)

        assert result is True
        deps['db'].stop_postgresql.assert_called_once_with(timeout=60, wait=False)

    def test_no_force_async_skips_async_switch(self):
        executor, deps = _make_executor()
        deps['db'].stop_postgresql.return_value = 0
        cmd = StopPostgresql(wait=True, force_async=False)

        executor._dispatch(cmd)

        deps['replication_manager'].change_replication_to_async.assert_not_called()

    def test_returns_false_on_nonzero_exit(self):
        executor, deps = _make_executor()
        deps['db'].stop_postgresql.return_value = 1
        cmd = StopPostgresql()

        result = executor._dispatch(cmd)

        assert result is False


class TestCheckpoint:
    def test_dispatches_to_db_checkpoint(self):
        executor, deps = _make_executor()
        deps['db'].checkpoint.return_value = True
        cmd = Checkpoint()

        result = executor._dispatch(cmd)

        assert result is True
        deps['db'].checkpoint.assert_called_once()

    def test_returns_false_when_checkpoint_fails(self):
        executor, deps = _make_executor()
        deps['db'].checkpoint.return_value = False
        cmd = Checkpoint()

        result = executor._dispatch(cmd)

        assert result is False


class TestStoreReplicsInfo:
    def test_dispatches_to_zk_write_replics_info(self):
        executor, deps = _make_executor()
        deps['zk'].write_replics_info.return_value = True
        replics_info = [{'host': 'pg1', 'lsn': 100}]
        cmd = StoreReplicsInfo(replics_info=replics_info, timeline_match=True)

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_replics_info.assert_called_once_with(replics_info)

    def test_returns_false_when_timeline_match_false(self):
        executor, deps = _make_executor()
        cmd = StoreReplicsInfo(replics_info=[{'host': 'pg1'}], timeline_match=False)

        result = executor._dispatch(cmd)

        assert result is False
        deps['zk'].write_replics_info.assert_not_called()

    def test_returns_false_when_replics_info_none(self):
        executor, deps = _make_executor()
        cmd = StoreReplicsInfo(replics_info=None, timeline_match=True)

        result = executor._dispatch(cmd)

        assert result is False
        deps['zk'].write_replics_info.assert_not_called()


class TestWriteHostStat:
    def test_dispatches_to_zk_write_host_stat(self):
        executor, deps = _make_executor()
        deps['zk'].write_host_stat.return_value = True
        db_state = {'role': 'primary', 'replics_info': [], 'wal_receiver': None}
        cmd = WriteHostStat(hostname='host1', db_state=db_state, stream_from=None)

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_host_stat.assert_called_once_with('host1', db_state, None)

    def test_passes_stream_from(self):
        executor, deps = _make_executor()
        deps['zk'].write_host_stat.return_value = True
        db_state = {'role': 'primary'}
        cmd = WriteHostStat(hostname='host1', db_state=db_state, stream_from='source')

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_host_stat.assert_called_once_with('host1', db_state, 'source')

    def test_returns_false_on_zk_failure(self):
        executor, deps = _make_executor()
        deps['zk'].write_host_stat.return_value = False
        cmd = WriteHostStat(hostname='host1', db_state={}, stream_from=None)

        result = executor._dispatch(cmd)

        assert result is False


class TestLeaveSyncGroup:
    def test_dispatches_to_replication_manager(self):
        executor, deps = _make_executor()
        cmd = LeaveSyncGroup()

        result = executor._dispatch(cmd)

        assert result is True
        deps['replication_manager'].leave_sync_group.assert_called_once()


class TestSleep:
    @patch('src.command_executor.time.sleep')
    def test_dispatches_to_time_sleep(self, mock_sleep):
        executor, _ = _make_executor()
        cmd = Sleep(seconds=3.0)

        result = executor._dispatch(cmd)

        assert result is True
        mock_sleep.assert_called_once_with(3.0)


class TestLog:
    def test_event_log_uses_log_event(self):
        executor, _ = _make_executor()
        cmd = Log(message='SWITCHOVER started', level='warning', event=True)

        with patch('src.command_executor.log_event') as mock_log_event:
            result = executor._dispatch(cmd)

        assert result is True
        mock_log_event.assert_called_once_with(
            'SWITCHOVER started', level='warning'
        )

    def test_plain_log_uses_logging(self):
        executor, _ = _make_executor()
        cmd = Log(message='waiting for sync', level='debug')

        with patch('src.command_executor.logging.log') as mock_log:
            result = executor._dispatch(cmd)

        assert result is True
        mock_log.assert_called_once()

    def test_plain_log_defaults_to_info_level(self):
        executor, _ = _make_executor()
        cmd = Log(message='hello')

        with patch('src.command_executor.logging.log') as mock_log:
            executor._dispatch(cmd)

        # First positional arg is the level (logging.INFO = 20).
        assert mock_log.call_args[0][0] == 20  # logging.INFO


# ---------------------------------------------------------------------------
# Switchover commands
# ---------------------------------------------------------------------------


class TestTransitionTo:
    def test_dispatches_to_zk_write_switchover_state(self):
        executor, deps = _make_executor()
        deps['zk'].write_switchover_state.return_value = True
        cmd = TransitionTo(phase=SwitchoverPhase.SYNC_SET)

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_switchover_state.assert_called_once_with(
            SwitchoverPhase.SYNC_SET
        )

    def test_returns_false_on_zk_failure(self):
        executor, deps = _make_executor()
        deps['zk'].write_switchover_state.return_value = False
        cmd = TransitionTo(phase=SwitchoverPhase.FAILED)

        result = executor._dispatch(cmd)

        assert result is False


class TestWriteCandidate:
    def test_dispatches_to_zk_write_switchover_candidate(self):
        executor, deps = _make_executor()
        deps['zk'].write_switchover_candidate.return_value = True
        cmd = WriteCandidate(candidate='host2')

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_switchover_candidate.assert_called_once_with('host2')


class TestWriteSideReplicas:
    def test_dispatches_to_zk_write_switchover_side_replicas(self):
        executor, deps = _make_executor()
        deps['zk'].write_switchover_side_replicas.return_value = True
        cmd = WriteSideReplicas(side_replicas=('host3', 'host4'))

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].write_switchover_side_replicas.assert_called_once_with(
            ['host3', 'host4']
        )


class TestSetSyncReplication:
    def test_dispatches_to_replication_manager(self):
        executor, deps = _make_executor()
        deps['replication_manager'].change_replication_to_sync_host.return_value = True
        cmd = SetSyncReplication(host='host2')

        result = executor._dispatch(cmd)

        assert result is True
        deps['replication_manager'].change_replication_to_sync_host.assert_called_once_with(
            'host2'
        )


class TestCleanupSwitchover:
    def test_dispatches_to_zk_cleanup_switchover(self):
        executor, deps = _make_executor()
        cmd = CleanupSwitchover()

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].cleanup_switchover.assert_called_once()


# ---------------------------------------------------------------------------
# Opaque commands
# ---------------------------------------------------------------------------


class TestDoFailover:
    def test_dispatches_to_do_failover_method(self):
        executor, deps = _make_executor()
        with patch.object(executor, '_do_failover', return_value=True) as mock_do:
            cmd = DoFailover(old_primary='host1')

            result = executor._dispatch(cmd)

        assert result is True
        mock_do.assert_called_once_with(old_primary='host1')

    def test_returns_false_when_do_failover_returns_false(self):
        executor, deps = _make_executor()
        with patch.object(executor, '_do_failover', return_value=False):
            cmd = DoFailover(old_primary=None)

            result = executor._dispatch(cmd)

        assert result is False


class TestRewindFromSource:
    def test_dispatches_to_rewind_from_source_callback(self):
        executor, deps = _make_executor()
        deps['rewind_from_source'].return_value = True
        cmd = RewindFromSource(
            new_primary='host2', is_postgresql_dead=True, limit=60.0
        )

        result = executor._dispatch(cmd)

        assert result is True
        deps['rewind_from_source'].assert_called_once_with(
            is_postgresql_dead=True, limit=60.0, new_primary='host2'
        )

    def test_returns_false_when_rewind_returns_false(self):
        executor, deps = _make_executor()
        deps['rewind_from_source'].return_value = False
        cmd = RewindFromSource(
            new_primary='host2', is_postgresql_dead=False, limit=30.0
        )

        result = executor._dispatch(cmd)

        assert result is False


class TestSetSimplePrimarySwitchTry:
    def test_dispatches_to_zk_set_simple_primary_switch_tried(self):
        executor, deps = _make_executor()
        cmd = SetSimplePrimarySwitchTry(hostname='host1')

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].set_simple_primary_switch_tried.assert_called_once_with('host1')


class TestDeleteHostOp:
    def test_dispatches_to_zk_delete_host_op(self):
        executor, deps = _make_executor()
        cmd = DeleteHostOp()

        result = executor._dispatch(cmd)

        assert result is True
        deps['zk'].delete_host_op.assert_called_once()


class TestCreateSlots:
    def test_dispatches_to_slot_manager(self):
        executor, deps = _make_executor()
        deps['slot_manager'].create_slots_for_hosts.return_value = True
        cmd = CreateSlots(hosts=('host3', 'host4'))

        result = executor._dispatch(cmd)

        assert result is True
        deps['slot_manager'].create_slots_for_hosts.assert_called_once_with(['host3', 'host4'])

    def test_returns_false_when_create_slots_fails(self):
        executor, deps = _make_executor()
        deps['slot_manager'].create_slots_for_hosts.return_value = False
        cmd = CreateSlots(hosts=('host3',))

        result = executor._dispatch(cmd)

        assert result is False


# ---------------------------------------------------------------------------
# run() — orchestration and fail-fast
# ---------------------------------------------------------------------------


class TestRun:
    def test_empty_plan_no_dispatch(self):
        executor, _ = _make_executor()
        machine = _StubMachine(plan=[])
        obs = MagicMock()

        executor.run(machine, obs)

    def test_nonempty_plan_dispatches(self):
        executor, deps = _make_executor()
        deps['zk'].write_failover_state.return_value = True
        machine = _StubMachine(plan=[WriteFailoverState(value='ok')])
        obs = MagicMock()

        executor.run(machine, obs)

        deps['zk'].write_failover_state.assert_called_once_with('ok')

    def test_fail_fast_stops_on_first_failing_command(self):
        executor, deps = _make_executor()
        deps['zk'].write_failover_state.return_value = False
        machine = _StubMachine(
            plan=[
                WriteFailoverState(value='first'),
                WriteFailoverState(value='second'),
            ]
        )
        obs = MagicMock()

        executor.run(machine, obs)

        # Fail-fast: only first cmd ran, second skipped.
        deps['zk'].write_failover_state.assert_called_once_with('first')

    def test_executes_all_commands_when_all_succeed(self):
        executor, deps = _make_executor()
        deps['zk'].write_failover_state.return_value = True
        deps['db'].checkpoint.return_value = True
        machine = _StubMachine(
            plan=[
                Checkpoint(),
                WriteFailoverState(value='ok'),
            ]
        )
        obs = MagicMock()

        executor.run(machine, obs)

        deps['db'].checkpoint.assert_called_once()
        assert deps['zk'].write_failover_state.call_count == 1

    def test_plan_exception_does_not_propagate(self):
        """Unexpected exception from machine.plan() is caught, not propagated."""
        executor, _ = _make_executor()

        class _CrashingMachine:
            def plan(self, observation):  # noqa: ANN001
                raise RuntimeError('plan bug')

        executor.run(_CrashingMachine(), MagicMock())


# ---------------------------------------------------------------------------
# Exception handling (ADR-0002)
# ---------------------------------------------------------------------------


class TestExceptionHandling:
    def test_postgres_connection_error_caught_returns_false(self):
        executor, deps = _make_executor()
        deps['db'].checkpoint.side_effect = PostgresConnectionError('conn lost')
        cmd = Checkpoint()

        result = executor._dispatch(cmd)

        assert result is False

    def test_zookeeper_exception_caught_returns_false(self):
        executor, deps = _make_executor()
        deps['zk'].write_failover_state.side_effect = ZookeeperException('zk down')
        cmd = WriteFailoverState(value='ok')

        result = executor._dispatch(cmd)

        assert result is False

    def test_postgres_error_stops_plan_execution(self):
        executor, deps = _make_executor()
        deps['db'].checkpoint.side_effect = PostgresConnectionError('conn lost')
        deps['zk'].write_failover_state.return_value = True
        machine = _StubMachine(
            plan=[
                Checkpoint(),
                WriteFailoverState(value='should_not_run'),
            ]
        )
        obs = MagicMock()

        executor.run(machine, obs)

        # Fail-fast: second cmd not executed (retry next iteration).
        deps['zk'].write_failover_state.assert_not_called()

    def test_uncaught_exception_propagates(self):
        executor, deps = _make_executor()
        deps['db'].checkpoint.side_effect = RuntimeError('unexpected')
        cmd = Checkpoint()

        with pytest.raises(RuntimeError):
            executor._dispatch(cmd)


# ---------------------------------------------------------------------------
# Unknown command
# ---------------------------------------------------------------------------


class TestUnknownCommand:
    def test_unknown_command_returns_false(self):
        executor, _ = _make_executor()

        # A bare object is not a known command type.
        result = executor._dispatch(object())  # type: ignore[arg-type]

        assert result is False
