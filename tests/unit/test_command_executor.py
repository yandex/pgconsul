# encoding: utf-8
"""Unit tests for the failover command executor."""

from unittest.mock import MagicMock, patch

import pytest

from src.command_executor import CommandExecutor
from src.commands import (
    ClearLocalState,
    Log,
    Promote,
    PromotionResult,
    RequestReturnToCluster,
    Sleep,
    StopPostgresql,
    StartTimer,
    StopTimer,
    SwitchoverStep,
)
from src.switchover import SwitchoverPhase, SwitchoverRecord
from src.exceptions import PostgresConnectionError
from src.zk import ZookeeperException


def _make_executor(*, switchover_step=None):
    zk = MagicMock()
    db = MagicMock()
    timings = MagicMock()
    promote = MagicMock(return_value=PromotionResult.SUCCESS)
    request_return_to_cluster = MagicMock()
    local_states = {
        'switchover_candidate': MagicMock(),
        'failover_participant': MagicMock(),
    }
    executor = CommandExecutor(
        zk=zk,
        db=db,
        timings=timings,
        promote=promote,
        request_return_to_cluster=request_return_to_cluster,
        local_states=local_states,
        switchover_step=switchover_step,
    )
    executor._local_operation_id = 'operation-1'
    return executor, {
        'zk': zk,
        'db': db,
        'timings': timings,
        'promote': promote,
        'request_return_to_cluster': request_return_to_cluster,
        'local_states': local_states,
    }


class _StubMachine:
    def __init__(self, plan):
        self._plan = plan

    def plan(self, observation):  # noqa: ANN001
        return self._plan


def test_clear_local_state_uses_current_operation():
    executor, deps = _make_executor()

    assert executor._dispatch(ClearLocalState('failover_participant')) is True
    deps['local_states']['failover_participant'].clear.assert_called_once_with(
        'operation-1',
    )


def test_timers_use_current_operation():
    executor, deps = _make_executor()
    deps['timings'].start.return_value = True
    deps['timings'].stop.return_value = True

    assert executor._dispatch(StartTimer('failover', ts=100.0)) is True
    assert executor._dispatch(StopTimer('failover', 'failover_duration')) is True
    deps['timings'].start.assert_called_once_with('failover', 'operation-1', 100.0)
    deps['timings'].stop.assert_called_once_with(
        'failover', 'operation-1', 'failover_duration',
    )


def test_sleep_and_log_dispatch():
    executor, _ = _make_executor()

    with patch('src.command_executor.time.sleep') as sleep:
        assert executor._dispatch(Sleep(3.0)) is True
    sleep.assert_called_once_with(3.0)

    with patch('src.command_executor.log_event') as log_event:
        assert executor._dispatch(Log('event', level='warning', event=True)) is True
    log_event.assert_called_once_with('event', level='warning')


def test_promote_dispatches_and_retries():
    executor, deps = _make_executor()
    command = Promote(scope='failover_participant', old_primary='host1')

    assert executor._dispatch(command) is True
    deps['promote'].assert_called_once_with(
        scope='failover_participant',
        operation_id='operation-1',
        old_primary='host1',
        start_postgresql=False,
    )

    deps['promote'].return_value = PromotionResult.RETRY
    assert executor._dispatch(command) is False


def test_request_return_to_cluster_dispatches():
    executor, deps = _make_executor()

    assert executor._dispatch(RequestReturnToCluster(
        new_primary='host2', role='replica', is_postgresql_dead=False,
    )) is True
    deps['request_return_to_cluster'].assert_called_once_with(
        'host2', 'replica', is_dead=False, start_source='archive',
    )


def test_primary_first_return_request_dispatches_source():
    executor, deps = _make_executor()

    assert executor._dispatch(RequestReturnToCluster(
        new_primary='host2', role='replica', is_postgresql_dead=False,
        start_source='primary',
    )) is True
    deps['request_return_to_cluster'].assert_called_once_with(
        'host2', 'replica', is_dead=False, start_source='primary',
    )


def test_switchover_step_dispatches_through_shared_executor():
    effect = MagicMock(return_value=True)
    executor, _ = _make_executor(switchover_step=effect)
    command = SwitchoverStep(
        action='cleanup',
        record=SwitchoverRecord(
            phase=SwitchoverPhase.CLEANUP,
            operation_id='operation-1',
        ),
        db_state={},
        zk_state={},
    )

    assert executor._dispatch(command) is True
    effect.assert_called_once_with(command)


def test_run_is_fail_fast_and_clears_operation_id():
    executor, deps = _make_executor()
    deps['db'].stop_postgresql.return_value = 1
    machine = _StubMachine([StopPostgresql(), ClearLocalState('failover_participant')])

    executor.run(machine, MagicMock(failover_version='failover-2'))

    deps['local_states']['failover_participant'].clear.assert_not_called()
    assert executor._local_operation_id is None


def test_run_catches_plan_exception():
    executor, _ = _make_executor()

    class _CrashingMachine:
        def plan(self, observation):  # noqa: ANN001
            raise RuntimeError('plan bug')

    executor.run(_CrashingMachine(), MagicMock(failover_version='failover-2'))


@pytest.mark.parametrize('exception', [
    PostgresConnectionError('db down'),
    ZookeeperException('zk down'),
])
def test_expected_io_exception_stops_command(exception):
    executor, deps = _make_executor()
    deps['db'].stop_postgresql.side_effect = exception

    assert executor._dispatch(StopPostgresql()) is False


def test_unexpected_exception_propagates():
    executor, deps = _make_executor()
    deps['db'].stop_postgresql.side_effect = RuntimeError('unexpected')

    with pytest.raises(RuntimeError):
        executor._dispatch(StopPostgresql())


def test_unknown_command_returns_false():
    executor, _ = _make_executor()

    assert executor._dispatch(object()) is False  # type: ignore[arg-type]
