import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def steps(monkeypatch):
    for name, attribute in [('faultstorm.cluster', 'ClusterManager'), ('faultstorm.config', 'TestConfig'),
                            ('faultstorm.network_latency', 'NetworkLatencyManager'), ('faultstorm_resetup', 'ResetupAction')]:
        module = ModuleType(name)
        setattr(module, attribute, MagicMock())
        monkeypatch.setitem(sys.modules, name, module)
    behave = ModuleType('behave')
    for name in ('given', 'when', 'then'):
        setattr(behave, name, lambda pattern: lambda function: function)
    monkeypatch.setitem(sys.modules, 'behave', behave)
    path = Path(__file__).resolve().parents[3] / 'tests/faultstorm/steps/resetup_steps.py'
    spec = importlib.util.spec_from_file_location('resetup_steps', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    clock = SimpleNamespace(now=0)

    def sleep(seconds):
        clock.now += seconds

    monkeypatch.setattr(module, 'time', SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.now, sleep=sleep))
    return module


@pytest.mark.parametrize('step', ['step_postgres_running', 'step_wait_postgres_running'])
def test_sql_ready_does_not_finish_resetup_while_daemon_is_starting(steps, step):
    calls = []

    def execute(node, command, timeout):
        calls.append(command)
        if command[-1] == 'SELECT 1':
            return '1'
        if len(calls) == 1:
            raise RuntimeError('resetup lock is held')
        return 'ready'

    steps.ClusterManager.exec_on_node.side_effect = execute
    arguments = (None, 'postgresql3') if step == 'step_postgres_running' else (None, 120, 'postgresql3')

    getattr(steps, step)(*arguments)

    assert len(calls) == 2


def test_resetup_wait_reports_last_failure_at_deadline(steps):
    steps.ClusterManager.exec_on_node.side_effect = RuntimeError('resetup lock is held')

    with pytest.raises(AssertionError, match='postgresql3.*7.*resetup lock is held'):
        steps.step_wait_postgres_running(None, 7, 'postgresql3')

    assert steps.time.monotonic() == 7
    assert all(call.kwargs['timeout'] <= 7 for call in steps.ClusterManager.exec_on_node.call_args_list)


@pytest.fixture
def probe(steps, tmp_path, monkeypatch):
    import subprocess

    lock_path = tmp_path / '.pg_resetup.lock'
    flag_path = tmp_path / '.pgconsul_rewind_fail.flag'
    monkeypatch.setattr(sys, 'argv', ['-c', str(lock_path), str(flag_path)])
    query = MagicMock(side_effect=['pgconsul RUNNING pid 42, uptime 0:00:10', 't'])
    monkeypatch.setattr(subprocess, 'check_output', query)
    return SimpleNamespace(run=lambda: exec(steps._RESETUP_READY_SCRIPT, {}), lock=lock_path, flag=flag_path, query=query)


def test_resetup_probe_waits_for_lock_owner_and_releases_its_own_lock(probe, capsys):
    import fcntl

    with probe.lock.open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SystemExit, match='resetup lock is held'):
            probe.run()
        probe.query.assert_not_called()

    probe.run()

    assert capsys.readouterr().out == 'ready\n'
    with probe.lock.open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    query = probe.query.call_args.args[0][-1]
    assert 'pg_is_in_recovery()' in query
    assert 'pg_stat_wal_receiver' in query
    assert "status = 'streaming'" in query


def test_resetup_probe_rejects_flag_even_with_free_lock(probe):
    probe.flag.touch()

    with pytest.raises(SystemExit, match='rewind-fail flag is set'):
        probe.run()

    probe.query.assert_not_called()
    assert probe.flag.exists()


@pytest.mark.parametrize('status,replica,error', [
    ('pgconsul STARTING', 't', 'pgconsul is not RUNNING'),
    ('pgconsul RUNNING pid 42', 'f', 'replica WAL receiver is not streaming'),
    ('pgconsul RUNNING pid 42', '', 'replica WAL receiver is not streaming'),
])
def test_resetup_probe_requires_running_daemon_and_streaming_replica(probe, status, replica, error):
    probe.query.side_effect = [status, replica]

    with pytest.raises(SystemExit, match=error):
        probe.run()


def test_resetup_probe_releases_lock_after_failed_query(probe):
    import fcntl

    probe.query.side_effect = RuntimeError('connection lost')

    with pytest.raises(RuntimeError, match='connection lost'):
        probe.run()

    with probe.lock.open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
