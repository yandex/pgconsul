"""Infrastructure tests for Behave failure logging."""

import logging
import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def after_step(monkeypatch):
    helpers = ModuleType('steps.helpers')
    helpers.LOG = logging.getLogger('helpers')
    steps = ModuleType('steps')
    steps.helpers = helpers
    latency = ModuleType('steps.latency')
    latency.init_latency_context = MagicMock()
    monkeypatch.delenv('DEBUG', raising=False)
    modules = {'steps': steps, 'steps.helpers': helpers, 'steps.latency': latency, 'docker': MagicMock()}
    with patch.dict(sys.modules, modules):
        hooks = runpy.run_path(str(Path(__file__).parents[1] / 'environment.py'))
    return hooks['after_step']


@pytest.mark.parametrize('filename', ['tests/features/pgconsul_util.feature', '<string>'])
@pytest.mark.parametrize('message_ready', [False, True])
def test_failed_step_logs_command_output_and_traceback(after_step, caplog, tmp_path, monkeypatch, filename, message_ready):
    monkeypatch.chdir(tmp_path)
    try:
        raise AssertionError('Expected "0", got "1", output was "NotEmptyError: maintenance"')
    except AssertionError as exc:
        step = SimpleNamespace(
            keyword='Then', name='command exit with return code "0"', status='failed', duration=0,
            filename=filename, line=877, error_message=str(exc) if message_ready else None,
            exception=exc, exc_traceback=exc.__traceback__,
        )
    with caplog.at_level(logging.INFO, logger='helpers'):
        after_step(SimpleNamespace(containers={}), step)
    assert 'NotEmptyError: maintenance' in caplog.text
    assert 'Traceback (most recent call last)' in caplog.text
    assert 'raise AssertionError' in caplog.text


def test_failed_step_without_exception_logs_error_message(after_step, caplog, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    step = SimpleNamespace(keyword='Then', name='check', status='failed', duration=0, filename='<string>',
                           error_message='Failure without exception', exception=None, exc_traceback=None)
    after_step(SimpleNamespace(containers={}), step)
    assert 'Failure without exception' in caplog.text


def test_passed_step_does_not_log_error_details(after_step, caplog):
    step = SimpleNamespace(keyword='Then', name='check', status='passed', duration=0,
                           error_message='stale error', exception=None)
    with caplog.at_level(logging.INFO, logger='helpers'):
        after_step(SimpleNamespace(containers={}), step)
    assert 'Finished step:' in caplog.text
    assert 'stale error' not in caplog.text
