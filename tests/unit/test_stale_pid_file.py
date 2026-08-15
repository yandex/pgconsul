# encoding: utf-8
"""
Regression test for a behave failure (slot.feature:24
"Slots created on promoted replica").

Root cause: when pgconsul restarts after an unclean shutdown, a stale PID
file may exist on disk. PIDLockFile.acquire() raises AlreadyLocked, and
acquire_pid_lock() calls os.kill(pidfile.read_pid(), 0) to check whether
the old process is still alive. However, read_pid() returns None when the
PID file is empty or contains garbage — os.kill(None, 0) raises TypeError,
which is NOT caught by the `except OSError` handler. The unhandled
TypeError crashes pgconsul on every startup attempt, so PostgreSQL is never
managed and the behave test hangs waiting for a connection that never
becomes available.

Reproduces: slot.feature:24 — pgconsul in postgresql1 never starts because
of a stale /var/run/pgconsul/pgconsul.pid left from a previous scenario.
"""

from unittest.mock import MagicMock, patch

import pytest


def test_acquire_pid_lock_with_stale_empty_pid_file():
    """
    acquire_pid_lock() must not crash with TypeError when the PID file is
    stale (read_pid() returns None). It should break the stale lock and
    re-acquire it.
    """
    from src.helpers import acquire_pid_lock

    # Simulate a stale PID file: acquire() raises AlreadyLocked,
    # read_pid() returns None (empty or corrupt PID file).
    mock_pidfile = MagicMock()
    import lockfile
    mock_pidfile.acquire.side_effect = [lockfile.AlreadyLocked('/fake'), None]
    mock_pidfile.read_pid.return_value = None

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile):
        # Must not raise TypeError
        result = acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')

    # Stale lock must be broken so pgconsul can proceed
    mock_pidfile.break_lock.assert_called_once()
    # Lock must be re-acquired after breaking
    assert mock_pidfile.acquire.call_count == 2
    # Must return the pidfile
    assert result is mock_pidfile


def test_acquire_pid_lock_with_dead_process():
    """
    acquire_pid_lock() must break the lock when the PID in the file refers
    to a dead process (OSError from os.kill).
    """
    from src.helpers import acquire_pid_lock

    mock_pidfile = MagicMock()
    import lockfile
    mock_pidfile.acquire.side_effect = [lockfile.AlreadyLocked('/fake'), None]
    mock_pidfile.read_pid.return_value = 99999

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile), \
         patch('os.kill', side_effect=ProcessLookupError('No such process')):
        result = acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')

    mock_pidfile.break_lock.assert_called_once()
    assert mock_pidfile.acquire.call_count == 2
    assert result is mock_pidfile


def test_acquire_pid_lock_with_live_process_exits():
    """
    acquire_pid_lock() must sys.exit(1) when the PID in the file refers to
    a live process.
    """
    from src.helpers import acquire_pid_lock

    mock_pidfile = MagicMock()
    import lockfile
    mock_pidfile.acquire.side_effect = lockfile.AlreadyLocked('/fake')
    mock_pidfile.read_pid.return_value = 12345

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile), \
         patch('os.kill', return_value=None):
        with pytest.raises(SystemExit) as exc_info:
            acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')
        assert exc_info.value.code == 1

    # Lock must NOT be broken for a live process
    mock_pidfile.break_lock.assert_not_called()


def test_acquire_pid_lock_free_lock():
    """
    acquire_pid_lock() must return the pidfile when the lock is free.
    """
    from src.helpers import acquire_pid_lock

    mock_pidfile = MagicMock()
    # acquire() succeeds on first call — no AlreadyLocked

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile):
        result = acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')

    mock_pidfile.acquire.assert_called_once()
    mock_pidfile.break_lock.assert_not_called()
    assert result is mock_pidfile
