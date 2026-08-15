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
    re-acquire it, then release it for DaemonContext.
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

    # Stale lock broken (once for stale recovery, once for DaemonContext release)
    assert mock_pidfile.break_lock.call_count == 2
    # Lock re-acquired after stale recovery
    assert mock_pidfile.acquire.call_count == 2
    # Must return the pidfile
    assert result is mock_pidfile


def test_acquire_pid_lock_with_dead_process():
    """
    acquire_pid_lock() must break the lock when the PID in the file refers
    to a dead process (OSError from os.kill), then release it for DaemonContext.
    """
    from src.helpers import acquire_pid_lock

    mock_pidfile = MagicMock()
    import lockfile
    mock_pidfile.acquire.side_effect = [lockfile.AlreadyLocked('/fake'), None]
    mock_pidfile.read_pid.return_value = 99999

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile), \
         patch('os.kill', side_effect=ProcessLookupError('No such process')):
        result = acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')

    # Stale lock broken (once for dead-PID recovery, once for DaemonContext release)
    assert mock_pidfile.break_lock.call_count == 2
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
    The lock is released (break_lock) so DaemonContext can acquire it.
    """
    from src.helpers import acquire_pid_lock

    mock_pidfile = MagicMock()
    # acquire() succeeds on first call — no AlreadyLocked

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile):
        result = acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')

    mock_pidfile.acquire.assert_called_once()
    # Lock released for DaemonContext
    mock_pidfile.break_lock.assert_called_once()
    assert result is mock_pidfile


def test_acquire_pid_lock_releases_lock_for_daemon_context():
    """
    acquire_pid_lock() must NOT leave the lock held — daemon.DaemonContext
    calls pidfile.acquire() again in its __enter__. If the lock is still
    held, DaemonContext crashes with AlreadyLocked on every startup.

    Regression: commit c67315f moved PID-lock acquisition into acquire_pid_lock()
    but left the lock acquired. DaemonContext(pidfile=pidfile) re-acquires →
    AlreadyLocked → pgconsul crashes in a restart loop → behave test hangs
    on "Then container became a primary" (PostgreSQL never managed).

    Contract: after acquire_pid_lock() returns, the lock must be released
    (break_lock called) so DaemonContext can acquire it cleanly.
    """
    from src.helpers import acquire_pid_lock

    mock_pidfile = MagicMock()

    with patch('src.helpers.PIDLockFile', return_value=mock_pidfile):
        acquire_pid_lock('/var/run/pgconsul/pgconsul.pid')

    # Lock must be released (break_lock called) so DaemonContext can acquire.
    mock_pidfile.break_lock.assert_called()
