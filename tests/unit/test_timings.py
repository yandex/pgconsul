# encoding: utf-8
"""
Unit tests for TimingTracker (src/timings.py).

Covers start/stop/clear/get_start delegation to Zookeeper and the _log_timing
behaviour: timings are always written to the main pgconsul log, and optionally
forwarded to an external command when one is configured.
"""

import logging
from unittest.mock import MagicMock, patch

from src.timings import TimingTracker


class TestTimingTrackerInit:
    """Constructor wiring."""

    def test_stores_zk_and_command(self):
        zk = MagicMock()
        tracker = TimingTracker(zk, 'echo %s %s')
        assert tracker._zk is zk
        assert tracker._log_timing_command == 'echo %s %s'

    def test_command_can_be_none(self):
        zk = MagicMock()
        tracker = TimingTracker(zk, None)
        assert tracker._log_timing_command is None


class TestTimingTrackerGetStart:
    """get_start delegates to zk.get_timing."""

    def test_returns_value_from_zk(self):
        zk = MagicMock()
        zk.get_timing.return_value = 1234.5
        tracker = TimingTracker(zk, None)
        assert tracker.get_start('failover') == 1234.5
        zk.get_timing.assert_called_once_with('failover')

    def test_returns_none_when_zk_returns_none(self):
        zk = MagicMock()
        zk.get_timing.return_value = None
        tracker = TimingTracker(zk, None)
        assert tracker.get_start('downtime') is None


class TestTimingTrackerStart:
    """start writes current timestamp (or provided ts) to ZK."""

    def test_uses_current_time_when_ts_not_provided(self):
        zk = MagicMock()
        tracker = TimingTracker(zk, None)
        with patch('src.timings.time.time', return_value=9999.0):
            tracker.start('failover')
        zk.write_timing.assert_called_once_with('failover', 9999.0)

    def test_uses_provided_ts(self):
        zk = MagicMock()
        tracker = TimingTracker(zk, None)
        tracker.start('downtime', ts=12345.0)
        zk.write_timing.assert_called_once_with('downtime', 12345.0)


class TestTimingTrackerClear:
    """clear delegates to zk.delete_timing."""

    def test_calls_zk_delete_timing(self):
        zk = MagicMock()
        tracker = TimingTracker(zk, None)
        tracker.clear('switchover')
        zk.delete_timing.assert_called_once_with('switchover')


class TestTimingTrackerStop:
    """stop reads start, clears ZK node, and logs the elapsed duration."""

    def test_logs_and_clears_when_start_exists(self, caplog):
        zk = MagicMock()
        zk.get_timing.return_value = 100.0
        tracker = TimingTracker(zk, None)
        with patch('src.timings.time.time', return_value=105.5):
            with caplog.at_level(logging.INFO):
                tracker.stop('failover')
        zk.delete_timing.assert_called_once_with('failover')
        assert any(
            'Timing failover: 5.500 seconds' in rec.getMessage()
            for rec in caplog.records
        )

    def test_uses_track_as_name_in_log(self, caplog):
        zk = MagicMock()
        zk.get_timing.return_value = 200.0
        tracker = TimingTracker(zk, None)
        with patch('src.timings.time.time', return_value=203.0):
            with caplog.at_level(logging.INFO):
                tracker.stop('switchover', track_as='switchover_failure')
        zk.delete_timing.assert_called_once_with('switchover')
        assert any(
            'Timing switchover_failure: 3.000 seconds' in rec.getMessage()
            for rec in caplog.records
        )

    def test_does_nothing_when_start_is_none(self, caplog):
        zk = MagicMock()
        zk.get_timing.return_value = None
        tracker = TimingTracker(zk, None)
        with caplog.at_level(logging.INFO):
            tracker.stop('downtime')
        zk.delete_timing.assert_not_called()
        assert not any('Timing' in rec.getMessage() for rec in caplog.records)


class TestLogTiming:
    """_log_timing always logs to the main log and optionally runs an external command."""

    def test_always_logs_to_main_log_without_command(self, caplog):
        zk = MagicMock()
        tracker = TimingTracker(zk, None)
        with caplog.at_level(logging.INFO):
            tracker._log_timing('failover', 42.5)
        assert any(
            'Timing failover: 42.500 seconds' in rec.getMessage()
            for rec in caplog.records
        )

    def test_logs_and_runs_external_command(self, caplog):
        zk = MagicMock()
        tracker = TimingTracker(zk, 'echo %s %s')
        with patch('src.timings.subprocess.run') as mock_run:
            with caplog.at_level(logging.INFO):
                tracker._log_timing('downtime', 10.0)
        # Main log entry present.
        assert any(
            'Timing downtime: 10.000 seconds' in rec.getMessage()
            for rec in caplog.records
        )
        # External command executed with formatted args.
        mock_run.assert_called_once_with('echo downtime 10.0', shell=True, timeout=10)

    def test_logs_warning_on_command_failure(self, caplog):
        zk = MagicMock()
        tracker = TimingTracker(zk, 'false %s %s')
        with patch('src.timings.subprocess.run', side_effect=Exception('boom')):
            with caplog.at_level(logging.INFO):
                tracker._log_timing('switchover', 1.0)
        # Main log entry still present.
        assert any(
            'Timing switchover: 1.000 seconds' in rec.getMessage()
            for rec in caplog.records
        )
        # Warning about the failed command present.
        assert any(
            rec.levelno == logging.WARNING
            and 'Failed to execute log_timing command' in rec.getMessage()
            for rec in caplog.records
        )
