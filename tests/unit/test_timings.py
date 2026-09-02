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
    """get_start delegates to operation-scoped ZK timing."""

    def test_returns_value_from_zk(self):
        zk = MagicMock()
        zk.get_operation_timing.return_value = 1234.5
        tracker = TimingTracker(zk, None)
        assert tracker.get_start('failover', 'op-1') == 1234.5
        zk.get_operation_timing.assert_called_once_with('failover', 'op-1')

    def test_returns_none_when_zk_returns_none(self):
        zk = MagicMock()
        zk.get_operation_timing.return_value = None
        tracker = TimingTracker(zk, None)
        assert tracker.get_start('downtime', 'op-1') is None

    def test_returns_none_without_operation_id(self):
        zk = MagicMock()
        tracker = TimingTracker(zk, None)
        assert tracker.get_start('downtime', None) is None
        zk.get_operation_timing.assert_not_called()


class TestTimingTrackerStart:
    """start writes current timestamp (or provided ts) to ZK."""

    def test_uses_current_time_when_ts_not_provided(self):
        zk = MagicMock()
        zk.start_operation_timing.return_value = True
        tracker = TimingTracker(zk, None)
        with patch('src.timings.time.time', return_value=9999.0):
            assert tracker.start('failover', 'op-1') is True
        zk.start_operation_timing.assert_called_once_with('failover', 'op-1', 9999.0)

    def test_uses_provided_ts(self):
        zk = MagicMock()
        zk.start_operation_timing.return_value = True
        tracker = TimingTracker(zk, None)
        assert tracker.start('downtime', 'op-1', ts=12345.0) is True
        zk.start_operation_timing.assert_called_once_with('downtime', 'op-1', 12345.0)


class TestTimingTrackerClear:
    """clear delegates to zk.delete_timing."""

    def test_calls_zk_delete_timing(self):
        zk = MagicMock()
        zk.delete_operation_timing.return_value = True
        tracker = TimingTracker(zk, None)
        assert tracker.clear('switchover', 'op-1') is True
        zk.delete_operation_timing.assert_called_once_with('switchover', 'op-1')


class TestTimingTrackerStop:
    """stop reads start, clears ZK node, and logs the elapsed duration."""

    def test_logs_and_clears_when_start_exists(self, caplog):
        zk = MagicMock()
        zk.get_operation_timing.return_value = 100.0
        zk.delete_operation_timing.return_value = True
        tracker = TimingTracker(zk, None)
        with patch('src.timings.time.time', return_value=105.5):
            with caplog.at_level(logging.INFO):
                assert tracker.stop('failover', 'op-1') is True
        zk.delete_operation_timing.assert_called_once_with('failover', 'op-1')
        assert any(
            'Timing failover: 5.500 seconds' in rec.getMessage()
            for rec in caplog.records
        )

    def test_uses_track_as_name_in_log(self, caplog):
        zk = MagicMock()
        zk.get_operation_timing.return_value = 200.0
        zk.delete_operation_timing.return_value = True
        tracker = TimingTracker(zk, None)
        with patch('src.timings.time.time', return_value=203.0):
            with caplog.at_level(logging.INFO):
                assert tracker.stop(
                    'switchover', 'op-1', track_as='switchover_failure',
                ) is True
        zk.delete_operation_timing.assert_called_once_with('switchover', 'op-1')
        assert any(
            'Timing switchover_failure: 3.000 seconds' in rec.getMessage()
            for rec in caplog.records
        )

    def test_does_nothing_when_start_is_none(self, caplog):
        zk = MagicMock()
        zk.get_operation_timing.return_value = None
        tracker = TimingTracker(zk, None)
        with caplog.at_level(logging.INFO):
            assert tracker.stop('downtime', 'op-1') is True
        zk.delete_operation_timing.assert_not_called()
        assert not any('Timing' in rec.getMessage() for rec in caplog.records)

    def test_does_not_log_when_scoped_delete_loses_race(self, caplog):
        zk = MagicMock()
        zk.get_operation_timing.return_value = 100.0
        zk.delete_operation_timing.return_value = False
        tracker = TimingTracker(zk, None)

        with caplog.at_level(logging.INFO):
            assert tracker.stop('failover', 'op-1') is False

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
