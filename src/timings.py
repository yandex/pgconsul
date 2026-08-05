# encoding: utf-8
"""
Timing tracker module.

Encapsulates downtime/failover/switchover timing measurements that are
persisted in ZooKeeper. Extracted mechanically from ``pgconsul`` (main.py)
without changing observable behaviour.
"""
import logging
import subprocess
import time

from .zk import Zookeeper


class TimingTracker:
    """
    Track named timings (downtime, failover, switchover) via ZooKeeper.

    Pure delegating wrapper: start/stop/clear/get_start map 1:1 to the
    former ``pgconsul._*_timing`` methods.
    """

    def __init__(self, zk: Zookeeper, log_timing_command: str | None):
        self._zk = zk
        self._log_timing_command = log_timing_command

    def get_start(self, name: str) -> float | None:
        return self._zk.get_timing(name)

    def start(self, name: str, ts: float | None = None) -> None:
        if ts is None:
            ts = time.time()
        self._zk.write_timing(name, ts)

    def clear(self, name: str) -> None:
        self._zk.delete_timing(name)

    def stop(self, name: str, track_as: str | None = None) -> None:
        start = self.get_start(name)
        end = time.time()
        if start is None:
            return
        self.clear(name)
        self._log_timing(track_as or name, end - start)

    def _log_timing(self, name: str, value: float) -> None:
        cmd = self._log_timing_command
        if not cmd:
            return
        try:
            # Format the command with name and value
            cmd = cmd % (name, value)
            # Execute the external program
            subprocess.run(cmd, shell=True, timeout=10)
        except Exception as e:
            logging.warning('Failed to execute log_timing command: %s', str(e))
