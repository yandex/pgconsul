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
    Track operation-scoped timings (downtime, failover, switchover) via ZK.

    A fixed node is reused for each timing name. The operation id prevents a
    delayed reader or cleanup from affecting another operation.
    """

    def __init__(self, zk: Zookeeper, log_timing_command: str | None):
        self._zk = zk
        self._log_timing_command = log_timing_command

    def get_start(self, name: str, operation_id: str | None) -> float | None:
        if operation_id is None:
            return None
        return self._zk.get_operation_timing(name, operation_id)

    def start(
        self,
        name: str,
        operation_id: str,
        ts: float | None = None,
    ) -> bool:
        if ts is None:
            ts = time.time()
        return self._zk.start_operation_timing(name, operation_id, ts)

    def clear(self, name: str, operation_id: str) -> bool:
        return self._zk.delete_operation_timing(name, operation_id)

    def stop(
        self,
        name: str,
        operation_id: str,
        track_as: str | None = None,
    ) -> bool:
        start = self.get_start(name, operation_id)
        end = time.time()
        if start is None:
            return True
        if not self.clear(name, operation_id):
            return False
        self._log_timing(track_as or name, end - start)
        return True

    def _log_timing(self, name: str, value: float) -> None:
        # Always log the timing to the main pgconsul log so downtime/failover/switchover
        # durations are visible even when no external log_timing command is configured.
        logging.info('Timing %s: %.3f seconds', name, value)

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
