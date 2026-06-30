# encoding: utf-8
import logging
import time


class PgConnGracePeriod:
    """
    Tracks consecutive PG connection-timeout failures.
    Decides whether to act immediately or wait out the grace period.
    """

    def __init__(self, grace_period: int) -> None:
        if grace_period < 0:
            logging.warning(
                'pg_conn_failure_grace_period=%.1f is negative, falling back to 0 '
                '(act immediately on first connection failure).',
                grace_period,
            )
            grace_period = 0
        self._grace_period = grace_period
        self._first_failure_ts: float | None = None

    def reset(self) -> None:
        """Call on every successful DB connection."""
        self._first_failure_ts = None

    def record_failure(self) -> None:
        """Call on PGConnectionTimeout. Keeps the timestamp of the *first* failure."""
        if self._first_failure_ts is None:
            self._first_failure_ts = time.time()

    def should_act(self, pg_running: bool) -> bool:
        """
        Returns True when it is time to force action (restart etc.).
        pg_running: whether systemctl reports the process as running.
        """
        if self._first_failure_ts is None:
            return True

        elapsed = time.time() - self._first_failure_ts

        if pg_running and elapsed < self._grace_period:
            logging.warning(
                'Connection to postgres failed (timeout), but systemctl reports it is RUNNING. '
                'Elapsed since first failure: %.1fs / grace period: %.1fs. Skipping.',
                elapsed,
                self._grace_period,
            )
            return False

        if pg_running:
            logging.error(
                'Connection to postgres failed for %.1fs (grace period: %.1fs), '
                'systemctl still reports RUNNING. Forcing action.',
                elapsed,
                self._grace_period,
            )
        else:
            logging.error(
                'Connection timeout and process status unknown or process not running '
                '(systemctl unavailable or reported not running). '
                'Restarting immediately (timeout threshold bypassed).'
            )
        return True
