# encoding: utf-8
import logging
import time


class PgConnGracePeriod:
    """Track how long a running PostgreSQL process has been unreachable."""

    def __init__(self, grace_period: int) -> None:
        if grace_period < 0:
            logging.warning(
                'pg_conn_failure_grace_period=%d is negative, falling back to 0 '
                '(act immediately on first connection timeout).',
                grace_period,
            )
            grace_period = 0
        self._grace_period = grace_period
        self._first_failure_ts: float | None = None

    def reset(self) -> None:
        """Forget a timeout series after a successful connection."""
        self._first_failure_ts = None

    def record_failure(self) -> None:
        """Record the first timeout in the current uninterrupted series."""
        if self._first_failure_ts is None:
            self._first_failure_ts = time.time()

    def should_act(self, pg_running: bool) -> bool:
        """Return whether recovery may proceed for the current timeout series."""
        if self._first_failure_ts is None:
            return True

        elapsed = time.time() - self._first_failure_ts
        if pg_running and elapsed < self._grace_period:
            logging.warning(
                'Connection to PostgreSQL timed out, but the service reports it is running. '
                'Elapsed since first failure: %.1fs / grace period: %ds. Skipping.',
                elapsed,
                self._grace_period,
            )
            return False

        if pg_running:
            logging.error(
                'Connection to PostgreSQL timed out for %.1fs (grace period: %ds), '
                'and the service still reports it is running. Forcing action.',
                elapsed,
                self._grace_period,
            )
        else:
            logging.error(
                'PostgreSQL connection timed out and the process is not running or its status is unknown. '
                'Forcing action immediately.'
            )
        return True
