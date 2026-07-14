# coding: utf8
"""
Describes exception classes used in pgconsul.
"""


class pgconsulException(Exception):
    """
    Generic pgconsul exception.
    """

    pass


class SwitchoverException(pgconsulException):
    """
    Exception for fatal errors during switchover.
    """

    pass


class FailoverException(pgconsulException):
    """
    Exception for fatal errors during operations on failover state.
    """

    pass


class PGIsShuttingDown(pgconsulException):
    """
    Postgres is shutting down
    """

    pass


class PGIsStartingUp(pgconsulException):
    """
    Postgres is starting up
    """

    pass


class ResetException(pgconsulException):
    """
    Exception for fatal errors during reset-all command
    """

    pass


class PGConnectionTimeout(pgconsulException):
    """
    Postgres connection attempt timed out.
    Raised by reconnect() to signal callers that the connection failed due to timeout.
    The restart policy (how many timeouts to tolerate before forcing a restart) is
    intentionally kept outside Postgres class — in PgConsul (main.py).
    """

    def __init__(self, timeout_count: int):
        super().__init__(f'PostgreSQL connection timed out (attempt {timeout_count})')
        self.timeout_count = timeout_count
