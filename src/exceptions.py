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


class ResetException(pgconsulException):
    """
    Exception for fatal errors during reset-all command
    """

    pass


class PostgresException(pgconsulException):
    """
    Base exception for all PostgreSQL-related errors.
    """

    pass


class PostgresConnectionError(PostgresException):
    """
    Raised when the connection to PostgreSQL is unavailable or interrupted.
    Distinguishes a missing DB connection from an empty query result.
    """

    pass


class PostgresQueryError(PostgresException):
    """
    Raised when a query executes but returns an unexpected or invalid result.
    """

    pass
