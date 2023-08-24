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
