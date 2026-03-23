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
