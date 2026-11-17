# encoding: utf-8
"""CLI lock contenders use a process-specific identity."""

import os

from src import helpers


def test_cli_lock_contender_name_is_distinct_from_daemon():
    name = helpers.get_cli_lock_contender_name()

    assert name != helpers.get_hostname()
    assert name.endswith(f'_{os.getpid()}')
