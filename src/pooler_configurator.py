"""
Pgbouncer handler for managing pgbouncer configuration during failover.
"""
import os

from . import helpers

PGBOUNCER_INI_PATH = '/etc/pgbouncer/pgbouncer.ini'


class PoolerConfigurator:
    """
    Manages pgbouncer.ini updates during failover.
    In production pgbouncer.ini is absent, so all methods are no-ops.
    Editing pgbouncer.ini is only for test topology where pgbouncer runs locally.
    """

    def __init__(self):
        self._enabled = os.path.exists(PGBOUNCER_INI_PATH)

    def before_populate_recovery_conf(self, primary_host):
        """
        Update pgbouncer.ini to point to the new primary host.
        Called before generating recovery.conf.
        """
        if self._enabled:
            cmd = (
                'sudo sed -i /etc/pgbouncer/pgbouncer.ini '
                f'-e "/^* = /s/host=.*$/host={primary_host} port=6432/"'
            )
            helpers.subprocess_popen(cmd)

    def before_promote(self):
        """
        Update pgbouncer.ini to point to localhost (self) before promote.
        """
        if self._enabled:
            cmd = (
                'sudo sed -i /etc/pgbouncer/pgbouncer.ini '
                '-e "/^* = /s/host=.*$/host=localhost/"'
            )
            helpers.subprocess_popen(cmd)
