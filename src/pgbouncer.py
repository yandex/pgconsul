"""
Pgbouncer handler for managing pgbouncer configuration during failover.
"""
from pgconsul import helpers


class PgbouncerHandler:
    """
    Handles pgbouncer configuration updates during promote and recovery_conf operations.
    """

    # Editing pgbouncer.ini is only for test topology
    def __init__(self, enabled=False):
        self._enabled = enabled

    def before_populate_recovery_conf(self, primary_host):
        """
        Update pgbouncer.ini to point to the new primary host.
        Called before generating recovery.conf.
        """
        if self._enabled:
            cmd = 'sudo sed -i /etc/pgbouncer/pgbouncer.ini -e "/^* = /s/host=.*$/host=' + primary_host + ' port=6432/"'
            helpers.subprocess_popen(cmd)

    def before_promote(self):
        """
        Update pgbouncer.ini to point to localhost (self) before promote.
        """
        if self._enabled:
            cmd = 'sudo sed -i /etc/pgbouncer/pgbouncer.ini -e "/^* = /s/host=.*$/host=localhost/"'
            helpers.subprocess_popen(cmd)
