"""
SsnManager — manages the full lifecycle of synchronous_standby_names (SSN):
  - calculating the SSN string for quorum mode
  - applying it to PostgreSQL via ALTER SYSTEM SET
  - persisting it to ZooKeeper
  - building replica host lists for switchover and failover
"""
import logging
from typing import Iterable

from . import helpers
from .pg import Postgres
from .zk import Zookeeper


class SsnManager:
    """
    Encapsulates all SSN (synchronous_standby_names) logic:
    calculation, application to DB, persistence to ZK, and host-list assembly.
    """

    def __init__(
        self,
        db: Postgres,
        zk: Zookeeper,
    ):
        self._db = db
        self._zk = zk

    def calculate_quorum_ssn(self, replica_hosts: list[str]) -> str:
        """
        Calculate the synchronous_standby_names value for quorum mode.

        Returns 'ANY N(app1,app2,...)' where N = ceil(len/2),
        or '' (empty string, async) if replica_hosts is empty.

        Duplicate hosts are removed before calculation so that the quorum
        size and the participant list reflect unique replicas only.
        """
        unique_hosts = sorted(set(replica_hosts)) if replica_hosts else []
        if not unique_hosts:
            return ''
        quorum_size = (len(unique_hosts) + 1) // 2
        app_names = sorted(map(helpers.app_name_from_fqdn, unique_hosts))
        return f"ANY {quorum_size}({','.join(app_names)})"

    def apply_and_persist(self, standby_names: str, start_msg: str, success_msg: str) -> bool:
        """
        Apply a new SSN value to PostgreSQL and, on success, persist it to ZK.

        Logs 'ACTION. {start_msg}' before the attempt and success_msg on
        success.  Returns True on success, False on failure.

        Note: No retry mechanism - if the DB call fails, the next iteration
        will retry automatically. This avoids blocking the main pgconsul loop.
        """
        logging.info(f'ACTION. {start_msg}')
        
        if self._db.change_replication_type(standby_names):
            logging.info(success_msg)
            if not self._zk.write_ssn_on_changes(standby_names):
                logging.warning('SSN applied to DB but failed to persist to ZK')
            return True

        logging.error('Failed to apply SSN %r', standby_names)
        return False

    @staticmethod
    def build_replica_hosts_for_promote(
        ha_replicas: Iterable[str] | None,
        old_primary: str | None = None,
    ) -> list[str]:
        hosts: set[str] = set(ha_replicas) if ha_replicas else set()
        if old_primary and hosts:
            hosts.add(old_primary)
        return sorted(hosts)
