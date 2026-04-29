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

#: How long (seconds) to keep retrying the DB call in apply_and_persist.
_DEFAULT_APPLY_RETRY_TIMEOUT = 10.0


class SsnManager:
    """
    Encapsulates all SSN (synchronous_standby_names) logic:
    calculation, application to DB, persistence to ZK, and host-list assembly.
    """

    def __init__(
        self,
        db: Postgres,
        zk: Zookeeper,
        apply_retry_timeout: float = _DEFAULT_APPLY_RETRY_TIMEOUT,
    ):
        self._db = db
        self._zk = zk
        self._apply_retry_timeout = apply_retry_timeout

    # ------------------------------------------------------------------
    # Calculation
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Application + persistence (combined)
    # ------------------------------------------------------------------

    def apply_and_persist(self, standby_names: str, start_msg: str, success_msg: str) -> bool:
        """
        Apply a new SSN value to PostgreSQL and, on success, persist it to ZK.

        The DB call is retried with exponential backoff for up to
        ``self._apply_retry_timeout`` seconds.  The ZK write is retried
        internally by :meth:`Zookeeper.write_ssn_on_changes`.

        Logs 'ACTION. {start_msg}' before the first attempt and success_msg on
        success.  Returns True on success, False if the DB timeout is exhausted.
        """
        logging.info(f'ACTION. {start_msg}')
        retrying_db_call = helpers.get_exponentially_retrying(
            self._apply_retry_timeout,
            'apply_and_persist/db',
            None,
            helpers.return_none_on_false(self._db.change_replication_type),
        )
        if retrying_db_call(standby_names):
            logging.info(success_msg)
            if not self._zk.write_ssn_on_changes(standby_names):
                logging.warning('SSN applied to DB but failed to persist to ZK')
            return True

        logging.error('Failed to apply SSN %r after retries', standby_names)
        return False

    # ------------------------------------------------------------------
    # Host-list assembly (pure / static)
    # ------------------------------------------------------------------

    @staticmethod
    def build_replica_hosts_for_promote(
        known_replicas: Iterable[str] | None,
        extra_host: str | None = None,
    ) -> list[str]:
        """
        Build the replica host list to pass to set_ssn_before_promote().

        Collects all hosts that will replicate from the new master after promote:
        - known_replicas: hosts already streaming from the candidate (side replicas
          during switchover) or HA replicas known from ZK (during failover)
        - extra_host: an additional host to append, or None. During switchover this
          is the current primary (lock_holder), which will start streaming from the
          new master after the switchover completes.

        extra_host is only included when known_replicas is non-empty.  In a
        two-host cluster where the old master did not appear among replicas
        (known_replicas is empty), the host is likely down and cannot
        acknowledge transactions, so we accept reduced durability guarantees
        and return an empty list (→ async replication).

        Args:
            known_replicas: iterable of replica FQDNs, or None
            extra_host: optional extra FQDN to append to the list

        Duplicates are removed so that each host appears only once.
        """
        hosts: set[str] = set(known_replicas) if known_replicas else set()
        if extra_host and hosts:
            hosts.add(extra_host)
        return sorted(hosts)
