import logging
import time
from configparser import RawConfigParser
from dataclasses import dataclass
from typing import Iterable

from . import helpers
from .pg import Postgres
from .list_removal_strategy import DelayedListRemovalStrategy
from .ssn_manager import SsnManager
from .types import DurabilityConfig, ReplicaInfos
from .zk import Zookeeper


@dataclass
class ReplicationManagerConfig:
    priority: int
    primary_unavailability_timeout: float
    quorum_removal_delay: float


class ReplicationManager:
    def __init__(self, config: ReplicationManagerConfig, db: Postgres, _zk: Zookeeper):
        self._config = config
        self._db = db
        self._zk = _zk
        self._ssn = SsnManager(db, _zk)
        self._zk_fail_timestamp: float | None = None
        self._removal_strategy = DelayedListRemovalStrategy(
            self._config.quorum_removal_delay
        )
        if self._config.quorum_removal_delay > 0:
            logging.info('Removing unavailable durability members after %ss', self._config.quorum_removal_delay)
        else:
            logging.info('Removing unavailable durability members immediately')
        self._previous_durability_members: list[str] | None = None

    def drop_zk_fail_timestamp(self):
        """
        Reset fail timestamp flag
        """
        self._zk_fail_timestamp = None

    def init_zk(self):
        if not self._zk.ensure_durability_path():
            logging.error("Can't create durability path in ZK")
            return False
        return True

    def should_close(self) -> bool:
        """
        Check if we are safe to stay open on zk conn loss.

        Raises:
            PostgresConnectionError: propagates to run_iteration() (ADR-0002 §1).
        """
        if self._zk_fail_timestamp is None:
            self._zk_fail_timestamp = time.time()
        info = self._db.get_replics_info(self._db.role)
        should_wait = False
        for replica in info:
            if int(replica['reply_time_ms']) / 1000 < self._zk_fail_timestamp:
                should_wait = True
        if should_wait:
            time.sleep(self._config.primary_unavailability_timeout)
            info = self._db.get_replics_info(self._db.role)

        connected = sum([1 for x in info if x['sync_state'] == 'quorum' and int(x['reply_time_ms']) / 1000 > self._zk_fail_timestamp])
        repl_state = self._db.get_replication_state()
        if repl_state[0] == 'async':
            return False
        elif repl_state[0] == 'sync':
            expected = int(repl_state[1].split('(')[0].split(' ')[1])
            logging.info(
                'Probably connect to ZK lost, check the need to close. '
                'Expected replicas num: %s, connected replicas(quorum) num %s',
                expected,
                connected,
            )
            return connected < expected
        else:
            raise RuntimeError(f'Unexpected replication state: {repl_state}')

    def desired_durability(
        self,
        db_state: dict,
        configured_ha_replicas: set[str],
        alive_hosts: Iterable[str],
        durability: DurabilityConfig | None,
    ) -> DurabilityConfig | None:
        """Calculate the ordinary primary policy without changing PostgreSQL."""
        alive_ha_replicas = configured_ha_replicas & set(alive_hosts)
        current = db_state.get('replication_state')
        if current is None:
            current = self._db.get_replication_state()
        logging.info('Current replication type is %s.', current)
        repl_state = current[0]
        my_hostname = helpers.get_hostname()

        current_members = list(durability.members) if durability is not None else []
        current_replicas = [host for host in current_members if host != my_hostname]

        streaming_app_names = {
            replica['application_name']
            for replica in db_state.get('replics_info') or []
            if replica.get('state') == 'streaming'
        }
        streaming_ha_replicas = {
            host for host in alive_ha_replicas
            if helpers.app_name_from_fqdn(host) in streaming_app_names
        }
        # Existing durability members are removed only when their own daemon
        # disappears from ZK. A new member must additionally be streaming.
        available_replicas = (
            (set(current_replicas) & alive_ha_replicas)
            | streaming_ha_replicas
        )

        # Log stable durability changes observed from ZK between iterations.
        if self._previous_durability_members is not None and set(current_members) != set(self._previous_durability_members):
            added = set(current_members) - set(self._previous_durability_members)
            removed = set(self._previous_durability_members) - set(current_members)
            logging.info(
                'DURABILITY-MEMBERS-CHANGED in ZK: from %s to %s (added: %s, removed: %s)',
                sorted(self._previous_durability_members),
                sorted(current_members),
                sorted(added) if added else 'none',
                sorted(removed) if removed else 'none'
            )
        self._previous_durability_members = current_members.copy()

        replicas_final = self._removal_strategy.get_hosts_to_keep(
            current_replicas, list(available_replicas),
        )
        new_durability = DurabilityConfig.build([my_hostname, *replicas_final])

        needed = 'sync' if replicas_final else 'async'
        logging.info('Needed replication type is %s.', needed)
        if needed != repl_state:
            logging.info('We should change replication from %s to %s', repl_state, needed)

        # Keep this marker easy to find in logs.
        if new_durability != durability:
            logging.info(
                'DURABILITY-MEMBERS-CHANGED: members are changing from %s to %s',
                sorted(current_members),
                list(new_durability.members),
            )

        return new_durability

    def set_ssn_before_promote(self, durability: DurabilityConfig | None) -> bool:
        """
        Set synchronous_standby_names on this replica before it is promoted
        to primary. This prevents a data-loss window between promote and the
        first regular iteration that would normally set SSN.
        """
        if durability is None:
            logging.warning('No durability config found before promote, SSN will be set to async')
            standby_names = ''
        else:
            standby_names = self._ssn.calculate_ssn_for_host(durability, helpers.get_hostname())
        display = standby_names if standby_names else '(async)'
        return self._ssn.apply_and_persist(
            standby_names,
            f'Setting SSN before promote: {display}.',
            'Set SSN before promote.',
        )

    def set_mandatory_sync_replica(
        self,
        durability: DurabilityConfig,
        mandatory: str,
    ) -> bool:
        """Preserve the stable quorum while requiring one replica."""
        return self._ssn.apply_ssn_with_mandatory(
            durability, helpers.get_hostname(), mandatory,
        )

    def change_replication_to_durability_config(self, durability: DurabilityConfig) -> bool:
        return self._ssn.reconcile_durability(durability, helpers.get_hostname())

    def apply_stable_durability_config(self, durability: DurabilityConfig) -> bool:
        return self._ssn.apply_stable_config(durability, helpers.get_hostname())

    def ssn_for_durability(self, durability: DurabilityConfig, primary: str) -> str:
        return self._ssn.calculate_ssn_for_host(durability, primary)

    def resume_durability_transition(self) -> bool:
        return self._ssn.resume_durability_transition(helpers.get_hostname())

    def durability_for_failover_winner(self, primary: str) -> DurabilityConfig | None:
        return self._ssn.durability_for_failover_winner(primary)

    def discard_transition_after_failover(self, primary: str) -> bool:
        return self._ssn.discard_transition_after_failover(primary)

    def get_switchover_candidate(self, replica_infos: ReplicaInfos) -> str | None:
        """Select the highest-priority live streaming durability member."""
        durability = self._zk.get_durability_config()
        if durability is None:
            return None
        hosts_by_app = {
            helpers.app_name_from_fqdn(host): host
            for host in durability.members
        }
        candidates: list[tuple[int, int, str]] = []
        for info in replica_infos:
            if info.get('state') != 'streaming':
                continue
            host = hosts_by_app.get(str(info.get('application_name')))
            if host is None or not self._zk.is_host_alive(
                host, timeout=1, catch_except=False,
            ):
                continue
            priority = self._zk.get_host_prio(host, catch_except=False)
            if priority is None:
                logging.warning('No priority is available for switchover candidate %s', host)
                continue
            try:
                write_diff = int(info.get('write_location_diff', 0))
                candidates.append((int(priority), write_diff, host))
            except (TypeError, ValueError):
                logging.warning('Invalid switchover candidate state for %s', host)
        if not candidates:
            return None
        return min(candidates, key=lambda item: (-item[0], item[1], item[2]))[2]


def build_replication_manager_config(config: RawConfigParser) -> ReplicationManagerConfig:
    """
    Build ReplicationManagerConfig from RawConfigParser with validation.
    
    Args:
        config: RawConfigParser instance with pgconsul configuration
        
    Returns:
        ReplicationManagerConfig instance
    """
    quorum_removal_delay = config.getfloat('primary', 'quorum_removal_delay')

    # Validate and adjust quorum_removal_delay
    if quorum_removal_delay < 0:
        logging.warning(
            'quorum_removal_delay is negative (%s), setting to 0 (immediate removal)',
            quorum_removal_delay
        )
        quorum_removal_delay = 0
    elif quorum_removal_delay > 120:
        logging.warning(
            'quorum_removal_delay is set to %s seconds, which is quite large. '
            'This may lead to prolonged unavailability in case of replica failures. '
            'Recommended range: 0-60 seconds. Setting to 120 seconds.',
            quorum_removal_delay
        )
        quorum_removal_delay = 120

    return ReplicationManagerConfig(
        priority=config.getint('global', 'priority'),
        primary_unavailability_timeout=config.getfloat('replica', 'primary_unavailability_timeout'),
        quorum_removal_delay=quorum_removal_delay,
    )


def create_replication_manager(config: RawConfigParser, db, zk):
    """
    Create ReplicationManager instance based on configuration.

    Args:
        config: RawConfigParser instance with pgconsul configuration
        db: Postgres instance
        zk: Zookeeper instance
        
    Returns:
        ReplicationManager instance
    """
    replication_config = build_replication_manager_config(config)
    
    return ReplicationManager(
        replication_config,
        db,
        zk,
    )
