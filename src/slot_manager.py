# encoding: utf-8
"""
Replication slot manager module.

Encapsulates all logic for creating, dropping and synchronizing
PostgreSQL physical replication slots based on ZooKeeper state.
"""
import logging
import os

from configparser import RawConfigParser
from dataclasses import dataclass

from . import helpers
from .exceptions import PostgresConnectionError
from .pg import Postgres
from .zk import Zookeeper, ZookeeperException


@dataclass
class ReplicationSlotManagerConfig:
    """Configuration for ReplicationSlotManager."""
    replication_slots_polling: bool
    use_replication_slots: bool
    drop_slot_countdown: int


class ReplicationSlotManager:
    """
    Manage PostgreSQL replication slots lifecycle.
    """

    def __init__(self, db: Postgres, zk: Zookeeper, config: ReplicationSlotManagerConfig):
        self._db = db
        self._zk = zk
        self._config = config
        self._drop_countdown: dict[str, int] = {}

    def handle_slots(self) -> None:
        """
        Synchronize replication slots with ZK state.
        Called from primary_iter, replica_iter and non_ha_replica_iter.
        """
        if not self._config.replication_slots_polling:
            return

        my_hostname = helpers.get_hostname()
        try:
            slot_lock_holders = set(
                self._zk.get_lock_contenders(
                    os.path.join(self._zk.HOST_REPLICATION_SOURCES, my_hostname),
                    read_lock=True,
                    catch_except=False,
                )
            )
        except ZookeeperException as e:
            logging.warning(
                'Could not get slot lock holders. %s '
                'Can not handle replication slots. We will skip it this time', e, exc_info=True
            )
            return

        all_hosts = self._zk.get_members()
        if not all_hosts:
            logging.warning(
                'Could not get all hosts list from ZK.'
                'Can not handle replication slots. We will skip it this time'
            )
            return

        non_holders_hosts = self._compute_non_holders(all_hosts, slot_lock_holders)

        # Do not drop our own slot
        if my_hostname in non_holders_hosts:
            non_holders_hosts.remove(my_hostname)

        slot_names_to_create = [helpers.app_name_from_fqdn(fqdn) for fqdn in slot_lock_holders]
        slot_names_to_drop = [helpers.app_name_from_fqdn(fqdn) for fqdn in non_holders_hosts]

        try:
            self._sync_slots(slot_names_to_create, slot_names_to_drop)
        except PostgresConnectionError:
            # Best-Effort (ADR-0002 §3): skip this iteration on DB loss.
            logging.warning('DB connection lost during replication slot sync. Skipping this time', exc_info=True)
            return

    def _sync_slots(self, to_create: list[str], to_drop: list[str]) -> None:
        """Create missing and drop stale replication slots in a single pass.

        Fetches the current slot list once and reuses it for both operations.
        Pure primitive: propagates PostgresConnectionError so handle_slots can
        classify it as Best-Effort (ADR-0002 §3).

        Raises:
            PostgresConnectionError: from get_replication_slots,
                _create_replication_slot or _drop_replication_slot.
        """
        current = self._db.get_replication_slots()
        self._create_missing_slots(to_create, current=current)

        for slot in to_drop:
            if slot not in current:
                continue
            self._db._drop_replication_slot(slot)

    def _create_missing_slots(self, slots: list[str], current: list[str]) -> None:
        """Create replication slots that are missing from `current`.

        Pure primitive (ADR-0001/ADR-0002): propagates PostgresConnectionError
        so each caller can classify it (Best-Effort §3 or critical §2).

        Raises:
            PostgresConnectionError: if the DB connection is lost while creating a slot.
        """
        if not slots:
            return
        logging.debug('Actual replication slots: %s', current)
        for slot in slots:
            if slot in current:
                continue
            self._db._create_replication_slot(slot)

    def _compute_non_holders(self, all_hosts: list[str], slot_lock_holders: set[str]) -> list[str]:
        """
        Update countdown for each host and return hosts whose countdown expired.
        """
        countdown_default = self._config.drop_slot_countdown
        non_holders_hosts: list[str] = []

        for host in all_hosts:
            if host in slot_lock_holders:
                self._drop_countdown[host] = countdown_default
            else:
                if host not in self._drop_countdown:
                    self._drop_countdown[host] = countdown_default
                self._drop_countdown[host] -= 1
                if self._drop_countdown[host] < 0:
                    non_holders_hosts.append(host)

        return non_holders_hosts

    def create_slots_for_hosts(self, hosts: list[str]) -> bool:
        """Create replication slots for the given list of host FQDNs.

        Used during failover and switchover (a §2 critical section per
        ADR-0002). Pure primitive: propagates PostgresConnectionError so the
        caller (_do_failover) can take a safe compensating action (release the
        leader lock). Never swallows a DB error and returns a safe default.

        Returns:
            True if slots were created (or creation was a no-op).

        Raises:
            PostgresConnectionError: propagated from get_replication_slots /
                _create_replication_slot if the DB connection is lost.
        """
        if not self._config.use_replication_slots:
            return True
        if not hosts:
            return True
        slot_names = [helpers.app_name_from_fqdn(fqdn) for fqdn in hosts]
        current = self._db.get_replication_slots()
        self._create_missing_slots(slot_names, current)
        return True

    def reset_on_promote(self) -> None:
        """
        Reset the drop countdown after a promote.
        """
        self._drop_countdown = {}


def create_replication_slot_manager(
    config: RawConfigParser, db: Postgres, zk: Zookeeper
) -> ReplicationSlotManager:
    """Factory: create ReplicationSlotManager from config object."""
    slot_config = ReplicationSlotManagerConfig(
        replication_slots_polling=config.getboolean('global', 'replication_slots_polling'),
        use_replication_slots=config.getboolean('global', 'use_replication_slots'),
        drop_slot_countdown=config.getint('global', 'drop_slot_countdown'),
    )
    return ReplicationSlotManager(db, zk, slot_config)
