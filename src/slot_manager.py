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
from .zk import Zookeeper


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
        except Exception as e:
            logging.warning(
                'Could not get slot lock holders. %s'
                'Can not handle replication slots. We will skip it this time', e
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
            create_ok, drop_ok = self._sync_slots(slot_names_to_create, slot_names_to_drop)
        except PostgresConnectionError:
            # Slot sync is best-effort: a lost DB connection is not fatal here,
            # the next iteration will retry. exc_info preserves traceback for diagnostics.
            logging.warning('Could not get replication slots from DB. Skipping slot handling this time', exc_info=True)
            return

        if not create_ok:
            logging.warning('Could not create replication slots. %s', slot_names_to_create)
        if not drop_ok:
            logging.warning('Could not drop replication slots. %s', slot_names_to_drop)

    def _sync_slots(self, to_create: list[str], to_drop: list[str]) -> tuple[bool, bool]:
        """Create and drop replication slots in a single pass.

        Fetches the current slot list once and reuses it for both operations.
        Returns (create_ok, drop_ok).

        Raises:
            PostgresConnectionError: from get_replication_slots; caught by handle_slots.
        """
        current = self._db.get_replication_slots()
        create_ok = self._create_missing(to_create, current=current, fail_fast=False)

        drop_ok = True
        for slot in to_drop:
            if slot not in current:
                continue
            try:
                self._db._drop_replication_slot(slot)
            except PostgresConnectionError:
                logging.warning('Failed to drop slot %s', slot, exc_info=True)
                drop_ok = False

        return create_ok, drop_ok

    def _create_missing(self, slots: list[str], current: list[str], fail_fast: bool) -> bool:
        """Create missing replication slots.

        When fail_fast is True, returns False on the first DB failure
        (failover/switchover path). Otherwise accumulates the result
        (periodic synchronization).
        """
        if not slots:
            return True
        logging.debug('Actual replication slots: %s', current)
        ok = True
        for slot in slots:
            if slot in current:
                continue
            try:
                self._db._create_replication_slot(slot)
            except PostgresConnectionError:
                logging.warning('Failed to create slot %s', slot, exc_info=True)
                if fail_fast:
                    return False
                ok = False

        return ok

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

        Used during failover and switchover.

        Raises:
            PostgresConnectionError: propagated from get_replication_slots /
                _create_replication_slot if the DB connection is lost.
                Callers (failover/switchover) must handle or propagate it.
        """
        if not self._config.use_replication_slots:
            return True
        if not hosts:
            return True
        slot_names = [helpers.app_name_from_fqdn(fqdn) for fqdn in hosts]
        current = self._db.get_replication_slots()
        if not self._create_missing(slot_names, current, fail_fast=True):
            logging.error('Could not create replication slots. Releasing the lock in ZK.')
            return False
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
