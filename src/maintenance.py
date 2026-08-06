# encoding: utf-8
"""
Maintenance handler module.

Encapsulates maintenance-mode logic: reading/writing ZK maintenance state,
switching replication to async on maintenance enter, and tracking the
in-maintenance flag. Moved from main.py (step 12c, ADR-0004).
"""
import logging

from configparser import RawConfigParser
from dataclasses import dataclass

from .log_formatters import log_event
from .pg import Postgres
from .replication_manager import ReplicationManager
from .zk import Zookeeper


@dataclass
class MaintenanceHandlerConfig:
    """Configuration for MaintenanceHandler."""
    stream_from: str | None
    change_replication_type: bool
    sync_replication_in_maintenance: bool


class MaintenanceHandler:
    """Handle maintenance-mode enter/exit and replication switching."""

    def __init__(
        self,
        zk: Zookeeper,
        db: Postgres,
        config: MaintenanceHandlerConfig,
        replication_manager: ReplicationManager,
    ):
        self._zk = zk
        self._db = db
        self._config = config
        self._replication_manager = replication_manager
        self.is_in_maintenance = False

    def update_status(self, db_state: dict, zk_state: dict, is_single_node: bool | None) -> None:
        """Read ZK maintenance flag and update local state + ZK bookkeeping."""
        maintenance_status = self._zk.get_maintenance_status()  # can be None, 'enable', 'disable' or '' - race between two nodes on maintenance disable
        if maintenance_status == 'enable':
            # maintenance node exists with 'enable' value, we are in maintenance now
            self.is_in_maintenance = True

            is_non_ha = self._config.stream_from is not None
            if is_non_ha:
                logging.debug('We are non-ha replica, skipping any maintenance-related changes in ZK')
                return

            role = db_state.get('role')
            db_alive = db_state.get('alive', False)
            db_timeline = db_state.get('timeline')
            zk_timeline = zk_state.get(self._zk.TIMELINE_INFO_PATH)
            if (
                role == 'primary'
                and db_alive
                and zk_timeline is not None
                and (db_timeline is None or zk_timeline > db_timeline)
            ):
                logging.warning(
                    'Timeline mismatch detected: zk_timeline=%s, db_timeline=%s. Stopping pooler and archiving.',
                    zk_timeline, db_state.get('timeline'),
                )
                self._db.pgpooler('stop')
                self._db.stop_archiving_wal()
                return
            if role == 'primary' and self._update_replication_on_maintenance_enter() and not is_single_node:
                return
            # Write current ts to zk on maintenance enabled, it's be dropped on disable
            maintenance_ts = self._zk.get_maintenance_ts()
            if maintenance_ts is None:
                self._zk.write_maintenance_ts()
            # Write current primary to zk on maintenance enabled, it's be dropped on disable
            current_primary = self._zk.get_maintenance_primary()
            primary_fqdn = db_state.get('primary_fqdn')
            if current_primary is None and primary_fqdn is not None:
                self._zk.write_maintenance_primary(primary_fqdn)
        elif maintenance_status == 'disable' or maintenance_status == '':
            # maintenance node exists with 'disable' value, we are not in maintenance now
            # and should delete this node. We delete it recursively, we don't won't to wait
            # all cluster members to delete each own node, because some of them may be
            # already dead and we can wait it infinitely. Maybe we should wait each member
            # with timeout and then delete recursively (TODO).
            if self.is_in_maintenance:
                log_event('MAINTENANCE ENDED', level='warning')
            self.is_in_maintenance = False
            if self._config.stream_from is None:
                self._zk.delete_maintenance()
                log_action = 'deleting maintenance node'
            else:
                log_action = 'not touching maintenance node as we are non-ha replica'
            logging.debug('Maintenance mode disabled, %s', log_action)
        elif maintenance_status is None:
            # maintenance node doesn't exists, we are not in maintenance mode
            self.is_in_maintenance = False
        else:
            logging.error('ALARM: unexpected maintenance status, %s', maintenance_status)

    def _update_replication_on_maintenance_enter(self) -> bool:
        if not self._config.change_replication_type:
            # Replication type change is restricted, we do nothing here
            return True
        if self._config.sync_replication_in_maintenance:
            # It is allowed to have sync replication in maintenance here
            return True
        current_replication = self._db.get_replication_state()
        if current_replication[0] == 'async':
            # Ok, it is already async
            return True
        return self._replication_manager.change_replication_to_async()


def build_maintenance_handler_config(config: RawConfigParser) -> MaintenanceHandlerConfig:
    """Parse INI sections for MaintenanceHandler (ADR-0004)."""
    return MaintenanceHandlerConfig(
        stream_from=config.get('global', 'stream_from', fallback=None),
        change_replication_type=config.getboolean('primary', 'change_replication_type'),
        sync_replication_in_maintenance=config.getboolean('primary', 'sync_replication_in_maintenance'),
    )


def create_maintenance_handler(
    config: RawConfigParser,
    db: Postgres,
    zk: Zookeeper,
    replication_manager: ReplicationManager,
) -> MaintenanceHandler:
    """Create MaintenanceHandler with injected dependencies (ADR-0004)."""
    return MaintenanceHandler(
        zk=zk,
        db=db,
        config=build_maintenance_handler_config(config),
        replication_manager=replication_manager,
    )
