# encoding: utf-8
"""
Maintenance handler module.

Encapsulates maintenance-mode state and ZK bookkeeping. Durability changes
are planned centrally by DurabilityMachine.
"""
import logging

from configparser import RawConfigParser
from dataclasses import dataclass

from .log_formatters import log_event
from .pg import Postgres
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
    ):
        self._zk = zk
        self._db = db
        self._config = config
        self._is_in_maintenance = False

    @property
    def is_in_maintenance(self) -> bool:
        """Read-only: whether this node is in maintenance mode."""
        return self._is_in_maintenance

    @property
    def wants_async_durability(self) -> bool:
        """Whether maintenance policy requests asynchronous replication."""
        return bool(
            self._config.stream_from is None
            and self._config.change_replication_type
            and not self._config.sync_replication_in_maintenance
        )

    def update_status(self, db_state: dict, zk_state: dict, is_single_node: bool | None) -> None:
        """Read ZK maintenance flag and update local state + ZK bookkeeping."""
        # can be None, 'enable', 'disable' or '' - race between two nodes on maintenance disable
        maintenance_status = self._zk.get_maintenance_status()
        if maintenance_status == 'enable':
            self._handle_maintenance_enable(db_state, zk_state, is_single_node)
        elif maintenance_status == 'disable' or maintenance_status == '':
            self._handle_maintenance_disable()
        elif maintenance_status is None:
            self._is_in_maintenance = False
        else:
            logging.error('ALARM: unexpected maintenance status, %s', maintenance_status)

    def _handle_maintenance_enable(self, db_state: dict, zk_state: dict, is_single_node: bool | None) -> None:
        """Process maintenance 'enable' state."""
        self._is_in_maintenance = True

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
        # Write ts and primary to ZK on maintenance enable (dropped on disable)
        if self._zk.get_maintenance_ts() is None:
            self._zk.write_maintenance_ts()
        current_primary = self._zk.get_maintenance_primary()
        primary_fqdn = db_state.get('primary_fqdn')
        if current_primary is None and primary_fqdn is not None:
            self._zk.write_maintenance_primary(primary_fqdn)

    def _handle_maintenance_disable(self) -> None:
        """Process maintenance 'disable' or '' (race between two nodes) state."""
        # Delete recursively — don't wait for all members, some may be dead (TODO: per-member timeout)
        if self._is_in_maintenance:
            log_event('MAINTENANCE ENDED', level='warning')
        self._is_in_maintenance = False
        if self._config.stream_from is None:
            self._zk.delete_maintenance()
            log_action = 'deleting maintenance node'
        else:
            log_action = 'not touching maintenance node as we are non-ha replica'
        logging.debug('Maintenance mode disabled, %s', log_action)

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
) -> MaintenanceHandler:
    """Create MaintenanceHandler with injected dependencies (ADR-0004)."""
    return MaintenanceHandler(
        zk=zk,
        db=db,
        config=build_maintenance_handler_config(config),
    )
