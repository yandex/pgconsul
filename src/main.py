"""
Main module. Pgconsul class defined here.
"""
# encoding: utf-8

import atexit
import functools
import logging
import os
import sys
import time
from dataclasses import dataclass

from configparser import RawConfigParser

from . import helpers, sdnotify
from .log_formatters import format_db_state_for_log, format_zk_state_for_log, log_event
from .command_manager import create_command_manager
from .cluster_state import ClusterState
from .durability import DurabilityManager, ssn_for
from .failover import SafeFailover
from .helpers import IterationTimer, get_hostname, register_sigterm_handler, should_run
from .exceptions import PostgresConnectionError, PostgresConnectionTimeout
from .pg import Postgres, create_postgres
from .pg_conn_grace_period import PgConnGracePeriod
from .replication_manager import ReplicationManager, create_replication_manager
from .slot_manager import ReplicationSlotManager, create_replication_slot_manager
from .switchover import SafeSwitchover
from .types import DbState, ReplicaInfo, ReplicaInfos, ZkState
from .zk import Zookeeper, ZookeeperException, create_zk


@dataclass
class PgconsulConfig:
    """All config values consumed by the Pgconsul orchestrator (ADR-0004)."""
    # [global]
    welcome_message: str
    working_dir: str
    iteration_timeout: float
    quorum_commit: bool
    use_lwaldump: bool
    update_prio_in_zk: bool
    use_replication_slots: bool
    replication_slots_polling: bool
    priority: str
    stream_from: str | None
    autofailover: bool
    switchover_replica_turn_timeout: float
    switchover_rollback_timeout: float
    max_rewind_retries: int
    election_timeout: int
    do_consecutive_primary_switch: bool
    max_allowed_switchover_lag_ms: int
    # [replica]
    allow_potential_data_loss: bool
    close_detached_after: float
    start_pooler: bool
    recovery_timeout: float
    can_delayed: bool
    primary_switch_disable_archive_restore: bool
    primary_switch_checks: int
    primary_switch_restart: bool
    primary_unavailability_timeout: float
    walreceiver_disable_timeout: float
    min_failover_timeout: float
    # [primary]
    change_replication_type: bool
    sync_replication_in_maintenance: bool
    # [debug]
    promote_checkpoint_sql: str | None
    failure_name: str | None
    failure_count: int
    sleep_before_disable_walreceiver: float
    election_lsn_read_sleep: float
    election_loser_timeout: int
    pg_conn_failure_grace_period: int = 10
    debug_wal_barrier_wait_file: str | None = None
    quorum_removal_delay: float = 0.0
    archive_restore_disable_timeout: float = 5.0


@dataclass
class RecoveryChecks:
    primary_switch: int = 0
    rewind: int = 0


class Pgconsul:
    """
    pgconsul class
    """

    def __init__(
        self,
        config: PgconsulConfig,
        db: Postgres,
        zk: Zookeeper,
        replication_manager: ReplicationManager,
        slot_manager: ReplicationSlotManager,
    ):
        logging.info('Initializing main class.')
        self.config = config
        if config.welcome_message:
            logging.info(config.welcome_message)

        self.is_in_maintenance = False
        self._pg_conn_grace = PgConnGracePeriod(config.pg_conn_failure_grace_period)

        self.db = db
        self.zk = zk
        self.startup_checks()

        register_sigterm_handler()

        self.checks = RecoveryChecks()
        self._is_single_node: bool | None = False
        self.notifier = sdnotify.Notifier()
        self.last_zk_host_stat_write: float = 0
        self._replication_manager = replication_manager
        self._slot_manager = slot_manager
        self._durability = DurabilityManager(db, zk, getattr(config, 'quorum_removal_delay', 0.0))
        self._safe_failover = SafeFailover(db, zk, config)
        self._safe_switchover = SafeSwitchover(db, zk, config)

    def _rewind_flag_path(self):
        return os.path.join(self.config.working_dir, '.pgconsul_rewind_fail.flag')

    def is_rewind_flag_set(self):
        return os.path.exists(self._rewind_flag_path())

    def set_rewind_flag(self):
        with open(self._rewind_flag_path(), 'w') as fobj:
            fobj.write(str(time.time()))

    def startup_checks(self):
        """
        Perform some basic checks on startup
        """
        logging.info('Running startup checks')

        prev_state = self.db.get_prev_state()
        if prev_state:
            # Ok, it means that current start is not the first one.
            # In this case we should check that we are able to do pg_rewind.
            if not self.db.is_alive():
                self.db.pgdata = prev_state.pgdata
            if not self.db.is_ready_for_pg_rewind():
                sys.exit(1)

        if (
            self.config.quorum_commit
            and not self.config.use_lwaldump
            and not self.config.allow_potential_data_loss
        ):
            logging.error("Using quorum_commit allow only with use_lwaldump or with allow_potential_data_loss")
            sys.exit(1)

        if (
            self.db.is_alive()
            and not self.db.check_extension_installed('lwaldump')
            and self.config.use_lwaldump
        ):
            logging.error("lwaldump is not installed")
            sys.exit(1)

        if self.db.is_alive() and not self.db.ensure_archive_mode():
            logging.error("archive mode is not enabled on instance - pgconsul support only archive mode yet ")
            sys.exit(1)

        logging.info('Startup checks passed')

    def check_zk_members_at_startup(self):
        members = self.zk.get_members(catch_except=False)
        tli = self.db.get_timeline()
        if not members and tli > 1:
            logging.error(
                'ZK "%s" empty but timeline indicates operating cluster (%i > 1)',
                self.zk.MEMBERS_PATH,
                tli,
            )
            self.db.pgpooler('stop')
            sys.exit(1)

    # pylint: disable=W0212
    def stop(self, *_):
        """
        Stop iterations
        """
        logging.info('Stopping')
        atexit._run_exitfuncs()
        os._exit(0)

    def _init_zk(self, my_prio):
        if not self._replication_manager.init_zk():
            return False

        members = self.zk.get_members() or []
        if self.config.update_prio_in_zk or helpers.get_hostname() not in members:
            if not self.zk.write_host_prio(my_prio):
                return False

        return True

    def start(self):
        """
        Start iterations
        """
        if not self.config.use_replication_slots and self.config.replication_slots_polling:
            logging.warning('Force disable replication_slots_polling because use_replication_slots is disabled.')
            self.config.replication_slots_polling = False

        my_prio = self.config.priority
        self.notifier.ready()
        zk_members_checked = False
        while should_run():
            if not self.zk.is_alive():
                self.zk.re_init()
                continue
            try:
                if not zk_members_checked:
                    self.check_zk_members_at_startup()
                    zk_members_checked = True
                if self._init_zk(my_prio):
                    break
            except ZookeeperException:
                logging.exception('Failed to init ZK')
            else:
                logging.error('Failed to init ZK')
            self.zk.re_init()
            time.sleep(self.config.iteration_timeout)

        while should_run():
            try:
                self.run_iteration(my_prio)
            except PostgresConnectionError as e:
                # Expected transient DB error (ADR-0002 §1): restart iteration.
                logging.warning('PostgreSQL error during iteration, will retry: %s', e)
            except Exception:
                logging.exception('Unexpected error during run_iteration')
        self.stop()

    def update_maintenance_status(self, db_state: DbState, zk_state: ZkState):
        maintenance_status = self.zk.get_maintenance_status()  # can be None, 'enable', 'disable' or '' - if we catch race between two nodes on maintenance disable
        if maintenance_status == 'enable':
            # maintenance node exists with 'enable' value, we are in maintenance now
            self.is_in_maintenance = True

            is_non_ha = self.config.stream_from is not None
            if is_non_ha:
                logging.debug('We are non-ha replica, skipping any maintenance-related changes in ZK')
                return

            role = db_state.role
            db_alive = db_state.alive
            cluster_state = self.zk.get_cluster_state()
            db_timeline = self.db.get_primary_timeline() if role == 'primary' and db_alive and cluster_state is not None else db_state.timeline
            zk_timeline = zk_state.timeline
            if (
                role == 'primary'
                and db_alive
                and zk_timeline is not None
                and (db_timeline is None or zk_timeline > db_timeline)
            ):
                logging.warning(
                    'Timeline mismatch detected: zk_timeline=%s, db_timeline=%s. Stopping pooler and archiving.',
                    zk_timeline, db_state.timeline,
                )
                self.db.pgpooler('stop')
                self.db.stop_archiving_wal()
                return
            if role == 'primary' and cluster_state is None and self._update_replication_on_maintenance_enter() and not self._is_single_node:
                return
            # Write current ts to zk on maintenance enabled, it's be dropped on disable
            maintenance_ts = self.zk.get_maintenance_ts()
            if maintenance_ts is None:
                self.zk.write_maintenance_ts()
            # Write current primary to zk on maintenance enabled, it's be dropped on disable
            current_primary = self.zk.get_maintenance_primary()
            primary_fqdn = db_state.primary_fqdn
            if current_primary is None and primary_fqdn is not None:
                self.zk.write_maintenance_primary(primary_fqdn)
        elif maintenance_status == 'disable' or maintenance_status == '':
            # maintenance node exists with 'disable' value, we are not in maintenance now
            # and should delete this node. We delete it recursively, we don't won't to wait
            # all cluster members to delete each own node, because some of them may be
            # already dead and we can wait it infinitely. Maybe we should wait each member
            # with timeout and then delete recursively (TODO).
            if self.is_in_maintenance:
                log_event('MAINTENANCE ENDED', level='warning')
            self.is_in_maintenance = False
            if self.config.stream_from is None:
                self.zk.delete_maintenance()
                log_action = 'deleting maintenance node'
            else:
                log_action = 'not touching maintenance node as we are non-ha replica'
            logging.debug('Maintenance mode disabled, %s', log_action)
        elif maintenance_status is None:
            # maintenance node doesn't exists, we are not in maintenance mode
            self.is_in_maintenance = False
        else:
            logging.error('ALARM: unexpected maintenance status, %s', maintenance_status)

    def _update_replication_on_maintenance_enter(self):
        if not self.config.change_replication_type:
            # Replication type change is restricted, we do nothing here
            return True
        if self.config.sync_replication_in_maintenance:
            # It is allowed to have sync replication in maintenance here
            return True
        current_replication = self.db.get_replication_state()
        if current_replication.mode == 'async':
            # Ok, it is already async
            return True
        return self._replication_manager.change_replication_to_async()

    def run_iteration(self, my_prio):
        logging.info('Start iteration on host: %s', helpers.get_hostname())
        timer = IterationTimer()
        if self.is_rewind_flag_set():
            logging.error('Rewind fail flag is set, releasing leader lock and skipping iteration. Remove %s to resume.', self._rewind_flag_path())
            self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)
            self.db.pgpooler('stop')
            self.notifier.notify()
            self.finish_iteration(timer)
            return

        db_state, terminal_state = self._snapshot_postgres_state()
        role = db_state.role
        logging.info('Role: %s', str(role))

        self.notifier.notify()
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(format_db_state_for_log(db_state))

        zk_state = self._snapshot_zookeeper_state(role, db_state)
        if zk_state is None:
            self.finish_iteration(timer)
            return

        if self.config.stream_from is None:
            try:
                self._persistent_iteration(db_state, terminal_state)
            except ZookeeperException:
                if role == 'primary':
                    self.db.pgpooler('stop')
                raise
        else:
            if role is None:
                self.dead_iter(db_state, zk_state, is_in_terminal_state=terminal_state)
            elif role == 'replica':
                self.non_ha_replica_iter(db_state, zk_state)
            else:
                self.db.pgpooler('stop')
                logging.error('Non-HA host became primary; waiting for cluster authority')
        self._complete_iteration(role, my_prio)
        self.finish_iteration(timer)

    def _snapshot_postgres_state(self):
        try:
            _, terminal_state = self.db.is_alive_and_in_terminal_state()
            self._pg_conn_grace.reset()
            if not terminal_state:
                logging.debug('Database is starting up or shutting down')
            db_state = self.db.get_state()
        except PostgresConnectionTimeout:
            self._pg_conn_grace.record_failure()
            try:
                pg_running = self.db.is_postgresql_running()
            except Exception:
                logging.exception('Failed to get PostgreSQL process status after a connection timeout')
                pg_running = False
            terminal_state = True
            db_state = DbState.from_dict({
                'alive': False,
                'running': pg_running,
                'role': None,
                'connection_timed_out': True,
            })
        return db_state, terminal_state

    def _snapshot_zookeeper_state(self, role, db_state):
        try:
            zk_state = self.zk.get_state()
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(format_zk_state_for_log(zk_state))
            helpers.write_status_file(db_state, zk_state, self.config.working_dir)
            self.update_maintenance_status(db_state, zk_state)
            self._zk_alive_refresh(role, db_state, zk_state)
            if db_state.replication_state is not None:
                self.zk.write_ssn_on_changes(db_state.replication_state.synchronous_standby_names)
            if self.is_in_maintenance:
                logging.warning('Cluster in maintenance mode')
                self.zk.write_host_maintenance_enabled()
                return None
        except ZookeeperException:
            logging.exception("Zookeeper exception while getting ZK state")
            if role == 'primary':
                self.zk.re_init()
                self.db.pgpooler('stop')
                self.db.stop_archiving_wal()
            elif role == 'replica' and not self.is_in_maintenance:
                logging.debug("Upper exception was for replica")
                self.handle_detached_replica(db_state)
                self.zk.re_init()
            else:
                self.zk.re_init()
            return None
        return zk_state

    def _persistent_iteration(self, db_state: DbState, terminal_state: bool) -> None:
        state = self.zk.get_cluster_state()
        hostname = helpers.get_hostname()
        if state is None:
            self._initialize_cluster_state(db_state, hostname)
            return
        self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)
        operation = state.operation
        is_handoff_source = operation['type'] == 'switchover' and operation['phase'] == 'promoting' and operation['source'] == hostname
        if db_state.role == 'primary' and hostname != state.desired_primary and not is_handoff_source:
            self.db.pgpooler('stop')
            self.db.stop_archiving_wal()
            if state.desired_primary and not self.db.is_host_unreachable(state.desired_primary):
                self._return_to_cluster(state.desired_primary, 'primary', preserve_ssn=True)
            return

        if db_state.role == 'primary' and (hostname == state.desired_primary or is_handoff_source):
            expected_timeline = state.possible_write_configurations[0 if is_handoff_source else -1].timeline
            if operation['phase'] != 'promoting' and self.db.get_primary_timeline() != expected_timeline:
                logging.error('Primary timeline differs from cluster state; manual recovery required')
                self.db.pgpooler('stop')
                return

        self._refresh_quorum_lease(state, db_state, hostname)
        if operation['type'] == 'failover':
            if db_state.role is None:
                self.zk.release_if_hold(self.zk.FAILOVER_MANAGER_LOCK_PATH)
                if not terminal_state or (db_state.connection_timed_out and not self._pg_conn_grace.should_act(db_state.running)):
                    return
                if hostname == state.desired_primary:
                    self.db.start_postgresql()
                elif operation['phase'] == 'promoting' and state.desired_primary and not self.db.is_host_unreachable(state.desired_primary):
                    self._return_to_cluster(state.desired_primary, self.db.role or 'replica', is_dead=True, preserve_ssn=True)
                return
            if db_state.role == 'replica':
                self.write_host_stat(hostname, db_state)
                self._slot_manager.handle_slots()
            elif db_state.role == 'primary' and hostname == state.desired_primary:
                self.db.ensure_archiving_wal()
                self._slot_manager.handle_slots()
                if db_state.replics_info is not None:
                    self.zk.write_replics_info(db_state.replics_info, need_lock=False)
            if operation['phase'] == 'voting' and db_state.role == 'primary' and hostname == state.desired_primary:
                fresh = self.zk.get_cluster_state()
                if fresh is not None and fresh.state_id == state.state_id:
                    self.db.ensure_pooler_started()
            self._safe_failover.tick(state, db_state, hostname)
            if operation['phase'] == 'promoting' and hostname != state.desired_primary:
                self._follow_desired_primary(state, db_state)
            return

        if db_state.role == 'replica':
            self.write_host_stat(hostname, db_state)
            self._slot_manager.handle_slots()
            self._safe_failover.tick(state, db_state, hostname)
            if operation['type'] == 'switchover' and operation['phase'] == 'promoting':
                self._safe_switchover.tick(state, db_state, hostname)
                if hostname != state.desired_primary:
                    self._turn_to_standby_target(state, db_state)
                return
            self._follow_desired_primary(state, db_state)
            return

        if db_state.role is None:
            if not terminal_state or (db_state.connection_timed_out and not self._pg_conn_grace.should_act(db_state.running)):
                return
            if state.desired_primary == hostname:
                if operation['type'] == 'switchover' and operation['phase'] == 'promoting' and time.time() >= operation['deadline']:
                    logging.error('Switchover target is unavailable after handoff; manual recovery may be required')
                self.db.start_postgresql()
            elif state.desired_primary and operation['phase'] == 'promoting' and not self.db.is_host_unreachable(state.desired_primary):
                self._return_to_cluster(state.desired_primary, self.db.role or 'replica', is_dead=True, preserve_ssn=True)
            return

        if db_state.role != 'primary' or hostname != state.desired_primary:
            if is_handoff_source:
                self._safe_switchover.tick(state, db_state, hostname)
            return
        self.zk.write_last_primary_availability_time(need_lock=False)
        self.db.ensure_archiving_wal()
        self._slot_manager.handle_slots()
        if db_state.replics_info is not None:
            self.zk.write_replics_info(db_state.replics_info, need_lock=False)
            self.write_host_stat(hostname, db_state)
        if db_state.timeline is not None:
            self.zk.write_timeline(self.db.get_primary_timeline(), need_lock=False)
        if operation['type'] == 'switchover':
            if operation['phase'] == 'tuning_ssn':
                self.db.ensure_pooler_started()
            self._safe_switchover.tick(state, db_state, hostname)
            return
        if operation['type'] == 'durability_transition':
            self._durability.progress(state)
            return
        current = state.possible_write_configurations[0]
        if current.replicas and self.db.get_fresh_ssn() != ssn_for(current):
            logging.error('Primary SSN differs from committed durability members; closing client writes')
            self.db.pgpooler('stop')
            self.db.change_replication_type(ssn_for(current))
            return
        self.db.ensure_pooler_started()
        self._safe_switchover.tick(state, db_state, hostname)
        ha_hosts = self.zk.get_ha_hosts(catch_except=False)
        members = self._durability.desired_members(current, db_state, set(ha_hosts or []))
        self._durability.start(state, members)

    def _initialize_cluster_state(self, db_state: DbState, hostname: str) -> None:
        if db_state.role != 'primary':
            return
        self.db.pgpooler('stop')
        timeline = self.db.get_primary_timeline()
        if self.zk.create_cluster_state(ClusterState.initial(hostname, timeline, {hostname})):
            logging.warning('Initialized cluster state from primary %s on timeline %s with no replica durability members', hostname, timeline)

    def _refresh_quorum_lease(self, state: ClusterState, db_state: DbState, hostname: str) -> None:
        path = self.zk.get_host_quorum_path(hostname)
        if state.desired_primary is None:
            return
        ha_hosts = self.zk.get_ha_hosts(catch_except=False)
        eligible = db_state.role == 'replica' and hostname in (ha_hosts or set()) and self.db.get_configured_primary_fqdn() == state.desired_primary
        if eligible:
            self.zk.try_acquire_lock(path)
        else:
            self.zk.release_if_hold(path)

    def _follow_desired_primary(self, state: ClusterState, db_state: DbState) -> None:
        desired = state.desired_primary
        if not desired or desired == helpers.get_hostname():
            return
        if state.operation['type'] == 'failover' and state.operation['phase'] == 'voting':
            return
        if db_state.primary_fqdn != desired:
            self.db.pgpooler('stop')
            if not self.db.is_host_unreachable(desired):
                if self.db.restore_command_is_vote_marker():
                    self.db.resume_restoring_wal()
                self._return_to_cluster(desired, db_state.role or 'replica')
            return
        self._acquire_replication_source_slot_lock(desired)
        if self.db.restore_command_is_vote_marker():
            self.db.resume_restoring_wal()
        self.db.enable_wal_receiver_if_disabled()
        self.db.ensure_replaying_wal()
        self.db.ensure_pooler_started()
        self._slot_manager.handle_slots()

    def _turn_to_standby_target(self, state: ClusterState, db_state: DbState) -> None:
        target = state.desired_primary
        if target is None or target == helpers.get_hostname():
            return
        self._acquire_replication_source_slot_lock(target)
        if self.db.restore_command_is_vote_marker():
            self.db.resume_restoring_wal()
        if self.db.get_configured_primary_fqdn() == target:
            self.db.enable_wal_receiver_if_disabled()
            self.db.ensure_replaying_wal()
            return
        self.db.pgpooler('stop')
        self.db.repoint_replica_to(target)

    def _complete_iteration(self, role, my_prio):
        self.db.re_init()
        self.zk.re_init()
        # Dead PostgreSQL probably means
        # that our node is being removed.
        # No point in updating all_hosts
        # in this case
        all_hosts = self.zk.get_members()
        prio = self.zk.get_host_prio()
        if role and all_hosts and not prio:
            if not self.zk.write_host_prio(my_prio):
                logging.warning('Could not write priority to ZK')

    def finish_iteration(self, timer):
        logging.info('Finished iteration ==============================')
        timer.sleep(self.config.iteration_timeout)


    def handle_detached_replica(self, db_state: DbState):
        close_detached_replica_after = self.config.close_detached_after
        if not close_detached_replica_after:
            return
        now = time.time()
        zk_write_delay = now - self.last_zk_host_stat_write
        if zk_write_delay < close_detached_replica_after:
            logging.debug(
                f'Replica ZK write delay {zk_write_delay:.2f} within '
                f'{close_detached_replica_after} seconds; keeping replica open'
            )
            return
        if not db_state.require('wal_receiver'):
            logging.debug('Stopping pooler for replica with lost ZK connection and without walreceiver running')
            self.db.pgpooler('stop')
            return
        walreceiver_delay = now - db_state.require('wal_receiver').require('last_msg_receipt_time_msec') // 1000
        if walreceiver_delay > close_detached_replica_after:
            logging.debug(
                f'Stopping pooler for replica with lost ZK connection '
                f'and walreceiver delay {walreceiver_delay} > {close_detached_replica_after}'
            )
            self.db.pgpooler('stop')
        else:
            logging.debug(
                f'Replica write delay {zk_write_delay}, but walreceiver delay {walreceiver_delay} within '
                f'{close_detached_replica_after}; keeping replica open'
            )

    def write_host_stat(self, hostname, db_state: DbState):
        stream_from = self.config.stream_from
        replics_info = db_state.replics_info
        wal_receiver_info = db_state.require('wal_receiver')
        if not stream_from:
            if not self.zk.ensure_host_ha(hostname):
                logging.warning('Could not write ha host in ZK.')
                return False
        else:
            if not self.zk.delete_host_ha(hostname):
                logging.warning('Could not delete ha host in ZK.')
                return False
        if wal_receiver_info is not None:
            if not self.zk.write_host_wal_receiver(wal_receiver_info, hostname):
                logging.warning('Could not write host wal_receiver_info to ZK.')
                return False
        if replics_info is not None:
            if not self.zk.write_host_replics_info(replics_info, hostname):
                logging.warning('Could not write host replics_info to ZK.')
                return False
        self.last_zk_host_stat_write = time.time()

    def remove_stale_operation(self, hostname):
        last_op = self.zk.get_host_op(hostname)
        if helpers.is_op_destructive(last_op):
            logging.warning('Stale operation %s detected. Removing track from zk.', last_op)
            self.zk.delete_host_op(hostname)

    def start_pooler(self):
        start_pooler = self.config.start_pooler
        _, pooler_service_running = self.db.pgpooler('status')
        if not pooler_service_running and start_pooler:
            self.db.pgpooler('start')

    def get_replics_info(self, zk_state: ZkState) -> ReplicaInfos | None:
        stream_from = self.config.stream_from
        if stream_from:
            return self.zk.get_stream_source_replics_info(stream_from)
        return zk_state.replics_info


    def _get_streaming_replica_from_replics_info(self, fqdn, replics_info: ReplicaInfos | None) -> ReplicaInfo | None:
        if not replics_info:
            return None
        app_name = helpers.app_name_from_fqdn(fqdn)
        for replica in replics_info:
            if replica.require('application_name') == app_name and replica.require('state') == 'streaming':
                return replica
        return None


    def non_ha_replica_iter(self, db_state: DbState, zk_state: ZkState):
        logging.info('Current replica is non ha.')
        if not zk_state.alive:
            return None
        my_hostname = helpers.get_hostname()
        self.remove_stale_operation(my_hostname)
        self.write_host_stat(my_hostname, db_state)
        stream_from = self.config.stream_from
        can_delayed = self.config.can_delayed
        replics_info = self.get_replics_info(zk_state) or []
        streaming = self._get_streaming_replica_from_replics_info(
            my_hostname, replics_info
        ) and bool(db_state.require('wal_receiver'))
        streaming_from_primary = self._get_streaming_replica_from_replics_info(
            my_hostname, zk_state.replics_info
        ) and bool(db_state.require('wal_receiver'))
        logging.info(
            'Streaming: %s, streaming from primary: %s, wal_receiver: %s, replics_info: %s',
            streaming,
            streaming_from_primary,
            db_state.require('wal_receiver'),
            replics_info,
        )
        current_primary = zk_state.desired_primary
        cluster_state = self.zk.get_cluster_state()
        if cluster_state is not None and cluster_state.operation['type'] == 'switchover' and cluster_state.operation['phase'] == 'promoting':
            if db_state.primary_fqdn == cluster_state.operation['source']:
                return self._return_to_cluster(cluster_state.desired_primary, 'replica', is_dead=False, preserve_ssn=True)
        if streaming_from_primary and not streaming:
            self._acquire_replication_source_slot_lock(current_primary)
        if streaming:
            self._acquire_replication_source_slot_lock(stream_from)
        elif not can_delayed:
            logging.warning('Seems that we are not really streaming WAL from %s.', stream_from)
            self._replication_manager.leave_sync_group()
            replication_source_is_dead = self.db.is_host_unreachable(primary=stream_from, check_primary=False)
            replication_source_replica_info = self._get_streaming_replica_from_replics_info(
                stream_from, zk_state.replics_info
            )
            wal_receiver_info = self.zk.get_host_wal_receiver(stream_from)
            logging.debug('wal_receiver_info: {}'.format(wal_receiver_info))
            replication_source_streams = bool(
                wal_receiver_info and wal_receiver_info.status == 'streaming'
            )
            logging.debug('replication_source_replica_info: %s', replication_source_replica_info)

            if replication_source_is_dead:
                # Replication source is dead. We need to streaming from primary while it became alive and start streaming from primary.
                if stream_from == current_primary or current_primary is None:
                    logging.warning(
                        'My replication source %s seems dead and it was primary. Waiting new primary appears in cluster or old became alive.',
                        stream_from,
                    )
                elif not streaming_from_primary:
                    logging.warning(
                        'My replication source %s seems dead. Try to stream from primary %s',
                        stream_from,
                        current_primary,
                    )
                    return self._return_to_cluster(current_primary, 'replica', is_dead=False)
                else:
                    logging.warning(
                        'My replication source %s seems dead. We are already streaming from primary %s. Waiting replication source became alive.',
                        stream_from,
                        current_primary,
                    )
            else:
                # Replication source is alive. We need to wait while it starts streaming from primary and start streaming from it.
                if replication_source_streams:
                    logging.warning(
                        'My replication source %s seems alive and streams, try to stream from it',
                        stream_from,
                    )
                    return self._return_to_cluster(stream_from, 'replica', is_dead=False)
                elif stream_from == current_primary:
                    logging.warning(
                        'My replication source %s seems alive and it is current primary, try to stream from it',
                        stream_from,
                    )
                    return self._return_to_cluster(stream_from, 'replica', is_dead=False)
                else:
                    logging.warning(
                        'My replication source %s seems alive. But it don\'t streaming. Waiting it starts streaming from primary.',
                        stream_from,
                    )
        self.start_pooler()
        if self.config.primary_switch_disable_archive_restore:
            if cluster_state is None or cluster_state.operation['type'] != 'switchover':
                self.db.ensure_restoring_wal()
        self._reset_simple_primary_switch_try()
        self._slot_manager.handle_slots()


    def dead_iter(self, db_state: DbState, zk_state: ZkState, is_in_terminal_state):
        if not zk_state.alive or db_state.require('alive'):
            return None
        if db_state.connection_timed_out and not self._pg_conn_grace.should_act(db_state.running):
            return None
        self.db.pgpooler('stop')
        if not is_in_terminal_state:
            logging.warning('Waiting for PostgreSQL to finish starting or stopping.')
            return None
        self._replication_manager.leave_sync_group()
        primary = zk_state.desired_primary
        if primary is None:
            logging.warning('No desired primary is available for non-HA replica')
            return None
        if primary == helpers.get_hostname():
            logging.error('Non-HA host is desired primary; manual recovery required')
            return None
        role = self.db.role or 'replica'
        if role == 'replica' and not helpers.is_op_destructive(self.zk.get_host_op(helpers.get_hostname())):
            previous = self.db.get_prev_state()
            if previous and previous.primary_fqdn == primary:
                self._acquire_replication_source_slot_lock(primary)
                return self.db.start_postgresql()
        return self._return_to_cluster(primary, role, is_dead=True, preserve_ssn=True)


    def _reset_simple_primary_switch_try(self):
        logging.debug('Resetting simple primary switch try')
        self.checks.primary_switch = 0
        self.zk.reset_simple_primary_switch_tried(get_hostname())

    def _set_simple_primary_switch_try(self):
        self.zk.set_simple_primary_switch_tried(get_hostname())

    def _is_simple_primary_switch_tried(self):
        return self.zk.get_simple_primary_switch_tried(get_hostname())

    def _try_simple_primary_switch_with_lock(self, *args, **kwargs):
        if not self.config.do_consecutive_primary_switch:
            return self._simple_primary_switch(*args, **kwargs)
        lock_holder = self.zk.get_current_lock_holder(self.zk.PRIMARY_SWITCH_LOCK_PATH)
        if (
            lock_holder is None and not self.zk.try_acquire_lock(self.zk.PRIMARY_SWITCH_LOCK_PATH)
        ) or lock_holder != helpers.get_hostname():
            return True
        result = self._simple_primary_switch(*args, **kwargs)
        self.zk.release_lock(self.zk.PRIMARY_SWITCH_LOCK_PATH)
        return result

    def _simple_primary_switch(self, limit, new_primary, is_dead):
        primary_switch_checks = self.config.primary_switch_checks
        need_restart = self.config.primary_switch_restart

        logging.info('Starting simple primary switch to {}'.format(new_primary))
        if self.checks.primary_switch >= primary_switch_checks:
            self._set_simple_primary_switch_try()

        if need_restart and not is_dead and self.stop_postgresql(timeout=limit) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            self._reset_simple_primary_switch_try()
            return True

        if self.db.recovery_conf('create', new_primary) != 0:
            logging.error('Could not generate recovery.conf. Will retry.')
            self._reset_simple_primary_switch_try()
            return True

        if not is_dead and not need_restart:
            if not self.db.reload():
                logging.error('Could not reload PostgreSQL. Skipping it.')
            logging.debug('ACTION. Ensuring WAL replaying from {}'.format(new_primary))
            self.db.ensure_replaying_wal()
        else:
            if self.db.start_postgresql() != 0:
                logging.error('Could not start PostgreSQL. Skipping it.')

        logging.debug('Waiting for recovery and archive recovery')
        if self._wait_for_recovery(new_primary, limit):
            self.db.ensure_replaying_wal()
            if self._check_archive_recovery(new_primary, limit):            #
                # We have reached consistent state but there is a small
                # chance that we are not streaming changes from new primary
                # with: "new timeline N forked off current database system
                # timeline N-1 before current recovery point M".
                # Checking it with the info from ZK.
                #
                if self._wait_for_streaming(new_primary, limit):
                    #
                    # The easy way succeeded.
                    #
                    logging.info('Simple switch primary to {} succeeded'.format(new_primary))
                    self._reset_simple_primary_switch_try()
                    return True
                else:
                    return False

    def _rewind_from_source(self, is_postgresql_dead, limit, new_primary, preserve_ssn=False):
        log_event('REWIND', detail='Starting pg_rewind from %s' % new_primary, level='warning')

        # Trying to connect to a new_primary. If not succeeded - exiting
        if not helpers.await_for(
            lambda: not self.db.is_host_unreachable(new_primary, check_primary=False),
            limit,
            'source database alive and ready for rewind',
        ):
            return None

        if not self.zk.write_host_op('rewind', helpers.get_hostname()):
            logging.error('Unable to save destructive op state: rewind')
            return None

        self.db.pgpooler('stop')

        if not is_postgresql_dead and (
            self.db.stop_postgresql(timeout=limit) if preserve_ssn else self.stop_postgresql(timeout=limit)
        ) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            return None

        self.checks.rewind += 1
        if self.db.do_rewind(new_primary) != 0:
            logging.error('Error while using pg_rewind. Will retry.')
            return True

        # Rewind has finished successfully so we can drop its operation node
        self.zk.delete_host_op(helpers.get_hostname())
        return self._attach_to_primary(new_primary, limit)

    def _attach_to_primary(self, new_primary, limit):
        """
        Generate recovery.conf and start PostgreSQL.
        """
        logging.info('Converting role to replica of %s.', new_primary)
        if self.db.recovery_conf('create', new_primary) != 0:
            logging.error('Could not generate recovery.conf. Will retry.')
            self._reset_simple_primary_switch_try()
            return None

        if self.db.start_postgresql() != 0:
            logging.error('Could not start PostgreSQL. Skipping it.')

        if not self._wait_for_recovery(new_primary, limit):
            self._reset_simple_primary_switch_try()
            return None

        self.db.enable_wal_receiver_if_disabled()
        if not self._wait_for_streaming(new_primary, limit):
            self._reset_simple_primary_switch_try()
            return None

        logging.info('Seems, that returning to cluster succeeded. Unbelievable!')
        self.db.checkpoint()
        return True

    def _get_db_state(self):
        state = self.db.get_database_cluster_state()
        if not state or state == '':
            logging.error('Could not get info from controlfile about current cluster state.')
            return None
        logging.info('Database cluster state is: %s' % state)
        return state

    def _acquire_replication_source_slot_lock(self, source):
        if not self.config.replication_slots_polling:
            return
        # We need to drop the slot in the old primary.
        # But we don't know who the primary was (probably there are many of them).
        # So, we need to release the lock on all hosts.
        replication_sources = self.zk.get_children(self.zk.HOST_REPLICATION_SOURCES)
        if replication_sources:
            for host in replication_sources:
                if source != host:
                    self.zk.release_if_hold(os.path.join(self.zk.HOST_REPLICATION_SOURCES, host), read_lock=True)
        else:
            logging.warning(
                'Could not get all hosts list from ZK.'
                'Can not release old replication slot locks. We will skip it this time'
            )
        if source:
            # And acquire lock (then new_primary will create replication slot)
            self.zk.acquire_lock(os.path.join(self.zk.HOST_REPLICATION_SOURCES, source), read_lock=True)

    def _return_to_cluster(self, new_primary, role, is_dead=False, preserve_ssn=False):
        """
        Return to cluster (try stupid method, if it fails we try rewind)
        """
        logging.info('RETURN')
        logging.info('Starting return to cluster. New primary: {}'.format(new_primary))

        self.checks.primary_switch += 1
        logging.debug("primary_switch checks is %d", self.checks.primary_switch)

        self._acquire_replication_source_slot_lock(new_primary)
        limit = self.config.recovery_timeout

        #
        # First we try to know if the cluster
        # has been turned off correctly.
        #
        state = self._get_db_state()
        if not state:
            return None

        #
        # If we are alive replica, we should first try an easy way:
        # stop PostgreSQL, regenerate recovery.conf, start PostgreSQL
        # and wait for recovery to finish. If last fails within
        # a reasonable time, we should go a way harder (see below).
        # Simple primary switch will not work if we were promoting or
        # rewinding and failed. So only hard way possible in this case.
        #
        last_op = self.zk.noexcept_get('%s/%s/op' % (self.zk.MEMBERS_PATH, helpers.get_hostname()))
        tried = self._is_simple_primary_switch_tried()
        if role == 'primary' or helpers.is_op_destructive(last_op) or tried:
            logging.info('Could not do a simple primary switch')
            logging.debug('Possible reasons: Role: %s, Last op is destructive: %s, Simple primary switch tried: %s',
                role, helpers.is_op_destructive(last_op), tried
            )
        else:
            logging.info('Trying to do a simple primary switch: {}'.format(new_primary))
            result = self._try_simple_primary_switch_with_lock(limit, new_primary, is_dead)
            if not result:
                logging.error('ACTION-FAILED. Could not simple switch to primary: %s, attempts: %s',
                    new_primary, self.checks.primary_switch)
            self.db.checkpoint()
            return None

        #
        # If our rewind attempts fail several times
        # we should create special flag-file and stop postgresql.
        #
        max_rewind_retries = self.config.max_rewind_retries
        if self.checks.rewind > max_rewind_retries:
            self.db.pgpooler('stop')
            if preserve_ssn:
                self.db.stop_postgresql(timeout=limit)
            else:
                self.stop_postgresql(timeout=limit)
            self.set_rewind_flag()
            log_event('RESETUP: Could not rewind %d times, setting rewind-failed flag' % max_rewind_retries, level='error')
            return

        #
        # The hard way starts here.
        #
        return self._rewind_from_source(is_dead, limit, new_primary, preserve_ssn=preserve_ssn)


    def _wait_for_recovery(self, new_primary, limit=-1):
        """
        Stop until postgresql complete recovery.
        With limit=-1 the loop here can be infinite.
        """

        def check_recovery_completion():
            self._acquire_replication_source_slot_lock(new_primary)
            is_db_alive, terminal_state = self.db.is_alive_and_in_terminal_state()
            if not terminal_state:
                logging.debug('PostgreSQL in nonterminal state.')
                return None
            if is_db_alive:
                logging.debug('PostgreSQL has completed recovery.')
                return True
            if self.db.get_postgresql_status() != 0:
                logging.error('PostgreSQL service seems to be dead. No recovery is possible in this case.')
                return False
            return None

        return helpers.await_for_value(check_recovery_completion, limit, "PostgreSQL has completed recovery")

    def _check_archive_recovery(self, new_primary, limit):
        """
        Returns True if postgresql is in recovery from archive
        and False if it hasn't started recovery within `limit` seconds
        """

        def check_recovery_start():
            if self._check_postgresql_streaming(new_primary):
                logging.debug('PostgreSQL is already streaming from {}'.format(new_primary))
                return True

            # we can get here with another role or
            # have role changed during this retrying cycle
            role = self.db.get_role()
            if role != 'replica':
                logging.warning('PostgreSQL role changed during archive recovery check. Now it doesn\'t make sense')
                self.db.pgpooler('stop')
                return False

            if self.db.is_replaying_wal(1):
                logging.debug('PostgreSQL is in archive recovery')
                return True
            return None

        return helpers.await_for_value(check_recovery_start, limit, 'PostgreSQL started archive recovery')

    def _get_replics_info_from_zk(self, primary) -> ReplicaInfos | None:
        if primary:
            return self.zk.get_host_replics_info(primary)
        else:
            return self.zk.get_replics_info()


    def _check_postgresql_streaming(self, primary):
        self._acquire_replication_source_slot_lock(primary)
        is_db_alive, terminal_state = self.db.is_alive_and_in_terminal_state()
        if not terminal_state:
            logging.debug('PostgreSQL in nonterminal state.')
            return None

        if not is_db_alive:
            logging.error('PostgreSQL is dead. Waiting for streaming is useless.')
            return False

        # we can get here with another role or
        # have role changed during this retrying cycle
        if self.db.get_role() != 'replica':
            self.db.pgpooler('stop')
            logging.warning("PostgreSQL is not a replica, so it can't be streaming.")
            return False

        try:
            replica_infos = self._get_replics_info_from_zk(primary)
        except ZookeeperException:
            logging.error("Can't get replics_info from ZK. Won't wait for timeout.")
            return False

        # Best-Effort (ADR-0002 §3): self-healing — a DB loss returns None so
        # the _wait_for_streaming loop retries on the next tick.
        try:
            if replica_infos is not None and (pgconsul._is_caught_up(replica_infos) and self.db.check_walreceiver()):
                logging.debug('PostgreSQL has started streaming from {}'.format(primary))
                return True
        except PostgresConnectionError:
            logging.warning('DB connection lost during streaming check', exc_info=True)

        return None

    def _wait_for_streaming(self, primary, limit=-1):
        """
        Stop until postgresql start streaming from primary.
        With limit=-1 the loop here can be infinite.
        """
        check_streaming = functools.partial(self._check_postgresql_streaming, primary)
        return helpers.await_for_value(check_streaming, limit, 'PostgreSQL started streaming from {}'.format(primary))


    def _zk_alive_refresh(self, role, db_state: DbState, zk_state: ZkState):
        self._replication_manager.drop_zk_fail_timestamp()
        if role is None:
            self.zk.release_lock(self.zk.get_host_alive_lock_path())
        else:
            self._is_single_node = self.zk.update_single_node_status(role)
            if self._is_single_node is None:
                return
            if self.zk.get_current_lock_holder(self.zk.get_host_alive_lock_path()) is None:
                logging.warning("I don't hold my alive lock, let's acquire it")
                self.zk.try_acquire_lock(self.zk.get_host_alive_lock_path())


    def stop_postgresql(self, timeout=60, wait=True, force_async=True):
        try:
            if force_async:
                self._replication_manager.change_replication_to_async(reset_sync_replication_in_zk=False)  # TODO : it can lead to data loss
        except (PostgresConnectionError, ZookeeperException):
            # Narrowed from except Exception: only expected DB/ZK errors are swallowed
            # so that the stop proceeds. Unexpected errors propagate to run_iteration().
            logging.exception('Could not disable synchronous replication.')
        return self.db.stop_postgresql(timeout=timeout, wait=wait)


def build_pgconsul_config(config: RawConfigParser) -> PgconsulConfig:
    """Parse INI sections for the orchestrator (ADR-0004)."""
    return PgconsulConfig(
        # [global]
        welcome_message=config.get('global', 'welcome_message', fallback=''),
        working_dir=config.get('global', 'working_dir'),
        iteration_timeout=config.getfloat('global', 'iteration_timeout'),
        quorum_commit=config.getboolean('global', 'quorum_commit'),
        use_lwaldump=config.getboolean('global', 'use_lwaldump'),
        update_prio_in_zk=config.getboolean('global', 'update_prio_in_zk'),
        use_replication_slots=config.getboolean('global', 'use_replication_slots'),
        replication_slots_polling=config.getboolean('global', 'replication_slots_polling'),
        priority=config.get('global', 'priority'),
        stream_from=config.get('global', 'stream_from', fallback=None),
        autofailover=config.getboolean('global', 'autofailover'),
        switchover_replica_turn_timeout=config.getfloat('global', 'switchover_replica_turn_timeout'),
        switchover_rollback_timeout=config.getfloat('global', 'switchover_rollback_timeout'),
        max_rewind_retries=config.getint('global', 'max_rewind_retries'),
        election_timeout=config.getint('global', 'election_timeout'),
        do_consecutive_primary_switch=config.getboolean('global', 'do_consecutive_primary_switch'),
        max_allowed_switchover_lag_ms=config.getint('global', 'max_allowed_switchover_lag_ms'),
        # [replica]
        allow_potential_data_loss=config.getboolean('replica', 'allow_potential_data_loss'),
        close_detached_after=config.getfloat('replica', 'close_detached_after'),
        start_pooler=config.getboolean('replica', 'start_pooler'),
        recovery_timeout=config.getfloat('replica', 'recovery_timeout'),
        can_delayed=config.getboolean('replica', 'can_delayed'),
        primary_switch_disable_archive_restore=config.getboolean('replica', 'primary_switch_disable_archive_restore'),
        primary_switch_checks=config.getint('replica', 'primary_switch_checks'),
        primary_switch_restart=config.getboolean('replica', 'primary_switch_restart'),
        primary_unavailability_timeout=config.getfloat('replica', 'primary_unavailability_timeout'),
        walreceiver_disable_timeout=config.getfloat('replica', 'walreceiver_disable_timeout'),
        min_failover_timeout=config.getfloat('replica', 'min_failover_timeout'),
        # [primary]
        change_replication_type=config.getboolean('primary', 'change_replication_type'),
        sync_replication_in_maintenance=config.getboolean('primary', 'sync_replication_in_maintenance'),
        quorum_removal_delay=config.getfloat('primary', 'quorum_removal_delay'),
        archive_restore_disable_timeout=config.getfloat('replica', 'archive_restore_disable_timeout', fallback=5.0),
        # [debug]
        promote_checkpoint_sql=config.get('debug', 'promote_checkpoint_sql', fallback=None),
        failure_name=config.get('debug', 'failure_name', fallback=None),
        failure_count=int(config.get('debug', 'failure_count', fallback='100000000')),
        sleep_before_disable_walreceiver=config.getfloat('debug', 'sleep_before_disable_walreceiver', fallback=0),
        election_lsn_read_sleep=config.getfloat('debug', 'election_lsn_read_sleep', fallback=0),
        election_loser_timeout=config.getint('debug', 'election_loser_timeout', fallback=0),
        debug_wal_barrier_wait_file=config.get('debug', 'debug_wal_barrier_wait_file', fallback=None),
        pg_conn_failure_grace_period=config.getint('global', 'pg_conn_failure_grace_period', fallback=10),
    )


def create_pgconsul(config: RawConfigParser) -> 'Pgconsul':
    """Create all components and inject them into Pgconsul (ADR-0004)."""
    pgconsul_config = build_pgconsul_config(config)

    cmd_manager = create_command_manager(config)
    db = create_postgres(config=config, cmd_manager=cmd_manager)
    zk = create_zk(config=config, allow_disconnected=True)
    replication_manager = create_replication_manager(config, db, zk)
    slot_manager = create_replication_slot_manager(config, db, zk)
    return Pgconsul(
        config=pgconsul_config,
        db=db,
        zk=zk,
        replication_manager=replication_manager,
        slot_manager=slot_manager,
    )


# Backward-compat alias: tests and external code import `pgconsul` (lowercase).
pgconsul = Pgconsul
