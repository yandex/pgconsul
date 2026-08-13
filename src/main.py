"""
Main module. Pgconsul class defined here.
"""
# encoding: utf-8

import atexit
import functools
import logging
import os
import random
import sys
import time
from dataclasses import dataclass

from configparser import RawConfigParser

from . import helpers, sdnotify
from .debug import DebugFailure, DebugFailureConfig
from .log_formatters import format_db_state_for_log, format_zk_state_for_log, log_event
from .command_executor import CommandExecutor
from .command_manager import CommandManager, create_command_manager
from .failover_election import ElectionError, FailoverElection
from .helpers import IterationTimer, get_hostname, register_sigterm_handler, should_run
from .exceptions import PostgresConnectionError
from .maintenance import MaintenanceHandler, create_maintenance_handler
from .pg import Postgres, create_postgres
from .replication_manager import ReplicationManager, create_replication_manager
from .slot_manager import ReplicationSlotManager, create_replication_slot_manager
from .switchover import (
    CandidateSwitchoverMachine,
    PrimarySwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)
from .return_to_cluster import (
    ReturnMachineConfig,
    ReturnObservation,
    ReturnToClusterMachine,
)
from .timings import TimingTracker
from .types import ReplicaInfos
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
    switchover_catchup_timeout: float
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


class Pgconsul:
    """
    pgconsul class
    """

    def __init__(
        self,
        config: PgconsulConfig,
        db: Postgres,
        zk: Zookeeper,
        cmd_manager: CommandManager,
        replication_manager: ReplicationManager,
        slot_manager: ReplicationSlotManager,
        timings: TimingTracker,
        maintenance_handler: MaintenanceHandler,
    ):
        logging.info('Initializing main class.')
        self.config = config
        if config.welcome_message:
            logging.info(config.welcome_message)

        self._cmd_manager = cmd_manager

        random.seed(os.urandom(16))

        self.db = db
        self.zk = zk
        self.startup_checks()

        register_sigterm_handler()

        self.checks = {'primary_switch': 0, 'rewind': 0}
        self._is_single_node: bool | None = False
        self.notifier = sdnotify.Notifier()
        self._master_lost_ts: float|None = None
        self._debug_counters: dict[str, int] = {}
        self.last_zk_host_stat_write: float = 0
        self._replication_manager = replication_manager
        self._slot_manager = slot_manager
        self._timings = timings
        self._maintenance = maintenance_handler

        # Debug failure injection (step 14e, ADR-0004).
        self._debug_failure = DebugFailure(
            DebugFailureConfig(
                failure_name=config.failure_name,
                failure_count=config.failure_count,
            )
        )

        # Switchover machine config (ADR-0004).
        sw_cfg = SwitchoverMachineConfig(
            catchup_timeout=config.switchover_catchup_timeout,
            rollback_timeout=config.switchover_rollback_timeout,
            max_allowed_lag_ms=config.max_allowed_switchover_lag_ms,
            min_failover_timeout=config.min_failover_timeout,
            allow_potential_data_loss=config.allow_potential_data_loss,
        )

        # Command executor — single imperative shell for cluster-op machines (ADR-0006 §5).
        self._executor = CommandExecutor(
            zk=zk,
            db=db,
            replication_manager=replication_manager,
            timings=timings,
            stop_postgresql=self.stop_postgresql,
            store_replics_info=self._store_replics_info,
            rewind_from_source=self._rewind_from_source,
            do_failover=self._do_failover,
            set_simple_primary_switch_try=self._set_simple_primary_switch_try,
            create_slots_for_hosts=self._slot_manager.create_slots_for_hosts,
            simple_primary_switch=self._simple_primary_switch,
            ensure_restoring_wal=self._ensure_restoring_wal,
        )

        # Primary-side switchover state machine (ADR-0005 §3, ADR-0006).
        self._sw_machine = PrimarySwitchoverMachine(
            zk=zk,
            config=sw_cfg,
            debug_failure=self._debug_failure,
        )

        # Candidate-side switchover state machine (ADR-0005 §3, ADR-0006).
        self._cand_machine = CandidateSwitchoverMachine(
            zk=zk,
            config=sw_cfg,
            debug_failure=self._debug_failure,
        )

        # Return-to-cluster state machine (MDB-41951, ADR-0006).
        self._return_machine = ReturnToClusterMachine()

    def _build_switchover_observation(
        self,
        sw_record: SwitchoverRecord,
        db_state: dict,
        zk_state: dict,
        *,
        is_candidate_side: bool = False,
    ) -> SwitchoverObservation:
        """Build observation — sole I/O read point for a switchover step (ADR-0006 §1).

        Called before executor.run(). All phase-specific reads happen here.

        When local PG is dead (dead_iter path), PG-dependent reads are skipped
        — the state machine handlers for pg_stopped / primary_shut do not need
        streaming_replicas or switchover_candidate. Without this guard, the
        builder raises PostgresConnectionError which propagates to
        run_iteration and restarts the iteration, trapping the old primary in
        an infinite loop (MDB-41951).
        """
        streaming_replicas: tuple[str, ...] = ()
        all_side_replicas_turned: bool | None = None
        switchover_candidate: str | None = None
        pg_alive = db_state.get('alive', False)
        if not is_candidate_side and pg_alive:
            streaming_replicas = tuple(self._get_streaming_replicas())
            switchover_candidate = self._get_switchover_candidate(db_state)
        elif not is_candidate_side and not pg_alive:
            logging.debug(
                'Skipping PG-dependent reads in switchover observation '
                '(local PG is dead, phase=%s)', sw_record.phase,
            )
        elif sw_record.side_replicas:
            all_side_replicas_turned = self._all_side_replicas_turned_to_the_candidate(
                list(sw_record.side_replicas)
            )
        return SwitchoverObservation.build(
            record=sw_record,
            zk=self.zk,
            db=self.db,
            timings=self._timings,
            my_hostname=helpers.get_hostname(),
            db_state=db_state,
            zk_state=zk_state,
            streaming_replicas=streaming_replicas,
            all_side_replicas_turned=all_side_replicas_turned,
            is_candidate_side=is_candidate_side,
            switchover_candidate=switchover_candidate,
        )

    def re_init_db(self):
        """Reinit db connection. Exits if cache is unusable."""
        try:
            self.db.re_init()
        except KeyError:
            logging.exception('Could not get data from PostgreSQL and cache-file. Exiting.')
            sys.exit(1)

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
                self.db.pgdata = prev_state['pgdata']
            if not self.db.is_ready_for_pg_rewind():
                sys.exit(1)

        # Abort startup if zk.MEMBERS_PATH is empty
        # (no one is participating in cluster), but
        # timeline indicates a mature (tli>1) and  operating database system.
        tli = self.db.get_timeline()
        if not self.zk.get_members_retry(self.config.iteration_timeout) and tli > 1:
            logging.error(
                'ZK "%s" empty but timeline indicates operating cluster (%i > 1)',
                self.zk.MEMBERS_PATH,
                tli,
            )
            self.db.pgpooler('stop')
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

        # clear path created by mistake
        self.zk.delete_legacy_timings_path()

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
        while True:
            if self._init_zk(my_prio):
                break
            logging.error('Failed to init ZK')
            self.zk.re_init()

        while should_run():
            try:
                self.run_iteration(my_prio)
            except PostgresConnectionError as e:
                # Expected transient DB error (ADR-0002 §1): restart iteration.
                logging.warning('PostgreSQL error during iteration, will retry: %s', e)
            except Exception:
                logging.exception('Unexpected error during run_iteration')
        self.stop()

    def run_iteration(self, my_prio):
        logging.info('Start iteration on host: %s', helpers.get_hostname())
        timer = IterationTimer()
        if self.is_rewind_flag_set():
            logging.error('Rewind fail flag is set, skipping iteration. Remove %s to resume.', self._rewind_flag_path())
            self.finish_iteration(timer)
            return
        _, terminal_state = self.db.is_alive_and_in_terminal_state()
        if not terminal_state:
            logging.debug('Database is starting up or shutting down')

        db_state = self.db.get_state()
        role = db_state.get('role')
        logging.info('Role: %s', str(role))
        logging.debug('db_state: {}'.format(db_state))

        self.notifier.notify()
        db_state_for_debug = db_state.copy()
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(format_db_state_for_log(db_state_for_debug))

        try:
            zk_state = self.zk.get_state()
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(format_zk_state_for_log(zk_state))
            helpers.write_status_file(db_state, zk_state, self.config.working_dir)
            self._maintenance.update_status(db_state, zk_state, self._is_single_node)
            self._zk_alive_refresh(role, db_state, zk_state)
            if db_state.get('replication_state') is not None:
                self.zk.write_ssn_on_changes(db_state.get('replication_state')[1])
            if self._maintenance.is_in_maintenance:
                logging.warning('Cluster in maintenance mode')
                self.zk.write_host_maintenance_enabled()
                self.finish_iteration(timer)
                return
        except ZookeeperException:
            logging.exception("Zookeeper exception while getting ZK state")
            if role == 'primary' and not self._maintenance.is_in_maintenance and not self._is_single_node:
                logging.debug("Upper exception was for primary")
                my_hostname = helpers.get_hostname()
                self.resolve_zk_primary_lock(my_hostname)
            elif role == 'replica' and not self._maintenance.is_in_maintenance:
                logging.debug("Upper exception was for replica")
                self.handle_detached_replica(db_state)
                self.zk.re_init()
            else:
                self.zk.re_init()

            self.finish_iteration(timer)
            return

        stream_from = self.config.stream_from
        if role is None:
            self.dead_iter(db_state, zk_state, is_in_terminal_state=terminal_state)
        elif role == 'primary':
            if self._is_single_node:
                self.single_node_primary_iter(db_state, zk_state)
            else:
                self.primary_iter(db_state, zk_state)
        elif role == 'replica':
            if stream_from:
                self.non_ha_replica_iter(db_state, zk_state)
            else:
                self.replica_iter(db_state, zk_state)
        self.re_init_db()
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

        self.finish_iteration(timer)

    def finish_iteration(self, timer):
        logging.info('Finished iteration ==============================')
        timer.sleep(self.config.iteration_timeout)

    def release_lock_and_return_to_cluster(self):
        my_hostname = helpers.get_hostname()
        self.db.pgpooler('stop')
        holder = self.zk.get_current_lock_holder()
        if holder == my_hostname:
            self.zk.release_lock()
        elif holder is not None:
            logging.warning('Lock in ZK is being held by %s. We should return to cluster here.', holder)
            self._return_to_cluster(holder, 'primary')

    def single_node_primary_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is single node
        """
        my_hostname = helpers.get_hostname()
        logging.info('primary is in single node state')
        if not self.zk.try_acquire_lock():
            logging.warning('Failed to aquire primary lock.')
            self.resolve_zk_primary_lock(my_hostname, close_master_without_lock=False)
            return None
        self._store_replics_info(db_state, zk_state)

        self.zk.write_timeline(db_state['timeline'])

        self.db.ensure_pooler_started()
        self.db.ensure_archiving_wal()

        # Enable async replication
        current_replication = self.db.get_replication_state()
        if current_replication[0] != 'async':
            self._replication_manager.change_replication_to_async()

    def primary_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is primary
        """
        my_hostname = helpers.get_hostname()
        try:
            stream_from = self.config.stream_from
            last_op = self.zk.get_host_op(my_hostname)
            # If we were promoting or rewinding
            # and failed we should not acquire lock
            if helpers.is_op_destructive(last_op):
                logging.warning('Could not acquire lock due to destructive operation fail: %s', last_op)
                return self.release_lock_and_return_to_cluster()
            if stream_from:
                logging.warning('Host not in HA group. We should return to stream_from.')
                return self.release_lock_and_return_to_cluster()

            current_promoting_host = zk_state.get(self.zk.CURRENT_PROMOTING_HOST)
            if current_promoting_host and current_promoting_host != helpers.get_hostname():
                logging.warning(
                    'Host %s was promoted. We should not be primary', zk_state[self.zk.CURRENT_PROMOTING_HOST]
                )
                self.resolve_zk_primary_lock(my_hostname)
                return None

            # We shouldn't try to acquire leader lock if our current timeline is incorrect
            if self.zk.get_current_lock_holder() is None:
                # Timeline holdoff (ADR-0005 §1): after releasing the leader lock
                # due to a newer ZK timeline, skip lock acquisition for a grace
                # period to let the newer-timeline primary take over.
                if self._is_timeline_holdoff_active():
                    return None
                # Make sure local timeline corresponds to that of the cluster.
                if not self._verify_timeline(db_state, zk_state, without_leader_lock=True):
                    return None

            if not self.zk.try_acquire_lock():
                self.resolve_zk_primary_lock(my_hostname)
                return None
            self.zk.write_last_primary_availability_time()

            self._reset_simple_primary_switch_try()

            # release replication source locks
            self._acquire_replication_source_slot_lock(None)

            self._slot_manager.handle_slots()

            self._store_replics_info(db_state, zk_state)

            # Make sure local timeline corresponds to that of the cluster.
            if not self._verify_timeline(db_state, zk_state):
                return None

            if zk_state[self.zk.FAILOVER_MUST_BE_RESET]:
                self.reset_failover_node(zk_state)
                return None

            # Check for unfinished failover and if self is last promoted host
            # In this case self is fully operational primary, need to reset
            # failover state in ZK. Otherwise need to try return to cluster as replica
            if zk_state[self.zk.FAILOVER_STATE_PATH] in ('promoting', 'checkpointing'):
                if zk_state[self.zk.CURRENT_PROMOTING_HOST] in (helpers.get_hostname(), None):
                    self.reset_failover_node(zk_state)
                    return None  # so zk_state will be updated in the next iter
                else:
                    logging.info(
                        'Failover state was "%s" and last promoted host was "%s"',
                        zk_state[self.zk.FAILOVER_STATE_PATH],
                        zk_state[self.zk.CURRENT_PROMOTING_HOST],
                    )
                    return self.release_lock_and_return_to_cluster()

            # Main operations: switchover state machine (ADR-0005 §3, step 14h).
            # All phases (scheduled … primary_shut) are handled by the state machine.
            sw_record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
            if sw_record.is_active() and sw_record.belongs_to(helpers.get_hostname()):
                obs = self._build_switchover_observation(sw_record, db_state, zk_state)
                self._executor.set_iteration_state(db_state, zk_state)
                return self._executor.run(self._sw_machine, obs)

            # Repairs: pooler, timings, archiving, replication type.
            self.db.ensure_pooler_started()
            # Here we are primary and pooler is opened
            # so we clear downtime and failover timings if they still exist
            # (was some errors during normal failover path)
            self._timings.stop('downtime')
            self._timings.stop('failover')

            # Ensure that wal archiving is enabled. It can be disabled earlier due to
            # some zk connectivity issues.
            self.db.ensure_archiving_wal()

            # Check if replication type (sync/normal) change is needed.
            ha_replics_config = self.zk.get_ha_replics(helpers.get_hostname())
            if ha_replics_config is None:
                return None
            try:
                logging.debug('Checking ha replics for aliveness')
                alive_hosts = self.zk.get_alive_hosts(timeout=3, catch_except=False)
                ha_replics = {replica for replica in ha_replics_config if replica in alive_hosts}
                logging.debug('alive_hosts: {}'.format(alive_hosts))
                logging.debug('ha_replics: {}'.format(ha_replics))
            except ZookeeperException:
                logging.exception('Fail to get replica status')
                ha_replics = ha_replics_config
            if len(ha_replics) != len(ha_replics_config):
                logging.debug(
                    'Some of the replics is unavailable, config replics %s alive replics %s',
                    str(ha_replics_config),
                    str(ha_replics),
                )
            logging.debug('Checking if changing replication type is needed.')
            change_replication = self.config.change_replication_type
            if change_replication:
                self._replication_manager.update_replication_type(db_state, ha_replics)

            # Stale cleanup runs last (ADR-0005 §2).
            self._drop_stale_switchover(db_state)

        except ZookeeperException:
            if not self.zk.try_acquire_lock():
                logging.error("Zookeeper error during primary iteration:")
                self.resolve_zk_primary_lock(my_hostname)
                return None

    def reset_failover_node(self, zk_state):
        logging.info('Resetting failover node (current state: "%s")', zk_state[self.zk.FAILOVER_STATE_PATH])
        if (
            self.zk.get_failover_state() == 'finished'
            or self.zk.write_failover_state('finished')
        ) and self.zk.delete_current_promoting_host():
            self.zk.delete_failover_must_be_reset()
            logging.info('Resetting failover state (was "%s", now "finished")', zk_state[self.zk.FAILOVER_STATE_PATH])
        else:
            self.zk.ensure_failover_must_be_reset()
            logging.info('Resetting failover failed, will try on next iteration.')

    def resolve_zk_primary_lock(self, my_hostname, close_master_without_lock=True):
        holder = self.zk.get_current_lock_holder()
        if holder is None:
            if close_master_without_lock and self._replication_manager.should_close():
                self.db.pgpooler('stop')
                # We need to stop archiving WAL because when network connectivity
                # returns, it can be another primary in cluster. We need to stop
                # archiving to prevent "wrong" WAL appears in archive.
                self.db.stop_archiving_wal()
            else:
                self.start_pooler()
            logging.warning('Lock in ZK is released but could not be acquired. Reconnecting to ZK.')
            self.zk.reconnect()
        elif holder != my_hostname:
            self.db.pgpooler('stop')
            logging.warning('Lock in ZK is being held by %s. We should return to cluster here.', holder)
            self._return_to_cluster(holder, 'primary')

    def handle_detached_replica(self, db_state):
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
        if not db_state['wal_receiver']:
            logging.debug('Stopping pooler for replica with lost ZK connection and without walreceiver running')
            self.db.pgpooler('stop')
            return
        walreceiver_delay = now - db_state['wal_receiver']['last_msg_receipt_time_msec'] // 1000
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

    def write_host_stat(self, hostname, db_state):
        # ZK logic moved to zk.write_host_stat (step 12d, Variant A)
        if self.zk.write_host_stat(hostname, db_state, self.config.stream_from):
            self.last_zk_host_stat_write = time.time()
            return True
        return False

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

    def get_replics_info(self, zk_state) -> ReplicaInfos | None:
        if self.config.stream_from:
            return self.zk.get_stream_source_replics_info(self.config.stream_from)
        return zk_state[self.zk.REPLICS_INFO_PATH]

    def change_primary(self, db_state, primary):
        logging.warning(
            'Seems that primary has been switched to %s '
            'while we are streaming WAL from %s. '
            'We should switch primary '
            'here.',
            primary,
            db_state['primary_fqdn'],
        )
        return self._return_to_cluster(primary, 'replica')

    def replica_return(self, db_state, zk_state):
        my_hostname = helpers.get_hostname()
        self.write_host_stat(my_hostname, db_state)
        holder = zk_state['lock_holder']
        limit = self.config.recovery_timeout

        logging.debug('ACTION. Replica is returning. So we resume WAL replay to {}'.format(holder))
        self.db.ensure_replaying_wal()

        if not self._check_archive_recovery(holder, limit) and not self._wait_for_streaming(holder, limit):
            # Wal receiver is not running and
            # postgresql isn't in archive recovery
            # We should try to restart
            logging.warning('We should try switch primary to {} again'.format(holder))
            return self._return_to_cluster(holder, 'replica', is_dead=False)

    def _get_streaming_replica_from_replics_info(self, fqdn, replics_info: ReplicaInfos):
        if not replics_info:
            return None
        app_name = helpers.app_name_from_fqdn(fqdn)
        for replica in replics_info:
            if replica['application_name'] == app_name and replica['state'] == 'streaming':
                return replica
        return None

    def _get_streaming_replicas(self):
        replics_info = self.db.get_replics_info('primary')
        streaming_app_names = {r['application_name'] for r in replics_info}
        all_hosts = self.zk.get_members() or []
        return [fqdn for fqdn in all_hosts if helpers.app_name_from_fqdn(fqdn) in streaming_app_names]

    def non_ha_replica_iter(self, db_state, zk_state):
        logging.info('Current replica is non ha.')
        if not zk_state['alive']:
            return None
        my_hostname = helpers.get_hostname()
        self.write_host_stat(my_hostname, db_state)
        stream_from = self.config.stream_from
        can_delayed = self.config.can_delayed
        replics_info = self.get_replics_info(zk_state) or []
        streaming = self._get_streaming_replica_from_replics_info(
            my_hostname, replics_info
        ) and bool(db_state['wal_receiver'])
        streaming_from_primary = self._get_streaming_replica_from_replics_info(
            my_hostname, zk_state.get(self.zk.REPLICS_INFO_PATH)
        ) and bool(db_state['wal_receiver'])
        logging.info(
            'Streaming: %s, streaming from primary: %s, wal_receiver: %s, replics_info: %s',
            streaming,
            streaming_from_primary,
            db_state['wal_receiver'],
            replics_info,
        )
        current_primary = zk_state['lock_holder']

        # in case we are streaming from primary and switchover is scheduled,
        # we should temporary switch to the new primary to avoid rewinds
        if streaming_from_primary and self._check_replica_switchover(db_state, zk_state):
            return self._accept_switchover_non_ha(zk_state)
        if streaming_from_primary and not streaming:
            self._acquire_replication_source_slot_lock(current_primary)
        if streaming:
            self._acquire_replication_source_slot_lock(stream_from)
        elif not can_delayed:
            logging.warning('Seems that we are not really streaming WAL from %s.', stream_from)
            self._replication_manager.leave_sync_group()
            replication_source_is_dead = self.db.is_host_unreachable(primary=stream_from, check_primary=False)
            replication_source_replica_info = self._get_streaming_replica_from_replics_info(
                stream_from, zk_state.get(self.zk.REPLICS_INFO_PATH)
            )
            wal_receiver_info = self.zk.get_host_wal_receiver(stream_from)
            logging.debug('wal_receiver_info: {}'.format(wal_receiver_info))
            replication_source_streams = bool(
                wal_receiver_info and wal_receiver_info.get('status') == 'streaming'
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
            if zk_state.get(self.zk.SWITCHOVER_STATE_PATH) is None:
                self.db.ensure_restoring_wal()
        self._reset_simple_primary_switch_try()
        self._slot_manager.handle_slots()

        # Stale cleanup runs last (ADR-0005 §2).
        self.remove_stale_operation(my_hostname)

    def _check_replica_switchover(self, db_state, zk_state):
        """
        Detect planned switchover condition.
        """
        switchover_info = zk_state[self.zk.SWITCHOVER_ROOT_PATH]
        if not switchover_info:
            return False

        logging.info('Switchover record found in ZK')

        # We check that switchover should happen from current timeline
        zk_tli = self.zk.get_timeline()
        sw_tli = switchover_info[self.zk.TIMELINE_INFO_PATH]
        if zk_tli != sw_tli:
            logging.warning('ZK timeline %s differs from switchover timeline %s, ignoring switchover', zk_tli, sw_tli)
            return False

        # The node contains hostname of current instance
        switchover_primary = switchover_info.get('hostname')
        if switchover_primary is not None and switchover_primary != db_state['primary_fqdn']:
            logging.error('current primary FQDN is not equal to hostname in switchover node, ignoring switchover')
            return False

        # Check the current replica has the same timeline
        if not self._check_my_timeline_sync():
            return False

        logging.info('Scheduled switchover checks passed OK.')
        return True

    def _accept_switchover_non_ha(self, zk_state):
        log_event('SWITCHOVER STARTED (non-HA)', level='warning')

        # Wait for appropriate switchover state
        switchover_state = zk_state[self.zk.SWITCHOVER_STATE_PATH]

        if switchover_state not in ('initiated', 'candidate_found'):
            logging.warning('Switchover state is %s, will not proceed.', switchover_state)
            return False

        switchover_candidate = zk_state[self.zk.SWITCHOVER_CANDIDATE]
        if switchover_candidate is None:
            logging.warning('Waiting for primary to choose switchover candidate...')
            return False

        logging.info('Current host is not-HA replica, temporarily switching to the new primary until switchover is complete')

        if self.config.primary_switch_disable_archive_restore:
            self.db.stop_restoring_wal()

        return self._return_to_cluster(switchover_candidate, 'replica', is_dead=False, skip_check=True)

    def replica_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is replica
        """
        if not zk_state['alive']:
            return None
        my_hostname = helpers.get_hostname()
        my_app_name = helpers.app_name_from_fqdn(my_hostname)
        holder = zk_state['lock_holder']
        self.write_host_stat(my_hostname, db_state)

        if self._is_single_node:
            logging.error("HA replica shouldn't exist inside a single node cluster")
            return None

        replics_info = zk_state[self.zk.REPLICS_INFO_PATH]
        streaming = False
        for i in replics_info or []:
            if i['application_name'] != my_app_name:
                continue
            if i['state'] == 'streaming':
                streaming = True

        # Early guard: switchover FAILED + no lock holder — fall back to failover
        # regardless of FQDN mismatch in the switchover record (MDB-41951 Fix #8).
        # After a failed promote the old primary may restart as a replica streaming
        # from the ex-candidate; its db_state['primary_fqdn'] then differs from
        # switchover.hostname, causing _check_replica_switchover() to return False
        # and hiding the is_failed() guard added in report-37.  This early check
        # catches that case before _check_replica_switchover() can reject it.
        _early_sw = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        if _early_sw.is_failed() and not self.zk.get_current_lock_holder():
            logging.warning(
                'Switchover failed (phase %s) and no primary lock holder — '
                'falling back to failover (early FQDN-mismatch guard, MDB-41951)',
                _early_sw.phase,
            )
            return self._accept_failover(switchover_in_progress=True)

        # Check and perform scheduled switchover if needed (ADR-0005 §3, step 15c).
        if self._check_replica_switchover(db_state, zk_state):
            self._replication_manager.enter_sync_group(replica_infos=replics_info)
            sw_record = SwitchoverRecord.from_zk_state(zk_state, self.zk)

            # Safety-net: scheduled + no lock holder + no autofailover → failover fallback.
            if (sw_record.phase == SwitchoverPhase.SCHEDULED
                    and not self.zk.get_current_lock_holder()
                    and not self.config.autofailover):
                logging.warning('Nobody holds the leader lock, but autofailover is disabled, falling back to failover')
                return self._accept_failover(switchover_in_progress=True)

            # Candidate: route through state machine (handles initiated, candidate_found).
            if sw_record.is_active() and sw_record.candidate == my_hostname:
                obs = self._build_switchover_observation(
                    sw_record, db_state, zk_state, is_candidate_side=True,
                )
                self._executor.set_iteration_state(db_state, zk_state)
                return self._executor.run(self._cand_machine, obs)

            # Not the candidate, but switchover candidate is known: return
            # to cluster. Gate on phase >= INITIATED: the candidate only
            # creates replication slots in plan_initiated (phase INITIATED).
            # Returning during SCHEDULED/SYNC_SET races with slot creation,
            # causing "replication slot does not exist" → fallback to rewind.
            if sw_record.candidate is not None and sw_record.phase in (
                SwitchoverPhase.INITIATED,
                SwitchoverPhase.CANDIDATE_FOUND,
                SwitchoverPhase.POOLER_STOPPED,
                SwitchoverPhase.PG_STOPPED,
                SwitchoverPhase.PRIMARY_SHUT,
                SwitchoverPhase.CANDIDATE_ACQUIRED,
                SwitchoverPhase.PROMOTED,
            ):
                if self.config.primary_switch_disable_archive_restore:
                    self.db.stop_restoring_wal()
                return self._return_to_cluster(sw_record.candidate, 'replica', is_dead=False, skip_check=True)

            # Switchover failed and no lock holder: fall back to failover so the
            # cluster recovers a primary instead of waiting forever (MDB-41951).
            if sw_record.is_failed() and not self.zk.get_current_lock_holder():
                logging.warning(
                    'Switchover failed (phase %s) and no primary lock holder — '
                    'falling back to failover', sw_record.phase,
                )
                return self._accept_failover(switchover_in_progress=True)

            # No candidate yet, or candidate known but phase < INITIATED — wait.
            logging.debug('Switchover in progress (phase %s), waiting', sw_record.phase)
            return False

        # If there is no primary lock holder and it is not a switchover
        # then we should consider current cluster state as failover.
        if holder is None:
            log_event('FAILOVER: Primary has died, starting failover procedure', level='error')
            logging.error('According to ZK primary has died. We should verify it and do failover if possible.')
            if self._master_lost_ts is None and zk_state[self.zk.TIMELINE_INFO_PATH] is not None:
                self._master_lost_ts = time.time()
            return self._accept_failover()
        self._master_lost_ts = None

        if holder != db_state['primary_fqdn'] and holder != my_hostname:
            self._replication_manager.leave_sync_group()
            return self.change_primary(db_state, holder)

        self._acquire_replication_source_slot_lock(holder)

        logging.debug('ACTION. Ensuring WAL replaying from {}'.format(holder))
        self.db.ensure_replaying_wal()

        if self.config.primary_switch_disable_archive_restore:
            if zk_state.get(self.zk.SWITCHOVER_STATE_PATH) is None:
                self.db.ensure_restoring_wal()

        if not streaming:
            logging.warning('Seems that we are not really streaming WAL from %s.', holder)
            self._replication_manager.leave_sync_group()

            return self.replica_return(db_state, zk_state)

        self.start_pooler()
        self._reset_simple_primary_switch_try()

        self._replication_manager.enter_sync_group(replica_infos=replics_info)
        self._slot_manager.handle_slots()

        # Stale cleanup runs last (ADR-0005 §2).
        self.remove_stale_operation(my_hostname)

    def dead_iter(self, db_state, zk_state, is_in_terminal_state):
        """
        Iteration if local postgresql is dead
        """
        if not zk_state['alive'] or db_state['alive']:
            return None

        self.db.pgpooler('stop')
        if not is_in_terminal_state:
            logging.warning('Waiting for PostgreSQL to finish starting or stopping.')
            return None

        if self._is_single_node:
            logging.info('ACTION. We are in single mode, starting Postgres')
            return self.db.start_postgresql()

        # Switchover guard: if a switchover is in progress and this host is the
        # old primary, do NOT release the leader lock here. The switchover state
        # machine (PrimarySwitchoverMachine) owns lock release in plan_pg_stopped
        # / plan_primary_shut. Releasing it here (when PG is dead between
        # pg_stopped and primary_shut) prematurely hands the lock to the
        # candidate before the old primary has drained WAL and done the final
        # stop, causing a race (MDB-41951).
        #
        # Instead of just waiting (return None), run the state machine so it can
        # advance pg_stopped → primary_shut (release lock, final PG stop). Without
        # this, the old primary gets stuck in an infinite loop: PG dead →
        # dead_iter → guard → return None → next iteration → dead_iter → ...
        sw_record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        if sw_record.is_active() and sw_record.belongs_to(helpers.get_hostname()):
            logging.warning(
                'Switchover in progress (phase %s) and local PG is dead — '
                'running switchover state machine to advance',
                sw_record.phase,
            )
            obs = self._build_switchover_observation(sw_record, db_state, zk_state)
            self._executor.set_iteration_state(db_state, zk_state)
            return self._executor.run(self._sw_machine, obs)

        self._replication_manager.leave_sync_group()
        self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)

        role = self.db.role  # it's previous role, before death
        last_primary = None
        if role == 'replica':
            prev_state = self.db.get_prev_state()
            last_primary = prev_state.get('primary_fqdn')

        holder = self.zk.get_current_lock_holder()
        if holder and holder != helpers.get_hostname():
            last_op = self.zk.get_host_op(helpers.get_hostname())
            if role == 'replica' and holder == last_primary and not helpers.is_op_destructive(last_op):
                if not is_in_terminal_state:
                    logging.warning('Waiting for postgres to finish starting or stopping.')
                    return None
                self._acquire_replication_source_slot_lock(last_primary)
                logging.info('Seems that primary has not changed but PostgreSQL is dead. Starting it.')
                return self.db.start_postgresql()

            #
            # We can get here in two cases:
            # We were primary and now we are dead.
            # We were replica, primary has changed and now we are dead.
            #
            logging.warning(
                'Seems that primary is %s and local PostgreSQL is dead. We should return to cluster here.', holder
            )
            return self._return_to_cluster(holder, role, is_dead=is_in_terminal_state)

        else:
            #
            # The only case we get here is absence of primary (no one holds the
            # lock) and our PostgreSQL is dead.
            #
            # TODO: BUG? should be acquire lock before starting PG ? replica may be promoting right now
            logging.error('Seems that all hosts (including me) are dead. Trying to start PostgreSQL.')
            if role == 'primary':
                last_tli = self.db.get_timeline()
                if not last_tli:
                    logging.error('Seems we have an error. Not doing anything.')
                    return None

                zk_timeline = zk_state[self.zk.TIMELINE_INFO_PATH]
                if zk_timeline is not None and zk_timeline != last_tli:
                    logging.error(
                        'Seems that I was primary before but not the last one in the cluster. Not doing anything.'
                    )
                    return None
            #
            # Role was primary. We need to disable archive_command before
            # starting postgres to prevent "wrong" last WAL in archive.
            #
            self.db.stop_archiving_wal_stopped()
            return self.db.start_postgresql()

    def _drop_stale_switchover(self, db_state):
        if not self.zk.try_acquire_lock(self.zk.SWITCHOVER_LOCK_PATH):
            return
        try:
            switchover_info = self.zk.get_switchover_primary_info()
            if not switchover_info:
                return
            switchover_state = self.zk.get_switchover_state()
            # ADR-0005 §4: stale only if record cannot belong to a resumable process.
            # States initiated/candidate_found with matching timeline are NOT stale.
            sw_tli = switchover_info.get(self.zk.TIMELINE_INFO_PATH)
            is_stale = (
                sw_tli is None
                or sw_tli < db_state['timeline']
                or switchover_state == 'failed'
            )
            if is_stale:
                logging.warning('Dropping stale switchover')
                logging.debug(
                    'Switchover info: state %s; info %s; db timeline %s',
                    switchover_state,
                    switchover_info,
                    db_state['timeline'],
                )
                self._cleanup_switchover()
                if switchover_info.get('hostname') != helpers.get_hostname():
                    # primary changed, so switchover finally happened
                    self._timings.stop('switchover')
                else:
                    self._timings.stop('switchover', track_as='switchover_failure')

        finally:
            # We want to release this lock regardless of what happened in 'try' block
            self.zk.release_lock(self.zk.SWITCHOVER_LOCK_PATH)

    def _cleanup_switchover(self):
        logging.info('Cleaning up switchover info...')
        self.zk.cleanup_switchover()

    def _verify_timeline(self, db_state, zk_state, without_leader_lock=False):
        """
        Make sure current timeline corresponds to the rest of the cluster (@ZK).
        Save timeline and some related info into zk
        """
        # Skip if role is not primary
        if self.db.role != 'primary':
            logging.error('We are not primary. Not doing anything.')
            return None

        # Establish whether local timeline corresponds to primary timeline at ZK.
        tli_res = zk_state[self.zk.TIMELINE_INFO_PATH] == db_state['timeline']
        # If it does, but there is no info on replicas,
        # close local PG instance.
        if tli_res:
            if zk_state.get('replics_info_written') is False:
                logging.error('Some error with ZK.')
                # Actually we should never get here but checking it just in case.
                # Here we should end iteration and check and probably close primary
                # at the begin of primary_iter
                return None
        # If ZK does not have timeline info, write it.
        elif zk_state[self.zk.TIMELINE_INFO_PATH] is None:
            if without_leader_lock:
                return True
            logging.warning('Could not get timeline from ZK. Saving it.')
            self.zk.write_timeline(db_state['timeline'])
        # If there is a mismatch in timeline:
        # - If ZK timeline is greater than local, there must be another primary.
        #   In that case local instance have no business holding the lock.
        # - If local timeline is greater, local instance has likely been
        #   promoted recently.
        #   Update ZK structure to reflect that.
        elif tli_res is False:
            self.db.checkpoint()
            zk_tli = zk_state[self.zk.TIMELINE_INFO_PATH]
            db_tli = db_state['timeline']
            if zk_tli and zk_tli > db_tli:
                logging.error('ZK timeline is newer than local. Releasing leader lock')
                self.db.pgpooler('stop')

                self.zk.release_lock()
                # Holdoff marker (ADR-0005 §1): let the newer-timeline primary
                # acquire the lock. Replaces the former blocking time.sleep.
                self._start_timeline_holdoff()
                return None
            elif zk_tli and zk_tli < db_tli:
                if without_leader_lock:
                    return True
                logging.warning('Timeline in ZK is older than ours. Updating it it ZK.')
                self.zk.write_timeline(db_tli)
        logging.debug('Timeline verification succeeded')
        return True

    # Timeline holdoff grace period (ADR-0005 §1): replaces the former
    # blocking time.sleep(10 * iteration_timeout) in _verify_timeline.
    TIMELINE_HOLDOFF_NAME = 'timeline_holdoff'
    TIMELINE_HOLDOFF_MULTIPLIER = 10

    def _start_timeline_holdoff(self) -> None:
        """Write holdoff timestamp to ZK so next iterations skip lock acquisition."""
        self.zk.write_timing(self.TIMELINE_HOLDOFF_NAME, time.time())

    def _is_timeline_holdoff_active(self) -> bool:
        """Check if timeline holdoff is still active; clear it if expired."""
        holdoff_ts = self.zk.get_timing(self.TIMELINE_HOLDOFF_NAME)
        if holdoff_ts is None:
            return False
        if time.time() - holdoff_ts < self.TIMELINE_HOLDOFF_MULTIPLIER * self.config.iteration_timeout:
            logging.debug('Timeline holdoff active, skipping lock acquisition')
            return True
        logging.info('Timeline holdoff expired, resuming lock acquisition')
        self.zk.delete_timing(self.TIMELINE_HOLDOFF_NAME)
        return False

    def _reset_simple_primary_switch_try(self):
        logging.debug('Resetting simple primary switch try')
        self.checks['primary_switch'] = 0
        self.zk.reset_simple_primary_switch_tried(get_hostname())

    def _set_simple_primary_switch_try(self):
        self.zk.set_simple_primary_switch_tried(get_hostname())

    def _is_simple_primary_switch_tried(self):
        return self.zk.get_simple_primary_switch_tried(get_hostname())

    def _ensure_restoring_wal(self):
        """Restore archive recovery (undo restore_command=/bin/false)."""
        logging.info('Ensuring WAL restoring is enabled')
        self.db.ensure_restoring_wal()

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
        if self.checks['primary_switch'] >= primary_switch_checks:
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
            if self._check_archive_recovery(new_primary, limit):
                #
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
                # Streaming did not start within the timeout — WAL likely
                # diverged. Fall through to signal failure so the caller
                # proceeds to pg_rewind.
                logging.warning('Simple primary switch: streaming did not start, falling back to rewind')
                return False
            # Archive recovery did not complete — fall through to failure.
            logging.warning('Simple primary switch: archive recovery check failed, falling back to rewind')
            return False
        # Recovery did not complete — fall through to failure.
        logging.warning('Simple primary switch: recovery did not complete, falling back to rewind')
        return False

    def _rewind_from_source(self, is_postgresql_dead, limit, new_primary):
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

        if not is_postgresql_dead and self.stop_postgresql(timeout=limit) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            return None

        self.checks['rewind'] += 1
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

    def _return_to_cluster(self, new_primary, role, is_dead=False, skip_check=False):
        """Return to cluster via state machine (MDB-41951, ADR-0006).

        Two-pass delegation: first pass tries simple switch; if it fails
        (fail-fast), second pass rebuilds the observation with fresh timeline
        data and delegates the rewind-vs-retry decision to the machine.
        """
        logging.info('Starting return to cluster. New primary: {}'.format(new_primary))
        self.checks['primary_switch'] += 1

        self._acquire_replication_source_slot_lock(new_primary)
        failover_state = self.zk.get_failover_state()
        if failover_state is not None and failover_state not in ('finished', 'promoting', 'checkpointing') and not skip_check:
            logging.info('Failover in progress (%s), cannot return to cluster.', failover_state)
            return None

        limit = self.config.recovery_timeout
        state = self._get_db_state()
        if not state:
            return None

        tried = self._is_simple_primary_switch_tried()
        self._executor.set_iteration_state(state, {})

        # Pass 1: try simple switch (if not already tried).
        if not tried:
            # db_state must be a dict (role, timeline, ...) for
            # ReturnObservation.build().  _get_db_state() returns a string
            # from pg_controldata, so fetch the structured state here.
            obs = ReturnObservation.build(
                zk=self.zk, db=self.db, my_hostname=helpers.get_hostname(),
                db_state=self.db.get_state() or {}, new_primary=new_primary,
                is_dead=is_dead, skip_check=skip_check,
                recovery_timeout=limit, simple_switch_tried=False,
                fallback_role=role,
            )
            consumed = self._executor.run(self._return_machine, obs)
            # If simple switch succeeded (plan fully executed), done.
            # If it failed (fail-fast), fall through to pass 2.
            if not consumed or self._executor.last_command_succeeded:
                return None

        # Pass 2: check divergence — rewind or retry.
        obs = ReturnObservation.build(
            zk=self.zk, db=self.db, my_hostname=helpers.get_hostname(),
            db_state=self.db.get_state() or {}, new_primary=new_primary,
            is_dead=is_dead, skip_check=skip_check, recovery_timeout=limit,
            simple_switch_tried=True,
            fallback_role=role,
        )
        self._executor.run(self._return_machine, obs)

        if self.checks['rewind'] > self.config.max_rewind_retries:
            self.db.pgpooler('stop')
            self.stop_postgresql(timeout=limit)
            self.set_rewind_flag()
            log_event('RESETUP: Could not rewind %d times, setting rewind-failed flag' % self.config.max_rewind_retries, level='error')
            return
        return None

    def _promote(self):
        if not self.zk.write_failover_state('promoting'):
            logging.error('Could not write failover state to ZK.')
            return False

        if not self.zk.write_current_promoting_host():
            logging.error('Could not write self as last promoted host.')
            return False

        if not self.db.promote():
            logging.error('Could not promote me as a new primary. We should release the lock in ZK here.')
            # We need to close here and recheck postgres role. If it was no actual
            # promote, we need too delete self as last promoted host, mark failover "finished"
            # and return to cluster. If self primary we need to continue promote despite on exit code
            # because self already accepted some data modification which will be loss if
            # we simply return False here.
            if self.db.get_role() != 'primary':
                self.db.pgpooler('stop')
                if not self.zk.delete_current_promoting_host():
                    logging.error('Could not remove self as current promoting host.')
                if not self.zk.write_failover_state('finished'):
                    logging.error('Could not write failover state to ZK.')
                return False

            logging.info('Promote command failed but we are current primary. Continue')

        self._timings.stop('downtime')

        self._slot_manager.reset_on_promote()

        if not self.zk.write_failover_state('checkpointing'):
            logging.warning('Could not write failover state to ZK.')

        logging.debug('Doing checkpoint after promoting.')
        # Post-promote critical section (ADR-0002 §2): cosmetic — promote already succeeded.
        try:
            self.db.checkpoint(query=self.config.promote_checkpoint_sql)
        except PostgresConnectionError:
            logging.warning('Could not checkpoint after failover.', exc_info=True)

        my_tli = self.db.get_timeline()

        if not self.zk.write_timeline(my_tli):
            logging.warning('Could not write timeline to ZK.')

        if not self.zk.write_failover_state('finished'):
            logging.error('Could not write failover state to ZK.')

        if not self.zk.delete_current_promoting_host():
            logging.error('Could not remove self as current promoting host.')

        return True

    def _promote_handle_slots(self):
        if not self.zk.write_failover_state('creating_slots'):
            logging.warning('Could not write failover state to ZK.')
        hosts = self.zk.get_ha_replics(helpers.get_hostname())
        if hosts is None:
            logging.error(
                'Could not get all hosts list from ZK. '
                'Replication slots should be created but we '
                'are unable to do it. Releasing the lock.'
            )
            return False
        return self._slot_manager.create_slots_for_hosts(list(hosts))

    def _check_my_timeline_sync(self):
        my_tli = self.db.get_timeline()
        try:
            zk_tli = self.zk.get_timeline()
        except ZookeeperException:
            logging.error('Could not get timeline from ZK.')
            return False
        if zk_tli is None:
            logging.warning('There was no timeline in ZK. Skipping this check.')
        elif zk_tli != my_tli:
            logging.error(
                'My timeline (%d) differs from timeline in ZK (%d). Checkpointing and skipping iteration.',
                my_tli,
                zk_tli,
            )
            self.db.checkpoint()
            return False
        return True

    def _check_last_failover_timeout(self):
        last_failover_ts = self.zk.get_last_failover_time()
        if last_failover_ts is None:
            logging.warning('There was no last failover ts in ZK. Skipping this check.')
            last_failover_ts = 0.0
        diff = time.time() - last_failover_ts
        if not helpers.check_last_failover_time(last_failover_ts, self.config):
            logging.info('Last time failover has been done %f seconds ago. Not doing anything.', diff)
            return False
        logging.info('Last failover has been done %f seconds ago.', diff)
        return True

    def _check_primary_unavailability_timeout(self):
        previous_primary_availability_time = self.zk.get_last_primary_availability_time()
        if previous_primary_availability_time is None:
            logging.error('Failed to get last primary availability time.')
            return False
        time_passed = time.time() - previous_primary_availability_time
        if time_passed < self.config.primary_unavailability_timeout:
            logging.info('Last time we seen primary %f seconds ago, not doing anything.', time_passed)
            return False
        return True

    def _can_do_failover(self, switchover_in_progress=False):
        autofailover = self.config.autofailover

        if not (autofailover or switchover_in_progress):
            logging.info("Autofailover is disabled. Not doing anything.")
            return False

        if not self._check_my_timeline_sync():
            return False

        if not self._check_last_failover_timeout():
            return False

        # Skip the libpq reachability check when called from a failed-switchover
        # fallback path (MDB-41951 Fix #9).  In that case the old primary has
        # already released the leader lock (primary_shut phase), so we *know*
        # it is no longer acting as primary.  The ex-candidate may still be
        # reachable as a plain replica; is_host_unreachable(check_primary=False)
        # would connect without target_session_attrs=primary, succeed, and
        # incorrectly abort the failover with "primary still accessible".
        if not switchover_in_progress and not self.db.is_host_unreachable(check_primary=False):
            logging.warning(
                'According to ZK primary has died but it is still accessible through libpq. Not doing anything.'
            )
            return False

        if not self._check_primary_unavailability_timeout():
            return False
        if self.db.is_replaying_wal(self.config.iteration_timeout):
            logging.info("Host is still replaying WAL, so it can't be promoted.")
            return False

        replica_infos = self.zk.noexcept_get_replics_info()
        if replica_infos is None:
            logging.error('Unable to get replics info from ZK.')
            return False

        allow_data_loss = self.config.allow_potential_data_loss
        logging.info(f'Data loss is: {allow_data_loss}')
        is_promote_safe = self._replication_manager.is_promote_safe(
            self.zk.get_alive_hosts(),
            replica_infos=replica_infos,
        )
        if not allow_data_loss and not is_promote_safe:
            logging.warning('Promote is not allowed with given configuration.')
            return False

        sleep_before_disable_walreceiver = self.config.sleep_before_disable_walreceiver
        if sleep_before_disable_walreceiver:
            logging.debug('Sleep for test purposes before disabling walreceiver: %s', sleep_before_disable_walreceiver)
            time.sleep(sleep_before_disable_walreceiver)

        disable_timeout = self.config.walreceiver_disable_timeout
        if not self.db.disable_wal_receiver(disable_timeout):
            return False

        return self._make_election(replica_infos, allow_data_loss)

    def _make_election(self, replica_infos: ReplicaInfos, allow_data_loss: bool) -> bool:
        election_timeout = self.config.election_timeout
        quorum_size = len(helpers.make_current_replics_quorum(replica_infos, self.zk.get_alive_hosts(all_hosts_timeout=election_timeout / 3)))
        host_lsn = self.db.get_wal_receive_lsn() or '0'

        election_lsn_read_sleep = self.config.election_lsn_read_sleep
        if election_lsn_read_sleep:
            logging.debug('Read lsn for election vote: %s. Sleep for test purposes: %s', host_lsn, election_lsn_read_sleep)
            time.sleep(election_lsn_read_sleep)

        election = FailoverElection(
            self.zk,
            election_timeout,
            replica_infos,
            self._replication_manager,
            allow_data_loss,
            int(self.config.priority),
            host_lsn,
            quorum_size,
        )
        try:
            election_loser_timeout = self.config.election_loser_timeout
            return election.make_election(election_loser_timeout)
        except (ZookeeperException, ElectionError):
            logging.exception('Error during failover election')
            return False

    def _get_switchover_candidate(self, db_state: dict | None = None):
        switchover_info = self.zk.get_switchover_primary_info()
        if switchover_info is None:
            return None
        if switchover_info.get('destination') is not None:
            return switchover_info.get('destination')
        replica_infos = self._get_extended_replica_infos(db_state)
        if replica_infos is None:
            return None
        if self.config.allow_potential_data_loss:
            app_name_map = {helpers.app_name_from_fqdn(host): host for host in self.zk.get_ha_hosts()}
            return app_name_map.get(helpers.get_oldest_replica(replica_infos))
        return self._replication_manager.get_ensured_sync_replica(replica_infos)

    def _get_extended_replica_infos(self, db_state: dict | None = None) -> ReplicaInfos | None:
        replica_infos = self.zk.get_replics_info()
        if not replica_infos:
            # Fall back to db_state['replics_info'] (fresh from pg_stat_replication)
            # when the global ZK node is stale, empty, or not yet written. Without
            # this fallback, switchover stalls with "no eligible candidate" because
            # the primary has valid replica data in db_state but not yet in ZK.
            if db_state is not None and db_state.get('replics_info'):
                logging.debug('ZK replics_info is empty, falling back to db_state')
                replica_infos = db_state['replics_info']
            else:
                logging.error('Unable to get replica infos from ZK or db_state.')
                return None
        app_name_map = {helpers.app_name_from_fqdn(host): host for host in self.zk.get_ha_hosts()}
        for info in replica_infos:
            hostname = app_name_map.get(info['application_name'])
            if not hostname:
                continue
            prio = self.zk.get_host_prio(hostname)
            info['priority'] = int(prio) if prio is not None else None
        return replica_infos

    def _accept_failover(self, switchover_in_progress=False):
        """
        Failover magic is here

        Critical section (ADR-0002 §2): a DB connection loss aborts the failover
        (return None, release the lock); any other error propagates to
        run_iteration() for logging and restart.
        """
        lock_acquired = False
        try:
            if not self._can_do_failover(switchover_in_progress):
                return None

            self._timings.start('downtime', ts=self._master_lost_ts)
            self._timings.start('failover', ts=self._master_lost_ts)

            #
            # All checks are done. Acquiring the lock in ZK, promoting and
            # writing last failover timestamp to ZK.
            #
            if not self.zk.try_acquire_lock():
                logging.info('Could not acquire lock in ZK. Not doing anything.')
                return None
            lock_acquired = True
            self.db.pg_wal_replay_resume()

            if not self._do_failover():
                self.zk.release_lock()
                return False

            self.zk.write_last_failover_time()
            self._timings.stop('failover')
        except PostgresConnectionError:
            # ADR-0002 §2: abort failover on DB loss; release the lock if it
            # was acquired. DB loss inside _do_failover is caught there and
            # returned as False (handled by the `if not self._do_failover()` branch).
            logging.warning('DB connection lost during failover. Aborting failover.')
            if lock_acquired:
                self.zk.release_lock()
            return None

    def _do_failover(self, old_primary=None):
        # Critical section (ADR-0002 §2): DB loss here is caught and returned
        # as False so the caller releases the leader lock. _do_failover owns
        # only the promote logic; the lock is managed by its callers.
        try:
            if not self.zk.delete_failover_state():
                logging.error('Could not remove previous failover state.')
                return False

            if not self._promote_handle_slots():
                return False

            if self._debug_failure('before_promote'):
                return False

            if not self._replication_manager.set_ssn_before_promote(
                self.zk.get_quorum_replics_for_promote(), old_primary=old_primary
            ):
                logging.error('Failed to set SSN before promote, aborting promote')
                return False

            if not self._promote():
                return False

            self._replication_manager.leave_sync_group()
            return True
        except PostgresConnectionError:
            logging.warning('DB connection lost during failover.', exc_info=True)
            return False

    def _wait_for_recovery(self, new_primary, limit):
        """Stop until postgresql complete recovery (ADR-0005 §1: no infinite wait)."""

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

    @staticmethod
    def _is_caught_up(replica_infos: ReplicaInfos):
        my_app_name = helpers.app_name_from_fqdn(helpers.get_hostname())
        for replica in replica_infos:
            if replica['application_name'] == my_app_name and replica['state'] == 'streaming':
                return True
        return False

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

    def _wait_for_streaming(self, primary, limit):
        """Stop until postgresql start streaming from primary (ADR-0005 §1: no infinite wait)."""
        check_streaming = functools.partial(self._check_postgresql_streaming, primary)
        return helpers.await_for_value(check_streaming, limit, 'PostgreSQL started streaming from {}'.format(primary))

    def _all_side_replicas_turned_to_the_candidate(self, side_replicas):
        side_replicas_app_names = {helpers.app_name_from_fqdn(r) for r in side_replicas}
        logging.debug('Side replicas names: %s', side_replicas_app_names)
        # Switchover critical section (ADR-0002 §2): return False on DB loss so
        # the await_for loop keeps waiting.
        try:
            replics_info = self.db.get_replics_info('replica')
        except PostgresConnectionError:
            logging.warning('Could not get replics info from candidate, assuming not all replicas turned yet', exc_info=True)
            return False
        turned_replicas_names = set()
        for r in replics_info:
            if r['application_name'] in side_replicas_app_names and r['state'] == 'streaming':
                turned_replicas_names.add(r['application_name'])
        waiting_replicas_names = side_replicas_app_names - turned_replicas_names
        logging.info('Replicas streaming from the candidate: %s, waiting for %s', turned_replicas_names, waiting_replicas_names)
        return turned_replicas_names == side_replicas_app_names

    def _candidate_is_sync_with_primary(self, replics_info, switchover_candidate):
        assert switchover_candidate is not None, "switchover candidate is None"
        candidate_appname = helpers.app_name_from_fqdn(switchover_candidate)
        replica = next(
            (r for r in replics_info if r.get('application_name') == candidate_appname),
            None
        )
        if replica is None:
            logging.warning("Could not find replica info for %s", switchover_candidate)
            return False
        replay_lag = replica.get('replay_lag_msec')
        logging.info("Replica %s has replay lag %sms", switchover_candidate, replay_lag)
        if replay_lag is None:
            logging.warning("Could not get replay lag for replica %s", switchover_candidate)
            return False
        max_allowed_lag_ms = self.config.max_allowed_switchover_lag_ms
        if replay_lag > max_allowed_lag_ms:
            if not self.config.allow_potential_data_loss:
                logging.warning("Replica %s cannot be primary for switchover, max allowed lag %sms", switchover_candidate, max_allowed_lag_ms)
                return False
            else:
                logging.warning("Replica %s has replay lag %s and allow data loss", switchover_candidate, replay_lag)
        return True

    def _zk_alive_refresh(self, role, db_state, zk_state):
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

    def _store_replics_info(self, db_state, zk_state):
        tli_res = None
        if zk_state[self.zk.TIMELINE_INFO_PATH]:
            tli_res = zk_state[self.zk.TIMELINE_INFO_PATH] == db_state['timeline']

        replics_info = db_state.get('replics_info')

        zk_state['replics_info_written'] = None
        if tli_res and replics_info is not None:
            zk_state['replics_info_written'] = self.zk.write_replics_info(replics_info)
            self.write_host_stat(helpers.get_hostname(), db_state)
            return True

        return False
    
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
        switchover_catchup_timeout=config.getfloat('global', 'switchover_catchup_timeout'),
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
        # [debug]
        promote_checkpoint_sql=config.get('debug', 'promote_checkpoint_sql', fallback=None),
        failure_name=config.get('debug', 'failure_name', fallback=None),
        failure_count=int(config.get('debug', 'failure_count', fallback='100000000')),
        sleep_before_disable_walreceiver=config.getfloat('debug', 'sleep_before_disable_walreceiver', fallback=0),
        election_lsn_read_sleep=config.getfloat('debug', 'election_lsn_read_sleep', fallback=0),
        election_loser_timeout=config.getint('debug', 'election_loser_timeout', fallback=0),
    )


def create_pgconsul(config: RawConfigParser) -> 'Pgconsul':
    """Create all components and inject them into Pgconsul (ADR-0004)."""
    pgconsul_config = build_pgconsul_config(config)

    cmd_manager = create_command_manager(config)
    db = create_postgres(config=config, cmd_manager=cmd_manager)
    zk = create_zk(config=config)
    replication_manager = create_replication_manager(config, db, zk)
    slot_manager = create_replication_slot_manager(config, db, zk)
    timings = TimingTracker(zk, config.get('commands', 'log_timing', fallback=None))
    maintenance_handler = create_maintenance_handler(config, db, zk, replication_manager)

    return Pgconsul(
        config=pgconsul_config,
        db=db,
        zk=zk,
        cmd_manager=cmd_manager,
        replication_manager=replication_manager,
        slot_manager=slot_manager,
        timings=timings,
        maintenance_handler=maintenance_handler,
    )


# Backward-compat alias: tests and external code import `pgconsul` (lowercase).
pgconsul = Pgconsul
