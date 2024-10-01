"""
Main module. pgconsul class defined here.
"""
# encoding: utf-8

import atexit
import functools
import json
import logging
import os
import random
import signal
import sys
import time
import traceback

import psycopg2

from . import helpers, sdnotify
from .command_manager import CommandManager
from .failover_election import ElectionError, FailoverElection
from .helpers import IterationTimer, get_hostname
from .pg import Postgres
from .plugin import PluginRunner, load_plugins
from .replication_manager import QuorumReplicationManager, SingleSyncReplicationManager
from .zk import Zookeeper, ZookeeperException


class pgconsul(object):
    """
    pgconsul class
    """

    DESTRUCTIVE_OPERATIONS = ['rewind']

    def __init__(self, **kwargs):
        logging.debug('Initializing main class.')
        self.config = kwargs.get('config')
        self._cmd_manager = CommandManager(self.config)
        self._should_run = True
        self.is_in_maintenance = False

        random.seed(os.urandom(16))

        plugins = load_plugins(self.config.get('global', 'plugins_path'))

        self.db = Postgres(config=self.config, plugins=PluginRunner(plugins['Postgres']), cmd_manager=self._cmd_manager)
        self.zk = Zookeeper(config=self.config, plugins=PluginRunner(plugins['Zookeeper']))
        self.startup_checks()

        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.checks = {'primary_switch': 0, 'failover': 0, 'rewind': 0}
        self._is_single_node = False
        self.notifier = sdnotify.Notifier()
        self._slot_drop_countdown = {}

        if self.config.getboolean('global', 'quorum_commit'):
            self._replication_manager = QuorumReplicationManager(
                self.config,
                self.db,
                self.zk,
            )
        else:
            self._replication_manager = SingleSyncReplicationManager(
                self.config,
                self.db,
                self.zk,
            )

    def _sigterm_handler(self, *_):
        self._should_run = False

    def re_init_db(self):
        """
        Reinit db connection
        """
        try:
            if not self.db.is_alive():
                db_state = self.db.get_state()
                logging.error(
                    'Could not get data from PostgreSQL. Seems, '
                    'that it is dead. Getting last role from cached '
                    'file. And trying to reconnect.'
                )
                if db_state.get('prev_state'):
                    self.db.role = db_state['prev_state']['role']
                    self.db.pg_version = db_state['prev_state']['pg_version']
                    self.db.pgdata = db_state['prev_state']['pgdata']
                self.db.reconnect()
        except KeyError:
            logging.error('Could not get data from PostgreSQL and cache-file. Exiting.')
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            sys.exit(1)
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())

    def re_init_zk(self):
        """
        Reinit zk connection
        """
        try:
            if not self.zk.is_alive():
                logging.warning('Some error with ZK client. Trying to reconnect.')
                self.zk.reconnect()
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())

    def startup_checks(self):
        """
        Perform some basic checks on startup
        """
        work_dir = self.config.get('global', 'working_dir')
        fname = '%s/.pgconsul_rewind_fail.flag' % work_dir

        if os.path.exists(fname):
            logging.error('Rewind fail flag exists. Exiting.')
            sys.exit(1)

        if self.db.is_alive() and not self.zk.is_alive():
            _, pooler_service_running = self.db.pgpooler('status')
            if self.db.role == 'primary' and pooler_service_running:
                self.db.pgpooler('stop')

        if not self.db.is_alive() and self.zk.is_alive():
            if self.zk.get_current_lock_holder() == helpers.get_hostname():
                res = self.zk.release_lock()
                if res:
                    logging.info('Released lock in ZK since postgres is dead.')

        db_state = self.db.get_state()
        if db_state['prev_state'] is not None:
            # Ok, it means that current start is not the first one.
            # In this case we should check that we are able to do pg_rewind.
            if not db_state['alive']:
                self.db.pgdata = db_state['prev_state']['pgdata']
            if not self.db.is_ready_for_pg_rewind():
                sys.exit(0)

        # Abort startup if zk.MEMBERS_PATH is empty
        # (no one is participating in cluster), but
        # timeline indicates a mature (tli>1) and  operating database system.
        tli = self.db.get_state().get('timeline', 0)
        if not self._get_zk_members() and tli > 1:
            logging.error(
                'ZK "%s" empty but timeline indicates operating cluster (%i > 1)',
                self.zk.MEMBERS_PATH,
                tli,
            )
            self.db.pgpooler('stop')
            sys.exit(1)

        if (
            self.config.getboolean('global', 'quorum_commit')
            and not self.config.getboolean('global', 'use_lwaldump')
            and not self.config.getboolean('replica', 'allow_potential_data_loss')
        ):
            logging.error("Using quorum_commit allow only with use_lwaldump or with allow_potential_data_loss")
            exit(1)

        if (
            self.db.is_alive()
            and not self.db.check_extension_installed('lwaldump')
            and self.config.getboolean('global', 'use_lwaldump')
        ):
            logging.error("lwaldump is not installed")
            exit(1)

        if self.db.is_alive() and not self.db.ensure_archive_mode():
            logging.error("archive mode is not enabled on instance - pgconsul support only archive mode yet ")
            exit(1)

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

        if not self.config.getboolean('global', 'update_prio_in_zk') and helpers.get_hostname() in self.zk.get_children(
            self.zk.MEMBERS_PATH
        ):
            logging.info("Don't have to write priority to ZK")
            return True

        return self.zk.ensure_path(self.zk.get_host_prio_path()) and self.zk.noexcept_write(
            self.zk.get_host_prio_path(), my_prio, need_lock=False
        )

    def start(self):
        """
        Start iterations
        """
        if (not self.config.getboolean('global', 'use_replication_slots') and
                self.config.getboolean('global', 'replication_slots_polling')):
            logging.warning('Force disable replication_slots_polling because use_replication_slots is disabled.')
            self.config.set('global', 'replication_slots_polling', 'no')

        my_prio = self.config.get('global', 'priority')
        self.notifier.ready()
        while True:
            if self._init_zk(my_prio):
                break
            logging.error('Failed to init ZK')
            self.re_init_zk()

        while self._should_run:
            try:
                self.run_iteration(my_prio)
            except Exception:
                for line in traceback.format_exc().split('\n'):
                    logging.error(line.rstrip())
        self.stop()

    def update_maintenance_status(self, role, primary_fqdn):
        maintenance_status = self.zk.get(self.zk.MAINTENANCE_PATH)  # can be None, 'enable', 'disable'

        if maintenance_status == 'enable':
            # maintenance node exists with 'enable' value, we are in maintenance now
            self.is_in_maintenance = True
            if role == 'primary' and self._update_replication_on_maintenance_enter() and not self._is_single_node:
                return
            # Write current ts to zk on maintenance enabled, it's be dropped on disable
            maintenance_ts = self.zk.get(self.zk.MAINTENANCE_TIME_PATH)
            if maintenance_ts is None:
                self.zk.write(self.zk.MAINTENANCE_TIME_PATH, time.time(), need_lock=False)
            # Write current primary to zk on maintenance enabled, it's be dropped on disable
            current_primary = self.zk.get(self.zk.MAINTENANCE_PRIMARY_PATH)
            if current_primary is None and primary_fqdn is not None:
                self.zk.write(self.zk.MAINTENANCE_PRIMARY_PATH, primary_fqdn, need_lock=False)
        elif maintenance_status == 'disable':
            # maintenance node exists with 'disable' value, we are not in maintenance now
            # and should delete this node. We delete it recursively, we don't won't to wait
            # all cluster members to delete each own node, because some of them may be
            # already dead and we can wait it infinitely. Maybe we should wait each member
            # with timeout and then delete recursively (TODO).
            logging.debug('Disabling maintenance mode, deleting maintenance node')
            self.zk.delete(self.zk.MAINTENANCE_PATH, recursive=True)
            self.is_in_maintenance = False
        elif maintenance_status is None:
            # maintenance node doesn't exists, we are not in maintenance mode
            self.is_in_maintenance = False

    def _update_replication_on_maintenance_enter(self):
        if not self.config.getboolean('primary', 'change_replication_type'):
            # Replication type change is restricted, we do nothing here
            return True
        if self.config.getboolean('primary', 'sync_replication_in_maintenance'):
            # It is allowed to have sync replication in maintenance here
            return True
        current_replication = self.db.get_replication_state()
        if current_replication[0] == 'async':
            # Ok, it is already async
            return True
        return self._replication_manager.change_replication_to_async()

    def run_iteration(self, my_prio):
        timer = IterationTimer()
        _, terminal_state = self.db.is_alive_and_in_terminal_state()
        if not terminal_state:
            logging.debug('Database is starting up or shutting down')
        role = self.db.get_role()
        logging.debug('Role: %s', str(role))

        db_state = self.db.get_state()
        self.notifier.notify()
        logging.debug(db_state)
        try:
            zk_state = self.zk.get_state()
            logging.debug(zk_state)
            helpers.write_status_file(db_state, zk_state, self.config.get('global', 'working_dir'))
            self.update_maintenance_status(role, db_state.get('primary_fqdn'))
            self._zk_alive_refresh(role, db_state, zk_state)
            if self.is_in_maintenance:
                logging.warning('Cluster in maintenance mode')
                self.zk.reconnect()
                self.zk.write(self.zk.get_host_maintenance_path(), 'enable')
                logging.debug('Finished iteration.')
                timer.sleep(self.config.getfloat('global', 'iteration_timeout'))
                return
        except ZookeeperException:
            logging.error("Zookeeper exception while getting ZK state")
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            if role == 'primary' and not self.is_in_maintenance and not self._is_single_node:
                logging.error("Upper exception was for primary")
                my_hostname = helpers.get_hostname()
                self.resolve_zk_primary_lock(my_hostname)
            else:
                self.re_init_zk()
            return
        stream_from = self.config.get('global', 'stream_from')
        if role is None:
            is_dead = terminal_state
            self.dead_iter(db_state, zk_state, is_actually_dead=is_dead)
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
        self.re_init_zk()

        # Dead PostgreSQL probably means
        # that our node is being removed.
        # No point in updating all_hosts
        # in this case
        all_hosts = self.zk.get_children(self.zk.MEMBERS_PATH)
        prio = self.zk.noexcept_get(self.zk.get_host_prio_path())
        if role and all_hosts and not prio:
            if not self.zk.noexcept_write(self.zk.get_host_prio_path(), my_prio, need_lock=False):
                logging.warning('Could not write priority to ZK')

        logging.debug('Finished iteration.')
        timer.sleep(self.config.getfloat('global', 'iteration_timeout'))

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
        logging.info('primary is in single node state')
        self.zk.try_acquire_lock()

        self._store_replics_info(db_state, zk_state)

        self.zk.write(self.zk.TIMELINE_INFO_PATH, db_state['timeline'])

        pooler_port_available, pooler_service_running = self.db.pgpooler('status')
        if pooler_service_running and not pooler_port_available:
            logging.warning('Service alive, but pooler not accepting connections, restarting.')
            self.db.pgpooler('stop')
            self.db.pgpooler('start')
        elif not pooler_service_running:
            logging.debug('Here we should open for load.')
            self.db.pgpooler('start')

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
            stream_from = self.config.get('global', 'stream_from')
            last_op = self.zk.get('%s/%s/op' % (self.zk.MEMBERS_PATH, my_hostname))
            # If we were promoting or rewinding
            # and failed we should not acquire lock
            if self.is_op_destructive(last_op):
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
                # Make sure local timeline corresponds to that of the cluster.
                if not self._verify_timeline(db_state, zk_state, without_leader_lock=True):
                    return None

            if not self.zk.try_acquire_lock():
                self.resolve_zk_primary_lock(my_hostname)
                return None
            self.zk.write(self.zk.LAST_PRIMARY_AVAILABILITY_TIME, time.time())

            self._reset_simple_primary_switch_try()

            self.checks['primary_switch'] = 0

            self._handle_slots()

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
            if zk_state[self.zk.FAILOVER_INFO_PATH] in ('promoting', 'checkpointing'):
                if zk_state[self.zk.CURRENT_PROMOTING_HOST] == helpers.get_hostname():
                    self.reset_failover_node(zk_state)
                    return None  # so zk_state will be updated in the next iter
                else:
                    logging.info(
                        'Failover state was "%s" and last promoted host was "%s"',
                        zk_state[self.zk.FAILOVER_INFO_PATH],
                        zk_state[self.zk.CURRENT_PROMOTING_HOST],
                    )
                    return self.release_lock_and_return_to_cluster()

            self._drop_stale_switchover(db_state)

            pooler_port_available, pooler_service_running = self.db.pgpooler('status')
            if pooler_service_running and not pooler_port_available:
                logging.warning('Service alive, but pooler not accepting connections, restarting.')
                self.db.pgpooler('stop')
                self.db.pgpooler('start')
            elif not pooler_service_running:
                logging.debug('Here we should open for load.')
                self.db.pgpooler('start')

            # Ensure that wal archiving is enabled. It can be disabled earlier due to
            # some zk connectivity issues.
            self.db.ensure_archiving_wal()

            # Check if replication type (sync/normal) change is needed.
            ha_replics_config = self._get_ha_replics()
            if ha_replics_config is None:
                return None
            try:
                logging.debug('Checking ha replics for aliveness')
                alive_hosts = self.zk.get_alive_hosts(timeout=3, catch_except=False)
                ha_replics = {replica for replica in ha_replics_config if replica in alive_hosts}
            except Exception:
                logging.exception('Fail to get replica status')
                ha_replics = ha_replics_config
            if len(ha_replics) != len(ha_replics_config):
                logging.debug(
                    'Some of the replics is unavailable, config replics % alive replics %s',
                    str(ha_replics_config),
                    str(ha_replics),
                )
            logging.debug('Checking if changing replication type is needed.')
            change_replication = self.config.getboolean('primary', 'change_replication_type')
            if change_replication:
                self._replication_manager.update_replication_type(db_state, ha_replics)

            # Check if scheduled switchover conditions exists
            # and local cluster state can handle switchover.
            if not self._check_primary_switchover(db_state, zk_state):
                return None

            # Perform switchover: shutdown user service,
            # release lock, write state.
            if not self._do_primary_switchover(zk_state):
                return None

            # Ensure that new primary will appear in time,
            # and transition current instance to replica.
            # Rollback state if this does not happen.
            if not self._transition_primary_switchover():
                # wait while replica free primary lock
                limit = self.config.getfloat('global', 'postgres_timeout')

                def wait_for_no_one_holds_primary_lock():
                    primary = self.zk.get_current_lock_holder()
                    logging.debug("Current primary lock holder: {0}".format(primary))
                    return primary is None

                return helpers.await_for(wait_for_no_one_holds_primary_lock, limit, 'no-one holds primary lock')

        except ZookeeperException:
            if not self.zk.try_acquire_lock():
                logging.error("Zookeeper error during primary iteration:")
                self.resolve_zk_primary_lock(my_hostname)
                return None
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return None

    def reset_failover_node(self, zk_state):
        if (
            self.zk.get(self.zk.FAILOVER_INFO_PATH) == 'finished'
            or self.zk.write(self.zk.FAILOVER_INFO_PATH, 'finished')
        ) and self.zk.delete(self.zk.CURRENT_PROMOTING_HOST):
            self.zk.delete(self.zk.FAILOVER_MUST_BE_RESET)
            logging.info('Resetting failover info (was "%s", now "finished")', zk_state[self.zk.FAILOVER_INFO_PATH])
        else:
            self.zk.ensure_path(self.zk.FAILOVER_MUST_BE_RESET)
            logging.info('Resetting failover failed, will try on next iteration.')

    def resolve_zk_primary_lock(self, my_hostname):
        holder = self.zk.get_current_lock_holder()
        if holder is None:
            if self._replication_manager.should_close():
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

    def write_host_stat(self, hostname, db_state):
        stream_from = self.config.get('global', 'stream_from')
        replics_info = db_state.get('replics_info')
        wal_receiver_info = db_state['wal_receiver']
        host_path = '{member_path}/{hostname}'.format(member_path=self.zk.MEMBERS_PATH, hostname=hostname)
        replics_info_path = '{host_path}/replics_info'.format(host_path=host_path)
        ha_path = '{host_path}/ha'.format(host_path=host_path)
        wal_receiver_path = '{host_path}/wal_receiver'.format(host_path=host_path)
        if not stream_from:
            if not self.zk.ensure_path(ha_path):
                logging.warning('Could not write ha host in ZK.')
                return False
        else:
            if self.zk.exists_path(ha_path) and not self.zk.delete(ha_path):
                logging.warning('Could not delete ha host in ZK.')
                return False
        if wal_receiver_info is not None:
            if not self.zk.write(wal_receiver_path, wal_receiver_info, preproc=json.dumps, need_lock=False):
                logging.warning('Could not write host wal_receiver_info to ZK.')
                return False
        if replics_info is not None:
            if not self.zk.write(replics_info_path, replics_info, preproc=json.dumps, need_lock=False):
                logging.warning('Could not write host replics_info to ZK.')
                return False

    def remove_stale_operation(self, hostname):
        op_path = '%s/%s/op' % (self.zk.MEMBERS_PATH, hostname)
        last_op = self.zk.noexcept_get(op_path)
        if self.is_op_destructive(last_op):
            logging.warning('Stale operation %s detected. Removing track from zk.', last_op)
            self.zk.delete(op_path)

    def start_pooler(self):
        start_pooler = self.config.getboolean('replica', 'start_pooler')
        _, pooler_service_running = self.db.pgpooler('status')
        if not pooler_service_running and start_pooler:
            self.db.pgpooler('start')

    def get_replics_info(self, zk_state):
        stream_from = self.config.get('global', 'stream_from')
        if stream_from:
            replics_info_path = '{member_path}/{hostname}/replics_info'.format(
                member_path=self.zk.MEMBERS_PATH, hostname=stream_from
            )
            replics_info = self.zk.noexcept_get(replics_info_path, preproc=json.loads)
        else:
            replics_info = zk_state[self.zk.REPLICS_INFO_PATH]
        return replics_info

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

        self.checks['failover'] = 0
        limit = self.config.getfloat('replica', 'recovery_timeout')

        # Try to resume WAL replaying, it can be paused earlier
        self.db.pg_wal_replay_resume()

        if not self._check_archive_recovery(limit) and not self._wait_for_streaming(limit):
            # Wal receiver is not running and
            # postgresql isn't in archive recovery
            # We should try to restart
            logging.warning('We should try switch primary one more time here.')
            return self._return_to_cluster(holder, 'replica', is_dead=False)

    def _get_streaming_replica_from_replics_info(self, fqdn, replics_info):
        if not replics_info:
            return None
        app_name = helpers.app_name_from_fqdn(fqdn)
        for replica in replics_info:
            if replica['application_name'] == app_name and replica['state'] == 'streaming':
                return replica
        return None

    def non_ha_replica_iter(self, db_state, zk_state):
        try:
            logging.info('Current replica is non ha.')
            if not zk_state['alive']:
                return None
            my_hostname = helpers.get_hostname()
            self.remove_stale_operation(my_hostname)
            self.write_host_stat(my_hostname, db_state)
            stream_from = self.config.get('global', 'stream_from')
            can_delayed = self.config.getboolean('replica', 'can_delayed')
            replics_info = self.get_replics_info(zk_state) or []
            self.checks['failover'] = 0
            streaming = self._get_streaming_replica_from_replics_info(my_hostname, replics_info) and bool(
                db_state['wal_receiver']
            )
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

            if streaming_from_primary and not streaming:
                self._acquire_replication_source_slot_lock(current_primary)
            if streaming:
                self._acquire_replication_source_slot_lock(stream_from)
            elif not can_delayed:
                logging.warning('Seems that we are not really streaming WAL from %s.', stream_from)
                self._replication_manager.leave_sync_group()
                replication_source_is_dead = self._check_host_is_really_dead(primary=stream_from)
                replication_source_replica_info = self._get_streaming_replica_from_replics_info(
                    stream_from, zk_state.get(self.zk.REPLICS_INFO_PATH)
                )
                wal_receiver_info = self._zk_get_wal_receiver_info(stream_from)
                replication_source_streams = bool(
                    wal_receiver_info and wal_receiver_info[0].get('status') == 'streaming'
                )
                logging.error(replication_source_replica_info)

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
            self.checks['primary_switch'] = 0
            self.start_pooler()
            self._reset_simple_primary_switch_try()
            self._handle_slots()
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return None

    def _accept_switchover(self, lock_holder, previous_primary):
        if not self._can_do_switchover():
            return None

        # WARNING: we shouldn't allow multiple hosts to enter this branch
        if not self.zk.write(self.zk.SWITCHOVER_STATE_PATH, 'candidate_found', need_lock=False):
            logging.error('Failed to state that we are the new primary candidate in ZK.')
            return None

        #
        # All checks are done. Waiting for primary shutdown, acquiring the lock in ZK,
        # promoting and writing last switchover timestamp to ZK.
        #
        limit = self.config.getfloat('global', 'postgres_timeout')
        # Current primary is lock holder. Otherwise consider last primary as current.
        current_primary = lock_holder or previous_primary
        if current_primary is not None and not helpers.await_for(
            lambda: self._check_primary_is_really_dead(current_primary), limit, 'primary is down'
        ):
            return None

        # Wait switchover_master_shut state only if current primary is alive, i.e. lock holder exists.
        if lock_holder is not None and not helpers.await_for(
            lambda: self.zk.get(self.zk.FAILOVER_INFO_PATH) == 'switchover_master_shut',
            limit,
            'failover state is switchover_master_shut',
        ):
            # Mark switchover node as failure
            self.zk.write(self.zk.SWITCHOVER_STATE_PATH, 'master_timed_out', need_lock=False)
            return False

        if not self.zk.try_acquire_lock(allow_queue=True, timeout=limit):
            logging.info('Could not acquire lock in ZK. Not doing anything.')
            return None

        if not self._do_failover():
            return False

        self._cleanup_switchover()
        self.zk.write(self.zk.LAST_SWITCHOVER_TIME_PATH, time.time())

    def replica_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is replica
        """
        try:
            if not zk_state['alive']:
                return None
            my_hostname = helpers.get_hostname()
            my_app_name = helpers.app_name_from_fqdn(my_hostname)
            self.remove_stale_operation(my_hostname)
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

            if self._detect_replica_switchover():
                logging.warning('Planned switchover condition detected')
                self._replication_manager.enter_sync_group(replica_infos=replics_info)
                return self._accept_switchover(holder, db_state.get('primary_fqdn'))

            # If there is no primary lock holder and it is not a switchover
            # then we should consider current cluster state as failover.
            if holder is None:
                logging.error('According to ZK primary has died. We should verify it and do failover if possible.')
                return self._accept_failover()

            self.checks['failover'] = 0

            if holder != db_state['primary_fqdn'] and holder != my_hostname:
                self._replication_manager.leave_sync_group()
                return self.change_primary(db_state, holder)
            self._acquire_replication_source_slot_lock(holder)

            self.db.ensure_replaying_wal()

            if not streaming:
                logging.warning('Seems that we are not really streaming WAL from %s.', holder)
                self._replication_manager.leave_sync_group()

                return self.replica_return(db_state, zk_state)

            self.checks['primary_switch'] = 0

            self.start_pooler()
            self._reset_simple_primary_switch_try()

            self._replication_manager.enter_sync_group(replica_infos=replics_info)
            self._handle_slots()
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return None

    def dead_iter(self, db_state, zk_state, is_actually_dead):
        """
        Iteration if local postgresql is dead
        """
        if not zk_state['alive'] or db_state['alive']:
            return None

        self.db.pgpooler('stop')

        self._replication_manager.leave_sync_group()
        self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)

        role = self.db.role
        last_tli = self.db.get_data_from_control_file('Latest checkpoint.s TimeLineID', preproc=int, log=False)
        last_primary = None
        if role == 'replica' and db_state.get('prev_state'):
            last_primary = db_state['prev_state'].get('primary_fqdn')

        holder = self.zk.get_current_lock_holder()
        if holder and holder != helpers.get_hostname():
            if role == 'replica' and holder == last_primary:
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
            return self._return_to_cluster(holder, role, is_dead=is_actually_dead)

        else:
            #
            # The only case we get here is absence of primary (no one holds the
            # lock) and our PostgreSQL is dead.
            #
            logging.error('Seems that all hosts (including me) are dead. Trying to start PostgreSQL.')
            if role == 'primary':
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
            switchover_info = self.zk.get(self.zk.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)
            if not switchover_info:
                return
            switchover_state = self.zk.get(self.zk.SWITCHOVER_STATE_PATH)
            if (
                switchover_state != 'scheduled'
                or switchover_info.get(self.zk.TIMELINE_INFO_PATH) is None
                or switchover_info[self.zk.TIMELINE_INFO_PATH] < db_state['timeline']
            ):
                logging.warning('Dropping stale switchover')
                logging.debug(
                    'Switchover info: state %s; info %s; db timeline %s',
                    switchover_state,
                    switchover_info,
                    db_state['timeline'],
                )
                self._cleanup_switchover()
        finally:
            # We want to release this lock regardless of what happened in 'try' block
            self.zk.release_lock(self.zk.SWITCHOVER_LOCK_PATH)

    def _cleanup_switchover(self):
        self.zk.delete(self.zk.SWITCHOVER_LSN_PATH)
        self.zk.delete(self.zk.SWITCHOVER_PRIMARY_PATH)
        self.zk.delete(self.zk.SWITCHOVER_STATE_PATH)
        self.zk.delete(self.zk.FAILOVER_INFO_PATH)

    def _update_single_node_status(self, role):
        """
        In case if current role is 'primary', we should determine new status
        and update it locally and in ZK.
        Otherwise, we should just update the status from ZK
        """
        if role == 'primary':
            ha_hosts = self.zk.get_ha_hosts()
            if ha_hosts is None:
                logging.error('Failed to update single node status because of empty ha host list.')
                return
            self._is_single_node = len(ha_hosts) == 1
            if self._is_single_node:
                self.zk.ensure_path(self.zk.SINGLE_NODE_PATH)
            else:
                self.zk.delete(self.zk.SINGLE_NODE_PATH)
        else:
            self._is_single_node = self.zk.exists_path(self.zk.SINGLE_NODE_PATH)

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
            self.zk.write(self.zk.TIMELINE_INFO_PATH, db_state['timeline'])
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
                #
                # This timeout is needed for primary with newer timeline
                # to acquire the lock in ZK.
                #
                time.sleep(10 * self.config.getfloat('global', 'iteration_timeout'))
                return None
            elif zk_tli and zk_tli < db_tli:
                if without_leader_lock:
                    return True
                logging.warning('Timeline in ZK is older than ours. Updating it it ZK.')
                self.zk.write(self.zk.TIMELINE_INFO_PATH, db_tli)
        logging.debug('Timeline verification succeeded')
        return True

    def _reset_simple_primary_switch_try(self):
        simple_primary_switch_path = self.zk.get_simple_primary_switch_try_path(get_hostname())
        if self.zk.noexcept_get(simple_primary_switch_path) != 'no':
            self.zk.noexcept_write(simple_primary_switch_path, 'no', need_lock=False)

    def _set_simple_primary_switch_try(self):
        simple_primary_switch_path = self.zk.get_simple_primary_switch_try_path(get_hostname())
        self.zk.noexcept_write(simple_primary_switch_path, 'yes', need_lock=False)

    def _is_simple_primary_switch_tried(self):
        if self.zk.noexcept_get(self.zk.get_simple_primary_switch_try_path(get_hostname())) == 'yes':
            return True
        return False

    def _try_simple_primary_switch_with_lock(self, *args, **kwargs):
        if not self.config.getboolean('global', 'do_consecutive_primary_switch'):
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
        primary_switch_checks = self.config.getint('replica', 'primary_switch_checks')
        need_restart = self.config.getboolean('replica', 'primary_switch_restart')

        logging.info('Starting simple primary switch.')
        if self.checks['primary_switch'] >= primary_switch_checks:
            self._set_simple_primary_switch_try()

        if need_restart and not is_dead and self.db.stop_postgresql(timeout=limit) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            self.checks['primary_switch'] = 0
            return True

        if self.db.recovery_conf('create', new_primary) != 0:
            logging.error('Could not generate recovery.conf. Will retry.')
            self.checks['primary_switch'] = 0
            return True

        if not is_dead and not need_restart:
            if not self.db.reload():
                logging.error('Could not reload PostgreSQL. Skipping it.')
            self.db.ensure_replaying_wal()
        else:
            if self.db.start_postgresql() != 0:
                logging.error('Could not start PostgreSQL. Skipping it.')

        if self._wait_for_recovery(limit) and self._check_archive_recovery(limit):
            #
            # We have reached consistent state but there is a small
            # chance that we are not streaming changes from new primary
            # with: "new timeline N forked off current database system
            # timeline N-1 before current recovery point M".
            # Checking it with the info from ZK.
            #
            if self._wait_for_streaming(limit, new_primary):
                #
                # The easy way succeeded.
                #
                logging.info('Simple primary switch succeeded.')
                return True
            else:
                return False

    def _rewind_from_source(self, is_postgresql_dead, limit, new_primary):
        logging.info("Starting pg_rewind")

        # Trying to connect to a new_primary. If not succeeded - exiting
        if not helpers.await_for(
            lambda: not self._check_host_is_really_dead(new_primary),
            limit,
            'source database alive and ready for rewind',
        ):
            return None

        if not self.zk.write('%s/%s/op' % (self.zk.MEMBERS_PATH, helpers.get_hostname()), 'rewind', need_lock=False):
            logging.error('Unable to save destructive op state: rewind')
            return None

        self.db.pgpooler('stop')

        if not is_postgresql_dead and self.db.stop_postgresql(timeout=limit) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            return None

        self.checks['rewind'] += 1
        if self.db.do_rewind(new_primary) != 0:
            logging.error('Error while using pg_rewind. Will retry.')
            return True

        # Rewind has finished successfully so we can drop its operation node
        self.zk.delete('%s/%s/op' % (self.zk.MEMBERS_PATH, helpers.get_hostname()))
        return self._attach_to_primary(new_primary, limit)

    def _attach_to_primary(self, new_primary, limit):
        """
        Generate recovery.conf and start PostgreSQL.
        """
        logging.info('Converting role to replica of %s.', new_primary)
        if self.db.recovery_conf('create', new_primary) != 0:
            logging.error('Could not generate recovery.conf. Will retry.')
            self.checks['primary_switch'] = 0
            return None

        if self.db.start_postgresql() != 0:
            logging.error('Could not start PostgreSQL. Skipping it.')

        if not self._wait_for_recovery(limit):
            self.checks['primary_switch'] = 0
            return None

        if not self._wait_for_streaming(limit):
            self.checks['primary_switch'] = 0
            return None

        logging.info('Seems, that returning to cluster succeeded. Unbelievable!')
        self.db.checkpoint()
        return True

    def _handle_slots(self):
        if not self.config.getboolean('global', 'replication_slots_polling'):
            return

        my_hostname = helpers.get_hostname()
        try:
            slot_lock_holders = set(self.zk.get_lock_contenders(os.path.join(self.zk.HOST_REPLICATION_SOURCES, my_hostname), read_lock=True, catch_except=False))
        except Exception as e:
            logging.warning(
                'Could not get slot lock holders. %s'
                'Can not handle replication slots. We will skip it this time', e
            )
            return
        all_hosts = self.zk.get_children(self.zk.MEMBERS_PATH)
        if not all_hosts:
            logging.warning(
                'Could not get all hosts list from ZK.'
                'Can not handle replication slots. We will skip it this time'
            )
            return
        non_holders_hosts = []

        for host in all_hosts:
            if host in slot_lock_holders:
                self._slot_drop_countdown[host] = self.config.getint('global', 'drop_slot_countdown')
            else:
                if host not in self._slot_drop_countdown:
                    self._slot_drop_countdown[host] = self.config.getint('global', 'drop_slot_countdown')
                self._slot_drop_countdown[host] -= 1
                if self._slot_drop_countdown[host] < 0:
                    non_holders_hosts.append(host)

        # create slots
        slot_names = [helpers.app_name_from_fqdn(fqdn) for fqdn in slot_lock_holders]

        if not self.db.replication_slots('create', slot_names):
            logging.warning('Could not create replication slots. %s', slot_names)

        # drop slots
        if my_hostname in non_holders_hosts:
            non_holders_hosts.remove(my_hostname)
        slot_names_to_drop = [helpers.app_name_from_fqdn(fqdn) for fqdn in non_holders_hosts]
        if not self.db.replication_slots('drop', slot_names_to_drop):
            logging.warning('Could not drop replication slots. %s', slot_names_to_drop)

    def _get_db_state(self):
        state = self.db.get_data_from_control_file('Database cluster state')
        if not state or state == '':
            logging.error('Could not get info from controlfile about current cluster state.')
            return None
        logging.info('Database cluster state is: %s' % state)
        return state

    def _acquire_replication_source_slot_lock(self, source):
        if not self.config.getboolean('global', 'replication_slots_polling'):
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
        # And acquire lock (then new_primary will create replication slot)
        self.zk.acquire_lock(os.path.join(self.zk.HOST_REPLICATION_SOURCES, source), read_lock=True)

    def _return_to_cluster(self, new_primary, role, is_dead=False):
        """
        Return to cluster (try stupid method, if it fails we try rewind)
        """
        logging.info('Starting returning to cluster.')
        if self.checks['primary_switch'] >= 0:
            self.checks['primary_switch'] += 1
        else:
            self.checks['primary_switch'] = 1
        logging.debug("primary_switch checks is %d", self.checks['primary_switch'])

        self._acquire_replication_source_slot_lock(new_primary)
        failover_state = self.zk.noexcept_get(self.zk.FAILOVER_INFO_PATH)
        if failover_state is not None and failover_state not in ('finished', 'promoting', 'checkpointing'):
            logging.info(
                'We are not able to return to cluster since failover is still in progress - %s.', failover_state
            )
            return None

        limit = self.config.getfloat('replica', 'recovery_timeout')
        try:
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
            logging.info('Last op is: %s' % str(last_op))
            if role != 'primary' and not self.is_op_destructive(last_op) and not self._is_simple_primary_switch_tried():
                logging.info('Trying to do a simple primary switch.')
                result = self._try_simple_primary_switch_with_lock(limit, new_primary, is_dead)
                logging.info('Primary switch count: %s finish with result: %s', self.checks['primary_switch'], result)
                return None

            #
            # If our rewind attempts fail several times
            # we should create special flag-file, stop posgresql and then exit.
            #
            max_rewind_retries = self.config.getint('global', 'max_rewind_retries')
            if self.checks['rewind'] > max_rewind_retries:
                self.db.pgpooler('stop')
                self.db.stop_postgresql(timeout=limit)
                work_dir = self.config.get('global', 'working_dir')
                fname = '%s/.pgconsul_rewind_fail.flag' % work_dir
                with open(fname, 'w') as fobj:
                    fobj.write(str(time.time()))
                logging.error('Could not rewind %d times. Exiting.', max_rewind_retries)
                sys.exit(1)

            #
            # The hard way starts here.
            #
            if not self._rewind_from_source(is_dead, limit, new_primary):
                return None

        except Exception:
            logging.error('Unexpected error while trying to return to cluster. Exiting.')
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            sys.exit(1)

    def _promote(self):
        if not self.zk.write(self.zk.FAILOVER_INFO_PATH, 'promoting'):
            logging.error('Could not write failover state to ZK.')
            return False

        if not self.zk.write(self.zk.CURRENT_PROMOTING_HOST, helpers.get_hostname()):
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
                if not self.zk.delete(self.zk.CURRENT_PROMOTING_HOST):
                    logging.error('Could not remove self as current promoting host.')
                if not self.zk.write(self.zk.FAILOVER_INFO_PATH, 'finished'):
                    logging.error('Could not write failover state to ZK.')
                return False

            logging.info('Promote command failed but we are current primary. Continue')

        self._slot_drop_countdown = {}

        if not self.zk.noexcept_write(self.zk.FAILOVER_INFO_PATH, 'checkpointing'):
            logging.warning('Could not write failover state to ZK.')

        logging.debug('Doing checkpoint after promoting.')
        if not self.db.checkpoint(query=self.config.get('debug', 'promote_checkpoint_sql', fallback=None)):
            logging.warning('Could not checkpoint after failover.')

        my_tli = self.db.get_data_from_control_file('Latest checkpoint.s TimeLineID', preproc=int, log=False)

        if not self.zk.write(self.zk.TIMELINE_INFO_PATH, my_tli):
            logging.warning('Could not write timeline to ZK.')

        if not self.zk.write(self.zk.FAILOVER_INFO_PATH, 'finished'):
            logging.error('Could not write failover state to ZK.')

        if not self.zk.delete(self.zk.CURRENT_PROMOTING_HOST):
            logging.error('Could not remove self as current promoting host.')

        return True

    def _promote_handle_slots(self):
        if self.config.getboolean('global', 'use_replication_slots'):
            if not self.zk.write(self.zk.FAILOVER_INFO_PATH, 'creating_slots'):
                logging.warning('Could not write failover state to ZK.')

            hosts = self._get_ha_replics()
            if hosts is None:
                logging.error(
                    'Could not get all hosts list from ZK. '
                    'Replication slots should be created but we '
                    'are unable to do it. Releasing the lock.'
                )
                return False
            # Create replication slots, regardless of whether replicas hold DCS locks for replication slots.
            hosts = [helpers.app_name_from_fqdn(fqdn) for fqdn in hosts]
            if not self.db.replication_slots('create', hosts):
                logging.error('Could not create replication slots. Releasing the lock in ZK.')
                return False

        return True

    def _check_my_timeline_sync(self):
        my_tli = self.db.get_data_from_control_file('Latest checkpoint.s TimeLineID', preproc=int, log=False)
        try:
            zk_tli = self.zk.get(self.zk.TIMELINE_INFO_PATH, preproc=int)
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
        try:
            last_failover_ts = self.zk.get(self.zk.LAST_FAILOVER_TIME_PATH, preproc=float)
        except ZookeeperException:
            logging.error('Can\'t get last failover time from ZK.')
            return False

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
        previous_primary_availability_time = self.zk.noexcept_get(self.zk.LAST_PRIMARY_AVAILABILITY_TIME, preproc=float)
        if previous_primary_availability_time is None:
            logging.error('Failed to get last primary availability time.')
            return False
        time_passed = time.time() - previous_primary_availability_time
        if time_passed < self.config.getfloat('replica', 'primary_unavailability_timeout'):
            logging.info('Last time we seen primary %f seconds ago, not doing anything.', time_passed)
            return False
        return True

    def _is_older_then_primary(self):
        try:
            lsn = self.zk.get(self.zk.SWITCHOVER_LSN_PATH)
            # If there is no lsn in ZK it means that primary is dead
            if lsn is None:
                return True
            # Our LSN should be greater than LSN in primary's pg_control
            # because of shutdown record. For more info about address:
            # https://www.postgresql.org/message-id/flat/A7683985-2EC2-40AD-AAAC-B44BD0F29723%40simply.name
            return self.db.get_replay_diff(lsn) > 0
        except ZookeeperException:
            return False

    def _can_do_failover(self):
        autofailover = self.config.getboolean('global', 'autofailover')

        if not autofailover:
            logging.info("Autofailover is disabled. Not doing anything.")
            return False

        if not self._check_my_timeline_sync():
            return False

        if not self._check_last_failover_timeout():
            return False
        if not self._check_host_is_really_dead():
            logging.warning(
                'According to ZK primary has died but it is still accessible through libpq. Not doing anything.'
            )
            return False
        if not self._check_primary_unavailability_timeout():
            return False
        if self.db.is_replaying_wal(self.config.getfloat('global', 'iteration_timeout')):
            logging.info("Host is still replaying WAL, so it can't be promoted.")
            return False

        replica_infos = self.zk.noexcept_get(self.zk.REPLICS_INFO_PATH, preproc=json.loads)
        if replica_infos is None:
            logging.error('Unable to get replics info from ZK.')
            return False

        allow_data_loss = self.config.getboolean('replica', 'allow_potential_data_loss')
        logging.info(f'Data loss is: {allow_data_loss}')
        is_promote_safe = self._replication_manager.is_promote_safe(
            self.zk.get_alive_hosts(),
            replica_infos=replica_infos,
        )
        if not allow_data_loss and not is_promote_safe:
            logging.warning('Promote is not allowed with given configuration.')
            return False
        self.db.pg_wal_replay_pause()
        election_timeout = self.config.getint('global', 'election_timeout')
        priority = self.config.getint('global', 'priority')
        election = FailoverElection(
            self.config,
            self.zk,
            election_timeout,
            replica_infos,
            self._replication_manager,
            allow_data_loss,
            priority,
            self.db.get_wal_receive_lsn(),
            len(helpers.make_current_replics_quorum(replica_infos, self.zk.get_alive_hosts(all_hosts_timeout=election_timeout / 3))),
        )
        try:
            return election.make_election()
        except (ZookeeperException, ElectionError):
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False

    def _get_switchover_candidate(self):
        switchover_info = self.zk.get(self.zk.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)
        if switchover_info is None:
            return None

        if switchover_info.get('destination') is not None:
            return switchover_info.get('destination')
        replica_infos = self._get_extended_replica_infos()
        if replica_infos is None:
            return None
        if self.config.getboolean('replica', 'allow_potential_data_loss'):
            app_name_map = {helpers.app_name_from_fqdn(host): host for host in self.zk.get_ha_hosts()}
            return app_name_map.get(helpers.get_oldest_replica(replica_infos))
        return self._replication_manager.get_ensured_sync_replica(replica_infos)

    def _get_extended_replica_infos(self):
        replica_infos = self.zk.get(self.zk.REPLICS_INFO_PATH, preproc=json.loads)
        if replica_infos is None:
            logging.error('Unable to get replica infos from ZK.')
            return None
        app_name_map = {helpers.app_name_from_fqdn(host): host for host in self.zk.get_ha_hosts()}
        for info in replica_infos:
            hostname = app_name_map.get(info['application_name'])
            if not hostname:
                continue
            info['priority'] = self.zk.get(self.zk.get_host_prio_path(hostname), preproc=int)
        return replica_infos

    def _can_do_switchover(self):
        if not self._is_older_then_primary():
            return False

        if not self._check_my_timeline_sync():
            return False

        switchover_candidate = self._get_switchover_candidate()

        # Make sanity check of switchover conditions, and proceed to
        # promotion immediately without failover or dead primary checks.
        if switchover_candidate != helpers.get_hostname():
            logging.info(
                f"Switchover candidate is: {switchover_candidate}. " "We are not a candidate, so we can't promote."
            )
            return False

        logging.info('We are switchover candidate, so we have to promote here.')
        # If primary is alive and it participates in switchover, then we can proceed
        if self.zk.get(self.zk.FAILOVER_INFO_PATH) == 'switchover_initiated':
            return True
        # If primary is dead but we can't failover, then we also proceed
        if self.zk.get_current_lock_holder(self.zk.PRIMARY_LOCK_PATH) is None:
            return True
        logging.warning("Primary holds the lock but didn't initiate switchover yet. " "Waiting for it...")
        return False

    def _accept_failover(self):
        """
        Failover magic is here
        """
        try:
            if not self._can_do_failover():
                return None

            #
            # All checks are done. Acquiring the lock in ZK, promoting and
            # writing last failover timestamp to ZK.
            #
            if not self.zk.try_acquire_lock():
                logging.info('Could not acquire lock in ZK. Not doing anything.')
                return None
            self.db.pg_wal_replay_resume()

            if not self._do_failover():
                return False

            self.zk.write(self.zk.LAST_FAILOVER_TIME_PATH, time.time())
        except Exception:
            logging.error('Unexpected error while trying to do failover. Exiting.')
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            sys.exit(1)

    def _do_failover(self):
        if not self.zk.delete(self.zk.FAILOVER_INFO_PATH):
            logging.error('Could not remove previous failover state. Releasing the lock.')
            self.zk.release_lock()
            return False

        if not self._promote_handle_slots() or not self._promote():
            self.zk.release_lock()
            return False
        self._replication_manager.leave_sync_group()
        return True

    def _wait_for_recovery(self, limit=-1):
        """
        Stop until postgresql complete recovery.
        With limit=-1 the loop here can be infinite.
        """

        def check_recovery_completion():
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

    def _check_archive_recovery(self, limit):
        """
        Returns True if postgresql is in recovery from archive
        and False if it hasn't started recovery within `limit` seconds
        """

        def check_recovery_start():
            if self._check_postgresql_streaming():
                logging.debug('PostgreSQL is already streaming from primary')
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

    def _get_replics_info_from_zk(self, primary):
        if primary:
            replics_info_path = '{member_path}/{hostname}/replics_info'.format(
                member_path=self.zk.MEMBERS_PATH, hostname=primary
            )
        else:
            replics_info_path = self.zk.REPLICS_INFO_PATH
        return self.zk.get(replics_info_path, preproc=json.loads)

    @staticmethod
    def _is_caught_up(replica_infos):
        my_app_name = helpers.app_name_from_fqdn(helpers.get_hostname())
        for replica in replica_infos:
            if replica['application_name'] == my_app_name and replica['state'] == 'streaming':
                return True
        return False

    def _check_postgresql_streaming(self, primary=None):
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

        if replica_infos is not None and (pgconsul._is_caught_up(replica_infos) and self.db.check_walreceiver()):
            logging.debug('PostgreSQL has started streaming from primary.')
            return True

        return None

    def _wait_for_streaming(self, limit=-1, primary=None):
        """
        Stop until postgresql start streaming from primary.
        With limit=-1 the loop here can be infinite.
        """
        check_streaming = functools.partial(self._check_postgresql_streaming, primary)
        return helpers.await_for_value(check_streaming, limit, 'PostgreSQL started streaming from primary')

    def _wait_for_lock(self, lock, limit=-1):
        """
        Wait until lock acquired
        """

        def is_lock_acquired():
            if self.zk.try_acquire_lock(lock):
                return True
            # There is a chance that our connection with ZK is dead
            # (and that is actual reason of not getting lock).
            # So we reinit connection here.
            self.re_init_zk()
            return False

        return helpers.await_for(is_lock_acquired, limit, f'acquired {lock} lock in ZK')

    def _check_host_is_really_dead(self, primary=None):
        return self._check_primary_is_really_dead(primary=primary, check_primary=False)

    def _check_primary_is_really_dead(self, primary=None, check_primary=True):
        """
        Returns True if primary is not accessible via postgres protocol
        and False otherwise
        """
        if not primary:
            primary = self.db.recovery_conf('get_primary')
            if not primary:
                return False
        append = self.config.get('global', 'append_primary_conn_string')
        if check_primary and ('target_session_attrs' not in append):
            ensure_connect_primary = 'target_session_attrs=primary'
        else:
            ensure_connect_primary = ''

        try:
            conn = psycopg2.connect('host=%s %s %s' % (primary, append, ensure_connect_primary))
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute('SELECT 42')
            if cur.fetchone()[0] == 42:
                return False
            return True
        except Exception as err:
            logging.debug('%s while trying to check primary health.', str(err))
            return True

    def _get_ha_replics(self):
        hosts = self.zk.get_ha_hosts()
        if not hosts:
            return None
        my_hostname = helpers.get_hostname()
        if my_hostname in hosts:
            hosts.remove(my_hostname)
        return set(hosts)

    def _get_zk_members(self):
        """
        Checks the presence of subnodes in MEMBERS_PATH at ZK.
        """
        while True:
            timer = IterationTimer()
            self.zk.ensure_path(self.zk.MEMBERS_PATH)
            members = self.zk.get_children(self.zk.MEMBERS_PATH)
            if members is not None:
                return members
            self.re_init_zk()
            timer.sleep(self.config.getfloat('global', 'iteration_timeout'))

    def _check_primary_switchover(self, db_state, zk_state):
        """
        Check if scheduled switchover is initiated.
        Perform sanity check on current local and cluster condition.
        Abort or postpone switchover if any of them fail.
        """
        switchover_info = zk_state[self.zk.SWITCHOVER_ROOT_PATH]

        # Scheduled switchover node exists.
        if not switchover_info:
            return None

        # The node contains hostname of current instance
        if switchover_info.get('hostname') != helpers.get_hostname():
            return None

        # Current instance is primary
        if self.db.get_role() != 'primary':
            logging.error('Current role is %s, but switchover requested.', self.db.get_role())
            return None

        # There were no failed attempts in the past
        state = self.zk.get(self.zk.SWITCHOVER_STATE_PATH)
        # Ignore silently if node does not exist
        if state is None:
            return None
        # Ignore failed or in-progress switchovers
        if state != 'scheduled':
            logging.warning('Switchover state is %s, will not proceed.', state)
            return None

        # Timeline of the current instance matches the timeline defined in
        # SS node.
        if int(switchover_info.get(self.zk.TIMELINE_INFO_PATH)) != db_state['timeline']:
            logging.warning(
                'Switchover node has timeline %s, but local is %s, ignoring switchover.',
                switchover_info.get(self.zk.TIMELINE_INFO_PATH),
                db_state['timeline'],
            )
            return None

        # Last switchover was more than N sec ago
        last_failover_ts = self.zk.get(self.zk.LAST_FAILOVER_TIME_PATH, preproc=float)

        last_switchover_ts = self.zk.get(self.zk.LAST_SWITCHOVER_TIME_PATH, preproc=float)

        last_role_transition_ts = None
        if last_failover_ts is not None or last_switchover_ts is not None:
            last_role_transition_ts = max(filter(lambda x: x is not None, [last_switchover_ts, last_failover_ts]))

        alive_replics_number = len([i for i in db_state['replics_info'] if i['state'] == 'streaming'])

        ha_replics = self._get_ha_replics()
        if ha_replics is None:
            return None
        ha_replic_cnt = len(ha_replics)

        if not helpers.check_last_failover_time(last_role_transition_ts, self.config) and (
            alive_replics_number < ha_replic_cnt
        ):
            logging.warning(
                'Last role transition was %.1f seconds ago,'
                ' and alive host count less than HA hosts in zk (HA: %d, ZK: %d) ignoring switchover.',
                time.time() - last_role_transition_ts,
                ha_replic_cnt,
                alive_replics_number,
            )
            return None

        # Ensure there is no other failover in progress.
        failover_state = self.zk.get(self.zk.FAILOVER_INFO_PATH)
        if failover_state not in ('finished', None):
            logging.error('Switchover requested, but current failover state is %s.', failover_state)
            return None

        switchover_candidate = self._get_switchover_candidate()
        if switchover_candidate is None:
            return False

        if not self._candidate_is_sync_with_primary(db_state, switchover_candidate):
            return False

        logging.info('Scheduled switchover checks passed OK.')
        return True

    def _do_primary_switchover(self, zk_state):
        """
        Perform steps required on scheduled switchover
        if current role is primary
        """
        logging.warning('Starting scheduled switchover')
        self.zk.write(self.zk.SWITCHOVER_STATE_PATH, 'initiated')
        # Deny user requests
        logging.warning('Starting checkpoint')
        self.db.checkpoint()
        self.db.pgpooler('stop')
        logging.warning('cluster was closed from user requests')
        # check once more if replica is sync with primary
        limit = self.config.getfloat('global', 'postgres_timeout')
        switchover_candidate = self._get_switchover_candidate()
        if not helpers.await_for(
            lambda: self._candidate_is_sync_with_primary_with_get_state(switchover_candidate=switchover_candidate),
            limit,
            "replay lag become zero",
        ):
            logging.error('check replica lsn diff failed - do not swtichover')
            return False

        # Store replics info
        db_state = self.db.get_state()
        if not self._store_replics_info(db_state, zk_state):
            logging.error('replics_info was not stored - do not switchover')
            return False

        # Announce intention to perform switchover to the rest of the cluster.
        if not self.zk.write(self.zk.FAILOVER_INFO_PATH, 'switchover_initiated'):
            logging.error(f'unable to write failover state to zk ({self.zk.FAILOVER_INFO_PATH})')
            return False

        if not helpers.await_for(
            lambda: self.zk.get(self.zk.SWITCHOVER_STATE_PATH) == "candidate_found", limit, "switchover candidate found"
        ):
            return False

        # Attempt to shut down local PG instance.
        # Failure is not critical.
        if self.db.stop_postgresql(timeout=limit) == 0:
            lsn = self._cmd_manager.get_control_parameter(db_state['pgdata'], "REDO location")
            self.zk.noexcept_write(self.zk.SWITCHOVER_LSN_PATH, lsn)
            if not self.zk.noexcept_write(self.zk.FAILOVER_INFO_PATH, 'switchover_master_shut'):
                logging.error(f'unable to write failover state to zk ({self.zk.FAILOVER_INFO_PATH})')
                return False
        else:
            logging.error('Unable to stop postgresql')
            return False

        # Release leader-lock.
        # Wait 5 secs for the actual release.
        self.zk.release_lock(lock_type=self.zk.PRIMARY_LOCK_PATH, wait=5)

        return True

    def _candidate_is_sync_with_primary_with_get_state(self, switchover_candidate):
        db_state = self.db.get_state()
        return self._candidate_is_sync_with_primary(db_state, switchover_candidate)

    def _candidate_is_sync_with_primary(self, db_state, switchover_candidate):
        if switchover_candidate is None:
            # nothing to check
            return True

        replics_info = db_state.get('replics_info', list())
        max_allowed_lag_ms = self.config.getint('global', 'max_allowed_switchover_lag_ms')
        for replica in replics_info:
            if replica.get('sync_state', '') != 'quorum':
                continue
            if replica.get('application_name', '') != helpers.app_name_from_fqdn(switchover_candidate):
                continue
            replay_lag = replica.get('replay_lag_msec', -1)
            logging.info(f"Replica {switchover_candidate} has replay lag {replay_lag}ms")
            if replay_lag > max_allowed_lag_ms:
                if not self.config.getboolean('replica', 'allow_potential_data_loss'):
                    logging.warning(
                        f"Replica {switchover_candidate} has replay lag {replay_lag}ms so cannot be primary for switchover, max allowed lag {max_allowed_lag_ms}ms"
                    )
                    return None
                else:
                    logging.warning(f"Replica {switchover_candidate} has replay lag {replay_lag} and allow data loss")
            return True

        return True

    def _transition_primary_switchover(self):
        """
        Wait for N seconds trying to find out new primary,
        then transition to replica.
        If timeout passed and no one took the lock, rollback
        the procedure.
        """
        timeout = self.config.getfloat('global', 'postgres_timeout')
        if helpers.await_for(
            lambda: self.zk.get(self.zk.SWITCHOVER_STATE_PATH) is None, timeout, 'new primary finished switchover'
        ):
            primary = self.zk.get_current_lock_holder(self.zk.PRIMARY_LOCK_PATH)
            if primary is not None:
                # From here switchover can be considered successful regardless of this host state
                self.zk.delete('%s/%s/op' % (self.zk.MEMBERS_PATH, helpers.get_hostname()))
                self._attach_to_primary(primary, self.config.getfloat('replica', 'recovery_timeout'))
                return True
        # Mark switchover node as failure
        self.zk.write(self.zk.SWITCHOVER_STATE_PATH, 'replica_timed_out', need_lock=False)
        return False

    def _detect_replica_switchover(self):
        """
        Detect planned switchover condition.
        """

        if self.zk.get(self.zk.SWITCHOVER_STATE_PATH) is None:
            return False

        db_state = self.db.get_state()

        switchover_info = self.zk.get(self.zk.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)
        if not switchover_info:
            return False

        # We check that switchover should happen from current timeline
        zk_tli = self.zk.get(self.zk.TIMELINE_INFO_PATH, preproc=int)
        if zk_tli != switchover_info[self.zk.TIMELINE_INFO_PATH]:
            return False

        # Scheduled switchover node with primary (fqdn, tli) info exists.

        # The scheduled switchover was commenced by primary:
        # 'switchover_initiated': the primary is in the process
        # of shutting itself down

        # If there is an ability to do failover instead of switchover, than let's do it.
        autofailover = self.config.getboolean('global', 'autofailover')
        failover_state = self.zk.get(self.zk.FAILOVER_INFO_PATH)
        if failover_state not in ['switchover_initiated', 'switchover_master_shut'] and autofailover:
            return False

        # The node contains hostname of current instance
        switchover_primary = switchover_info.get('hostname')
        if switchover_primary is not None and switchover_primary != db_state['primary_fqdn']:
            logging.error('current primary FQDN is not equal to hostname in switchover node, ignoring switchover')
            return False

        return True

    def _zk_alive_refresh(self, role, db_state, zk_state):
        self._replication_manager.drop_zk_fail_timestamp()
        if role is None:
            self.zk.release_lock(self.zk.get_host_alive_lock_path())
        else:
            self._update_single_node_status(role)
            if self.zk.get_current_lock_holder(self.zk.get_host_alive_lock_path()) is None:
                logging.warning("I don't hold my alive lock, let's acquire it")
                self.zk.try_acquire_lock(self.zk.get_host_alive_lock_path())

    def _zk_get_wal_receiver_info(self, host):
        return self.zk.get(f'{self.zk.MEMBERS_PATH}/{host}/wal_receiver', preproc=json.loads)

    def is_op_destructive(self, op):
        return op in self.DESTRUCTIVE_OPERATIONS

    def _store_replics_info(self, db_state, zk_state):
        tli_res = None
        if zk_state[self.zk.TIMELINE_INFO_PATH]:
            tli_res = zk_state[self.zk.TIMELINE_INFO_PATH] == db_state['timeline']

        replics_info = db_state.get('replics_info')

        zk_state['replics_info_written'] = None
        if tli_res and replics_info is not None:
            zk_state['replics_info_written'] = self.zk.write(
                self.zk.REPLICS_INFO_PATH, replics_info, preproc=json.dumps
            )
            self.write_host_stat(helpers.get_hostname(), db_state)
            return True

        return False
