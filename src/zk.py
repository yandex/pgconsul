# encoding: utf-8
"""
Zookeeper wrapper module. Zookeeper class defined here.
"""

import json
import logging
import os
import traceback
import time

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import LockTimeout, NoNodeError, KazooException, ConnectionClosedError
from kazoo.handlers.threading import KazooTimeoutError, SequentialThreadingHandler
from kazoo.recipe.lock import Lock
from kazoo.security import make_digest_acl

from . import helpers


def _get_host_path(path, hostname):
    if hostname is None:
        hostname = helpers.get_hostname()
    return path % hostname


class ZookeeperException(Exception):
    """Exception for wrapping all zookeeper connector inner exceptions"""


class Zookeeper(object):
    """
    Zookeeper class
    """

    PRIMARY_LOCK_PATH = 'leader'
    LAST_PRIMARY_PATH = 'last_leader'
    PRIMARY_SWITCH_LOCK_PATH = 'remaster'
    SYNC_REPLICA_LOCK_PATH = 'sync_replica'

    QUORUM_PATH = 'quorum'
    QUORUM_MEMBER_LOCK_PATH = f'{QUORUM_PATH}/members/%s'

    REPLICS_INFO_PATH = 'replics_info'
    TIMELINE_INFO_PATH = 'timeline'
    FAILOVER_STATE_PATH = 'failover_state'
    FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    CURRENT_PROMOTING_HOST = 'current_promoting_host'
    LAST_FAILOVER_TIME_PATH = 'last_failover_time'
    LAST_PRIMARY_AVAILABILITY_TIME = 'last_master_activity_time'
    LAST_SWITCHOVER_TIME_PATH = 'last_switchover_time'
    SWITCHOVER_ROOT_PATH = 'switchover'
    SWITCHOVER_LOCK_PATH = f'{SWITCHOVER_ROOT_PATH}/lock'
    # A JSON string with primary fqmdn and its timeline
    SWITCHOVER_PRIMARY_PATH = f'{SWITCHOVER_ROOT_PATH}/master'
    SWITCHOVER_CANDIDATE = f'{SWITCHOVER_ROOT_PATH}/candidate'
    # A simple string with current scheduled switchover state
    SWITCHOVER_STATE_PATH = f'{SWITCHOVER_ROOT_PATH}/state'
    MAINTENANCE_PATH = 'maintenance'
    MAINTENANCE_TIME_PATH = f'{MAINTENANCE_PATH}/ts'
    MAINTENANCE_PRIMARY_PATH = f'{MAINTENANCE_PATH}/master'
    HOST_MAINTENANCE_PATH = f'{MAINTENANCE_PATH}/%s'
    HOST_ALIVE_LOCK_PATH = 'alive/%s'
    HOST_REPLICATION_SOURCES = 'replication_sources'

    SINGLE_NODE_PATH = 'is_single_node'

    ELECTION_ENTER_LOCK_PATH = 'enter_election'
    ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    ELECTION_WINNER_PATH = 'election_winner'
    ELECTION_STATUS_PATH = 'election_status'
    ELECTION_VOTE_PATH = 'election_vote/%s'

    MEMBERS_PATH = 'all_hosts'
    SIMPLE_PRIMARY_SWITCH_TRY_PATH = f'{MEMBERS_PATH}/%s/tried_remaster'
    HOST_PRIO_PATH = f'{MEMBERS_PATH}/%s/prio'
    SSN_PATH = f'{MEMBERS_PATH}/%s/synchronous_standby_names'
    SSN_VALUE_PATH = f'{SSN_PATH}/value'
    SSN_DATE_PATH = f'{SSN_PATH}/last_update'

    def __init__(self, config, plugins, lock_contender_name=None):
        self._lock_contender_name = lock_contender_name
        self._plugins = plugins
        self._zk_hosts = config.get('global', 'zk_hosts')
        self._release_lock_after_acquire_failed = config.getboolean('global', 'release_lock_after_acquire_failed')
        self._timeout = config.getfloat('global', 'iteration_timeout')
        self._zk_connect_max_delay = config.getfloat('global', 'zk_connect_max_delay')
        self._zk_auth = config.getboolean('global', 'zk_auth')
        self._zk_ssl = config.getboolean('global', 'zk_ssl')
        self._verify_certs = config.getboolean('global', 'verify_certs')
        if self._zk_auth:
            self._zk_username = config.get('global', 'zk_username')
            self._zk_password = config.get('global', 'zk_password')
            if not self._zk_username or not self._zk_password:
                logging.error('zk_username, zk_password required when zk_auth enabled')
        if self._zk_ssl:
            self._cert = config.get('global', 'certfile')
            self._key = config.get('global', 'keyfile')
            self._ca = config.get('global', 'ca_cert')
            if not self._cert or not self._key or not self._ca:
                logging.error('certfile, keyfile, ca_cert required when zk_auth enabled')
        try:
            self._locks = {}
            prefix = config.get('global', 'zk_lockpath_prefix')
            self._path_prefix = prefix if prefix is not None else helpers.get_lockpath_prefix()
            self._lockpath = self._path_prefix + self.PRIMARY_LOCK_PATH

            if not self._init_client():
                raise Exception('Could not connect to ZK.')
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())


    def get_lock_contender_name(self):
        if self._lock_contender_name:
            return self._lock_contender_name
        return helpers.get_hostname()


    def __del__(self):
        self._zk.remove_listener(self._listener)
        self._zk.stop()

    def _create_kazoo_client(self):
        conn_retry_options = {'max_tries': 10, 'delay': 0.5, 'backoff': 1.5, 'max_delay': self._zk_connect_max_delay}
        command_retry_options = {'max_tries': 0, 'delay': 0, 'backoff': 1, 'max_delay': 5}
        args = {
            'hosts': self._zk_hosts,
            'handler': SequentialThreadingHandler(),
            'timeout': self._timeout,
            'connection_retry': conn_retry_options,
            'command_retry': command_retry_options,
        }
        if self._zk_auth:
            acl = make_digest_acl(self._zk_username, self._zk_password, all=True)
            args.update(
                {
                    'default_acl': [acl],
                    'auth_data': [
                        (
                            'digest',
                            '{username}:{password}'.format(username=self._zk_username, password=self._zk_password),
                        )
                    ],
                }
            )
        if self._zk_ssl:
            args.update(
                {
                    'use_ssl': True,
                    'certfile': self._cert,
                    'keyfile': self._key,
                    'ca': self._ca,
                    'verify_certs': self._verify_certs,
                }
            )
        self._zk = KazooClient(**args)

    def _listener(self, state):
        if state == KazooState.LOST:
            # In the event that a LOST state occurs, its certain that the lock and/or the lease has been lost.
            logging.error("Connection to ZK lost, clean all locks")
            self._locks = {}
            self._plugins.run('on_lost')
        elif state == KazooState.SUSPENDED:
            logging.warning("Being disconnected from ZK.")
            self._plugins.run('on_suspend')
        elif state == KazooState.CONNECTED:
            logging.info("Reconnected to ZK.")
            self._plugins.run('on_connect')

    def _wait(self, event):
        event.wait(self._timeout)

    def _get(self, path):
        event = self._zk.get_async(path)
        self._wait(event)
        return event.get_nowait()

    #
    # We assume data is already converted to text.
    #
    def _write(self, path, data, need_lock=True):
        if need_lock and self.get_current_lock_holder() != self.get_lock_contender_name():
            return False
        event = self._zk.exists_async(path)
        self._wait(event)
        if event.get_nowait():  # Node exists
            event = self._zk.set_async(path, data.encode())
        else:
            event = self._zk.create_async(path, value=data.encode())
        self._wait(event)
        # raise Timeout exception if set not done yet
        event.get_nowait()
        if event.exception:
            logging.error('Failed to write to node: %s.' % path)
            logging.error(event.exception)
        return not event.exception

    def _init_lock(self, name, read_lock=False):
        path = self._path_prefix + name
        if read_lock:
            lock = self._zk.ReadLock(path, self.get_lock_contender_name())
        else:
            lock = self._zk.Lock(path, self.get_lock_contender_name())
        self._locks[name] = lock

    def _acquire_lock(self, name, allow_queue, timeout, read_lock=False):
        if timeout is None:
            timeout = self._timeout
        if self._zk.state != KazooState.CONNECTED:
            logging.warning('Not able to acquire %s ' % name + 'lock without alive connection.')
            return False
        lock = self._get_lock(name, read_lock)
        contenders = lock.contenders()
        if len(contenders) != 0:
            if not read_lock:
                contenders = contenders[:1]
            if self.get_lock_contender_name() in contenders:
                logging.debug('We already hold the %s lock.', name)
                return True
            if not (allow_queue or read_lock):
                logging.warning('%s lock is already taken by %s.', name[0].upper() + name[1:], contenders[0])
                return False
        try:
            acquired = lock.acquire(blocking=True, timeout=timeout)
            if not acquired:
                logging.warning('Unable to acquire lock "%s", but not because of timeout...', name)
        except LockTimeout:
            logging.warning('Unable to obtain lock %s within timeout (%s s)', name, timeout)
            acquired = False
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            acquired = False
        if not acquired and self._release_lock_after_acquire_failed:
            logging.debug('Try to release and delete lock "%s", to recreate on next iter', name)
            try:
                self.release_lock(name)
            except Exception:
                for line in traceback.format_exc().split('\n'):
                    logging.error(line.rstrip())
        return acquired

    def _get_lock(self, name, read_lock) -> Lock:
        if name in self._locks:
            return self._locks[name]
        else:
            logging.debug('No lock instance for %s. Creating one.', name)
            self._init_lock(name, read_lock=read_lock)
            return self._locks[name]

    def _delete_lock(self, name: str):
        if name in self._locks:
            del self._locks[name]

    def _release_lock(self, name: str):
        if name in self._locks:
            lock = self._locks[name] # type: Lock
            self._delete_lock(name)
            return lock.release()

    def is_alive(self):
        """
        Return True if we are connected to zk
        """
        if self._zk.state == KazooState.CONNECTED:
            return True
        return False

    def reconnect(self):
        """
        Reconnect to zk
        """
        try:
            for lock in self._locks.items():
                if lock[1]:
                    lock[1].release()
        except (KazooException, KazooTimeoutError):
            pass

        try:
            self._locks = {}
            self._zk.remove_listener(self._listener)
            self._zk.stop()
            self._zk.close()

            return self._init_client() and self.is_alive()
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False

    def _init_client(self) -> bool:
        self._create_kazoo_client()
        event = self._zk.start_async()
        event.wait(self._timeout)
        if not self._zk.connected:
            return False

        self._zk.add_listener(self._listener)
        self._init_lock(self.PRIMARY_LOCK_PATH)
        return True


    def get(self, key, preproc=None, debug=False):
        """
        Get key value from zk
        """
        path = self._path_prefix + key
        try:
            res = self._get(path)
        except NoNodeError:
            if debug:
                logging.debug(f"NoNodeError when trying to get {key}")
            return None
        except (KazooException, KazooTimeoutError) as exception:
            raise ZookeeperException(exception)
        value = res[0].decode('utf-8')
        if preproc:
            try:
                return preproc(value)
            except ValueError:
                if debug:
                    logging.debug(f"Failed to preproc value {value} (key {key})")
                return None
        else:
            return value

    @helpers.return_none_on_error
    def noexcept_get(self, key, preproc=None):
        """
        Get key value from zk, without ZK exception forwarding
        """
        return self.get(key, preproc)

    @helpers.return_none_on_error
    def get_mtime(self, key):
        """
        Returns modification time of ZK node
        """
        return getattr(self._get_meta(key), 'last_modified', None)

    def _get_meta(self, key):
        """
        Get metadata from key.
        returns kazoo.protocol.states.ZnodeStat
        """
        path = self._path_prefix + key
        try:
            (_, meta) = self._get(path)
        except NoNodeError:
            return None
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return None
        else:
            return meta

    def ensure_path(self, path):
        """
        Check that path exists and create if not
        """
        if not path.startswith(self._path_prefix):
            path = os.path.join(self._path_prefix, path)
        event = self._zk.ensure_path_async(path)
        try:
            self._wait(event)
            return event.get_nowait()
        except (KazooException, KazooTimeoutError):
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return None

    def exists_path(self, path, catch_except=True):
        if not path.startswith(self._path_prefix):
            path = os.path.join(self._path_prefix, path)
        event = self._zk.exists_async(path)
        try:
            self._wait(event)
            return bool(event.get_nowait())
        except (KazooException, KazooTimeoutError) as e:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            if not catch_except:
                raise e
            return False

    def get_children(self, path, catch_except=True):
        """
        Get children nodes of path
        """
        try:
            if not path.startswith(self._path_prefix):
                path = os.path.join(self._path_prefix, path)
            event = self._zk.get_children_async(path)
            self._wait(event)
            return event.get_nowait()
        except NoNodeError as e:
            for line in traceback.format_exc().split('\n'):
                logging.debug(line.rstrip())
            if not catch_except:
                raise e
            return None
        except Exception as e:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            if not catch_except:
                raise e
            return None

    def get_state(self):
        """
        Get current zk state (if possible)
        """
        data = {'alive': self.is_alive()}
        if not data['alive']:
            raise ZookeeperException("Zookeeper connection is unavailable now")
        data[self.REPLICS_INFO_PATH] = self.get(self.REPLICS_INFO_PATH, preproc=json.loads)
        data[self.LAST_FAILOVER_TIME_PATH] = self.get(self.LAST_FAILOVER_TIME_PATH, preproc=float)
        data[self.LAST_SWITCHOVER_TIME_PATH] = self.get(self.LAST_SWITCHOVER_TIME_PATH, preproc=float)
        data[self.FAILOVER_STATE_PATH] = self.get(self.FAILOVER_STATE_PATH)
        data[self.FAILOVER_MUST_BE_RESET] = self.exists_path(self.FAILOVER_MUST_BE_RESET)
        data[self.CURRENT_PROMOTING_HOST] = self.get(self.CURRENT_PROMOTING_HOST)
        data['lock_version'] = self.get_current_lock_version()
        data['lock_holder'] = self.get_current_lock_holder()
        data['single_node'] = self.exists_path(self.SINGLE_NODE_PATH)
        data[self.TIMELINE_INFO_PATH] = self.get(self.TIMELINE_INFO_PATH, preproc=int)
        data[self.SWITCHOVER_ROOT_PATH] = self.get(self.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)
        data[self.SWITCHOVER_CANDIDATE] = self.get(self.SWITCHOVER_CANDIDATE)
        data[self.SWITCHOVER_STATE_PATH] = self.get(self.SWITCHOVER_STATE_PATH)
        data[self.MAINTENANCE_PATH] = {
            'status': self.get(self.MAINTENANCE_PATH),
            'ts': self.get(self.MAINTENANCE_TIME_PATH),
        }
        data[self.LAST_PRIMARY_PATH] = self.get(self.LAST_PRIMARY_PATH)
        data['synchronous_standby_names'] = self._get_ssn_info()

        data['alive'] = self.is_alive()
        if not data['alive']:
            raise ZookeeperException("Zookeeper connection is unavailable now")
        return data

    def _get_ssn_info(self):
        ssn_info = dict()
        all_hosts = self.get_children(self.MEMBERS_PATH, catch_except=True)
        for host in all_hosts:
            path_value = _get_host_path(self.SSN_VALUE_PATH, host)
            path_date = _get_host_path(self.SSN_DATE_PATH, host)
            ssn_info[host] = (self.get(path_value), self.get(path_date))
        return ssn_info

    def _preproc_write(self, key, data, preproc):
        path = self._path_prefix + key
        if preproc:
            sdata = preproc(data)
        else:
            sdata = str(data)
        return path, sdata

    def write(self, key, data, preproc=None, need_lock=True):
        """
        Write value to key in zk
        """
        path, sdata = self._preproc_write(key, data, preproc)
        try:
            return self._write(path, sdata, need_lock=need_lock)
        except (KazooException, KazooTimeoutError) as exception:
            raise ZookeeperException(exception)

    def noexcept_write(self, key, data, preproc=None, need_lock=True):
        """
        Write value to key in zk without zk exceptions forwarding
        """
        path, sdata = self._preproc_write(key, data, preproc)
        try:
            return self._write(path, sdata, need_lock=need_lock)
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False

    def delete(self, key, recursive=False):
        """
        Delete key from zk
        """
        path = self._path_prefix + key
        try:
            self._zk.delete(path, recursive=recursive)
            return True
        except NoNodeError:
            logging.info('No node %s was found in ZK to delete it.' % key)
            return True
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False

    def get_current_lock_version(self):
        """
        Get current leader lock version
        """
        children = self.get_children(self._lockpath)
        if children and len(children) > 0:
            return min([i.split('__')[-1] for i in children])
        return None

    def get_lock_contenders(self, name, catch_except=True, read_lock=False):
        """
        Get a list of all hostnames that are competing for the lock,
        including the holder.
        """
        try:
            contenders = self._get_lock(name, read_lock).contenders()
            if len(contenders) > 0:
                return contenders
        except Exception as e:
            for line in traceback.format_exc().split('\n'):
                logging.debug(line.rstrip())
            if not catch_except:
                raise e
        return []

    def get_current_lock_holder(self, name=None, catch_except=True):
        """
        Get hostname of lock holder
        """
        name = name or self.PRIMARY_LOCK_PATH
        lock_contenders = self.get_lock_contenders(name, catch_except)
        if len(lock_contenders) > 0:
            return lock_contenders[0]
        else:
            return None

    def acquire_lock(self, lock_type, allow_queue=False, timeout=None, read_lock=False):
        result = self._acquire_lock(lock_type, allow_queue, timeout, read_lock=read_lock)
        if not result:

            raise ZookeeperException(f'Failed to acquire lock {lock_type}')
        logging.debug(f'Success acquire lock: {lock_type}')

    def try_acquire_lock(self, lock_type=None, allow_queue=False, timeout=None, read_lock=False):
        """
        Acquire lock (leader by default)
        """
        lock_type = lock_type or self.PRIMARY_LOCK_PATH
        acquired = self._acquire_lock(lock_type, allow_queue, timeout, read_lock=read_lock)
        if lock_type == self.PRIMARY_LOCK_PATH and acquired:
            self.write(self.LAST_PRIMARY_PATH, helpers.get_hostname())
        return acquired

    def release_lock(self, lock_type=None, wait=0):
        """
        Release lock (leader by default)
        """
        lock_type = lock_type or self.PRIMARY_LOCK_PATH
        # If caller decides to rely on kazoo internal API,
        # release the lock and return immediately.
        if not wait:
            return self._release_lock(lock_type)

        # Otherwise, make sure the lock is actually released.

        for _ in range(wait):
            try:
                self._release_lock(lock_type)
                holder = self.get_current_lock_holder(name=lock_type)
                if holder != self.get_lock_contender_name():
                    return True
            except ConnectionClosedError:
                # ok, shit happens, now we should reconnect to ensure that we actually released the lock
                self.reconnect()
            logging.warning('Unable to release lock "%s", retrying', lock_type)
            time.sleep(1)
        raise RuntimeError('unable to release lock after %i attempts' % wait)

    def release_if_hold(self, lock_type, wait=0, read_lock=False):
        if read_lock:
            holders = self.get_lock_contenders(lock_type, read_lock=read_lock)
        else:
            holders = [self.get_current_lock_holder(lock_type)]
        if self.get_lock_contender_name() not in holders:
            return True
        return self.release_lock(lock_type, wait)

    def get_host_alive_lock_path(self, hostname=None):
        return _get_host_path(self.HOST_ALIVE_LOCK_PATH, hostname)

    def get_host_maintenance_path(self, hostname=None):
        return _get_host_path(self.HOST_MAINTENANCE_PATH, hostname)

    def get_host_quorum_path(self, hostname=None):
        return _get_host_path(self.QUORUM_MEMBER_LOCK_PATH, hostname)

    def get_host_prio_path(self, hostname=None):
        return _get_host_path(self.HOST_PRIO_PATH, hostname)

    def get_simple_primary_switch_try_path(self, hostname=None):
        return _get_host_path(self.SIMPLE_PRIMARY_SWITCH_TRY_PATH, hostname)

    def get_ssn_value_path(self, hostname=None):
        return _get_host_path(self.SSN_VALUE_PATH, hostname)

    def get_ssn_date_path(self, hostname=None):
        return _get_host_path(self.SSN_DATE_PATH, hostname)

    def write_ssn(self, value):
        hostname = helpers.get_hostname()
        self.ensure_path(self.get_ssn_value_path(hostname))
        self.ensure_path(self.get_ssn_date_path(hostname))
        self.noexcept_write(self.get_ssn_value_path(hostname), value, need_lock=False)
        self.noexcept_write(self.get_ssn_date_path(hostname), time.time(), need_lock=False)

    def get_election_vote_path(self, hostname=None):
        if hostname is None:
            hostname = helpers.get_hostname()
        return self.ELECTION_VOTE_PATH % hostname

    def get_ha_hosts(self, catch_except=True):
        all_hosts = self.get_children(self.MEMBERS_PATH, catch_except=catch_except)
        if all_hosts is None:
            logging.error('Failed to get HA host list from ZK')
            return None
        ha_hosts = []
        for host in all_hosts:
            path = f"{self.MEMBERS_PATH}/{host}/ha"
            if self.exists_path(path, catch_except=catch_except):
                ha_hosts.append(host)
        logging.debug(f"HA hosts are: {ha_hosts}")
        return ha_hosts

    def is_host_alive(self, hostname, timeout=0.0, catch_except=True):
        alive_path = self.get_host_alive_lock_path(hostname)
        return helpers.await_for(
            lambda: self.get_current_lock_holder(alive_path, catch_except) is not None, timeout, f'{hostname} is alive'
        )

    def _is_host_in_sync_quorum(self, hostname):
        host_quorum_path = self.get_host_quorum_path(hostname)
        return self.get_current_lock_holder(host_quorum_path) is not None

    def get_sync_quorum_hosts(self):
        all_hosts = self.get_children(self.MEMBERS_PATH)
        if all_hosts is None:
            logging.error('Failed to get HA host list from ZK')
            return []
        return [host for host in all_hosts if self._is_host_in_sync_quorum(host)]

    def get_alive_hosts(self, timeout=1, catch_except=True, all_hosts_timeout=None):
        ha_hosts = self.get_ha_hosts(catch_except=catch_except)
        if ha_hosts is None:
            return []
        if all_hosts_timeout:
            minimal_total_timeout = timeout * len(ha_hosts)
            if minimal_total_timeout > all_hosts_timeout:
                logging.warning("Expected timeout for checking host aliveness will be ignored.")
                logging.debug("The minimal total timeout for checking the aliveness of all hosts (%s s) "
                                "is greater than the expected one - all_hosts_timeout (%s s)."
                                "Consider increasing the election timeout.",
                                minimal_total_timeout, all_hosts_timeout)
            else:
                timeout = all_hosts_timeout / len(ha_hosts)
        alive_hosts = [host for host in ha_hosts if self.is_host_alive(host, timeout, catch_except)]
        return alive_hosts
