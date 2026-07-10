# encoding: utf-8
"""
Zookeeper wrapper module. Zookeeper class defined here.
"""

import json
import logging
import time
from configparser import RawConfigParser
from dataclasses import dataclass

from . import helpers
from .zk_client import (
    LockHandle,
    ZkClient,
    ZkClientError,
    ZkConnectionClosedError,
    ZkConnectionState,
    ZkLockTimeout,
    ZkNoNodeError,
    ZkSessionExpiredError,
    create_zk_client,
)


@dataclass
class ZookeeperConfig:
    release_lock_after_acquire_failed: bool
    timeout: float
    path_prefix: str
    lock_contender_name: str | None = None


class ZookeeperException(Exception):
    """Exception for wrapping all zookeeper connector inner exceptions"""


class Zookeeper(object):
    """
    Zookeeper class
    """

    PRIMARY_LOCK_PATH = 'leader'
    LAST_PRIMARY_PATH = 'last_leader'
    PRIMARY_SWITCH_LOCK_PATH = 'remaster'

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
    SWITCHOVER_PRIMARY_PATH = f'{SWITCHOVER_ROOT_PATH}/master'
    SWITCHOVER_CANDIDATE = f'{SWITCHOVER_ROOT_PATH}/candidate'
    SWITCHOVER_SIDE_REPLICAS = f'{SWITCHOVER_ROOT_PATH}/side_replicas'
    SWITCHOVER_STATE_PATH = f'{SWITCHOVER_ROOT_PATH}/state'
    MAINTENANCE_PATH = 'maintenance'
    MAINTENANCE_TIME_PATH = f'{MAINTENANCE_PATH}/ts'
    MAINTENANCE_PRIMARY_PATH = f'{MAINTENANCE_PATH}/master'
    HOST_MAINTENANCE_PATH = f'{MAINTENANCE_PATH}/%s'
    HOST_ALIVE_LOCK_PATH = 'alive/%s'
    HOST_REPLICATION_SOURCES = 'replication_sources'
    TIMINGS_PATH = 'timing/%s'

    SINGLE_NODE_PATH = 'is_single_node'

    ELECTION_ENTER_LOCK_PATH = 'enter_election'
    ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    ELECTION_WINNER_PATH = 'election_winner'
    ELECTION_STATUS_PATH = 'election_status'
    ELECTION_VOTE_PATH = 'election_vote/%s'

    MEMBERS_PATH = 'all_hosts'
    SIMPLE_PRIMARY_SWITCH_TRY_PATH = f'{MEMBERS_PATH}/%s/tried_remaster'
    HOST_PRIO_PATH = f'{MEMBERS_PATH}/%s/prio'
    HOST_OP_PATH = f'{MEMBERS_PATH}/%s/op'
    HOST_REPLICS_INFO_PATH = f'{MEMBERS_PATH}/%s/replics_info'
    HOST_WAL_RECEIVER_PATH = f'{MEMBERS_PATH}/%s/wal_receiver'
    HOST_HA_PATH = f'{MEMBERS_PATH}/%s/ha'
    SSN_PATH = f'{MEMBERS_PATH}/%s/synchronous_standby_names'
    SSN_VALUE_PATH = f'{SSN_PATH}/value'
    SSN_DATE_PATH = f'{SSN_PATH}/last_update'

    def __init__(self, zk_client: ZkClient, config: ZookeeperConfig):
        self.config = config
        self._locks: dict[str, LockHandle] = {}
        self._lockpath = self.config.path_prefix + self.PRIMARY_LOCK_PATH
        self._zk_client = zk_client
        self._zk_client.set_state_listener(self._listener)
        self._init_lock(self.PRIMARY_LOCK_PATH)

    def close(self) -> None:
        """Release all locks and close ZK connection."""
        for lock in list(self._locks.values()):
            try:
                if lock:
                    lock.release()
            except Exception:
                logging.debug("Error releasing lock during close", exc_info=True)
        self._locks = {}
        self._zk_client.close()

    def _get_lock_contender_name(self):
        if self.config.lock_contender_name:
            return self.config.lock_contender_name
        return helpers.get_hostname()

    def _listener(self, state: ZkConnectionState):
        """Business logic listener for ZkClient state changes."""
        if state == ZkConnectionState.LOST:
            logging.error("Connection to ZK lost, clean all locks.")
            self._locks = {}
        elif state == ZkConnectionState.SUSPENDED:
            logging.warning("Being disconnected from ZK.")
        elif state == ZkConnectionState.CONNECTED:
            logging.info("Reconnected to ZK.")

    def _write(self, path, data, need_lock=True):
        # Each locked write checks lock ownership via a ZK round-trip (contenders()).
        # Local caching would risk stale state; the round-trip is intentional.
        if need_lock and self.get_current_lock_holder() != self._get_lock_contender_name():
            return False
        return self._zk_client.write(path, data)

    def _init_lock(self, name, read_lock=False):
        path = self.config.path_prefix + name
        if read_lock:
            lock = self._zk_client.make_read_lock(path, self._get_lock_contender_name())
        else:
            lock = self._zk_client.make_lock(path, self._get_lock_contender_name())
        self._locks[name] = lock

    def _acquire_lock(self, name, allow_queue, timeout, read_lock=False):
        if timeout is None:
            timeout = self.config.timeout
        if not self._zk_client.is_connected():
            logging.warning('Not able to acquire %s ' % name + 'lock without alive connection.')
            return False
        lock = self._get_lock(name, read_lock)
        try:
            contenders = lock.contenders()
        except ZkClientError:
            logging.exception('Failed to read contenders for lock "%s"', name)
            return False
        if len(contenders) != 0:
            if not read_lock:
                contenders = contenders[:1]
            if self._get_lock_contender_name() in contenders:
                logging.debug('We already hold the %s lock.', name)
                return True
            if not (allow_queue or read_lock):
                logging.warning('%s lock is already taken by %s.', name[0].upper() + name[1:], contenders[0])
                return False
        try:
            acquired = lock.acquire(blocking=True, timeout=timeout)
            if not acquired:
                logging.warning('Unable to acquire lock "%s", but not because of timeout...', name)
        except ZkLockTimeout:
            logging.warning('Unable to obtain lock %s within timeout (%s s)', name, timeout)
            acquired = False
        except ZkClientError:
            logging.exception('Unexpected error while acquiring lock "%s"', name)
            acquired = False
        if not acquired and self.config.release_lock_after_acquire_failed:
            logging.debug('Try to release and delete lock "%s", to recreate on next iter', name)
            try:
                self.release_lock(name)
            except Exception:
                logging.exception('Error releasing lock "%s" after failed acquire', name)
        return acquired

    def _get_lock(self, name, read_lock) -> LockHandle:
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
            lock = self._locks[name]
            self._delete_lock(name)
            return lock.release()

    def is_alive(self):
        """Return True if we are connected to zk"""
        return self._zk_client.is_alive()

    def reconnect(self):
        """Reconnect and restore locks.

        Owns lock lifecycle: ZkClient.reconnect rebuilds the connection (backoff),
        this method drops stale locks and re-inits only PRIMARY_LOCK_PATH.
        Other locks are re-acquired lazily on the next iteration that needs them.
        """
        logging.debug("Reconnecting to ZooKeeper")
        for _, lock in list(self._locks.items()):
            try:
                if lock:
                    lock.release()
            except ZkClientError:
                pass

        self._locks = {}

        connected = self._zk_client.reconnect()

        if connected:
            self._init_lock(self.PRIMARY_LOCK_PATH)

        return connected

    def get(self, key, preproc=None, debug=False):
        """Get key value from zk"""
        try:
            value = self._zk_client.get(key)
        except ZkNoNodeError:
            if debug:
                logging.debug(f"NoNodeError when trying to get {key}")
            return None
        except ZkSessionExpiredError as exception:
            logging.error('ZK session expired during get operation')
            raise ZookeeperException(exception)
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        if value is None:
            return None
        if preproc:
            try:
                return preproc(value)
            except ValueError:
                if debug:
                    logging.debug(f"Failed to preproc {preproc.__name__} value {value} (key {key})")
                return None
        return value

    @helpers.return_none_on_error
    def noexcept_get(self, key, preproc=None):
        """Get key value from zk, without ZK exception forwarding"""
        return self.get(key, preproc)

    def ensure_path(self, path):
        """Check that path exists and create if not. Returns stat or None on error."""
        try:
            return self._zk_client.ensure_path(path)
        except ZkClientError:
            logging.exception('Failed to ensure path: %s', path)
            return None

    def exists_path(self, path, catch_except=True):
        try:
            return self._zk_client.exists(path)
        except ZkClientError as e:
            logging.exception('Error checking if path exists: %s', path)
            if not catch_except:
                raise ZookeeperException(e)
            return False

    def get_children(self, path, catch_except=True):
        """Get children nodes of path.
        Returns list ([] when node absent). Returns None / raises ZookeeperException on error.
        """
        try:
            return self._zk_client.get_children(path)
        except ZkClientError as e:
            logging.exception('Error getting children of path: %s', path)
            if not catch_except:
                raise ZookeeperException(e)
            return None

    def get_state(self):
        """Get current zk state (if possible)"""
        data = {'alive': self.is_alive()}
        if not data['alive']:
            raise ZookeeperException("Zookeeper connection is unavailable now")
        data[self.REPLICS_INFO_PATH] = self.get(self.REPLICS_INFO_PATH, preproc=json.loads)
        data[self.LAST_FAILOVER_TIME_PATH] = self.get(self.LAST_FAILOVER_TIME_PATH, preproc=float)
        data[self.LAST_SWITCHOVER_TIME_PATH] = self.get(self.LAST_SWITCHOVER_TIME_PATH, preproc=float)
        data[self.FAILOVER_STATE_PATH] = self.get(self.FAILOVER_STATE_PATH)
        data[self.FAILOVER_MUST_BE_RESET] = self.exists_path(self.FAILOVER_MUST_BE_RESET)
        data[self.CURRENT_PROMOTING_HOST] = self.get(self.CURRENT_PROMOTING_HOST)
        data['lock_version'] = self._zk_client.lock_version(self._lockpath)
        data['lock_holder'] = self.get_current_lock_holder()
        data['single_node'] = self.exists_path(self.SINGLE_NODE_PATH)
        data[self.TIMELINE_INFO_PATH] = self.get(self.TIMELINE_INFO_PATH, preproc=int)
        data[self.SWITCHOVER_ROOT_PATH] = self.get(self.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)
        data[self.SWITCHOVER_CANDIDATE] = self.get(self.SWITCHOVER_CANDIDATE)
        data[self.SWITCHOVER_SIDE_REPLICAS] = self.get(self.SWITCHOVER_SIDE_REPLICAS, preproc=json.loads)
        data[self.SWITCHOVER_STATE_PATH] = self.get(self.SWITCHOVER_STATE_PATH)
        data[self.MAINTENANCE_PATH] = {
            'status': self.get(self.MAINTENANCE_PATH),
            'ts': self.get(self.MAINTENANCE_TIME_PATH),
        }
        data[self.LAST_PRIMARY_PATH] = self.get(self.LAST_PRIMARY_PATH)
        data['synchronous_standby_names'] = self._get_ssn_info()

        # Final liveness check: connection may have dropped during the reads above.
        if not self.is_alive():
            raise ZookeeperException("Zookeeper connection is unavailable now")
        return data

    def _get_ssn_info(self) -> dict:
        ssn_info: dict = {}
        all_hosts = self.get_children(self.MEMBERS_PATH, catch_except=True)
        if not all_hosts:
            return ssn_info
        for host in all_hosts:
            path_value = helpers.get_host_path(self.SSN_VALUE_PATH, host)
            path_date = helpers.get_host_path(self.SSN_DATE_PATH, host)
            ssn_info[host] = (self.get(path_value), self.get(path_date))
        return ssn_info

    def _preproc_write(self, key, data, preproc):
        if preproc:
            sdata = preproc(data)
        else:
            sdata = str(data)
        return key, sdata

    def write(self, key, data, preproc=None, need_lock=True):
        """Write value to key in zk"""
        key, sdata = self._preproc_write(key, data, preproc)
        try:
            return self._write(key, sdata, need_lock=need_lock)
        except ZkSessionExpiredError as exception:
            logging.error('ZK session expired during write operation')
            raise ZookeeperException(exception)
        except ZkClientError as exception:
            logging.exception('Failed to write zk node %s (data size: %d bytes): %s', key, len(sdata), sdata)
            raise ZookeeperException(exception)

    def noexcept_write(self, key, data, preproc=None, need_lock=True):
        """Write value to key in zk without zk exceptions forwarding"""
        try:
            return self.write(key, data, preproc=preproc, need_lock=need_lock)
        except Exception:
            logging.exception('Failed to write zk node')
            return False

    def delete(self, key, recursive=False):
        """Delete key from zk"""
        return self._zk_client.delete(key, recursive=recursive)

    def get_lock_contenders(self, name, catch_except=True, read_lock=False):
        """Get all hostnames competing for the lock, including the holder."""
        try:
            contenders = self._get_lock(name, read_lock).contenders()
            if len(contenders) > 0:
                return contenders
        except Exception as e:
            logging.debug('Error getting lock contenders for "%s"', name, exc_info=True)
            if not catch_except:
                raise e
        return []

    def get_current_lock_holder(self, name=None, catch_except=True):
        """Get hostname of lock holder"""
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
        """Acquire lock (leader by default)"""
        lock_type = lock_type or self.PRIMARY_LOCK_PATH
        acquired = self._acquire_lock(lock_type, allow_queue, timeout, read_lock=read_lock)
        if lock_type == self.PRIMARY_LOCK_PATH and acquired:
            self.write(self.LAST_PRIMARY_PATH, helpers.get_hostname())
        return acquired

    def release_lock(self, lock_type=None, wait=0):
        """Release lock (leader by default)"""
        lock_type = lock_type or self.PRIMARY_LOCK_PATH
        if not wait:
            return self._release_lock(lock_type)

        for _ in range(wait):
            try:
                self._release_lock(lock_type)
                holder = self.get_current_lock_holder(name=lock_type)
                if holder != self._get_lock_contender_name():
                    return True
            except ZkConnectionClosedError:
                self.reconnect()
            logging.warning('Unable to release lock "%s", retrying', lock_type)
            time.sleep(1)
        raise RuntimeError('unable to release lock after %i attempts' % wait)

    def release_if_hold(self, lock_type, wait=0, read_lock=False):
        if read_lock:
            holders = self.get_lock_contenders(lock_type, read_lock=read_lock)
        else:
            holders = [self.get_current_lock_holder(lock_type)]
        if self._get_lock_contender_name() not in holders:
            return True
        return self.release_lock(lock_type, wait)

    def get_host_alive_lock_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_ALIVE_LOCK_PATH, hostname)

    def _get_host_maintenance_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_MAINTENANCE_PATH, hostname)

    def get_host_quorum_path(self, hostname=None):
        return helpers.get_host_path(self.QUORUM_MEMBER_LOCK_PATH, hostname)

    def _get_host_prio_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_PRIO_PATH, hostname)

    def _get_simple_primary_switch_try_path(self, hostname=None):
        return helpers.get_host_path(self.SIMPLE_PRIMARY_SWITCH_TRY_PATH, hostname)

    def _get_ssn_value_path(self, hostname=None):
        return helpers.get_host_path(self.SSN_VALUE_PATH, hostname)

    def _get_ssn_date_path(self, hostname=None):
        return helpers.get_host_path(self.SSN_DATE_PATH, hostname)

    def _get_timing_path(self, timing_name):
        return self.TIMINGS_PATH % timing_name

    def write_ssn_on_changes(self, value) -> bool:
        """
        Persist value as the current SSN for this host in ZooKeeper.
        Writes value and timestamp only when stored value differs.
        """
        try:
            hostname = helpers.get_hostname()
            value_path = self._get_ssn_value_path(hostname)
            date_path = self._get_ssn_date_path(hostname)

            self.ensure_path(value_path)
            self.ensure_path(date_path)

            if self.get(value_path) != value:
                self.write(value_path, value, need_lock=False)
                self.write(date_path, time.time(), need_lock=False)

            return True
        except Exception as exc:
            logging.exception(exc)
            return False

    def _get_election_vote_path(self, hostname=None):
        if hostname is None:
            hostname = helpers.get_hostname()
        return self.ELECTION_VOTE_PATH % hostname

    # === Election methods ===

    def get_election_host_vote(self, hostname) -> tuple[int, int] | None:
        """Returns (lsn, priority) for hostname's election vote, or None if unavailable."""
        vote_path = self._get_election_vote_path(hostname)
        lsn = self.get(vote_path + '/lsn', preproc=int, debug=True)
        if lsn is None:
            logging.error("Failed to get '%s' lsn for elections.", hostname)
            return None
        priority = self.get(vote_path + '/prio', preproc=int, debug=True)
        if priority is None:
            logging.error("Failed to get '%s' priority for elections.", hostname)
            return None
        return lsn, priority

    def write_election_vote(self, lsn, prio) -> bool:
        """Write current host's election vote (lsn and priority)."""
        vote_path = self._get_election_vote_path()
        if not self.ensure_path(vote_path):
            return False
        try:
            self.write(vote_path + '/lsn', lsn, need_lock=False)
            self.write(vote_path + '/prio', prio, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write election vote')
            return False

    def delete_election_vote(self, hostname) -> bool:
        """Delete election vote node for hostname."""
        return self.delete(self._get_election_vote_path(hostname), recursive=True)

    def get_election_status(self) -> str | None:
        return self.get(self.ELECTION_STATUS_PATH)

    def write_election_status(self, status: str) -> bool:
        try:
            self.write(self.ELECTION_STATUS_PATH, status, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write election status')
            return False

    def get_election_winner(self) -> str | None:
        return self.get(self.ELECTION_WINNER_PATH)

    def write_election_winner(self, hostname: str) -> bool:
        try:
            self.write(self.ELECTION_WINNER_PATH, hostname, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write election winner')
            return False

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

    # === Host-level business methods ===

    def _get_host_op_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_OP_PATH, hostname)

    def get_host_op(self, hostname=None):
        return self.noexcept_get(self._get_host_op_path(hostname))

    def write_host_op(self, op: str, hostname=None) -> bool:
        return self.noexcept_write(self._get_host_op_path(hostname), op, need_lock=False)

    def delete_host_op(self, hostname=None) -> bool:
        return self.delete(self._get_host_op_path(hostname))

    def _get_host_ha_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_HA_PATH, hostname)

    def ensure_host_ha(self, hostname=None) -> bool:
        result = self.ensure_path(self._get_host_ha_path(hostname))
        return result is not None

    def delete_host_ha(self, hostname=None) -> bool:
        path = self._get_host_ha_path(hostname)
        if not self.exists_path(path):
            return True
        try:
            self.delete(path)
            return True
        except Exception:
            logging.exception('Failed to delete host ha path')
            return False

    def _get_host_replics_info_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_REPLICS_INFO_PATH, hostname)

    def write_host_replics_info(self, replics_info, hostname=None) -> bool:
        return self.noexcept_write(
            self._get_host_replics_info_path(hostname), replics_info, preproc=json.dumps, need_lock=False
        )

    def get_host_replics_info(self, hostname) -> list | None:
        return self.get(self._get_host_replics_info_path(hostname), preproc=json.loads)

    def _get_host_wal_receiver_path(self, hostname):
        return helpers.get_host_path(self.HOST_WAL_RECEIVER_PATH, hostname)

    def write_host_wal_receiver(self, wal_receiver_info, hostname=None) -> bool:
        return self.noexcept_write(
            self._get_host_wal_receiver_path(hostname), wal_receiver_info, preproc=json.dumps, need_lock=False
        )

    def get_host_wal_receiver(self, hostname) -> dict | None:
        return self.get(self._get_host_wal_receiver_path(hostname), preproc=json.loads)

    # === Maintenance methods ===

    def get_maintenance_status(self) -> str | None:
        return self.get(self.MAINTENANCE_PATH)

    def write_maintenance_status(self, status: str) -> bool:
        """Write maintenance status ('enable'/'disable') to the main maintenance path."""
        try:
            self.write(self.MAINTENANCE_PATH, status, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write maintenance status')
            return False

    def get_host_maintenance_status(self, hostname=None) -> str | None:
        """Return the maintenance status string for a specific host."""
        return self.get(self._get_host_maintenance_path(hostname))

    def delete_maintenance(self) -> bool:
        return self.delete(self.MAINTENANCE_PATH, recursive=True)

    def get_maintenance_ts(self) -> str | None:
        return self.get(self.MAINTENANCE_TIME_PATH)

    def write_maintenance_ts(self) -> bool:
        try:
            self.write(self.MAINTENANCE_TIME_PATH, time.time(), need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write maintenance timestamp')
            return False

    def get_maintenance_primary(self) -> str | None:
        return self.get(self.MAINTENANCE_PRIMARY_PATH)

    def write_maintenance_primary(self, primary_fqdn: str) -> bool:
        try:
            self.write(self.MAINTENANCE_PRIMARY_PATH, primary_fqdn, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write maintenance primary')
            return False

    def write_host_maintenance_enabled(self, hostname=None) -> bool:
        try:
            self.write(self._get_host_maintenance_path(hostname), 'enable', need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write host maintenance enabled')
            return False

    # === Timeline methods ===

    def get_timeline(self) -> int | None:
        return self.get(self.TIMELINE_INFO_PATH, preproc=int)

    def write_timeline(self, timeline: int) -> bool:
        try:
            self.write(self.TIMELINE_INFO_PATH, timeline)
            return True
        except Exception:
            logging.exception('Failed to write timeline')
            return False

    # === Global replics_info methods ===

    def get_replics_info(self) -> list | None:
        return self.get(self.REPLICS_INFO_PATH, preproc=json.loads)

    def noexcept_get_replics_info(self) -> list | None:
        return self.noexcept_get(self.REPLICS_INFO_PATH, preproc=json.loads)

    def write_replics_info(self, replics_info) -> bool:
        try:
            self.write(self.REPLICS_INFO_PATH, replics_info, preproc=json.dumps)
            return True
        except Exception:
            logging.exception('Failed to write replics_info')
            return False

    # === Failover state methods ===

    def get_failover_state(self) -> str | None:
        return self.noexcept_get(self.FAILOVER_STATE_PATH)

    def write_failover_state(self, state: str) -> bool:
        try:
            self.write(self.FAILOVER_STATE_PATH, state)
            return True
        except Exception:
            logging.exception('Failed to write failover state')
            return False

    def delete_failover_state(self) -> bool:
        return self.delete(self.FAILOVER_STATE_PATH)

    def write_current_promoting_host(self, hostname=None) -> bool:
        try:
            if hostname is None:
                hostname = helpers.get_hostname()
            self.write(self.CURRENT_PROMOTING_HOST, hostname)
            return True
        except Exception:
            logging.exception('Failed to write current promoting host')
            return False

    def delete_current_promoting_host(self) -> bool:
        return self.delete(self.CURRENT_PROMOTING_HOST)

    def ensure_failover_must_be_reset(self) -> bool:
        result = self.ensure_path(self.FAILOVER_MUST_BE_RESET)
        return result is not None

    def delete_failover_must_be_reset(self) -> bool:
        return self.delete(self.FAILOVER_MUST_BE_RESET)

    def get_last_failover_time(self) -> float | None:
        return self.noexcept_get(self.LAST_FAILOVER_TIME_PATH, preproc=float)

    def write_last_failover_time(self) -> bool:
        try:
            self.write(self.LAST_FAILOVER_TIME_PATH, time.time(), need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write last failover time')
            return False

    def get_last_primary_availability_time(self) -> float | None:
        return self.noexcept_get(self.LAST_PRIMARY_AVAILABILITY_TIME, preproc=float)

    def write_last_primary_availability_time(self) -> bool:
        try:
            self.write(self.LAST_PRIMARY_AVAILABILITY_TIME, time.time())
            return True
        except Exception:
            logging.exception('Failed to write last primary availability time')
            return False

    # === Switchover methods ===

    def get_switchover_state(self) -> str | None:
        return self.get(self.SWITCHOVER_STATE_PATH)

    def write_switchover_state(self, state: str) -> bool:
        try:
            self.write(self.SWITCHOVER_STATE_PATH, state, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write switchover state')
            return False

    def get_switchover_primary_info(self) -> dict | None:
        return self.get(self.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)

    def write_switchover_candidate(self, candidate: str) -> bool:
        try:
            self.write(self.SWITCHOVER_CANDIDATE, candidate)
            return True
        except Exception:
            logging.exception('Failed to write switchover candidate')
            return False

    def write_switchover_side_replicas(self, replicas: list) -> bool:
        try:
            self.write(self.SWITCHOVER_SIDE_REPLICAS, replicas, preproc=json.dumps)
            return True
        except Exception:
            logging.exception('Failed to write switchover side replicas')
            return False

    def write_last_switchover_time(self) -> bool:
        try:
            self.write(self.LAST_SWITCHOVER_TIME_PATH, time.time(), need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write last switchover time')
            return False

    def cleanup_switchover(self) -> None:
        """Clean up all switchover-related nodes."""
        paths_to_delete = [
            self.SWITCHOVER_CANDIDATE,
            self.SWITCHOVER_SIDE_REPLICAS,
            self.SWITCHOVER_STATE_PATH,
            self.SWITCHOVER_PRIMARY_PATH,
            self.FAILOVER_STATE_PATH,
        ]
        for path in paths_to_delete:
            self.delete(path)

    # === Timing methods ===

    def get_timing(self, name: str) -> float | None:
        return self.noexcept_get(self._get_timing_path(name), preproc=float)

    def write_timing(self, name: str, ts: float) -> None:
        try:
            self.ensure_path(self._get_timing_path(name))
            self.noexcept_write(self._get_timing_path(name), ts, need_lock=False)
        except Exception:
            logging.exception('Failed to write timing: %s', name)

    def delete_timing(self, name: str) -> bool:
        return self.delete(self._get_timing_path(name), recursive=True)

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

    def ensure_quorum_path(self) -> bool:
        """Ensure the quorum path exists in ZK. Returns True on success."""
        result = self.ensure_path(self.QUORUM_PATH)
        return result is not None

    def get_quorum(self) -> list | None:
        """Return current quorum host list from ZK, or None on error."""
        return self.get(self.QUORUM_PATH, preproc=helpers.load_json_or_default)

    def write_quorum(self, hosts: list) -> bool:
        """Persist quorum host list to ZK."""
        try:
            self.write(self.QUORUM_PATH, hosts, preproc=json.dumps, need_lock=False)
            return True
        except Exception:
            logging.exception('Failed to write quorum')
            return False

    def clear_quorum(self) -> bool:
        """Write empty list to quorum path."""
        return self.write_quorum([])

    def get_quorum_replics_for_promote(self):
        quorum = self.get_quorum() or []
        my_hostname = helpers.get_hostname()
        return {h for h in quorum if h != my_hostname}

    # === Members / host priority methods ===

    def get_root_children(self) -> list | None:
        """Return list of top-level nodes under the ZK path prefix."""
        return self.get_children("")

    def get_member_path(self, hostname: str) -> str:
        """Return the ZK path for a cluster member."""
        return f'{self.MEMBERS_PATH}/{hostname}'

    def member_exists(self, hostname: str) -> bool:
        """Return True if the member node exists in ZK."""
        return self.exists_path(self.get_member_path(hostname))

    def ensure_member(self, hostname: str) -> bool:
        """Ensure the member node exists in ZK. Returns True on success."""
        return self.ensure_path(self.get_member_path(hostname)) is not None

    def get_members(self, catch_except=True) -> list | None:
        """Return list of all cluster member hostnames."""
        return self.get_children(self.MEMBERS_PATH, catch_except=catch_except)

    def get_host_prio(self, hostname=None) -> str | None:
        """Return stored priority value for hostname (current host if None)."""
        return self.noexcept_get(self._get_host_prio_path(hostname))

    def write_host_prio(self, prio, hostname=None) -> bool:
        """Persist priority for hostname (current host if None)."""
        return self.noexcept_write(self._get_host_prio_path(hostname), prio, need_lock=False)

    # === Single-node status methods ===

    def set_single_node(self) -> None:
        """Mark cluster as single-node in ZK."""
        self.ensure_path(self.SINGLE_NODE_PATH)

    def clear_single_node(self) -> None:
        """Remove single-node marker from ZK."""
        self.delete(self.SINGLE_NODE_PATH)

    # === Simple primary switch tracking ===

    def get_simple_primary_switch_tried(self, hostname=None) -> bool:
        """Return True if simple primary switch was already tried for hostname."""
        return self.noexcept_get(self._get_simple_primary_switch_try_path(hostname)) == 'yes'

    def set_simple_primary_switch_tried(self, hostname=None) -> None:
        """Mark simple primary switch as tried for hostname."""
        self.noexcept_write(self._get_simple_primary_switch_try_path(hostname), 'yes', need_lock=False)

    def reset_simple_primary_switch_tried(self, hostname=None) -> None:
        """Reset simple primary switch flag for hostname."""
        if self.noexcept_get(self._get_simple_primary_switch_try_path(hostname)) != 'no':
            self.noexcept_write(self._get_simple_primary_switch_try_path(hostname), 'no', need_lock=False)

    # === Stream-source replica info ===

    def get_stream_source_replics_info(self, stream_from: str) -> list | None:
        """Return replics_info for a non-HA replica's stream source host."""
        path = '{member_path}/{hostname}/replics_info'.format(
            member_path=self.MEMBERS_PATH, hostname=stream_from
        )
        return self.noexcept_get(path, preproc=__import__('json').loads)

    # === Legacy cleanup ===

    def delete_legacy_timings_path(self) -> None:
        """Delete mistakenly-created literal 'timing/%s' node."""
        self.delete(self.TIMINGS_PATH)

    def get_alive_hosts(self, timeout=1, catch_except=True, all_hosts_timeout=None):
        ha_hosts = self.get_ha_hosts(catch_except=catch_except)
        if ha_hosts is None:
            return []
        if all_hosts_timeout:
            minimal_total_timeout = timeout * len(ha_hosts)
            if minimal_total_timeout > all_hosts_timeout:
                logging.warning("Expected timeout for checking host aliveness will be ignored.")
                logging.debug(
                    "The minimal total timeout for checking the aliveness of all hosts (%s s) "
                    "is greater than the expected one - all_hosts_timeout (%s s)."
                    "Consider increasing the election timeout.",
                    minimal_total_timeout,
                    all_hosts_timeout,
                )
            else:
                timeout = all_hosts_timeout / len(ha_hosts)
        alive_hosts = [host for host in ha_hosts if self.is_host_alive(host, timeout, catch_except)]
        return alive_hosts


def create_zk(config: RawConfigParser, plugins, lock_contender_name=None) -> Zookeeper:
    """Factory: build and connect a Zookeeper instance from config."""
    prefix = config.get('global', 'zk_lockpath_prefix')
    zk_config = ZookeeperConfig(
        release_lock_after_acquire_failed=config.getboolean('global', 'release_lock_after_acquire_failed'),
        timeout=config.getfloat('global', 'iteration_timeout'),
        path_prefix=prefix if prefix is not None else helpers.get_lockpath_prefix(),
        lock_contender_name=lock_contender_name,
    )

    try:
        # Create and connect the client first (no listener yet — set after Zookeeper is constructed)
        zk_client = create_zk_client(config, path_prefix=zk_config.path_prefix)
        if not zk_client.init():
            raise Exception('Could not connect to ZK.')
    except Exception:
        logging.exception('Could not initialize ZooKeeper connection')
        raise

    return Zookeeper(
        zk_client=zk_client,
        plugins=plugins,
        config=zk_config,
    )
