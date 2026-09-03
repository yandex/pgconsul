# encoding: utf-8
"""
Zookeeper wrapper module. Zookeeper class defined here.
"""

import json
import logging
import time
import uuid
from configparser import RawConfigParser
from dataclasses import dataclass

from . import helpers
from .failover import FailoverHealthReport, FailoverProbe, FailoverRequest
from .types import DesiredPrimary, DurabilityConfig, DurabilityState
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
    DESIRED_PRIMARY_PATH = 'desired_primary'
    DURABILITY_MEMBERS_PATH = 'durability_members'

    REPLICS_INFO_PATH = 'replics_info'
    TIMELINE_INFO_PATH = 'timeline'
    TIMELINE_HIGH_WATERMARK_PATH = 'timeline_high_watermark'
    FAILOVER_STATE_PATH = 'failover_state'
    FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    LAST_FAILOVER_TIME_PATH = 'last_failover_time'
    LAST_PRIMARY_AVAILABILITY_TIME = 'last_master_activity_time'
    LAST_SWITCHOVER_TIME_PATH = 'last_switchover_time'
    SWITCHOVER_ROOT_PATH = 'switchover'
    SWITCHOVER_LOCK_PATH = f'{SWITCHOVER_ROOT_PATH}/lock'
    SWITCHOVER_MANAGER_LOCK_PATH = f'{SWITCHOVER_ROOT_PATH}/manager'
    SWITCHOVER_RECORD_PATH = f'{SWITCHOVER_ROOT_PATH}/record'
    SWITCHOVER_ACKS_PATH = f'{SWITCHOVER_ROOT_PATH}/acks'
    SWITCHOVER_VERSION_KEY = 'switchover_version'
    MAINTENANCE_PATH = 'maintenance'
    MAINTENANCE_TIME_PATH = f'{MAINTENANCE_PATH}/ts'
    MAINTENANCE_PRIMARY_PATH = f'{MAINTENANCE_PATH}/master'
    HOST_MAINTENANCE_PATH = f'{MAINTENANCE_PATH}/%s'
    HOST_ALIVE_LOCK_PATH = 'alive/%s'
    HOST_REPLICATION_SOURCES = 'replication_sources'
    TIMINGS_PATH = 'timing/%s'

    SINGLE_NODE_PATH = 'is_single_node'

    ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    ELECTION_WINNER_PATH = 'election_winner'
    ELECTION_VOTES_PATH = 'election_vote'
    ELECTION_VOTE_PATH = 'election_vote/%s'
    FAILOVER_MEMBERS_PATH = 'failover_members'
    FAILOVER_VERSION_PATH = 'failover_version'
    FAILOVER_PARTICIPANTS_PATH = 'failover_participant'
    FAILOVER_PARTICIPANT_PATH = 'failover_participant/%s'
    FAILOVER_PROBE_PATH = 'failover_probe'
    FAILOVER_REQUEST_PATH = 'failover_request'
    FAILOVER_HEALTH_PATH = 'failover_health'
    FAILOVER_HOST_HEALTH_PATH = f'{FAILOVER_HEALTH_PATH}/%s'

    MEMBERS_PATH = 'all_hosts'
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

    def _drop_all_locks(self) -> None:
        """Release all held locks, swallow errors, clear the registry."""
        for lock in list(self._locks.values()):
            try:
                if lock:
                    lock.release()
            except Exception:
                logging.debug("Error releasing lock", exc_info=True)
        self._locks = {}

    def close(self) -> None:
        """Release all locks and close ZK connection."""
        self._drop_all_locks()
        self._zk_client.close()

    def __enter__(self) -> 'Zookeeper':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

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
        if need_lock and not self.is_lock_holder():
            return False
        return self._zk_client.write(path, data)

    def is_lock_holder(self, name=None) -> bool:
        """Check current ownership at action time."""
        return self.get_current_lock_holder(name) == self._get_lock_contender_name()

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
        except ZkNoNodeError:
            # Lock path does not exist yet — no contenders, proceed to acquire.
            logging.debug('Lock "%s" path does not exist yet, no contenders', name)
            contenders = []
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
            released = lock.release()
            if released:
                self._delete_lock(name)
            return released

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
        self._drop_all_locks()

        connected = self._zk_client.reconnect()

        if connected:
            self._init_lock(self.PRIMARY_LOCK_PATH)

        return connected

    def re_init(self):
        """Reconnect to ZK if the connection is lost."""
        try:
            if not self.is_alive():
                logging.warning('Some error with ZK client. Trying to reconnect.')
                self.reconnect()
        except Exception:
            logging.exception('Unexpected error during re_init')

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
        data[self.ELECTION_WINNER_PATH] = self.get_election_winner()
        data[self.FAILOVER_MUST_BE_RESET] = self.exists_path(self.FAILOVER_MUST_BE_RESET)
        data['lock_version'] = self._zk_client.lock_version(self._lockpath)
        data['lock_holder'] = self.get_current_lock_holder()
        data['single_node'] = self.is_single_node()
        data[self.TIMELINE_INFO_PATH] = self.get(self.TIMELINE_INFO_PATH, preproc=int)
        record, version = self.get_switchover_record()
        data[self.SWITCHOVER_RECORD_PATH] = record
        data[self.SWITCHOVER_VERSION_KEY] = version
        data[self.MAINTENANCE_PATH] = {
            'status': self.get(self.MAINTENANCE_PATH),
            'ts': self.get(self.MAINTENANCE_TIME_PATH),
        }
        data[self.LAST_PRIMARY_PATH] = self.get(self.LAST_PRIMARY_PATH)
        durability, _ = self.get_durability_state()
        data[self.DURABILITY_MEMBERS_PATH] = durability.to_dict()
        desired, desired_version = self.get_desired_primary()
        data[self.DESIRED_PRIMARY_PATH] = desired.to_dict() if desired is not None else None
        data[f'{self.DESIRED_PRIMARY_PATH}_version'] = desired_version
        probe, _ = self.get_failover_probe()
        data[self.FAILOVER_PROBE_PATH] = probe.to_dict() if probe is not None else None
        request, request_version = self.get_failover_request()
        data[self.FAILOVER_REQUEST_PATH] = request.to_dict() if request is not None else None
        data[f'{self.FAILOVER_REQUEST_PATH}_version'] = request_version
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

    def delete(self, key, recursive=False) -> bool:
        """Delete key from zk. Returns True on success or when absent, False on error."""
        try:
            return self._zk_client.delete(key, recursive=recursive)
        except ZkClientError:
            logging.exception('Failed to delete zk node %s', key)
            return False

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
        if lock_type == self.PRIMARY_LOCK_PATH and not self._primary_lock_is_desired():
            logging.warning('Refusing leader lock: local host is not the desired primary')
            return False
        acquired = self._acquire_lock(lock_type, allow_queue, timeout, read_lock=read_lock)
        if lock_type == self.PRIMARY_LOCK_PATH and acquired:
            contender = self._get_lock_contender_name()
            desired, version = self.get_desired_primary()
            if desired is None and version is None:
                if self.write_desired_primary(
                    DesiredPrimary.steady(contender),
                    version,
                ) is None:
                    logging.warning('Desired primary changed while materializing lock ownership')
                    self._release_lock(lock_type)
                    return False
            elif desired is None or desired.hostname != contender:
                logging.warning('Desired primary changed while acquiring leader lock')
                self._release_lock(lock_type)
                return False
            self.write(self.LAST_PRIMARY_PATH, helpers.get_hostname())
        return acquired

    def _primary_lock_is_desired(self) -> bool:
        desired, version = self.get_desired_primary()
        if desired is None:
            # Only a genuinely absent node permits legacy/bootstrap ownership.
            return version is None
        return desired.hostname == self._get_lock_contender_name()

    def force_release_primary_lock(self, expected_holder: str) -> bool:
        """Delete only the versioned contender still owned by expected_holder."""
        try:
            holder = self._zk_client.get_lock_holder_node(self.PRIMARY_LOCK_PATH)
            if holder is None:
                return True
            path, identifier, version = holder
            if identifier != expected_holder:
                logging.warning(
                    'Primary lock holder changed: expected=%s actual=%s',
                    expected_holder,
                    identifier,
                )
                return False
            return self._zk_client.compare_and_delete(path, version)
        except ZkClientError as exception:
            raise ZookeeperException(exception)

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

    def _get_host_prio_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_PRIO_PATH, hostname)

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
        except Exception:
            logging.exception('Failed to write SSN on changes')
            return False

    def _get_election_vote_path(self, hostname=None):
        if hostname is None:
            hostname = helpers.get_hostname()
        return self.ELECTION_VOTE_PATH % hostname

    # === Election methods ===

    def get_election_host_vote(
        self,
        hostname: str,
        failover_version: str,
        timeline: int,
    ) -> tuple[int, int] | None:
        """Returns (lsn, priority) for hostname's election vote, or None if unavailable."""
        vote = self.get_election_host_vote_with_timeline(hostname, failover_version)
        if vote is None or vote[2] != timeline:
            return None
        return vote[0], vote[1]

    def get_election_host_vote_with_timeline(
        self,
        hostname: str,
        failover_version: str,
    ) -> tuple[int, int, int] | None:
        """Return a vote with its actual timeline for branch-safe failover."""
        vote_path = self._get_election_vote_path(hostname)
        vote = self.get(vote_path, preproc=json.loads)
        if not isinstance(vote, dict):
            return None
        if vote.get('failover_version') != failover_version:
            return None
        try:
            return (
                int(vote['flush_lsn']),
                int(vote['priority']),
                int(vote['timeline']),
            )
        except (KeyError, TypeError, ValueError):
            logging.error("Invalid election vote from '%s': %s", hostname, vote)
            return None

    def write_election_vote(
        self,
        lsn: int,
        prio: int,
        failover_version: str,
        timeline: int,
    ) -> bool:
        """Write current host's election vote (lsn and priority)."""
        vote_path = self._get_election_vote_path()
        try:
            return self.write(
                vote_path,
                {
                    'failover_version': failover_version,
                    'timeline': timeline,
                    'flush_lsn': int(lsn),
                    'priority': int(prio),
                },
                preproc=json.dumps,
                need_lock=False,
            )
        except Exception:
            logging.exception('Failed to write election vote')
            return False

    def get_election_winner(self) -> str | None:
        return self.get(self.ELECTION_WINNER_PATH)

    def write_election_winner(self, hostname: str) -> bool:
        try:
            failover_version = self.get_failover_version()
            if failover_version is None:
                return False
            desired, desired_version = self.get_desired_primary()
            if desired is None or desired.operation_id != failover_version:
                logging.error('Desired primary is not fenced by current failover')
                return False
            if desired.hostname not in (None, hostname):
                logging.error('Another desired primary is already selected: %s', desired.hostname)
                return False
            # Persist the decision first. Participants cannot act while the
            # global phase is still VOTING; a retry then completes the CAS.
            if not self.write(self.ELECTION_WINNER_PATH, hostname, need_lock=False):
                return False
            if desired.hostname is None:
                desired = DesiredPrimary(hostname, failover_version, 'failover')
                if self.write_desired_primary(desired, desired_version) is None:
                    return False
            return True
        except Exception:
            logging.exception('Failed to write election winner')
            return False

    # === Desired primary and failover health probing ===

    def get_desired_primary(self) -> tuple[DesiredPrimary | None, int | None]:
        try:
            value, version = self._zk_client.get_with_version(self.DESIRED_PRIMARY_PATH)
        except ZkNoNodeError:
            return None, None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        if not value:
            return None, version
        try:
            record = json.loads(value)
            if not isinstance(record, dict):
                raise ValueError('desired primary is not an object')
            return DesiredPrimary.from_dict(record), version
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid desired primary: %r', value)
            return None, version

    def write_desired_primary(
        self,
        desired: DesiredPrimary,
        version: int | None,
    ) -> int | None:
        try:
            return self._zk_client.compare_and_set(
                self.DESIRED_PRIMARY_PATH,
                json.dumps(desired.to_dict()),
                version,
            )
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    def get_failover_request(self) -> tuple[FailoverRequest | None, int | None]:
        try:
            value, version = self._zk_client.get_with_version(self.FAILOVER_REQUEST_PATH)
        except ZkNoNodeError:
            return None, None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        if not value:
            return None, version
        try:
            record = json.loads(value)
            if not isinstance(record, dict):
                raise ValueError('failover request is not an object')
            return FailoverRequest.from_dict(record), version
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid failover request: %r', value)
            return None, version

    def write_failover_request(
        self,
        request: FailoverRequest,
        version: int | None,
    ) -> int | None:
        try:
            return self._zk_client.compare_and_set(
                self.FAILOVER_REQUEST_PATH,
                json.dumps(request.to_dict()),
                version,
            )
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    def get_failover_probe(self) -> tuple[FailoverProbe | None, int | None]:
        try:
            value, version = self._zk_client.get_with_version(self.FAILOVER_PROBE_PATH)
        except ZkNoNodeError:
            return None, None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        if not value:
            return None, version
        try:
            record = json.loads(value)
            if not isinstance(record, dict):
                raise ValueError('failover probe is not an object')
            return FailoverProbe.from_dict(record), version
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid failover probe: %r', value)
            return None, version

    def start_failover_probe(
        self,
        primary: str,
        durabilities: tuple[DurabilityConfig, ...],
        durability_version: int,
        probe_timeout: float,
    ) -> FailoverProbe | None:
        if not self.is_lock_holder(self.ELECTION_MANAGER_LOCK_PATH):
            logging.error('Only the failover manager may start a health probe')
            return None
        current, version = self.get_failover_probe()
        members = tuple(sorted({
            host for durability in durabilities for host in durability.members
        }))
        quorums = tuple(durability.members for durability in durabilities)
        now = time.time()
        if (
            current is not None
            and current.primary == primary
            and current.durability_members == members
            and current.durability_version == durability_version
            and current.quorum_memberships == quorums
            and current.expires_at > now
        ):
            return current
        probe = FailoverProbe(
            probe_id=(current.probe_id + 1) if current is not None else 1,
            primary=primary,
            durability_members=members,
            durability_version=durability_version,
            operation_id=uuid.uuid4().hex,
            durability_quorums=quorums,
            expires_at=now + probe_timeout,
        )
        try:
            new_version = self._zk_client.compare_and_set(
                self.FAILOVER_PROBE_PATH,
                json.dumps(probe.to_dict()),
                version,
            )
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        return probe if new_version is not None else None

    def write_failover_health(self, report: FailoverHealthReport) -> bool:
        return self.write(
            self.FAILOVER_HOST_HEALTH_PATH % helpers.get_hostname(),
            report.to_dict(),
            preproc=json.dumps,
            need_lock=False,
        )

    def get_failover_health(
        self,
        hostname: str,
        probe: FailoverProbe,
    ) -> FailoverHealthReport | None:
        value = self.get(
            self.FAILOVER_HOST_HEALTH_PATH % hostname,
            preproc=json.loads,
        )
        if not isinstance(value, dict):
            return None
        try:
            report = FailoverHealthReport.from_dict(value)
        except (KeyError, TypeError, ValueError):
            logging.warning('Invalid failover health report from %s: %r', hostname, value)
            return None
        if (
            report.probe_id != probe.probe_id
            or report.primary != probe.primary
            or report.durability_version != probe.durability_version
        ):
            return None
        return report

    def get_failover_members(self) -> list[str]:
        return self.get(self.FAILOVER_MEMBERS_PATH, preproc=json.loads) or []

    def write_failover_members(self, members: list[str]) -> bool:
        return self.write(
            self.FAILOVER_MEMBERS_PATH,
            sorted(members),
            preproc=json.dumps,
            need_lock=False,
        )

    def get_failover_version(self) -> str | None:
        return self.get(self.FAILOVER_VERSION_PATH)

    def write_failover_version(self, version: str) -> bool:
        return self.write(self.FAILOVER_VERSION_PATH, version, need_lock=False)

    def get_failover_participant_state(self, hostname: str, version: str) -> str | None:
        path = self.FAILOVER_PARTICIPANT_PATH % hostname
        value = self.get(path, preproc=json.loads)
        if not isinstance(value, dict) or value.get('failover_version') != version:
            return None
        state = value.get('state')
        return state if isinstance(state, str) else None

    def write_failover_participant_state(self, state: str, version: str) -> bool:
        path = self.FAILOVER_PARTICIPANT_PATH % helpers.get_hostname()
        return self.write(
            path,
            {'failover_version': version, 'state': state},
            preproc=json.dumps,
            need_lock=False,
        )

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

    def get_ha_replics(self, my_hostname: str) -> set | None:
        """HA hosts excluding the current host, or None if no hosts."""
        hosts = self.get_ha_hosts()
        if not hosts:
            return None
        if my_hostname in hosts:
            hosts.remove(my_hostname)
        return set(hosts)

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
        return self.delete(path)

    def _get_host_replics_info_path(self, hostname=None):
        return helpers.get_host_path(self.HOST_REPLICS_INFO_PATH, hostname)

    def write_host_replics_info(self, replics_info, hostname=None) -> bool:
        return self.noexcept_write(
            self._get_host_replics_info_path(hostname), replics_info, preproc=json.dumps, need_lock=False
        )

    def get_host_replics_info(self, hostname) -> list | None:
        return self.get(self._get_host_replics_info_path(hostname), preproc=json.loads)

    def _get_host_wal_receiver_path(self, hostname=None):
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
            return self.write(self.MAINTENANCE_PATH, status, need_lock=False)
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
            return self.write(self.MAINTENANCE_TIME_PATH, time.time(), need_lock=False)
        except Exception:
            logging.exception('Failed to write maintenance timestamp')
            return False

    def get_maintenance_primary(self) -> str | None:
        return self.get(self.MAINTENANCE_PRIMARY_PATH)

    def write_maintenance_primary(self, primary_fqdn: str) -> bool:
        try:
            return self.write(self.MAINTENANCE_PRIMARY_PATH, primary_fqdn, need_lock=False)
        except Exception:
            logging.exception('Failed to write maintenance primary')
            return False

    def write_host_maintenance_enabled(self, hostname=None) -> bool:
        if not self.write(self._get_host_maintenance_path(hostname), 'enable', need_lock=False):
            raise ZookeeperException('Failed to write host maintenance enabled')
        return True

    # === Timeline methods ===

    def get_timeline(self) -> int | None:
        return self.get(self.TIMELINE_INFO_PATH, preproc=int)

    def write_timeline(self, timeline: int) -> bool:
        try:
            return self.write(self.TIMELINE_INFO_PATH, timeline)
        except Exception:
            logging.exception('Failed to write timeline')
            return False

    def reserve_timeline(
        self,
        operation_id: str,
        observed_highest: int,
    ) -> int | None:
        """CAS-reserve a never-before-used promotion timeline."""
        try:
            try:
                raw, version = self._zk_client.get_with_version(
                    self.TIMELINE_HIGH_WATERMARK_PATH,
                )
            except ZkNoNodeError:
                raw, version = None, None
            value = json.loads(raw) if raw is not None else None
            if isinstance(value, dict):
                current = int(value['timeline'])
                if (
                    value.get('operation_id') == operation_id
                    and current > observed_highest
                ):
                    return current
            else:
                current = observed_highest
            target = max(current, observed_highest) + 1
            new_value = json.dumps({
                'timeline': target,
                'operation_id': operation_id,
            })
            if self._zk_client.compare_and_set(
                self.TIMELINE_HIGH_WATERMARK_PATH,
                new_value,
                version,
            ) is None:
                return None
            return target
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid timeline high-water mark')
            return None
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    def get_timeline_high_watermark(self) -> int | None:
        try:
            raw, _ = self._zk_client.get_with_version(
                self.TIMELINE_HIGH_WATERMARK_PATH,
            )
        except ZkNoNodeError:
            return None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        try:
            value = json.loads(raw) if raw is not None else None
            return int(value['timeline']) if isinstance(value, dict) else None
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid timeline high-water mark')
            return None

    def ensure_timeline_high_watermark(self, observed_highest: int) -> bool:
        """Initialize or advance the mark from the live primary's history."""
        try:
            try:
                raw, version = self._zk_client.get_with_version(
                    self.TIMELINE_HIGH_WATERMARK_PATH,
                )
            except ZkNoNodeError:
                raw, version = None, None
            value = json.loads(raw) if raw is not None else None
            if isinstance(value, dict):
                current = int(value['timeline'])
                if current >= observed_highest:
                    return True
            new_value = json.dumps({
                'timeline': observed_highest,
                'operation_id': 'observed-primary-history',
            })
            return self._zk_client.compare_and_set(
                self.TIMELINE_HIGH_WATERMARK_PATH,
                new_value,
                version,
            ) is not None
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid timeline high-water mark')
            return False
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    # === Global replics_info methods ===

    def get_replics_info(self) -> list | None:
        return self.get(self.REPLICS_INFO_PATH, preproc=json.loads)

    def noexcept_get_replics_info(self) -> list | None:
        return self.noexcept_get(self.REPLICS_INFO_PATH, preproc=json.loads)

    def write_replics_info(self, replics_info) -> bool:
        try:
            return self.write(self.REPLICS_INFO_PATH, replics_info, preproc=json.dumps)
        except Exception:
            logging.exception('Failed to write replics_info')
            return False

    # === Failover state methods ===

    def get_failover_state(self) -> str | None:
        """Return the optional persistent failover phase marker."""
        return self.noexcept_get(self.FAILOVER_STATE_PATH)

    def write_failover_state(self, state: str) -> bool:
        try:
            return self.write(self.FAILOVER_STATE_PATH, state, need_lock=False)
        except Exception:
            logging.exception('Failed to write failover state')
            return False

    def delete_failover_state(self) -> bool:
        return self.delete(self.FAILOVER_STATE_PATH)

    def cleanup_failover(self) -> bool:
        """Delete failover metadata, removing the state marker last."""
        paths = (
            (self.ELECTION_VOTES_PATH, True),
            (self.ELECTION_WINNER_PATH, False),
            (self.FAILOVER_MEMBERS_PATH, False),
            (self.FAILOVER_VERSION_PATH, False),
            (self.FAILOVER_PARTICIPANTS_PATH, True),
            (self.FAILOVER_REQUEST_PATH, False),
        )
        if not all(self.delete(path, recursive=recursive) for path, recursive in paths):
            return False
        return self.delete_failover_state()

    def ensure_failover_must_be_reset(self) -> bool:
        result = self.ensure_path(self.FAILOVER_MUST_BE_RESET)
        return result is not None

    def delete_failover_must_be_reset(self) -> bool:
        return self.delete(self.FAILOVER_MUST_BE_RESET)

    def get_last_failover_time(self) -> float | None:
        return self.noexcept_get(self.LAST_FAILOVER_TIME_PATH, preproc=float)

    def get_last_role_transition_time(self) -> float | None:
        timestamps = (self.get_last_failover_time(), self.get_last_switchover_time())
        return max((value for value in timestamps if value is not None), default=None)

    def write_last_failover_time(self) -> bool:
        try:
            return self.write(self.LAST_FAILOVER_TIME_PATH, time.time(), need_lock=False)
        except Exception:
            logging.exception('Failed to write last failover time')
            return False

    def get_last_primary_availability_time(self) -> float | None:
        return self.noexcept_get(self.LAST_PRIMARY_AVAILABILITY_TIME, preproc=float)

    def write_last_primary_availability_time(self) -> bool:
        try:
            return self.write(self.LAST_PRIMARY_AVAILABILITY_TIME, time.time())
        except Exception:
            logging.exception('Failed to write last primary availability time')
            return False

    # === Switchover methods ===

    def get_switchover_record(self) -> tuple[dict | None, int | None]:
        """Read the complete switchover record and its ZK version atomically."""
        try:
            value, version = self._zk_client.get_with_version(self.SWITCHOVER_RECORD_PATH)
        except ZkNoNodeError:
            return None, None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        if value is None:
            return None, version
        try:
            record = json.loads(value)
        except (TypeError, ValueError):
            logging.error('Invalid switchover record: %r', value)
            return None, version
        if not isinstance(record, dict):
            logging.error('Switchover record is not an object: %r', value)
            return None, version
        return record, version

    def write_switchover_record(self, record: dict, version: int | None) -> int | None:
        """CAS-write a complete switchover record."""
        try:
            return self._zk_client.compare_and_set(
                self.SWITCHOVER_RECORD_PATH,
                json.dumps(record),
                version,
            )
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    def get_last_switchover_time(self) -> float | None:
        return self.noexcept_get(self.LAST_SWITCHOVER_TIME_PATH, preproc=float)

    def write_last_switchover_time(self) -> bool:
        try:
            return self.write(self.LAST_SWITCHOVER_TIME_PATH, time.time(), need_lock=False)
        except Exception:
            logging.exception('Failed to write last switchover time')
            return False

    def cleanup_switchover(self, version: int) -> bool:
        """Clear switchover metadata without resetting its ZK version."""
        return self.write_switchover_record({}, version) is not None

    def _get_switchover_ack_path(self, hostname: str) -> str:
        return f'{self.SWITCHOVER_ACKS_PATH}/{hostname}'

    def write_switchover_ack(self, hostname: str, operation_id: str, state: dict) -> bool:
        """Publish a host-local acknowledgement for one switchover operation."""
        value = {'operation_id': operation_id, **state}
        return self.noexcept_write(
            self._get_switchover_ack_path(hostname),
            value,
            preproc=json.dumps,
            need_lock=False,
        )

    def get_switchover_ack(self, hostname: str, operation_id: str) -> dict | None:
        """Return an acknowledgement only when it belongs to the active operation."""
        value = self.noexcept_get(self._get_switchover_ack_path(hostname), preproc=json.loads)
        if not isinstance(value, dict) or value.get('operation_id') != operation_id:
            return None
        return value

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

    def get_operation_timing(self, name: str, operation_id: str) -> float | None:
        """Return a timing only when it belongs to the requested operation."""
        try:
            raw, _ = self._zk_client.get_with_version(
                self._get_timing_path(name),
            )
        except ZkNoNodeError:
            return None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        try:
            value = json.loads(raw) if raw is not None else None
            if not isinstance(value, dict) or value.get('operation_id') != operation_id:
                return None
            return float(value['started_at'])
        except (KeyError, TypeError, ValueError):
            logging.error('Invalid operation timing %s: %r', name, raw)
            return None

    def start_operation_timing(
        self,
        name: str,
        operation_id: str,
        started_at: float,
    ) -> bool:
        """CAS-start one operation timing without moving an existing start."""
        path = self._get_timing_path(name)
        try:
            try:
                raw, version = self._zk_client.get_with_version(path)
            except ZkNoNodeError:
                raw, version = None, None
            try:
                current = json.loads(raw) if raw is not None else None
            except (TypeError, ValueError):
                current = None
            if (
                isinstance(current, dict)
                and current.get('operation_id') == operation_id
            ):
                return True
            value = json.dumps({
                'operation_id': operation_id,
                'started_at': started_at,
            })
            return self._zk_client.compare_and_set(path, value, version) is not None
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    def delete_operation_timing(self, name: str, operation_id: str) -> bool:
        """CAS-delete a timing only when it still belongs to this operation."""
        path = self._get_timing_path(name)
        try:
            try:
                raw, version = self._zk_client.get_with_version(path)
            except ZkNoNodeError:
                return True
            try:
                current = json.loads(raw) if raw is not None else None
            except (TypeError, ValueError):
                current = None
            if (
                not isinstance(current, dict)
                or current.get('operation_id') != operation_id
            ):
                return True
            return self._zk_client.compare_and_delete(path, version)
        except ZkClientError as exception:
            raise ZookeeperException(exception)

    def is_host_alive(self, hostname, timeout=0.0, catch_except=True):
        alive_path = self.get_host_alive_lock_path(hostname)
        return helpers.await_for(
            lambda: self.get_current_lock_holder(alive_path, catch_except) is not None, timeout, f'{hostname} is alive'
        )

    def ensure_durability_path(self) -> bool:
        """Ensure the durable-membership state path exists."""
        return self.ensure_path(self.DURABILITY_MEMBERS_PATH) is not None

    def get_durability_state(self) -> tuple[DurabilityState, int | None]:
        """Read stable durability members, transition, and ZK version."""
        try:
            value, version = self._zk_client.get_with_version(self.DURABILITY_MEMBERS_PATH)
        except ZkNoNodeError:
            return DurabilityState(None), None
        except ZkClientError as exception:
            raise ZookeeperException(exception)
        if not value:
            return DurabilityState(None), version
        try:
            record = json.loads(value)
            if not isinstance(record, dict):
                raise ValueError('durability state is not an object')
            return DurabilityState.from_dict(record), version
        except (KeyError, TypeError, ValueError):
            logging.exception('Invalid durability state: %r', value)
            return DurabilityState(None), version

    def get_durability_config(self) -> DurabilityConfig | None:
        """Return stable durability members for ordinary reconciliation."""
        state, _ = self.get_durability_state()
        return state.stable

    def write_durability_state(self, state: DurabilityState, version: int | None) -> int | None:
        """CAS-write durability state while owning both durability locks."""
        if not self.is_lock_holder():
            logging.error('Cannot write durability state without the primary lock')
            return None
        if not self.is_lock_holder(self.ELECTION_MANAGER_LOCK_PATH):
            logging.error('Cannot write durability state without election manager lock')
            return None
        try:
            return self._zk_client.compare_and_set(
                self.DURABILITY_MEMBERS_PATH,
                json.dumps(state.to_dict()),
                version,
            )
        except ZkClientError as exception:
            raise ZookeeperException(exception)

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

    def get_members_retry(self, iteration_timeout: float) -> list | None:
        """Ensure MEMBERS_PATH and return members, retrying until available."""
        while True:
            self.ensure_path(self.MEMBERS_PATH)
            members = self.get_members()
            if members is not None:
                return members
            self.re_init()
            time.sleep(iteration_timeout)

    def get_host_prio(self, hostname=None, catch_except=True) -> str | None:
        """Return stored priority value for hostname (current host if None)."""
        path = self._get_host_prio_path(hostname)
        if catch_except:
            return self.noexcept_get(path)
        return self.get(path)

    def write_host_prio(self, prio, hostname=None) -> bool:
        """Persist priority for hostname (current host if None)."""
        return self.noexcept_write(self._get_host_prio_path(hostname), prio, need_lock=False)

    # === Single-node status methods ===

    def is_single_node(self, catch_except=True) -> bool:
        """Return True if the single-node marker exists in ZK."""
        return self.exists_path(self.SINGLE_NODE_PATH, catch_except=catch_except)

    def set_single_node(self) -> None:
        """Mark cluster as single-node in ZK."""
        if not self.ensure_path(self.SINGLE_NODE_PATH):
            raise ZookeeperException('Failed to set single-node status')

    def clear_single_node(self) -> None:
        """Remove single-node marker from ZK."""
        if not self.delete(self.SINGLE_NODE_PATH):
            raise ZookeeperException('Failed to clear single-node status')

    def update_single_node_status(self, role: str) -> bool:
        """Update the single-node marker and return its new status."""
        if role == 'primary':
            ha_hosts = self.get_ha_hosts(catch_except=False)
            is_single = len(ha_hosts) == 1
            if is_single:
                self.set_single_node()
            else:
                self.clear_single_node()
            return is_single
        else:
            return self.is_single_node(catch_except=False)

    # === Stream-source replica info ===

    def get_stream_source_replics_info(self, stream_from: str) -> list | None:
        """Return replics_info for a non-HA replica's stream source host."""
        path = '{member_path}/{hostname}/replics_info'.format(
            member_path=self.MEMBERS_PATH, hostname=stream_from
        )
        return self.noexcept_get(path, preproc=json.loads)

    # === Host stat writing (step 12d, Variant A) ===

    def write_host_stat(self, hostname: str, db_state: dict, stream_from: str | None) -> bool:
        """Write host statistics (HA status, wal_receiver, replics_info) to ZK.

        Returns True on success, False if any ZK write failed.
        Writes are not transactional — on partial failure already-written data
        is not rolled back; the next iteration overwrites stale values.
        Pure ZK logic moved from main.py (step 12d, Variant A).
        """
        replics_info = db_state.get('replics_info')
        wal_receiver_info = db_state.get('wal_receiver')
        if not stream_from:
            if not self.ensure_host_ha(hostname):
                logging.warning('Could not write ha host in ZK.')
                return False
        else:
            if not self.delete_host_ha(hostname):
                logging.warning('Could not delete ha host in ZK.')
                return False
        if wal_receiver_info is not None:
            if not self.write_host_wal_receiver(wal_receiver_info, hostname):
                logging.warning('Could not write host wal_receiver_info to ZK.')
                return False
        if replics_info is not None:
            if not self.write_host_replics_info(replics_info, hostname):
                logging.warning('Could not write host replics_info to ZK.')
                return False
        return True

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


def create_zk(config: RawConfigParser, lock_contender_name=None) -> Zookeeper:
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
        config=zk_config,
    )
