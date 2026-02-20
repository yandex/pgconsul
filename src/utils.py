"""
Utility functions for various tasks like switchover, ZK init, etc
"""
# encoding: utf-8

import copy
import json
import sys
import logging
import time
from operator import itemgetter
from os import getpid

from . import read_config, zk, helpers
from .exceptions import SwitchoverException, FailoverException
from .zk import ZookeeperException


class Switchover:
    """
    1. Collect coordinates of the systems being switched over
    2. Check if there is already a switchover in progress. If there is,
       signal its state and coordinates in log.
    3. Initiate switchover.
    4. in blocking mode, attach to ZK and wait for changes in state (either fail
    or success.)
    5. If not in progress, initiate. If nonblocking mode is enabled, return.
    """

    def __init__(
        self,
        conf=None,
        primary=None,
        syncrep=None,
        timeline=None,
        new_primary=None,
        timeout=60,
        config_path='/etc/pgconsul.conf',
        from_cli=False,
    ):
        """
        Define configuration of the switchover: if None, then autodetect from
        ZK.
        """
        self.timeout = timeout
        self._log = logging.getLogger('switchover')
        # Might be useful to read from default config in case the class is being
        # called from outside of the CLI utility.
        if conf is None:
            conf = read_config({'config_file': config_path})
        self._conf = conf
        lock_contender_name = None
        if from_cli:
            lock_contender_name = helpers.get_hostname() + '_' + str(getpid())
        self._zk = zk.Zookeeper(config=conf, plugins=None, lock_contender_name=lock_contender_name)
        # If primary or syncrep or timeline is provided, use them instead.
        # Autodetect (from ZK) if none.
        self._new_primary = new_primary
        self.primary = primary
        self.syncrep = syncrep
        self.timeline = timeline

    def set_lock_owners(self):
        self._plan = self._get_lock_owners(self.primary, self.syncrep, self.timeline)

    def is_possible(self):
        """
        Check, whether it's possible to perform switchover now.
        """
        if self.in_progress():
            logging.error('Switchover is already in progress: %s', self.state())
            return False
        if self._new_primary is not None:
            is_alive = self._zk.is_host_alive(self._new_primary, self.timeout / 2)
            if not is_alive:
                logging.error('Cannot promote dead host: %s', self._new_primary)
                return False
            is_ha = self._is_ha(self._new_primary)
            if not is_ha:
                logging.error('Cannot promote non ha host: %s', self._new_primary)
                return False
        else:
            replicas_info = self._zk.get(self._zk.REPLICS_INFO_PATH, preproc=json.loads)
            if replicas_info:
                connected_app_names = set(map(itemgetter('application_name'), replicas_info))
                ha_hosts = self._zk.get_ha_hosts()
                replicas = {host: helpers.app_name_from_fqdn(host) for host in ha_hosts}

                for replica, app_name in replicas.items():
                    if self._zk.is_host_alive(replica, 1) and app_name in connected_app_names:
                        # Ok, there is a suitable candidate for switchover
                        return True
            logging.error('Cannot promote because there are no suitable replica for switchover.')
            return False
        return True

    def perform(self, min_replicas=None, block=True, timeout=None):
        """
        Perform the actual switchover.
        """
        ha_group = self._zk.get_alive_hosts(timeout=10)
        if timeout is None:
            timeout = self.timeout
        switch_correct = self._initiate_switchover(
            primary=self._plan['primary'], timeline=self._plan['timeline'], new_primary=self._new_primary
        )
        if not switch_correct:
            return True
        if not block:
            return True
        limit = timeout
        while True:
            in_progress = self.in_progress(return_true_on_zk_fail=True)
            if not in_progress:
                break
            self._log.debug('current switchover status: %(progress)s, failover: %(failover)s', self.state())
            if limit <= 0:
                raise SwitchoverException(f'timeout exceeded, current status: {in_progress}')
            time.sleep(1)
            limit -= 1
        self._wait_for_primary()
        state = self.state()
        self._log.debug('full state: %s', state)
        self._wait_for_replicas(ha_group, min_replicas)
        # We delete all zk states after switchover complete
        self._log.info('switchover finished, zk status "%(progress)s"', state)
        result = state['progress'] is None
        return result

    def in_progress(self, primary=None, timeline=None, return_true_on_zk_fail=False):
        """
        Return True if the cluster is currently in the process of switching
        over; or if return_true_on_zk_fail is True, and we got ZookeeperException.
        Optionally check for specific hostname being currently the primary
        and having a particular timeline.
        """
        try:
            state = self.state(raise_zk_exceptions=return_true_on_zk_fail)
        except ZookeeperException as exc:
            if return_true_on_zk_fail:
                self._log.warning('Failed to get switchover state: %s', exc)
                return True
            raise

        self._log.debug('current switchover state: %s', state['progress'])
        # Check if cluster is in process of switching over
        if state['progress'] in ('failed', None):
            return False
        # The constraint, if specified, must match for this function to return
        # True (actual state)
        conditions = [
            primary is None or primary == state['info'].get('primary'),
            timeline is None or timeline == state['info'].get(self._zk.TIMELINE_INFO_PATH),
        ]
        if all(conditions):
            return state['progress']
        return False

    def state(self, raise_zk_exceptions=False):
        """
        Current cluster state.
        if raise_zk_exceptions is true - function will not catch ZookeeperException
        """
        get = self._zk.noexcept_get
        if raise_zk_exceptions:
            get = self._zk.get
        return {
            'progress': get(self._zk.SWITCHOVER_STATE_PATH),
            'info': get(self._zk.SWITCHOVER_PRIMARY_PATH, preproc=json.loads) or {},
            'failover': get(self._zk.FAILOVER_STATE_PATH),
            'replicas': get(self._zk.REPLICS_INFO_PATH, preproc=json.loads) or {},
        }

    def plan(self):
        """
        Get switchover plan
        """
        return copy.deepcopy(self._plan)

    def _get_lock_owners(self, primary=None, syncrep=None, timeline=None):
        """
        Waiting for leader lock owner, get syncreplica lock owner, and timeline.
        """
        def check_primary_lock_holder():
            return self._zk.get_current_lock_holder(self._zk.PRIMARY_LOCK_PATH)

        if primary is None:
            primary = helpers.await_for_value(check_primary_lock_holder, self.timeout, "Primary holds the leader lock")
        else:
            self._log.info(f'Use {primary} as current primary')

        if primary is None:
            self._log.error('Switchover is impossible because no one holds the leader lock.')
            sys.exit(1)

        owners = {
            'primary': primary,
            'sync_replica': syncrep or self._zk.get_current_lock_holder(self._zk.SYNC_REPLICA_LOCK_PATH),
            'timeline': timeline or self._zk.noexcept_get(self._zk.TIMELINE_INFO_PATH, preproc=int),
        }
        self._log.debug('lock holders: %s', owners)
        return owners

    def reset(self, force=False):
        """
        Reset state and hostname-timeline
        """
        self._log.info('resetting ZK switchover nodes')
        if not force and self.in_progress():
            raise SwitchoverException('attempted to reset state while switchover is in progress')
        self._lock(self._zk.SWITCHOVER_LOCK_PATH)
        if not self._zk.delete(self._zk.SWITCHOVER_CANDIDATE):
            raise SwitchoverException(f'unable to delete node {self._zk.SWITCHOVER_CANDIDATE}')
        if not self._zk.noexcept_write(self._zk.SWITCHOVER_PRIMARY_PATH, '{}', need_lock=False):
            raise SwitchoverException(f'unable to reset node {self._zk.SWITCHOVER_PRIMARY_PATH}')
        if not self._zk.noexcept_write(self._zk.SWITCHOVER_STATE_PATH, 'failed', need_lock=False):
            raise SwitchoverException(f'unable to reset node {self._zk.SWITCHOVER_STATE_PATH}')
        return True

    def _is_ha(self, hostname):
        """
        Checks whether given host is ha replica.
        """
        ha_path = f'{self._zk.MEMBERS_PATH}/{hostname}/ha'
        return self._zk.exists_path(ha_path)

    def _lock(self, node):
        """
        Lock switchover structure in ZK
        """
        if not self._zk.ensure_path(node):
            raise SwitchoverException(f'unable to create switchover node ({node})')
        if not self._zk.try_acquire_lock(lock_type=node, allow_queue=True, timeout=self.timeout):
            raise SwitchoverException(f'unable to lock switchover node ({node})')

    def _initiate_switchover(self, primary, timeline, new_primary):
        """
        Write primary coordinates and 'scheduled' into state node to
        initiate switchover.
        1. Lock the hostname-timeline json node.
        2. Set hostname, timeline and destination.
        3. Set state to 'scheduled'
        """
        if primary == new_primary:
            self._log.info('Host %s already is primary, no need to switch', primary)
            return False
        switchover_task = {
            'hostname': primary,
            self._zk.TIMELINE_INFO_PATH: timeline,
            'destination': new_primary,
        }
        self._log.info('initiating switchover with %s', switchover_task)
        self._lock(self._zk.SWITCHOVER_LOCK_PATH)
        if not self._zk.write(self._zk.SWITCHOVER_PRIMARY_PATH, switchover_task, preproc=json.dumps, need_lock=False):
            raise SwitchoverException(f'unable to write to {self._zk.SWITCHOVER_PRIMARY_PATH}')
        if not self._zk.write(self._zk.SWITCHOVER_STATE_PATH, 'scheduled', need_lock=False):
            raise SwitchoverException(f'unable to write to {self._zk.SWITCHOVER_STATE_PATH}')
        self._log.debug('state: %s', self.state())
        return True

    def _wait_for_replicas(self, ha_group, min_replicas=None, timeout=None):
        """
        Wait for replicas to appear
        """
        if timeout is None:
            timeout = self.timeout
        if min_replicas is None:
            min_replicas = len(ha_group) - 1
        min_replicas = min(min_replicas, len(ha_group) - 1)
        ha_group_app_names = {helpers.app_name_from_fqdn(host) for host in ha_group}
        self._log.debug('waiting for %d replicas to appear ...', min_replicas)
        for _ in range(timeout):
            time.sleep(1)
            replicas = self.state()['replicas']
            streaming_ha_replicas = [
                f'{x["application_name"]}@{x["primary_location"]}' for x in replicas
                if x['state'] == 'streaming' and x['application_name'] in ha_group_app_names
            ]
            self._log.debug('replicas up: %s', (', '.join(streaming_ha_replicas) or 'none'))
            if len(streaming_ha_replicas) >= min_replicas:
                return replicas
        raise SwitchoverException(
            f'expected {min_replicas} replicas to appear within {timeout} secs, got {len(streaming_ha_replicas)}'
        )

    def _wait_for_primary(self, timeout=None):
        """
        Wait for primary to hold the lock
        """
        if timeout is None:
            timeout = self.timeout
        if not helpers.await_for(
            lambda: self._zk.get_current_lock_holder(self._zk.PRIMARY_LOCK_PATH) not in (None, self._plan['primary']),
            timeout, 'new primary to acquire lock'
        ):
            raise SwitchoverException(f'no one took primary lock in {timeout} secs')

class Failover:
    def __init__(
        self,
        conf=None,
        config_path='/etc/pgsync.conf',
    ):
        """
        Define configuration of the failover: if None, then autodetect from
        ZK.
        """
        self._log = logging.getLogger('failover')
        # Might be useful to read from default config in case the class is being
        # called from outside the CLI utility.
        if conf is None:
            conf = read_config({'config_file': config_path})
        self._conf = conf
        self._zk = zk.Zookeeper(config=conf, plugins=None)

    def reset(self):
        """
        Reset state and hostname-timeline
        """
        self._log.info('resetting ZK failover nodes')
        if not self._zk.delete(self._zk.FAILOVER_STATE_PATH):
            raise FailoverException(f'unable to reset node {self._zk.FAILOVER_STATE_PATH}')
        return True
