import json
import logging
import time

from . import helpers


class SingleSyncReplicationManager:
    def __init__(self, config, db, _zk):
        self._config = config
        self._db = db
        self._zk = _zk
        self._zk_fail_timestamp = None

    def init_zk(self):
        return True

    def drop_zk_fail_timestamp(self):
        """
        Reset fail timestamp flag
        """
        self._zk_fail_timestamp = None

    def should_close(self):
        """
        Check if we are safe to stay open on zk conn loss
        """
        try:
            if self._zk_fail_timestamp is None:
                self._zk_fail_timestamp = time.time()
            info = self._db.get_replics_info(self._db.role)
            should_wait = False
            for replica in info:
                if replica['reply_time_ms'] / 1000 < self._zk_fail_timestamp:
                    should_wait = True
            if should_wait:
                time.sleep(self._config.getfloat('replica', 'primary_unavailability_timeout'))
                info = self._db.get_replics_info(self._db.role)

            connected = sum([1 for x in info if x['sync_state'] == 'sync' and x['reply_time_ms'] / 1000 > self._zk_fail_timestamp])
            repl_state = self._db.get_replication_state()
            if repl_state[0] == 'async':
                return False
            elif repl_state[0] == 'sync':
                logging.info(
                    'Probably connect to ZK lost, check the need to close. Connected replicas(sync) num %s',
                    connected,
                )
                return connected < 1
            else:
                raise RuntimeError(f'Unexpected replication state: {repl_state}')
        except Exception as exc:
            logging.error('Error while checking for close conditions: %s', repr(exc))
            return True

    def update_replication_type(self, db_state, ha_replics):
        """
        Change replication (if we should).
        """
        holder_fqdn = self._zk.get_current_lock_holder(self._zk.SYNC_REPLICA_LOCK_PATH)
        if holder_fqdn == helpers.get_hostname():
            logging.info('We are primary but holding sync_replica lock. Releasing it now.')
            self._zk.release_lock(self._zk.SYNC_REPLICA_LOCK_PATH)
            return

        current = self._db.get_replication_state()
        logging.info('Current replication type is %s.', current)
        needed = _get_needed_replication_type(self._config, self._db, db_state, ha_replics)
        logging.info('Needed replication type is %s.', needed)

        if needed == 'async':
            if current[0] == 'async':
                logging.debug('We should not change replication type here.')
            else:
                self.change_replication_to_async()
            return

        if holder_fqdn is None:
            logging.error(
                'Sync replication type requires explicit '
                'lock holder but no one seem to hold lock '
                'right now. Not doing anything.'
            )
            return

        if current == (needed, helpers.app_name_from_fqdn(holder_fqdn)):
            logging.debug('We should not change replication type here.')
            # https://www.postgresql.org/message-id/15617-8dfbde784d8e3258%40postgresql.org
            self._db.check_walsender(db_state['replics_info'], holder_fqdn)
        else:
            logging.info("Here we should turn synchronous replication on.")
            if self._db.change_replication_to_sync_host(holder_fqdn):
                logging.info('Turned synchronous replication ON.')

    def change_replication_to_async(self):
        logging.warning("We should kill synchronous replication here.")
        #
        # We need to reset `sync` state of replication in `replics_info`
        # node in zk before killing synchronous replication here.
        # We have race condition between the moment of turning off sync
        # replication and the moment of delivering this information to zk.
        # (I.e. `change_replication_type` here and `write_host_stat` with
        # actual async status in next iteration).
        # If connection between primary (we here) and zookeeper will be lost
        # then current sync replica will think that it is actual sync and
        # will decide that it can promote, but actually status is async.
        # To prevent this we rewrite replication status of sync replica
        # in zk to async.
        #
        if not self._reset_sync_replication_in_zk():
            logging.warning('Unable to reset replication status to async in ZK')
            logging.warning('Killing synchronous replication is impossible')
            return False
        if self._db.change_replication_to_async():
            logging.info('Turned synchronous replication OFF.')
            return True
        return False

    def enter_sync_group(self, replica_infos):
        sync_replica_lock_holder = self._zk.get_current_lock_holder(self._zk.SYNC_REPLICA_LOCK_PATH)
        if sync_replica_lock_holder is None:
            self._zk.acquire_lock(self._zk.SYNC_REPLICA_LOCK_PATH)
            return None

        if sync_replica_lock_holder == helpers.get_hostname():
            other = self._zk.get_lock_contenders(self._zk.SYNC_REPLICA_LOCK_PATH)
            if len(other) > 1:
                logging.info(
                    'We are holding sync_replica lock in ZK '
                    'but %s is alive and has higher priority. '
                    'Releasing sync_replica lock.' % other[1]
                )
                self._zk.release_lock(self._zk.SYNC_REPLICA_LOCK_PATH)

        if self._check_if_we_are_priority_replica(replica_infos, sync_replica_lock_holder):
            logging.info('We have higher priority than current synchronous replica. Trying to acquire the lock.')
            self._zk.acquire_lock(self._zk.SYNC_REPLICA_LOCK_PATH, allow_queue=True)

    def leave_sync_group(self):
        self._zk.release_if_hold(self._zk.SYNC_REPLICA_LOCK_PATH)

    def is_promote_safe(self, host_group, replica_infos):
        sync_replica = self.get_ensured_sync_replica(replica_infos)
        logging.info(f'sync replica is {sync_replica}')
        return sync_replica in host_group

    def get_ensured_sync_replica(self, replica_infos):
        app_name_map = {helpers.app_name_from_fqdn(host): host for host in self._zk.get_ha_hosts()}
        for replica in replica_infos:
            if replica['sync_state'] == 'sync':
                return app_name_map.get(replica['application_name'])
        return None

    def _check_if_we_are_priority_replica(self, replica_infos, sync_replica_lock_holder):
        """
        Check if we are asynchronous replica and we have higher priority than
        current synchronous replica.
        """
        prefix = self._zk.MEMBERS_PATH
        my_hostname = helpers.get_hostname()
        my_app_name = helpers.app_name_from_fqdn(my_hostname)
        if sync_replica_lock_holder is None:
            return False

        for replica in replica_infos:
            if replica['application_name'] != my_app_name:
                continue
            if replica['sync_state'] != 'async':
                return False

        my_priority = self._config.getint('global', 'priority')
        sync_priority = self._zk.get(f'{prefix}/{sync_replica_lock_holder}/prio', preproc=int)
        if sync_priority is None:
            sync_priority = 0
        if my_priority > sync_priority:
            return True

        return False

    def _reset_sync_replication_in_zk(self):
        """
        This is ugly hack to prevent race condition between 2 moments:
        1. Actual replication status in PostgreSQL became `async`
        2. Information about this will be appear in zookeeper.
        We need to reset `sync` replication status in replics_info
        """
        replics_info = self._zk.get(self._zk.REPLICS_INFO_PATH, preproc=json.loads)
        if replics_info is None:
            return False
        for replica in replics_info:
            if replica['sync_state'] == 'sync':
                replica['sync_state'] = 'async'
        return self._zk.write(self._zk.REPLICS_INFO_PATH, replics_info, preproc=json.dumps)


class QuorumReplicationManager:
    def __init__(self, config, db, _zk):
        self._config = config
        self._db = db
        self._zk = _zk
        self._zk_fail_timestamp = None

    def drop_zk_fail_timestamp(self):
        """
        Reset fail timestamp flag
        """
        self._zk_fail_timestamp = None

    def should_close(self):
        """
        Check if we are safe to stay open on zk conn loss
        """
        try:
            if self._zk_fail_timestamp is None:
                self._zk_fail_timestamp = time.time()
            info = self._db.get_replics_info(self._db.role)
            should_wait = False
            for replica in info:
                if replica['reply_time_ms'] / 1000 < self._zk_fail_timestamp:
                    should_wait = True
            if should_wait:
                time.sleep(self._config.getfloat('replica', 'primary_unavailability_timeout'))
                info = self._db.get_replics_info(self._db.role)

            connected = sum([1 for x in info if x['sync_state'] == 'quorum' and x['reply_time_ms'] / 1000 > self._zk_fail_timestamp])
            repl_state = self._db.get_replication_state()
            if repl_state[0] == 'async':
                return False
            elif repl_state[0] == 'sync':
                expected = int(repl_state[1].split('(')[0].split(' ')[1])
                logging.info(
                    'Probably connect to ZK lost, check the need to close. '
                    'Expected replicas num: %s, connected replicas(quorum) num %s',
                    expected,
                    connected,
                )
                return connected < expected
            else:
                raise RuntimeError(f'Unexpected replication state: {repl_state}')
        except Exception as exc:
            logging.error('Error while checking for close conditions: %s', repr(exc))
            return True

    def init_zk(self):
        if not self._zk.ensure_path(self._zk.QUORUM_PATH):
            logging.error("Can't create quorum path in ZK")
            return False
        return True

    def update_replication_type(self, db_state, ha_replics):
        """
        Change replication (if we should).
        """
        current = self._db.get_replication_state()
        logging.info('Current replication type is %s.', current)
        needed = _get_needed_replication_type(self._config, self._db, db_state, ha_replics)
        logging.info('Needed replication type is %s.', needed)

        if needed == 'async':
            if current[0] == 'async':
                logging.debug('We should not change replication type here.')
                return
            self._zk.write(self._zk.QUORUM_PATH, [], preproc=json.dumps)
            self.change_replication_to_async()
        else:  # needed == 'sync'
            if current[0] == 'async':
                logging.info("Here we should turn synchronous replication on.")
            quorum_hosts = self._zk.get_sync_quorum_hosts()
            logging.info(f'Quorum hosts will be: {quorum_hosts}')
            if not quorum_hosts:
                logging.error('No quorum: Not doing anything.')
                return
            quorum = self._zk.get(self._zk.QUORUM_PATH, preproc=helpers.load_json_or_default)
            if quorum is None:
                quorum = []
            if set(quorum_hosts) == set(quorum) and current[0] != 'async':
                return
            if self._db.change_replication_to_quorum(quorum_hosts):
                self._zk.write(self._zk.QUORUM_PATH, quorum_hosts, preproc=json.dumps)
                logging.info('Turned synchronous replication ON.')

    def change_replication_to_async(self):
        self._zk.write(self._zk.QUORUM_PATH, [], preproc=json.dumps)
        logging.warning("We should kill synchronous replication here.")
        if self._db.change_replication_to_async():
            logging.info('Turned synchronous replication OFF.')
            return True
        return False

    def enter_sync_group(self, **_kwargs):
        self._zk.acquire_lock(self._zk.get_host_quorum_path())

    def leave_sync_group(self):
        self._zk.release_if_hold(self._zk.get_host_quorum_path())

    def is_promote_safe(self, host_group, **kwargs):
        sync_quorum = self._zk.get(self._zk.QUORUM_PATH, preproc=helpers.load_json_or_default)
        alive_replics = helpers.make_current_replics_quorum(kwargs['replica_infos'], host_group)
        logging.info('Sync quorum was: %s', sync_quorum)
        logging.info('Alive hosts was: %s', host_group)
        logging.info('Alive replics was: %s', alive_replics)
        if sync_quorum is None:
            sync_quorum = []
        hosts_in_quorum = len(set(sync_quorum) & alive_replics)
        logging.info('%s >= %s', hosts_in_quorum, len(sync_quorum) // 2 + 1)
        return hosts_in_quorum >= len(sync_quorum) // 2 + 1

    def get_ensured_sync_replica(self, replica_infos):
        quorum = self._zk.get(self._zk.QUORUM_PATH, preproc=helpers.load_json_or_default)
        if quorum is None:
            quorum = []
        sync_quorum = {helpers.app_name_from_fqdn(host): host for host in quorum}
        quorum_info = [info for info in replica_infos if info['application_name'] in sync_quorum]
        return sync_quorum.get(helpers.get_oldest_replica(quorum_info))


def _get_needed_replication_type(config, db, db_state, ha_replics):
    """
    return replication type we should set at this moment
    """
    # Number of alive-and-well replica instances
    streaming_replicas = {i['application_name'] for i in db_state['replics_info'] if i['state'] == 'streaming'}
    replics_number = len(streaming_replicas & {helpers.app_name_from_fqdn(host) for host in ha_replics})

    metric = config.get('primary', 'change_replication_metric')
    logging.info(f"Check needed repl type: Metric is {metric}, replics_number is {replics_number}.")

    if 'count' in metric:
        if replics_number == 0:
            return 'async'

    if 'time' in metric:
        current_day = time.localtime().tm_wday
        current_hour = time.localtime().tm_hour
        key = 'end' if current_day in (5, 6) else 'day'
        sync_hours = config.get('primary', 'week%s_change_hours' % key)

        start, stop = [int(i) for i in sync_hours.split('-')]
        if not start <= current_hour <= stop:
            return 'sync'

    if 'load' in metric:
        over = config.getfloat('primary', 'overload_sessions_ratio')
        try:
            ratio = float(db.get_sessions_ratio())
        except Exception:
            ratio = 0.0
        if ratio >= over:
            return 'async'

    return 'sync'
