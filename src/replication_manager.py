import json
import logging
import time

from . import helpers
from .pg import Postgres
from .list_removal_strategy import DelayedListRemovalStrategy
from .replication_manager_factory import ReplicationManagerConfig
from .types import ReplicaInfos
from .zk import Zookeeper


class ReplicationManager:
    def __init__(self, config: ReplicationManagerConfig, db: Postgres, _zk: Zookeeper):
        self._config = config
        self._db = db
        self._zk = _zk
        self._zk_fail_timestamp: float | None = None
        self._async_waiting_timestamp: float | None = None
        # Choose removal strategy based on configuration
        my_hostname = helpers.get_hostname()
        # Always use DelayedListRemovalStrategy, with delay=0 for immediate removal
        self._removal_strategy = DelayedListRemovalStrategy(
            my_hostname,
            self._config.quorum_removal_delay
        )
        if self._config.quorum_removal_delay > 0:
            logging.info(f'Using DelayedListRemovalStrategy with delay {self._config.quorum_removal_delay}s')
        else:
            logging.info('Using DelayedListRemovalStrategy with delay 0s (immediate removal)')
        # Track previous quorum state to detect changes
        self._previous_quorum: list | None = None

    def drop_zk_fail_timestamp(self):
        """
        Reset fail timestamp flag
        """
        self._zk_fail_timestamp = None

    def init_zk(self):
        if not self._zk.ensure_path(self._zk.QUORUM_PATH):
            logging.error("Can't create quorum path in ZK")
            return False
        return True

    def _get_needed_replication_type(self, db_state, ha_replics):
        replication_type = self._get_needed_replication_type_without_await_before_async(db_state, ha_replics)
        if replication_type == 'async':
            now = time.time()
            if self._async_waiting_timestamp is None:
                self._async_waiting_timestamp = now
            if now - self._async_waiting_timestamp < self._config.before_async_unavailability_timeout:
                return 'sync'
            return 'async'
        else:
            self._async_waiting_timestamp = None
            return replication_type


    def _get_needed_replication_type_without_await_before_async(self, db_state, ha_replics):
        """
        return replication type we should set at this moment
        """
        # Number of alive-and-well replica instances
        streaming_replicas = {i['application_name'] for i in db_state['replics_info'] if i['state'] == 'streaming'}
        replics_number = len(streaming_replicas & {helpers.app_name_from_fqdn(host) for host in ha_replics})

        metric = self._config.change_replication_metric
        logging.info(f"Check needed repl type: Metric is {metric}, replics_number is {replics_number}.")

        if 'count' in metric:
            if replics_number == 0:
                logging.debug("Needed repl type is async, because there is no streaming ha replicas")
                return 'async'

        if 'time' in metric:
            current_day = time.localtime().tm_wday
            current_hour = time.localtime().tm_hour
            sync_hours = self._config.weekend_change_hours if current_day in (5, 6) else self._config.weekday_change_hours

            start, stop = [int(i) for i in sync_hours.split('-')]
            if not start <= current_hour <= stop:
                key = 'end' if current_day in (5, 6) else 'day'
                logging.debug("Needed repl type is sync, because current_hour %d in [%d, %d] interval (see week%s_change_hours option)",
                            current_hour, start, stop, key)
                return 'sync'

        if 'load' in metric:
            over = self._config.overload_sessions_ratio
            try:
                ratio = float(self._db.get_sessions_ratio())
            except Exception:
                ratio = 0.0
            if ratio >= over:
                logging.debug("Needed repl type is async, because current sessions ratio %f > overload_sessions_ratio %f",
                            ratio, over)
                return 'async'

        logging.debug("Needed repl type is sync by default")
        return 'sync'


    def should_close(self) -> bool:
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
                time.sleep(self._config.primary_unavailability_timeout)
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

    def update_replication_type(self, db_state, ha_replics):
        """
        Change replication (if we should).
        """
        current = self._db.get_replication_state()
        logging.info('Current replication type is %s.', current)
        repl_state = current[0]
        needed = self._get_needed_replication_type(db_state, ha_replics)
        logging.info('Needed replication type is %s.', needed)

        if needed != repl_state:
            logging.info('We should change replication from {} to {}'.format(repl_state, needed))

        if needed == 'async':
            if repl_state == 'async':
                logging.debug('We should not change replication type here.')
                return
            self._zk.write(self._zk.QUORUM_PATH, [], preproc=json.dumps)
            self.change_replication_to_async()
            return

        # needed == 'sync'
        if repl_state == 'async':
            logging.info("Here we should turn synchronous replication on.")
        quorum_hosts = self._zk.get_sync_quorum_hosts()
        if not quorum_hosts:
            logging.error('ACTION-FAILED. No quorum hosts holding locks: Not doing anything.')
            return
        
        quorum = self._zk.get(self._zk.QUORUM_PATH, preproc=helpers.load_json_or_default)
        if quorum is None:
            quorum = []

        # Log quorum change from ZK between iterations
        if self._previous_quorum is not None and set(quorum) != set(self._previous_quorum):
            logging.debug(f'Current QUORUM in ZK: {quorum}')
            added = set(quorum) - set(self._previous_quorum)
            removed = set(self._previous_quorum) - set(quorum)
            logging.info(
                'QUORUM-HOSTS-CHANGED in ZK: from %s to %s (added: %s, removed: %s)',
                sorted(self._previous_quorum),
                sorted(quorum),
                sorted(added) if added else 'none',
                sorted(removed) if removed else 'none'
            )
        self._previous_quorum = quorum.copy() if quorum else []
        
        # Apply removal strategy: may keep replicas that temporarily lost quorum locks
        # to prevent mass removal during network flaps (see DelayedListRemovalStrategy)
        quorum_hosts_final = self._removal_strategy.get_hosts_to_keep(quorum, quorum_hosts)
        
        if set(quorum_hosts_final) == set(quorum) and repl_state != 'async':
            logging.debug('We should not change replication type here.')
            return
        
        # Log quorum hosts change for easy log search
        if set(quorum_hosts_final) != set(quorum):
            logging.info(
                'QUORUM-HOSTS-CHANGED: Quorum hosts are changing from %s to %s',
                sorted(quorum),
                sorted(quorum_hosts_final)
            )
        
        if self.change_replication_to_quorum(quorum_hosts_final):
            self._zk.write(self._zk.QUORUM_PATH, quorum_hosts_final, preproc=json.dumps)
            if repl_state == 'async':
                logging.info('Turned synchronous replication ON.')
            else:
                logging.info('Updated synchronous replication quorum.')

    def change_replication_to_quorum(self, replica_list):
        quorum_size = (len(replica_list) + 1) // 2
        replica_app_name_list = list(map(helpers.app_name_from_fqdn, replica_list))
        replication_type = f"ANY {quorum_size}({','.join(replica_app_name_list)})"
        logging.info(f'ACTION. Changing synchronous replication to {replication_type}.')
        if self._db.change_replication_type(replication_type):
            logging.info(f'Changed synchronous replication.')
            self._zk.write_ssn_on_changes(replication_type)
            return True
        return False

    def change_replication_to_async(self, reset_sync_replication_in_zk=True):
        if reset_sync_replication_in_zk:
            self._zk.write(self._zk.QUORUM_PATH, [], preproc=json.dumps)
        logging.warning("We should kill synchronous replication here.")
        logging.info('ACTION. Turning synchronous replication OFF.')
        if self._db.change_replication_type(''):
            logging.info('Turned synchronous replication OFF.')
            self._zk.write_ssn_on_changes('')
            return True
        return False

    def change_replication_to_sync_host(self, sync_replica):
        quorum_hosts = [sync_replica]
        if self.change_replication_to_quorum(quorum_hosts):
            self._zk.write(self._zk.QUORUM_PATH, quorum_hosts, preproc=json.dumps)
            return True
        return False

    def enter_sync_group(self, replica_infos: ReplicaInfos):
        self._zk.acquire_lock(self._zk.get_host_quorum_path())

    def leave_sync_group(self):
        self._zk.release_if_hold(self._zk.get_host_quorum_path())

    def is_promote_safe(self, host_group, replica_infos: ReplicaInfos):
        sync_quorum = self._zk.get(self._zk.QUORUM_PATH, preproc=helpers.load_json_or_default)
        alive_replics = helpers.make_current_replics_quorum(replica_infos, host_group)
        logging.info('Sync quorum was: %s', sync_quorum)
        logging.info('Alive hosts was: %s', host_group)
        logging.info('Alive replics was: %s', alive_replics)
        if sync_quorum is None:
            sync_quorum = []
        hosts_in_quorum = len(set(sync_quorum) & alive_replics)
        logging.info('%s >= %s', hosts_in_quorum, len(sync_quorum) // 2 + 1)
        return hosts_in_quorum >= len(sync_quorum) // 2 + 1

    def get_ensured_sync_replica(self, replica_infos: ReplicaInfos):
        quorum = self._zk.get(self._zk.QUORUM_PATH, preproc=helpers.load_json_or_default)
        if quorum is None:
            quorum = []
        sync_quorum = {helpers.app_name_from_fqdn(host): host for host in quorum}
        quorum_info = [info for info in replica_infos if info['application_name'] in sync_quorum]
        return sync_quorum.get(helpers.get_oldest_replica(quorum_info))
