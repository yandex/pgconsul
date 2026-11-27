"""Single-timeline failover election over persistent cluster state."""

import logging
import time
from typing import Any

from . import helpers
from .cluster_state import ClusterState, WriteConfiguration
from .durability import ssn_for
from .pg import Postgres
from .timings import TimingTracker
from .types import DbState
from .zk import Zookeeper


class Failover:
    def __init__(self, db: Postgres, zk: Zookeeper, config: Any, timings: TimingTracker):
        self.db = db
        self.zk = zk
        self.config = config
        self.timings = timings

    @staticmethod
    def electorate(state: ClusterState) -> set[str]:
        return {host for configuration in state.possible_write_configurations for host in configuration.replicas}

    def tick(self, state: ClusterState, db_state: DbState, hostname: str) -> None:
        operation = state.operation
        if operation['type'] == 'failover':
            if operation['phase'] == 'voting':
                self._vote(state, db_state, hostname)
                if hostname in operation['electorate'] and db_state.role == 'replica':
                    self._coordinate_voting(state)
            elif operation['phase'] == 'promoting':
                self._promote(state, db_state, hostname)
                if hostname in operation['electorate'] and db_state.role in ('replica', 'primary'):
                    self._coordinate_promotion(state)
            return
        self.zk.release_if_hold(self.zk.FAILOVER_MANAGER_LOCK_PATH)
        if self.config.autofailover and db_state.role == 'replica':
            self._maybe_start(state, hostname)

    def _maybe_start(self, state: ClusterState, hostname: str) -> None:
        primary = state.desired_primary
        if primary is None or hostname not in self.electorate(state) or time.time() < state.next_failover_after:
            return
        if len({configuration.timeline for configuration in state.possible_write_configurations}) != 1:
            logging.error('Automatic failover v1 cannot handle multiple input timelines')
            return
        if any(configuration.primary != primary for configuration in state.possible_write_configurations):
            logging.error('Automatic failover v1 requires one previous primary')
            return
        if state.operation['type'] not in ('idle', 'durability_transition', 'switchover'):
            return
        if state.operation['type'] == 'switchover' and state.operation['phase'] != 'tuning_ssn':
            return
        last_seen = self.zk.get_last_primary_availability_time()
        if last_seen is None or time.time() - last_seen < self.config.primary_unavailability_timeout:
            return
        if self.zk.is_host_alive(primary) or not self.db.is_host_unreachable(primary, check_primary=False):
            return
        if not self.zk.try_acquire_lock(self.zk.FAILOVER_MANAGER_LOCK_PATH):
            return
        fresh = self.zk.get_cluster_state()
        if fresh is None or fresh.state_id != state.state_id:
            return
        electorate = sorted(self.electorate(state))
        operation = {
            'type': 'failover', 'phase': 'voting', 'previous_primary': primary,
            'source_operation': state.operation, 'electorate': electorate,
            'deadline': time.time() + self.config.election_timeout,
        }
        cooldown = operation['deadline'] + self.config.min_failover_timeout
        if self.zk.cas_cluster_state(fresh, fresh.evolve(operation=operation, next_failover_after=cooldown)):
            self.timings.start('downtime', ts=last_seen)
            self.timings.start('failover', ts=last_seen)
            self._fail_switchover_request(operation)
            if state.operation['type'] == 'switchover':
                self.timings.stop('switchover', track_as='switchover_failure')

    def _fail_switchover_request(self, operation: dict) -> bool:
        source = operation.get('source_operation', {})
        request_id = source.get('request_id')
        if request_id is None:
            return True
        request = self.zk.get_switchover_request()
        if not request or request[0].get('request_id') != request_id:
            return False
        if request[0].get('status') == 'failed':
            return True
        if request[0].get('status') == 'done':
            return False
        return self.zk.set_switchover_request(dict(request[0], status='failed'), request[1])

    def _vote(self, state: ClusterState, db_state: DbState, hostname: str) -> None:
        if hostname not in state.operation['electorate'] or db_state.role != 'replica':
            return
        old_vote = self.zk.get_failover_vote(hostname)
        if old_vote and old_vote.get('state_id') == state.state_id and old_vote.get('valid') is True:
            return
        previous_primary = state.operation['previous_primary']
        if not self.db.is_host_unreachable(previous_primary, check_primary=False):
            return
        try:
            self.db.disable_archive_restore_for_vote(self.config.archive_restore_disable_timeout)
            if not self.db.disable_wal_receiver(self.config.walreceiver_disable_timeout):
                return
            timeline, lsn = self.db.lwaldump_with_timeline()
            expected_timeline = state.possible_write_configurations[0].timeline
            if timeline != expected_timeline:
                vote = {'state_id': state.state_id, 'valid': False, 'timeline': timeline}
            else:
                vote = {
                    'state_id': state.state_id, 'valid': True, 'timeline': timeline, 'lsn': lsn,
                    'next_timeline': self.db.get_next_local_timeline(timeline),
                }
            self.zk.write_failover_vote(vote)
        except Exception:
            logging.exception('Could not measure durable WAL for failover vote')
            self.zk.write_failover_vote({'state_id': state.state_id, 'valid': False})

    def _valid_votes(self, state: ClusterState) -> dict[str, dict]:
        result = {}
        timeline = state.possible_write_configurations[0].timeline
        for host in state.operation['electorate']:
            vote = self.zk.get_failover_vote(host)
            if vote and vote.get('state_id') == state.state_id and vote.get('valid') is True and vote.get('timeline') == timeline:
                result[host] = vote
        return result

    def _coordinate_voting(self, state: ClusterState) -> None:
        if not self.zk.try_acquire_lock(self.zk.FAILOVER_MANAGER_LOCK_PATH):
            return
        fresh = self.zk.get_cluster_state()
        if fresh is None or fresh.state_id != state.state_id:
            return
        if not self._fail_switchover_request(fresh.operation):
            return
        votes = self._valid_votes(fresh)
        if all(len(set(configuration.replicas) & votes.keys()) >= configuration.read_quorum for configuration in fresh.possible_write_configurations):
            winner = max(votes, key=lambda host: (votes[host]['lsn'], host))
            source = fresh.possible_write_configurations[-1]
            target = WriteConfiguration(votes[winner]['next_timeline'], winner, tuple(sorted(votes)))
            operation = dict(fresh.operation, phase='promoting')
            self.zk.cas_cluster_state(fresh, fresh.evolve(
                desired_primary=winner, operation=operation,
                possible_write_configurations=(*fresh.possible_write_configurations, target),
            ))
            logging.warning('Selected failover winner %s from timeline %s at LSN %s', winner, source.timeline, votes[winner]['lsn'])
            return
        if time.time() >= fresh.operation['deadline']:
            if self.zk.cas_cluster_state(fresh, fresh.evolve(operation=fresh.operation['source_operation'])):
                self.timings.stop('failover', track_as='failover_failure')
                self.zk.release_if_hold(self.zk.FAILOVER_MANAGER_LOCK_PATH)

    def _promote(self, state: ClusterState, db_state: DbState, hostname: str) -> None:
        if hostname != state.desired_primary:
            return
        target = state.possible_write_configurations[-1]
        expected_ssn = ssn_for(target)
        if db_state.role == 'replica':
            if self.db.get_fresh_ssn() != expected_ssn:
                self.db.change_replication_type(expected_ssn)
                return
            fresh = self.zk.get_cluster_state()
            if fresh is None or fresh.state_id != state.state_id:
                return
            self.db.promote()
            return
        if db_state.role != 'primary':
            return
        actual_timeline = self.db.get_primary_timeline()
        if actual_timeline <= max(config.timeline for config in state.possible_write_configurations[:-1]):
            logging.error('Promoted primary did not advance the timeline; manual recovery required')
            self.db.pgpooler('stop')
            return
        if actual_timeline != target.timeline:
            self.zk.write_failover_participant({'state_id': state.state_id, 'promoted': True, 'timeline': actual_timeline})
            return
        if self.db.get_fresh_ssn() != expected_ssn:
            self.db.change_replication_type(expected_ssn)
            return
        if self.db.write_durability_barrier(state.state_id):
            self.zk.write_failover_participant({'state_id': state.state_id, 'promoted': True, 'timeline': actual_timeline, 'barrier': True})

    def _coordinate_promotion(self, state: ClusterState) -> None:
        if not self.zk.try_acquire_lock(self.zk.FAILOVER_MANAGER_LOCK_PATH):
            return
        fresh = self.zk.get_cluster_state()
        if fresh is None or fresh.state_id != state.state_id:
            return
        if not self._fail_switchover_request(fresh.operation):
            return
        target = fresh.possible_write_configurations[-1]
        result = self.zk.get_failover_participant(target.primary)
        if not result or result.get('state_id') != fresh.state_id or not result.get('promoted'):
            return
        if result['timeline'] != target.timeline:
            actual = WriteConfiguration(result['timeline'], target.primary, target.members)
            self.zk.cas_cluster_state(fresh, fresh.evolve(possible_write_configurations=(*fresh.possible_write_configurations[:-1], actual)))
            return
        if result.get('barrier'):
            if self.zk.cas_cluster_state(fresh, fresh.evolve(
                operation={'type': 'idle', 'phase': None}, possible_write_configurations=(target,), next_failover_after=0.0,
            )):
                self.zk.write_last_failover_time()
                self.timings.stop('failover')
                self.zk.release_if_hold(self.zk.FAILOVER_MANAGER_LOCK_PATH)
