"""Persistent switchover protocol without PostgreSQL synchronous-quorum patches."""

import logging
import time
from typing import Any
from uuid import uuid4

from . import helpers
from .cluster_state import ClusterState, WriteConfiguration
from .durability import ssn_for
from .pg import Postgres
from .types import DbState
from .zk import Zookeeper


class SafeSwitchover:
    def __init__(self, db: Postgres, zk: Zookeeper, config: Any):
        self.db = db
        self.zk = zk
        self.config = config

    def tick(self, state: ClusterState, db_state: DbState, hostname: str) -> None:
        operation = state.operation
        if operation['type'] == 'idle' and hostname == state.desired_primary and db_state.role == 'primary':
            self._maybe_start(state, db_state)
        elif operation['type'] == 'switchover' and operation['phase'] == 'tuning_ssn':
            if hostname == operation['source'] and db_state.role == 'primary':
                self._tune(state)
        elif operation['type'] == 'switchover' and operation['phase'] == 'promoting':
            if hostname == operation['source'] and db_state.role == 'primary':
                self._retire_source(state, db_state)
            elif hostname == state.desired_primary:
                self._promote_target(state, db_state)

    def _set_request_status(self, request_id: str, status: str) -> bool:
        request = self.zk.get_switchover_request()
        if request is None:
            return False
        value, version = request
        if value.get('request_id') != request_id:
            return False
        if value.get('status') == status:
            return True
        if value.get('status') in ('done', 'failed'):
            return False
        return self.zk.set_switchover_request(dict(value, status=status), version)

    def _maybe_start(self, state: ClusterState, db_state: DbState) -> None:
        request = self.zk.get_switchover_request()
        if request is None:
            return
        value, _ = request
        if value.get('status') != 'requested' or value.get('source_primary') not in (None, state.desired_primary):
            return
        source = state.possible_write_configurations[0]
        connected = {
            host for host in source.replicas
            if self.zk.is_host_alive(host) and any(
                row.application_name == helpers.app_name_from_fqdn(host) and row.state == 'streaming'
                for row in db_state.replics_info or []
            )
        }
        requested = value.get('target_primary')
        if requested is not None:
            if requested not in connected:
                return
            target = requested
        elif connected:
            target = max(connected, key=lambda host: (int(self.zk.get_host_prio(host) or 0), host))
        else:
            return
        operation = {
            'type': 'switchover', 'phase': 'tuning_ssn', 'request_id': value['request_id'],
            'source': state.desired_primary, 'target': target, 'original_members': list(source.members),
            'deadline': time.time() + float(value.get('timeout') or self.config.switchover_rollback_timeout),
        }
        if self.zk.cas_cluster_state(state, state.evolve(operation=operation)):
            self._set_request_status(value['request_id'], 'in_progress')

    def _tune(self, state: ClusterState) -> None:
        operation = state.operation
        self._set_request_status(operation['request_id'], 'in_progress')
        request = self.zk.get_switchover_request()
        request_failed = bool(request and request[0].get('request_id') == operation['request_id'] and request[0].get('status') == 'failed')
        if request_failed or time.time() >= operation['deadline']:
            self._set_request_status(operation['request_id'], 'failed')
            if len(state.possible_write_configurations) == 1:
                self.zk.cas_cluster_state(state, state.evolve(operation={'type': 'idle', 'phase': None}))
            else:
                self.zk.cas_cluster_state(state, state.evolve(operation={'type': 'durability_transition', 'phase': 'in_progress'}))
            return
        source = state.possible_write_configurations[-1]
        handoff_members = tuple(sorted((operation['source'], operation['target'])))
        if source.members != handoff_members:
            if len(state.possible_write_configurations) != 1:
                self._finish_tuning_transition(state)
            else:
                handoff = WriteConfiguration(source.timeline, source.primary, handoff_members)
                self.zk.cas_cluster_state(state, state.evolve(possible_write_configurations=(source, handoff)))
            return
        if len(state.possible_write_configurations) != 1:
            self._finish_tuning_transition(state)
            return
        if self.db.get_fresh_ssn() != ssn_for(source):
            self.db.change_replication_type(ssn_for(source))
            return
        if not self.db.write_durability_barrier(state.state_id):
            return
        target_members = tuple(sorted(set(operation['original_members']) - {operation['source']}))
        target = WriteConfiguration(self.db.get_next_local_timeline(source.timeline), operation['target'], target_members)
        self.zk.cas_cluster_state(state, state.evolve(
            desired_primary=operation['target'], operation=dict(operation, phase='promoting'),
            possible_write_configurations=(source, target),
        ))

    def _finish_tuning_transition(self, state: ClusterState) -> None:
        target = state.possible_write_configurations[-1]
        expected = ssn_for(target)
        if self.db.get_fresh_ssn() != expected:
            self.db.change_replication_type(expected)
            return
        if not self.db.write_durability_barrier(state.state_id):
            return
        self.zk.cas_cluster_state(state, state.evolve(possible_write_configurations=(target,)))

    def _retire_source(self, state: ClusterState, db_state: DbState) -> None:
        target = state.operation['target']
        relevant = {
            helpers.app_name_from_fqdn(host) for host in state.operation['original_members']
            if host != state.operation['source']
        }
        connected = {
            row.application_name for row in db_state.replics_info or [] if row.state == 'streaming' and row.application_name in relevant
        }
        target_name = helpers.app_name_from_fqdn(target)
        if target_name in connected and connected - {target_name}:
            return
        logging.warning('Switchover handoff is irreversible; retiring source primary')
        self.db.pgpooler('stop')
        self.db.stop_postgresql(timeout=self.config.recovery_timeout)

    def _promote_target(self, state: ClusterState, db_state: DbState) -> None:
        target = state.possible_write_configurations[-1]
        expected_ssn = ssn_for(target)
        if db_state.role == 'replica':
            if self.db.get_fresh_ssn() != expected_ssn:
                self.db.change_replication_type(expected_ssn)
                return
            connected = {
                row.application_name for row in db_state.replics_info or [] if row.state == 'streaming'
            }
            ready = sum(helpers.app_name_from_fqdn(host) in connected for host in target.replicas)
            if ready < target.write_quorum:
                return
            fresh = self.zk.get_cluster_state()
            if fresh is None or fresh.state_id != state.state_id:
                return
            self.db.promote()
            return
        if db_state.role != 'primary':
            if time.time() >= state.operation['deadline']:
                logging.error('Switchover target unavailable after handoff; manual recovery required')
            return
        actual_timeline = self.db.get_primary_timeline()
        if actual_timeline != target.timeline:
            actual = WriteConfiguration(actual_timeline, target.primary, target.members)
            self.zk.cas_cluster_state(state, state.evolve(possible_write_configurations=(state.possible_write_configurations[0], actual)))
            return
        if self.db.get_fresh_ssn() != expected_ssn:
            self.db.change_replication_type(expected_ssn)
            return
        if not self.db.write_durability_barrier(state.state_id):
            return
        if not self._set_request_status(state.operation['request_id'], 'done'):
            return
        if self.zk.cas_cluster_state(state, state.evolve(
            operation={'type': 'idle', 'phase': None}, possible_write_configurations=(target,),
        )):
            self.zk.write_last_switchover_time()


def create_switchover_request(zk: Zookeeper, source: str | None, target: str | None, timeout: float) -> str | None:
    request_id = str(uuid4())
    data = {
        'request_id': request_id, 'status': 'requested', 'timeout': timeout,
        'source_primary': source, 'target_primary': target,
    }
    current = zk.get_switchover_request()
    if current and current[0].get('status') not in ('done', 'failed'):
        return None
    version = current[1] if current else None
    return request_id if zk.set_switchover_request(data, version) else None
