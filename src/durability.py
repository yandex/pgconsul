"""Durability membership reconciliation and the persisted SSN barrier protocol."""

import logging
import time

from . import helpers
from .cluster_state import ClusterState, WriteConfiguration
from .pg import Postgres
from .types import DbState
from .zk import Zookeeper


def ssn_for(configuration: WriteConfiguration) -> str:
    if not configuration.replicas:
        return ''
    names = ','.join(sorted(helpers.app_name_from_fqdn(host) for host in configuration.replicas))
    return f'ANY {configuration.write_quorum}({names})'


class DurabilityManager:
    def __init__(self, db: Postgres, zk: Zookeeper, removal_delay: float):
        self.db = db
        self.zk = zk
        self.removal_delay = removal_delay
        self._missing_since: dict[str, float] = {}

    def desired_members(self, current: WriteConfiguration, db_state: DbState, ha_hosts: set[str]) -> tuple[str, ...]:
        leases = set(self.zk.get_sync_quorum_hosts()) & ha_hosts
        primary = current.primary
        streaming = {
            host for host in ha_hosts
            if any(
                row.application_name == helpers.app_name_from_fqdn(host) and row.state == 'streaming'
                for row in db_state.replics_info or []
            )
        }
        members = set(current.members)
        for host in members - {primary}:
            if host in leases:
                self._missing_since.pop(host, None)
            else:
                self._missing_since.setdefault(host, time.monotonic())

        additions = sorted((leases & streaming) - members)
        if additions:
            return tuple(sorted(members | {additions[0]}))

        removed = {
            host for host in members - {primary}
            if host not in leases and time.monotonic() - self._missing_since[host] >= self.removal_delay
        }
        return tuple(sorted(members - removed))

    def start(self, state: ClusterState, members: tuple[str, ...]) -> bool:
        if state.operation['type'] != 'idle' or len(state.possible_write_configurations) != 1:
            return False
        source = state.possible_write_configurations[0]
        if members == source.members:
            return False
        target = WriteConfiguration(source.timeline, source.primary, members)
        return self.zk.cas_cluster_state(state, state.evolve(
            operation={'type': 'durability_transition', 'phase': 'in_progress'},
            possible_write_configurations=(source, target),
        ))

    def progress(self, state: ClusterState) -> bool:
        if state.operation != {'type': 'durability_transition', 'phase': 'in_progress'}:
            return False
        target = state.possible_write_configurations[-1]
        expected_ssn = ssn_for(target)
        if self.db.get_fresh_ssn() != expected_ssn:
            logging.info('Applying target SSN for durability transition: %s', expected_ssn or '(async)')
            self.db.change_replication_type(expected_ssn)
            return False
        if not self.db.write_durability_barrier(state.state_id):
            return False
        return self.zk.cas_cluster_state(state, state.evolve(
            operation={'type': 'idle', 'phase': None},
            possible_write_configurations=(target,),
        ))
