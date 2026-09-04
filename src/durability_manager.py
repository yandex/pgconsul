"""Durability membership, SSN and crash-safe membership transitions."""
import logging
import uuid
from configparser import RawConfigParser
from dataclasses import dataclass
from typing import Iterable

from . import helpers
from .list_removal_strategy import DelayedListRemovalStrategy
from .pg import Postgres
from .types import DurabilityConfig, DurabilityState, DurabilityTransition
from .zk import Zookeeper


@dataclass
class DurabilityManagerConfig:
    quorum_removal_delay: float
    manual_exclusion_timeout: float = 24 * 60 * 60


class DurabilityManager:
    """Owns ordinary durability policy and its SSN/ZooKeeper realization."""

    def __init__(self, config: DurabilityManagerConfig, db: Postgres, zk: Zookeeper):
        self._db = db
        self._zk = zk
        self._removal_strategy = DelayedListRemovalStrategy(config.quorum_removal_delay)
        self._manual_exclusion_timeout = config.manual_exclusion_timeout
        if config.quorum_removal_delay > 0:
            logging.info('Removing unavailable durability members after %ss', config.quorum_removal_delay)
        else:
            logging.info('Removing unavailable durability members immediately')
        self._previous_durability_members: list[str] | None = None

    def init_zk(self) -> bool:
        if not self._zk.ensure_durability_path():
            logging.error("Can't create durability path in ZK")
            return False
        return True

    def desired_durability(
        self,
        db_state: dict,
        configured_ha_replicas: set[str],
        alive_hosts: Iterable[str],
        durability: DurabilityConfig | None,
    ) -> DurabilityConfig:
        """Calculate the ordinary primary target without changing PostgreSQL."""
        alive_ha_replicas = configured_ha_replicas & set(alive_hosts)
        excluded = self._zk.get_active_durability_exclusions(
            configured_ha_replicas, self._manual_exclusion_timeout,
        )
        current = db_state.get('replication_state')
        if current is None:
            current = self._db.get_replication_state()
        logging.info('Current replication type is %s.', current)
        repl_state = current[0]
        hostname = helpers.get_hostname()
        current_members = list(durability.members) if durability is not None else []
        current_replicas = [host for host in current_members if host != hostname]
        streaming_app_names = {
            replica['application_name'] for replica in db_state.get('replics_info') or []
            if replica.get('state') == 'streaming'
        }
        streaming_ha_replicas = {
            host for host in alive_ha_replicas
            if host not in excluded and helpers.app_name_from_fqdn(host) in streaming_app_names
        }
        current_replicas = [host for host in current_replicas if host not in excluded]
        available_replicas = (set(current_replicas) & alive_ha_replicas) | streaming_ha_replicas
        if excluded:
            logging.info('Manual durability exclusions: %s', sorted(excluded))
        self._log_members_change(current_members)
        replicas = self._removal_strategy.get_hosts_to_keep(current_replicas, list(available_replicas))
        target = DurabilityConfig.build([hostname, *replicas])
        needed = 'sync' if replicas else 'async'
        logging.info('Needed replication type is %s.', needed)
        if needed != repl_state:
            logging.info('We should change replication from %s to %s', repl_state, needed)
        if target != durability:
            logging.info('DURABILITY-MEMBERS-CHANGED: members are changing from %s to %s', sorted(current_members), list(target.members))
        return target

    def _log_members_change(self, current_members: list[str]) -> None:
        previous = self._previous_durability_members
        if previous is not None and set(current_members) != set(previous):
            added, removed = set(current_members) - set(previous), set(previous) - set(current_members)
            logging.info(
                'DURABILITY-MEMBERS-CHANGED in ZK: from %s to %s (added: %s, removed: %s)',
                sorted(previous),
                sorted(current_members),
                sorted(added) if added else 'none',
                sorted(removed) if removed else 'none',
            )
        self._previous_durability_members = current_members.copy()

    def set_ssn_before_promote(self, durability: DurabilityConfig | None) -> bool:
        if durability is None:
            logging.warning('No durability config found before promote, SSN will be set to async')
            standby_names = ''
        else:
            standby_names = self.ssn_for_durability(durability, helpers.get_hostname())
        display = standby_names if standby_names else '(async)'
        return self._apply_and_persist(standby_names, f'Setting SSN before promote: {display}.', 'Set SSN before promote.')

    def set_mandatory_sync_replica(self, durability: DurabilityConfig, mandatory: str) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot apply mandatory SSN without the primary lock')
            return False
        standby_names = self.calculate_ssn_with_mandatory(
            durability, helpers.get_hostname(), mandatory,
        )
        return self._apply_and_persist(standby_names, f'Pinning mandatory synchronous replica: {standby_names}.', 'Pinned mandatory synchronous replica.')

    def change_replication_to_durability_config(self, desired: DurabilityConfig) -> bool:
        return self._reconcile(desired, helpers.get_hostname())

    def apply_stable_durability_config(self, durability: DurabilityConfig) -> bool:
        return self._apply_config(durability, helpers.get_hostname())

    def ssn_for_durability(self, durability: DurabilityConfig, primary: str) -> str:
        return self._quorum_ssn(durability.replicas_for(primary))

    calculate_quorum_ssn = staticmethod(lambda replicas: DurabilityManager._quorum_ssn(replicas))

    def calculate_ssn_for_host(self, durability: DurabilityConfig, primary: str) -> str:
        return self.ssn_for_durability(durability, primary)

    def calculate_ssn_with_mandatory(
        self, durability: DurabilityConfig, primary: str, mandatory: str,
    ) -> str:
        replicas = durability.replicas_for(primary)
        if mandatory not in replicas:
            raise ValueError('Mandatory replica is absent from durability members')
        return f'EVERY({helpers.app_name_from_fqdn(mandatory)}), {self._quorum_ssn(replicas)}'

    @staticmethod
    def _quorum_ssn(replicas: list[str]) -> str:
        hosts = sorted(set(replicas))
        if not hosts:
            return ''
        return f"ANY {(len(hosts) + 1) // 2}({','.join(sorted(map(helpers.app_name_from_fqdn, hosts)))})"

    def resume_durability_transition(self, primary: str | None = None) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot resume durability transition without the primary lock')
            return False
        state, version = self._zk.get_durability_state()
        return state.transition is None or self._complete_transition(
            state, version, primary or helpers.get_hostname(),
        )

    def reconcile_durability(self, desired: DurabilityConfig, primary: str) -> bool:
        """Reconcile an explicit target; used by the durability protocol tests."""
        return self._reconcile(desired, primary)

    def durability_for_failover_winner(self, primary: str) -> DurabilityConfig | None:
        state, _ = self._zk.get_durability_state()
        transition = state.transition
        source = transition.source if transition is not None else None
        if transition is not None and (source is None or primary not in source.members):
            return transition.target if primary in transition.target.members else None
        return state.stable

    def discard_transition_after_failover(self, primary: str) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot discard durability transition without the primary lock')
            return False
        state, version = self._zk.get_durability_state()
        if state.transition is None:
            return True
        if state.stable is None:
            logging.error('Cannot discard durability transition without stable membership')
            return False
        stable = state.stable
        if primary not in stable.members:
            if primary not in state.transition.target.members:
                logging.error('Failover winner %s is absent from both durability quorums', primary)
                return False
            stable = state.transition.target
        logging.info('Completing durability transition after failover with members %s', list(stable.members))
        return self._zk.write_durability_state(DurabilityState(stable), version) is not None

    def _reconcile(self, desired: DurabilityConfig, primary: str) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot reconcile durability without the primary lock')
            return False
        state, version = self._zk.get_durability_state()
        if state.transition is not None:
            completed = self._complete_transition(state, version, primary)
            return completed and state.transition.target == desired
        if state.stable == desired:
            return True
        if state.stable is None:
            transition = DurabilityTransition(None, desired, uuid.uuid4().hex)
        else:
            target = self._next_config(state.stable, desired, primary)
            self._validate_transition(state.stable, target)
            transition = DurabilityTransition(state.stable, target, uuid.uuid4().hex)
        prepared = DurabilityState(state.stable, transition)
        transition_version = self._zk.write_durability_state(prepared, version)
        if transition_version is None:
            return False
        completed = self._complete_transition(prepared, transition_version, primary)
        return completed and transition.target == desired

    @staticmethod
    def _next_config(source: DurabilityConfig, desired: DurabilityConfig, primary: str) -> DurabilityConfig:
        source_members, desired_members = set(source.members), set(desired.members)
        added = sorted(desired_members - source_members)
        if added:
            host = primary if primary in added else added[0]
            return DurabilityConfig.build([*source.members, host])
        return DurabilityConfig.build(host for host in source.members if host in desired_members)

    @staticmethod
    def _validate_transition(source: DurabilityConfig, target: DurabilityConfig) -> None:
        source_members, target_members = set(source.members), set(target.members)
        if source_members < target_members and len(target_members - source_members) == 1:
            return
        if target_members < source_members:
            return
        raise ValueError('Durability transition must add exactly one host or only remove hosts')

    validate_transition = staticmethod(_validate_transition)

    def _complete_transition(self, state: DurabilityState, version: int | None, primary: str) -> bool:
        transition = state.transition
        assert transition is not None
        if transition.source is not None:
            try:
                self._validate_transition(transition.source, transition.target)
            except ValueError:
                logging.exception('Invalid durability transition: %s', transition)
                return False
        if state.stable not in (transition.source, transition.target):
            logging.error('Invalid durability transition state: %s', state)
            return False
        if not self._apply_config(transition.target, primary):
            return False
        if not self._db.advance_wal_barrier(transition.operation_id):
            return False
        return self._zk.write_durability_state(DurabilityState(transition.target), version) is not None

    def _apply_config(self, durability: DurabilityConfig, primary: str) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot apply SSN without the primary lock')
            return False
        standby_names = self.ssn_for_durability(durability, primary)
        display = standby_names if standby_names else '(async)'
        return self._apply_and_persist(standby_names, f'Changing synchronous replication to {display}.', 'Changed synchronous replication.')

    def _apply_and_persist(self, standby_names: str, start_msg: str, success_msg: str) -> bool:
        logging.info('ACTION. %s', start_msg)
        if not self._db.change_replication_type(standby_names):
            logging.error('Failed to apply SSN %r', standby_names)
            return False
        logging.info(success_msg)
        if not self._zk.write_ssn_on_changes(standby_names):
            logging.warning('SSN applied to DB but failed to persist to ZK')
        return True


def build_durability_manager_config(config: RawConfigParser) -> DurabilityManagerConfig:
    quorum_removal_delay = config.getfloat('primary', 'quorum_removal_delay')
    if quorum_removal_delay < 0:
        logging.warning('quorum_removal_delay is negative (%s), setting to 0 (immediate removal)', quorum_removal_delay)
        quorum_removal_delay = 0
    elif quorum_removal_delay > 120:
        logging.warning('quorum_removal_delay is set to %s seconds; setting to 120 seconds.', quorum_removal_delay)
        quorum_removal_delay = 120
    manual_exclusion_timeout = config.getfloat(
        'primary', 'manual_durability_exclusion_timeout', fallback=24 * 60 * 60,
    )
    if manual_exclusion_timeout <= 0:
        logging.warning('manual_durability_exclusion_timeout must be positive; using one day')
        manual_exclusion_timeout = 24 * 60 * 60
    return DurabilityManagerConfig(
        quorum_removal_delay=quorum_removal_delay,
        manual_exclusion_timeout=manual_exclusion_timeout,
    )


def create_durability_manager(config: RawConfigParser, db: Postgres, zk: Zookeeper) -> DurabilityManager:
    return DurabilityManager(build_durability_manager_config(config), db, zk)
