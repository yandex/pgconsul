"""
SsnManager — manages the full lifecycle of synchronous_standby_names (SSN):
  - calculating the SSN string for quorum mode
  - applying it to PostgreSQL via ALTER SYSTEM SET
  - persisting it to ZooKeeper
"""
import logging
import uuid

from . import helpers
from .pg import Postgres
from .types import (
    DurabilityConfig,
    DurabilityState,
    DurabilityTransition,
)
from .zk import Zookeeper


class SsnManager:
    """
    Encapsulates SSN calculation, application, and persistence.
    """

    def __init__(
        self,
        db: Postgres,
        zk: Zookeeper,
    ):
        self._db = db
        self._zk = zk

    def calculate_quorum_ssn(self, replica_hosts: list[str]) -> str:
        """
        Calculate the synchronous_standby_names value for quorum mode.

        Returns 'ANY N(app1,app2,...)' where N = ceil(len/2),
        or '' (empty string, async) if replica_hosts is empty.

        Duplicate hosts are removed before calculation so that the quorum
        size and the participant list reflect unique replicas only.
        """
        unique_hosts = sorted(set(replica_hosts)) if replica_hosts else []
        if not unique_hosts:
            return ''
        quorum_size = (len(unique_hosts) + 1) // 2
        app_names = sorted(map(helpers.app_name_from_fqdn, unique_hosts))
        return f"ANY {quorum_size}({','.join(app_names)})"

    def calculate_ssn_for_host(self, config: DurabilityConfig, hostname: str) -> str:
        replicas = config.replicas_for(hostname)
        return self.calculate_quorum_ssn(replicas)

    def calculate_ssn_with_mandatory(
        self,
        config: DurabilityConfig,
        primary: str,
        mandatory: str,
    ) -> str:
        """Keep the original quorum and additionally require one replica."""
        replicas = config.replicas_for(primary)
        if mandatory not in replicas:
            raise ValueError('Mandatory replica is absent from durability members')
        quorum = self.calculate_quorum_ssn(replicas)
        mandatory_app = helpers.app_name_from_fqdn(mandatory)
        return f'EVERY({mandatory_app}), {quorum}'

    def apply_ssn_with_mandatory(
        self,
        config: DurabilityConfig,
        primary: str,
        mandatory: str,
    ) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot apply mandatory SSN without the primary lock')
            return False
        standby_names = self.calculate_ssn_with_mandatory(
            config, primary, mandatory,
        )
        return self.apply_and_persist(
            standby_names,
            f'Pinning mandatory synchronous replica: {standby_names}.',
            'Pinned mandatory synchronous replica.',
        )

    @staticmethod
    def next_config(source: DurabilityConfig, desired: DurabilityConfig, primary: str) -> DurabilityConfig:
        """Move toward desired membership by adding one or removing any hosts."""
        source_members = set(source.members)
        desired_members = set(desired.members)
        added = sorted(desired_members - source_members)
        removed = sorted(source_members - desired_members)
        if added:
            host = primary if primary in added else added[0]
            return DurabilityConfig.build([*source.members, host])
        if removed:
            return DurabilityConfig.build(
                host for host in source.members if host in desired_members
            )
        return source

    @staticmethod
    def validate_transition(source: DurabilityConfig, target: DurabilityConfig) -> None:
        """Allow one-host expansion or an arbitrary pure contraction."""
        source_members = set(source.members)
        target_members = set(target.members)
        if source_members < target_members:
            if len(target_members - source_members) == 1:
                return
            raise ValueError('Durability transition must add exactly one host')
        if target_members < source_members:
            return
        raise ValueError('Durability transition must only add or only remove hosts')

    def reconcile_durability(self, desired: DurabilityConfig, primary: str) -> bool:
        """Advance or resume one crash-safe durability membership transition."""
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
            transition = DurabilityTransition(
                source=None,
                target=desired,
                operation_id=uuid.uuid4().hex,
            )
            prepared = DurabilityState(stable=None, transition=transition)
            transition_version = self._zk.write_durability_state(prepared, version)
            if transition_version is None:
                return False
            return self._complete_transition(prepared, transition_version, primary)

        target = self.next_config(state.stable, desired, primary)
        self.validate_transition(state.stable, target)
        transition = DurabilityTransition(
            source=state.stable,
            target=target,
            operation_id=uuid.uuid4().hex,
        )
        prepared = DurabilityState(stable=state.stable, transition=transition)
        transition_version = self._zk.write_durability_state(prepared, version)
        if transition_version is None:
            return False
        completed = self._complete_transition(prepared, transition_version, primary)
        return completed and target == desired

    def resume_durability_transition(self, primary: str) -> bool:
        """Complete only a transition already persisted in ZK."""
        if not self._zk.is_lock_holder():
            logging.error('Cannot resume durability transition without the primary lock')
            return False
        state, version = self._zk.get_durability_state()
        if state.transition is None:
            return True
        return self._complete_transition(state, version, primary)

    def durability_for_failover_winner(self, primary: str) -> DurabilityConfig | None:
        """Select the quorum that contains a winner admitted during transition."""
        state, _ = self._zk.get_durability_state()
        transition = state.transition
        source = transition.source if transition is not None else None
        if transition is not None and (source is None or primary not in source.members):
            if primary in transition.target.members:
                return transition.target
            return None
        return state.stable

    def discard_transition_after_failover(self, primary: str) -> bool:
        """Materialize the quorum that admitted the failover winner."""
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
        logging.info(
            'Completing durability transition after failover with members %s',
            list(stable.members),
        )
        return self._zk.write_durability_state(
            DurabilityState(stable), version,
        ) is not None

    def _complete_transition(self, state: DurabilityState, version: int | None, primary: str) -> bool:
        transition = state.transition
        if transition is None:
            return True
        if transition.source is not None:
            try:
                self.validate_transition(transition.source, transition.target)
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

    def _apply_config(self, config: DurabilityConfig, primary: str) -> bool:
        if not self._zk.is_lock_holder():
            logging.error('Cannot apply SSN without the primary lock')
            return False
        standby_names = self.calculate_ssn_for_host(config, primary)
        display = standby_names if standby_names else '(async)'
        return self.apply_and_persist(
            standby_names,
            f'Changing synchronous replication to {display}.',
            'Changed synchronous replication.',
        )

    def apply_stable_config(self, config: DurabilityConfig, primary: str) -> bool:
        """Restore PostgreSQL SSN when membership is already stable in ZK."""
        return self._apply_config(config, primary)

    def apply_and_persist(self, standby_names: str, start_msg: str, success_msg: str) -> bool:
        """
        Apply a new SSN value to PostgreSQL and, on success, persist it to ZK.

        Logs 'ACTION. {start_msg}' before the attempt and success_msg on
        success.  Returns True on success, False on failure.

        Note: No retry mechanism - if the DB call fails, the next iteration
        will retry automatically. This avoids blocking the main pgconsul loop.
        """
        logging.info(f'ACTION. {start_msg}')

        if self._db.change_replication_type(standby_names):
            logging.info(success_msg)
            if not self._zk.write_ssn_on_changes(standby_names):
                logging.warning('SSN applied to DB but failed to persist to ZK')
            return True

        logging.error('Failed to apply SSN %r', standby_names)
        return False
