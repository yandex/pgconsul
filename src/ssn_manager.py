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

    @staticmethod
    def next_config(source: DurabilityConfig, desired: DurabilityConfig, primary: str) -> DurabilityConfig:
        """Move toward desired membership by adding or removing one host."""
        source_members = set(source.members)
        desired_members = set(desired.members)
        added = sorted(desired_members - source_members)
        removed = sorted(source_members - desired_members)
        if added:
            host = primary if primary in added else added[0]
            return DurabilityConfig.build([*source.members, host])
        if removed:
            return DurabilityConfig.build(host for host in source.members if host != removed[0])
        return source

    @staticmethod
    def validate_transition(source: DurabilityConfig, target: DurabilityConfig) -> None:
        """Require one adjacent expansion or contraction."""
        source_members = set(source.members)
        target_members = set(target.members)
        if len(source_members ^ target_members) != 1 or not (
            source_members <= target_members or target_members <= source_members
        ):
            raise ValueError('Durability transition must add or remove exactly one host')

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

    def discard_transition_after_failover(self) -> bool:
        """Keep failover's stable membership and discard the old primary's transition."""
        if not self._zk.is_lock_holder():
            logging.error('Cannot discard durability transition without the primary lock')
            return False
        state, version = self._zk.get_durability_state()
        if state.transition is None:
            return True
        if state.stable is None:
            logging.error('Cannot discard durability transition without stable membership')
            return False
        logging.info(
            'Discarding durability transition after failover; keeping stable members %s',
            list(state.stable.members),
        )
        return self._zk.write_durability_state(
            DurabilityState(state.stable), version,
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
