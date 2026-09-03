"""Pure, non-owning state machine for durability configuration changes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .. import helpers
from ..commands import Decision, Plan
from ..switchover import DurabilityPinMode, SwitchoverPhase, SwitchoverRecord
from ..types import DurabilityConfig, DurabilityState, StrEnum


class DurabilityAction(StrEnum):
    """One bounded durability reconciliation effect."""

    RESUME = 'resume'
    RECONCILE = 'reconcile'
    REAPPLY_STABLE = 'reapply_stable'
    COMPLETE_SWITCHOVER_PIN = 'complete_switchover_pin'
    ACK_SWITCHOVER_EXPANSION = 'ack_switchover_expansion'


@dataclass(frozen=True)
class DurabilityObservation:
    """Immutable facts consumed by one durability-machine iteration."""

    hostname: str
    role: str | None
    db_timeline: int | None
    zk_timeline: int | None
    lock_holder: str | None
    desired_primary: str | None
    state: DurabilityState
    current_ssn_known: bool
    current_ssn: str | None
    stable_ssn: str | None
    failover_active: bool
    election_winner: str | None
    maintenance_wants_async: bool
    single_node: bool
    ordinary_changes_enabled: bool
    switchover: SwitchoverRecord
    switchover_acks: Mapping[str, Mapping[str, Any]]
    streaming_applications: frozenset[str]
    ordinary_desired: DurabilityConfig | None = None

    @property
    def may_change_postgres(self) -> bool:
        return bool(
            self.role == 'primary'
            and self.lock_holder == self.hostname
            and self.desired_primary == self.hostname
            and self.db_timeline is not None
            and self.db_timeline == self.zk_timeline
        )

    @property
    def stable_ssn_applied(self) -> bool:
        if self.stable_ssn is None:
            return True
        return bool(
            self.current_ssn_known
            and (self.current_ssn or '') == self.stable_ssn
        )


@dataclass(frozen=True)
class DurabilityStep:
    """Opaque bounded effect emitted by :class:`DurabilityMachine`."""

    action: DurabilityAction
    desired: DurabilityConfig | None = None
    primary: str | None = None
    mandatory: str | None = None
    operation_id: str | None = None


class DurabilityMachine:
    """Select one durability action without suppressing the main iteration."""

    def needs_ordinary_target(self, obs: DurabilityObservation) -> bool:
        """Whether the shell must read inputs for ordinary reconciliation."""
        return bool(
            obs.may_change_postgres
            and obs.state.transition is None
            and not obs.failover_active
            and not obs.maintenance_wants_async
            and obs.switchover.phase is None
            and not obs.single_node
            and obs.ordinary_changes_enabled
        )

    def decide(self, obs: DurabilityObservation) -> Decision:
        if obs.failover_active:
            return self._decide_failover(obs)
        if not obs.may_change_postgres:
            return Decision([], False)

        # The target SSN may already be installed. Finish its persisted barrier
        # before considering any newer policy.
        if obs.state.transition is not None:
            return self._decision(DurabilityAction.RESUME)

        if obs.maintenance_wants_async:
            return self._reconcile(
                obs,
                DurabilityConfig.build([obs.hostname]),
                reapply_stable=True,
            )

        if obs.switchover.phase is not None:
            return self._decide_switchover(obs)

        if obs.single_node:
            return self._reconcile(
                obs,
                DurabilityConfig.build([obs.hostname]),
                reapply_stable=True,
            )
        if not obs.ordinary_changes_enabled:
            return Decision([], False)
        return self._reconcile(
            obs,
            obs.ordinary_desired,
            reapply_stable=True,
        )

    def _decide_failover(self, obs: DurabilityObservation) -> Decision:
        # A coordinator may need to start another election.  Leave an
        # unfinished transition frozen until failover metadata is cleaned up.
        return Decision([], False)

    def _decide_switchover(self, obs: DurabilityObservation) -> Decision:
        record = obs.switchover
        if record.phase == SwitchoverPhase.PREPARING_DURABILITY:
            return self._decide_switchover_pin(obs)
        if (
            record.phase == SwitchoverPhase.WAITING_ARCHIVE
            and record.durability_pin_mode == DurabilityPinMode.EXPANDING
            and record.durability_pin_owner == obs.hostname
        ):
            return self._decide_switchover_expansion(obs)
        return Decision([], False)

    def _decide_switchover_pin(
        self,
        obs: DurabilityObservation,
    ) -> Decision:
        record = obs.switchover
        candidate = record.selected_candidate
        if (
            record.operation_id is None
            or record.hostname is None
            or candidate is None
            or record.durability_pin_owner != record.hostname
            or record.hostname != obs.hostname
        ):
            return Decision([], False)
        if record.durability_pin_mode == DurabilityPinMode.MANDATORY:
            mandatory = candidate
        elif record.durability_pin_mode == DurabilityPinMode.CONTRACTING:
            mandatory = None
        else:
            return Decision([], False)
        desired = (
            DurabilityConfig.build(record.original_durability_members)
            if mandatory is not None
            else DurabilityConfig.build([record.hostname, candidate])
        )
        if desired != obs.state.stable:
            return self._decision(
                DurabilityAction.RECONCILE,
                desired=desired,
            )
        ack = obs.switchover_acks.get(record.hostname)
        if ack and ack.get('durability_ready') is True:
            return Decision([], False)
        return self._decision(
            DurabilityAction.COMPLETE_SWITCHOVER_PIN,
            desired=desired,
            mandatory=mandatory,
            operation_id=record.operation_id,
        )

    def _decide_switchover_expansion(
        self,
        obs: DurabilityObservation,
    ) -> Decision:
        record = obs.switchover
        if record.operation_id is None:
            return Decision([], False)
        desired = self.switchover_expansion_target(obs)
        if desired != obs.state.stable:
            return self._decision(
                DurabilityAction.RECONCILE,
                desired=desired,
            )
        ack = obs.switchover_acks.get(obs.hostname)
        if ack and ack.get('durability_expanded') is True:
            return Decision([], False)
        return self._decision(
            DurabilityAction.ACK_SWITCHOVER_EXPANSION,
            desired=desired,
            operation_id=record.operation_id,
        )

    @staticmethod
    def switchover_expansion_target(
        obs: DurabilityObservation,
    ) -> DurabilityConfig:
        """Keep stable members and add sides still streaming from candidate."""
        record = obs.switchover
        candidate = record.selected_candidate
        stable = obs.state.stable
        members = list(stable.members) if stable is not None else []
        members.extend(
            host for host in (record.hostname, candidate)
            if host is not None
        )
        for host in record.side_replicas:
            ack = obs.switchover_acks.get(host)
            if (
                ack
                and ack.get('source') == candidate
                and ack.get('restore_disabled') is True
                and helpers.app_name_from_fqdn(host)
                in obs.streaming_applications
            ):
                members.append(host)
        return DurabilityConfig.build(members)

    @staticmethod
    def _reconcile(
        obs: DurabilityObservation,
        desired: DurabilityConfig | None,
        *,
        reapply_stable: bool,
    ) -> Decision:
        if desired is None:
            return Decision([], False)
        if desired != obs.state.stable:
            return DurabilityMachine._decision(
                DurabilityAction.RECONCILE,
                desired=desired,
            )
        if reapply_stable and not obs.stable_ssn_applied:
            return DurabilityMachine._decision(
                DurabilityAction.REAPPLY_STABLE,
                desired=desired,
            )
        return Decision([], False)

    @staticmethod
    def _decision(
        action: DurabilityAction,
        *,
        desired: DurabilityConfig | None = None,
        primary: str | None = None,
        mandatory: str | None = None,
        operation_id: str | None = None,
    ) -> Decision:
        return Decision([
            DurabilityStep(
                action,
                desired=desired,
                primary=primary,
                mandatory=mandatory,
                operation_id=operation_id,
            ),
        ], False)

    def plan(self, obs: DurabilityObservation) -> Plan:
        return self.decide(obs).plan
