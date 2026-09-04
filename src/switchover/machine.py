"""Pure manager-owned switchover state machine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..commands import Decision, Plan, SwitchoverAction, SwitchoverStep
from .types import SwitchoverPhase, SwitchoverRecord


@dataclass(frozen=True)
class SwitchoverObservation:
    """Immutable snapshot consumed by one switchover planning step."""

    record: SwitchoverRecord
    my_hostname: str
    role: str | None
    lock_holder: str | None
    zk_timeline: int | None
    current_time: float
    desired_hostname: str | None
    desired_operation_id: str | None
    failover_active: bool
    promotion_succeeded: bool
    record_valid: bool
    db_state: Mapping[str, Any]
    zk_state: Mapping[str, Any]

    @property
    def committed_handoff(self) -> bool:
        return self.record.handoff_is_committed()

    @property
    def early_candidate_lock(self) -> bool:
        return bool(
            self.record.phase == SwitchoverPhase.TURNING_SIDES
            and self.record.selected_candidate is not None
            and self.desired_operation_id == self.record.local_operation_id
            and self.desired_hostname == self.record.selected_candidate
        )


class SwitchoverMachine:
    """Return an ordered, side-effect-free plan for the current snapshot."""

    @staticmethod
    def _step(action: SwitchoverAction, obs: SwitchoverObservation) -> SwitchoverStep:
        return SwitchoverStep(
            action=action,
            record=obs.record,
            db_state=obs.db_state,
            zk_state=obs.zk_state,
        )

    def decide(self, obs: SwitchoverObservation) -> Decision:
        """Return the plan and whether it suppresses ordinary reconciliation."""
        record = obs.record
        if record.phase is None:
            return Decision([], False)
        if not obs.record_valid:
            return Decision([self._step('cleanup_invalid', obs)], True)
        if record.phase == SwitchoverPhase.CLEANUP:
            return Decision([self._step('cleanup', obs)], True)
        if record.phase == SwitchoverPhase.FAILED:
            return Decision([self._step('schedule_cleanup', obs)], True)
        if (
            record.deadline_at is None
            and obs.my_hostname == record.hostname
            and obs.role == 'primary'
            and obs.lock_holder == obs.my_hostname
        ):
            return Decision([self._step('initialize_deadline', obs)], True)
        if (
            record.deadline_at is not None
            and obs.current_time >= record.deadline_at
            and not obs.promotion_succeeded
        ):
            timeout_action: SwitchoverAction = (
                'recover_committed_handoff_timeout'
                if record.handoff_is_committed()
                else 'rollback_pre_handoff_timeout'
            )
            return Decision([self._step(timeout_action, obs)], True)
        if record.handoff_is_committed() and obs.failover_active:
            return Decision([], False)
        if record.phase == SwitchoverPhase.WAITING_ARCHIVE:
            # Archive availability fences returns, not normal primary repair or
            # automatic failover.  P remains locally BLOCKED by the handoff.
            if obs.my_hostname == record.hostname:
                return Decision([self._step('primary_fence_return', obs)], True)
            if obs.my_hostname == record.selected_candidate:
                return Decision([self._step('candidate_wait_archive', obs)], False)
            return Decision([self._step('side_wait_archive', obs)], False)
        if record.phase == SwitchoverPhase.RECOVERING:
            if obs.my_hostname == record.selected_candidate:
                return Decision([self._step('candidate_wait_recovery', obs)], False)
            return Decision([], False)
        plan: Plan = []
        if (
            record.failure_reason is not None
            and record.handoff_is_committed()
            and not obs.promotion_succeeded
        ):
            plan.append(self._step('recover_committed_handoff_timeout', obs))
            return Decision(plan, True)
        if (
            obs.lock_holder is None
            and not obs.committed_handoff
            and not obs.early_candidate_lock
        ):
            plan.append(self._step('recover_pre_handoff', obs))
            return Decision(plan, True)
        if (
            record.phase == SwitchoverPhase.FALLBACK
            or (
                not obs.committed_handoff
                and obs.lock_holder is not None
                and obs.lock_holder != record.hostname
                and not (
                    obs.early_candidate_lock
                    and obs.lock_holder == record.selected_candidate
                )
            )
        ):
            plan.append(self._step('schedule_cleanup', obs))
            return Decision(plan, True)
        if obs.my_hostname == record.hostname:
            primary_actions: dict[SwitchoverPhase, SwitchoverAction] = {
                SwitchoverPhase.SCHEDULED: 'primary_schedule',
                SwitchoverPhase.PREPARING_DURABILITY: 'primary_prepare_durability',
                SwitchoverPhase.PREPARING_CANDIDATE: 'primary_prepare_candidate',
                SwitchoverPhase.TURNING_SIDES: 'primary_turn_sides',
                SwitchoverPhase.HANDOFF_COMMITTED: 'primary_confirm_promotion',
            }
            primary_action = primary_actions.get(record.phase)
            return Decision([self._step(primary_action, obs)] if primary_action else [], True)
        if obs.my_hostname == record.selected_candidate:
            candidate_actions: dict[SwitchoverPhase, SwitchoverAction] = {
                SwitchoverPhase.PREPARING_CANDIDATE: 'candidate_prepare',
                SwitchoverPhase.TURNING_SIDES: 'candidate_prepare',
                SwitchoverPhase.HANDOFF_COMMITTED: 'candidate_promote',
            }
            candidate_action = candidate_actions.get(record.phase)
            return Decision([self._step(candidate_action, obs)] if candidate_action else [], True)
        if record.handoff_is_committed() and obs.lock_holder is None:
            return Decision([], False)
        if record.phase in (
            SwitchoverPhase.TURNING_SIDES,
            SwitchoverPhase.HANDOFF_COMMITTED,
        ):
            if (
                record.phase == SwitchoverPhase.TURNING_SIDES
                and (
                    obs.my_hostname not in record.side_turn_permitted
                    or obs.desired_hostname != record.selected_candidate
                    or obs.desired_operation_id != record.operation_id
                )
            ):
                return Decision([], True)
            plan.append(self._step('side_turn', obs))
        return Decision(plan, True)

    def plan(self, obs: SwitchoverObservation) -> Plan:
        """Compatibility projection for callers that only execute commands."""
        return self.decide(obs).plan

    def owns_iteration(self, obs: SwitchoverObservation) -> bool:
        """Compatibility projection for callers that only inspect ownership."""
        return self.decide(obs).owns_iteration
