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
            and self.record.operation_id is not None
            and self.record.selected_candidate is not None
            and self.desired_operation_id == self.record.operation_id
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
            return Decision([self._step('handle_timeout', obs)], True)
        if record.handoff_is_committed() and obs.failover_active:
            return Decision([], False)
        plan: Plan = []
        if record.phase == SwitchoverPhase.FAILED:
            plan.append(self._step('schedule_cleanup', obs))
            return Decision(plan, True)
        if (
            record.failure_reason is not None
            and record.handoff_is_committed()
            and not obs.promotion_succeeded
        ):
            plan.append(self._step('handle_timeout', obs))
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
            plan.append(self._step('run_primary', obs))
            return Decision(plan, True)
        if obs.my_hostname == record.selected_candidate:
            plan.append(self._step('run_candidate', obs))
            return Decision(plan, True)
        if record.handoff_is_committed() and obs.lock_holder is None:
            return Decision([], False)
        plan.append(self._step('run_side_replica', obs))
        return Decision(plan, True)

    def plan(self, obs: SwitchoverObservation) -> Plan:
        """Compatibility projection for callers that only execute commands."""
        return self.decide(obs).plan

    def owns_iteration(self, obs: SwitchoverObservation) -> bool:
        """Compatibility projection for callers that only inspect ownership."""
        return self.decide(obs).owns_iteration
