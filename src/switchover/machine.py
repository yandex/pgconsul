"""Pure manager-owned switchover state machine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..commands import Plan, SwitchoverAction, SwitchoverStep
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
        return self.record.handoff_is_committed() and self.record.expected_timeline is not None

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

    def owns_iteration(self, obs: SwitchoverObservation) -> bool:
        """Whether ordinary reconciliation must stay suppressed this iteration."""
        record = obs.record
        if record.phase is None:
            return False
        if not obs.record_valid:
            return True
        if record.phase == SwitchoverPhase.CLEANUP:
            return True
        if (
            record.deadline_at is not None
            and obs.current_time >= record.deadline_at
            and not obs.promotion_succeeded
        ):
            return True
        if record.handoff_is_committed() and obs.failover_active:
            return False
        if record.phase == SwitchoverPhase.FAILED:
            return True
        if (
            record.failure_reason is not None
            and record.handoff_is_committed()
            and not obs.promotion_succeeded
        ):
            return True
        if obs.my_hostname == record.selected_candidate:
            if (
                record.phase == SwitchoverPhase.HANDOFF_COMMITTED
                and not obs.committed_handoff
                and obs.lock_holder is None
            ):
                return False
            return True
        if record.handoff_is_committed() and obs.lock_holder is None:
            return obs.my_hostname == record.hostname
        return True

    def plan(self, obs: SwitchoverObservation) -> Plan:
        record = obs.record
        if record.phase is None:
            return []
        if not obs.record_valid:
            return [self._step('cleanup_invalid', obs)]
        if record.phase == SwitchoverPhase.CLEANUP:
            return [self._step('cleanup', obs)]
        if (
            record.deadline_at is None
            and obs.my_hostname == record.hostname
            and obs.role == 'primary'
            and obs.lock_holder == obs.my_hostname
        ):
            return [self._step('initialize_deadline', obs)]
        if (
            record.deadline_at is not None
            and obs.current_time >= record.deadline_at
            and not obs.promotion_succeeded
        ):
            return [self._step('handle_timeout', obs)]
        if record.handoff_is_committed() and obs.failover_active:
            return []
        plan: Plan = []
        if obs.role == 'primary' and obs.lock_holder == obs.my_hostname:
            plan.append(self._step('resume_durability', obs))
        if record.phase == SwitchoverPhase.FAILED:
            plan.append(self._step('schedule_cleanup', obs))
            return plan
        if (
            record.failure_reason is not None
            and record.handoff_is_committed()
            and not obs.promotion_succeeded
        ):
            plan.append(self._step('handle_timeout', obs))
            return plan
        if (
            obs.lock_holder is None
            and not obs.committed_handoff
            and not obs.early_candidate_lock
        ):
            plan.append(self._step('recover_pre_handoff', obs))
            return plan
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
            return plan
        if obs.my_hostname == record.hostname:
            plan.append(self._step('run_primary', obs))
            return plan
        if obs.my_hostname == record.selected_candidate:
            if (
                record.phase == SwitchoverPhase.HANDOFF_COMMITTED
                and not obs.committed_handoff
                and obs.lock_holder is None
            ):
                return []
            plan.append(self._step('run_candidate', obs))
            return plan
        if record.handoff_is_committed() and obs.lock_holder is None:
            return []
        plan.append(self._step('run_side_replica', obs))
        return plan
