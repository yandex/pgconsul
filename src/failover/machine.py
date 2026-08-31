# encoding: utf-8
"""Failover state-machine entry point."""

from typing import Callable

from ..commands import FailoverTransitionTo, Plan
from .coordinator import FailoverCoordinatorMachine
from .participant import FailoverParticipantMachine
from .types import FailoverMachineConfig, FailoverObservation, FailoverPhase


class FailoverMachine:
    """Route one failover step to its coordinator or participant side."""

    def __init__(
        self,
        config: FailoverMachineConfig | None = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._coordinator = FailoverCoordinatorMachine(config, debug_failure)
        self._participant = FailoverParticipantMachine(config, debug_failure)

    def can_start(self, obs: FailoverObservation) -> bool:
        return self._coordinator.can_start_failover(obs)

    def plan(self, obs: FailoverObservation) -> Plan:
        failed_winner = (
            (obs.phase == FailoverPhase.FAILED or obs.phase is None and obs.must_reset)
            and obs.election_winner == obs.my_hostname
            and obs.lock_holder == obs.my_hostname
        )
        if failed_winner:
            return self._participant.plan_failed(obs)

        coordinator_winner_must_act = (
            obs.is_coordinator
            and obs.election_winner == obs.my_hostname
            and (
                (
                    obs.phase == FailoverPhase.WINNER_SELECTED
                    and obs.lock_holder != obs.my_hostname
                )
                or (
                    obs.phase == FailoverPhase.PROMOTING
                    and obs.winner_status is None
                )
            )
        )
        if obs.is_coordinator and not coordinator_winner_must_act:
            coordinator_plan = self._coordinator.plan(obs)
            return_plan = self._participant.plan_return_to_cluster(obs)
            failed = any(
                isinstance(command, FailoverTransitionTo)
                and command.phase == FailoverPhase.FAILED
                for command in coordinator_plan
            )
            if return_plan and not failed:
                if obs.phase == FailoverPhase.FINISHED:
                    return return_plan
                return [*coordinator_plan, *return_plan]
            return coordinator_plan
        return self._participant.plan(obs)
