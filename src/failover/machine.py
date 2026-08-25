# encoding: utf-8
"""Failover state-machine entry point."""

from typing import Callable

from ..commands import Plan, ReleaseLock, StopPooler
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
        cleanup = obs.must_reset or obs.phase in (
            FailoverPhase.FINISHED,
            FailoverPhase.FAILED,
        )
        prefix: Plan = []
        if not cleanup and obs.role == 'primary' and obs.election_winner != obs.my_hostname:
            prefix.append(StopPooler())
            if obs.lock_holder == obs.my_hostname:
                return [*prefix, ReleaseLock()]

        if obs.is_coordinator and (cleanup or obs.election_winner != obs.my_hostname):
            return [*prefix, *self._coordinator.plan(obs)]
        return [*prefix, *self._participant.plan(obs)]
