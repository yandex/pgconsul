# encoding: utf-8
"""Primary-side switchover state machine (ADR-0005 §3, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. Cross-host phases are persisted in ZK; primary-only command
groups are persisted on the local filesystem.
"""

import logging
from typing import Callable, cast

from ..commands import (
    AcquireLock,
    Checkpoint,
    ClearLocalState,
    CleanupSwitchover,
    DeleteHostOp,
    InitializeFailover,
    Log,
    Plan as CommandPlan,
    ReleaseLock,
    ReturnToCluster,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    StartTimer,
    StartPostgresql,
    StopTimer,
    StopPooler,
    StopPostgresql,
    StoreReplicsInfo,
    TransitionTo,
    WriteCandidate,
    WriteLocalState,
    WriteSideReplicas,
)
from ..helpers import app_name_from_fqdn
from ..types import ReplicaInfos
from ..types import is_timed_out, is_transition_allowed
from .types import (
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
)

class PrimarySwitchoverMachine:
    """Primary-side switchover state machine (ADR-0005 §3, ADR-0006)."""

    # Phases where primary waits for candidate to promote — timeout gate
    # short-circuits to FAILED after promote_timeout (ADR-0007 §2 analog).
    _PROMOTE_WAIT_PHASES = frozenset({
        SwitchoverPhase.PRIMARY_SHUT,
        SwitchoverPhase.CANDIDATE_ACQUIRED,
    })
    _CANDIDATE_PHASES = frozenset({
        SwitchoverPhase.SYNC_SET,
        SwitchoverPhase.INITIATED,
        SwitchoverPhase.CANDIDATE_FOUND,
        SwitchoverPhase.POOLER_STOPPED,
        SwitchoverPhase.PG_STOPPED,
    })

    def __init__(
        self,
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._cfg = config or SwitchoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Return Command Plan for current observation (pure, no I/O).

        Empty Plan = nothing to do, retry next iteration (ADR-0006 §2).
        """
        if obs.record.requires_primary_lock():
            if obs.lock_holder is None:
                if obs.record.hostname == obs.my_hostname and obs.role == 'primary':
                    return [AcquireLock(allow_queue=False, timeout=0)]
                return self._plan_fallback()
            if obs.lock_holder != obs.record.hostname:
                logging.error(
                    'Switchover primary %s does not hold the primary lock (holder=%s)',
                    obs.record.hostname,
                    obs.lock_holder,
                )
                return [TransitionTo(SwitchoverPhase.FAILED)]

        # Timeout gate: if candidate didn't promote in time → FAILED.
        if obs.record.phase in self._PROMOTE_WAIT_PHASES and is_timed_out(
            obs.downtime_started_ts, self._cfg.promote_timeout, 'Candidate promote'
        ):
            return [TransitionTo(SwitchoverPhase.FAILED)]

        phase = obs.record.phase
        if (phase, obs.local_phase) in (
            (SwitchoverPhase.SCHEDULED, SwitchoverPhase.SYNC_SET),
            (SwitchoverPhase.CANDIDATE_FOUND, SwitchoverPhase.POOLER_STOPPED),
            (SwitchoverPhase.CANDIDATE_FOUND, SwitchoverPhase.PG_STOPPED),
        ):
            phase = obs.local_phase
        if phase in self._CANDIDATE_PHASES and obs.record.selected_candidate is None:
            logging.error('Switchover %s: candidate is None, aborting', phase)
            return [TransitionTo(SwitchoverPhase.FAILED)]

        match phase:
            case SwitchoverPhase.SCHEDULED:
                return self.plan_scheduled(obs)
            case SwitchoverPhase.SYNC_SET:
                return self.plan_sync_set(obs)
            case SwitchoverPhase.INITIATED:
                return self.plan_initiated(obs)
            case SwitchoverPhase.CANDIDATE_FOUND:
                return self.plan_candidate_found(obs)
            case SwitchoverPhase.POOLER_STOPPED:
                return self.plan_pooler_stopped(obs)
            case SwitchoverPhase.PG_STOPPED:
                return self.plan_pg_stopped(obs)
            case SwitchoverPhase.PRIMARY_SHUT | SwitchoverPhase.PROMOTED:
                # PROMOTED: candidate promoted — old primary rewinds (same handler).
                return self.plan_primary_shut(obs)
            case SwitchoverPhase.FAILED:
                return self.plan_failed(obs)
            case SwitchoverPhase.FALLBACK:
                return self.plan_fallback(obs)
            case _:
                logging.debug('No primary-side planner for switchover phase %s', obs.record.phase)
                return []

    def _candidate_is_sync(self, replics_info: ReplicaInfos, candidate: str) -> bool:
        """Pure predicate: candidate in sync with primary (uses config, not pgconsul config)."""
        candidate_appname = app_name_from_fqdn(candidate)
        replica = next(
            (r for r in replics_info if r.get('application_name') == candidate_appname),
            None,
        )
        if replica is None:
            logging.warning('Could not find replica info for %s', candidate)
            return False
        # LSN-based catchup: if replay_location_diff=0 and write_location_diff=0,
        # candidate has caught up — replay_lag_msec may be frozen (pooler stopped).
        if replica.get('replay_location_diff') == 0 and replica.get('write_location_diff') == 0:
            logging.info('Replica %s LSN caught up (replay_location_diff=0)', candidate)
            return True
        replay_lag = replica.get('replay_lag_msec')
        logging.info('Replica %s has replay lag %sms', candidate, replay_lag)
        if replay_lag is None:
            logging.warning('Could not get replay lag for replica %s', candidate)
            return False
        try:
            replay_lag_ms = int(replay_lag)
        except (TypeError, ValueError):
            logging.warning('Invalid replay lag %r for replica %s, treating as not in sync', replay_lag, candidate)
            return False
        if replay_lag_ms > self._cfg.max_allowed_lag_ms:
            logging.warning(
                'Replica %s cannot be primary for switchover, max allowed lag %sms',
                candidate, self._cfg.max_allowed_lag_ms,
            )
            return False
        return True

    def _last_transition_ok(self, obs: 'SwitchoverObservation') -> bool:
        """Last role transition old enough, or enough replicas alive."""
        # plan_scheduled checks this before calling us.
        assert obs.ha_replics is not None
        last_role_transition_ts = obs.last_role_transition_ts or 0.0
        alive_replics_number = len([i for i in obs.replics_info if i.get('state') == 'streaming'])
        if not is_transition_allowed(
            last_role_transition_ts,
            self._cfg.min_role_transition_timeout,
            now=obs.current_time,
        ) and (
            alive_replics_number < len(obs.ha_replics)
        ):
            logging.warning(
                'Switchover scheduled: last role transition was %.1f seconds ago,'
                ' and alive host count less than HA hosts (HA: %d, alive: %d) ignoring switchover.',
                obs.current_time - last_role_transition_ts,
                len(obs.ha_replics),
                alive_replics_number,
            )
            return False
        return True

    def plan_scheduled(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """scheduled → sync_set: sanity-check, choose candidate, set sync replication.

        Empty Plan when a gate fails (retry next iteration).
        """
        if obs.record.hostname != obs.my_hostname:
            logging.warning(
                'Switchover scheduled: hostname %s differs from current %s, ignoring',
                obs.record.hostname, obs.my_hostname,
            )
            return []
        if obs.role != 'primary':
            logging.error('Switchover scheduled: current role is %s, ignoring switchover', obs.role)
            return []
        if obs.zk_timeline != obs.record.timeline:
            logging.warning(
                'Switchover scheduled: ZK timeline %s differs from switchover timeline %s, ignoring',
                obs.zk_timeline, obs.record.timeline,
            )
            return [TransitionTo(SwitchoverPhase.FAILED)]
        if obs.ha_replics is None:
            logging.warning('Switchover scheduled: HA replicas are empty, ignoring switchover')
            return []
        if not self._last_transition_ok(obs):
            return []

        # --- Choose candidate ---

        candidate = obs.switchover_candidate
        if candidate is None:
            logging.info('Switchover scheduled: no eligible candidate, waiting')
            return []

        if obs.record.destination is not None:
            candidate_appname = app_name_from_fqdn(candidate)
            if not any(
                replica.get('application_name') == candidate_appname
                for replica in obs.replics_info
            ):
                logging.warning(
                    'Switchover scheduled: requested candidate %s is not streaming, aborting',
                    candidate,
                )
                return [TransitionTo(SwitchoverPhase.FAILED)]

        # --- Check candidate is in sync ---

        if not self._candidate_is_sync(obs.replics_info, candidate):
            logging.info('Switchover scheduled: candidate %s not yet in sync, waiting', candidate)
            return []

        # --- Action: set sync replication, transition to sync_set ---

        logging.info('Scheduled switchover checks passed OK.')

        plan: CommandPlan = []
        if obs.switchover_started_ts is None:
            plan.append(StartTimer('switchover'))

        # Persist candidate so plan_sync_set reads it next iteration
        # (anywhere-switchover without destination needs this).
        plan.append(WriteCandidate(candidate=candidate))

        logging.warning('Starting sync replication %s', candidate)
        plan.append(SetSyncReplication(host=candidate))

        plan.append(WriteLocalState('switchover_primary', SwitchoverPhase.SYNC_SET))
        return plan

    def plan_sync_set(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """sync_set → initiated: fix candidate + side replicas, write initiated.

        Emits TransitionTo(FAILED) if candidate is None.
        """
        candidate = cast(str, obs.record.selected_candidate)

        side_replicas = tuple(r for r in obs.streaming_replicas if r != candidate)

        logging.info('Switchover sync_set: candidate=%s side_replicas=%s', candidate, side_replicas)

        return [
            WriteCandidate(candidate=candidate),
            WriteSideReplicas(side_replicas=side_replicas),
            TransitionTo(SwitchoverPhase.INITIATED),
            ClearLocalState('switchover_primary'),
        ]

    def plan_initiated(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """initiated: wait (non-blocking) for candidate to set candidate_found.

        Emits pre-shutdown prep when detected; aborts if candidate is dead.
        No phase transition — candidate writes candidate_found, primary detects it.
        """
        candidate = cast(str, obs.record.selected_candidate)

        if obs.candidate_alive is not True:
            logging.warning(
                'Switchover initiated: candidate %s is no longer alive, aborting switchover',
                candidate,
            )
            return [TransitionTo(SwitchoverPhase.FAILED)]

        logging.debug('Switchover initiated: waiting for candidate_found')
        return []

    def plan_candidate_found(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """candidate_found → pooler_stopped: stop pooler, start downtime timer.

        Splits old monolithic handler for granular kill-9 recovery (ADR-0006 §4).
        Sync check moves to plan_pooler_stopped.
        """
        plan: CommandPlan = [StoreReplicsInfo(), Checkpoint()]
        if obs.downtime_started_ts is None:
            plan.append(StartTimer('downtime'))
        plan.extend([
            StopPooler(),
            Log(
                message='Cluster closed from user requests (pooler stopped)',
                level='warning',
            ),
        ])
        if self._debug_failure('primary_switchover_before_catchup'):
            plan.append(TransitionTo(SwitchoverPhase.FAILED))
            return plan
        plan.append(WriteLocalState('switchover_primary', SwitchoverPhase.POOLER_STOPPED))
        return plan

    def plan_pooler_stopped(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """pooler_stopped → pg_stopped: non-blocking sync check, stop PG.

        Empty Plan if not in sync (retry next iteration).
        Catchup timeout gate: if candidate didn't catch up in catchup_timeout → FAILED.
        """
        candidate = cast(str, obs.record.selected_candidate)

        if not self._candidate_is_sync(obs.replics_info, candidate):
            if is_timed_out(obs.downtime_started_ts, self._cfg.catchup_timeout, 'Switchover catchup'):
                logging.error('Switchover pooler_stopped: catchup timeout exceeded, aborting')
                return [TransitionTo(SwitchoverPhase.FAILED)]
            logging.info('Switchover pooler_stopped: candidate %s not yet in sync, waiting', candidate)
            return []

        logging.warning('Candidate %s is in sync, stopping PostgreSQL', candidate)

        return [
            StopPostgresql(wait=False),  # Non-blocking first stop.
            WriteLocalState('switchover_primary', SwitchoverPhase.PG_STOPPED),
        ]

    def plan_pg_stopped(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """pg_stopped → primary_shut: drain WAL, release lock, final PG stop."""
        plan: CommandPlan = []

        if self._debug_failure('primary_switchover_before_release'):
            plan.append(TransitionTo(SwitchoverPhase.FAILED))
            return plan

        plan.append(TransitionTo(SwitchoverPhase.PRIMARY_SHUT))  # Idempotency fence.
        plan.append(ClearLocalState('switchover_primary'))

        plan.append(ReleaseLock(wait=5))
        plan.append(StopPostgresql(wait=True))  # Final blocking stop.

        if self._debug_failure('primary_switchover_after_release'):
            return plan

        plan.append(SetSimplePrimarySwitchTry(cast(str, obs.record.selected_candidate)))  # Signal return-to-cluster.
        return plan

    def plan_primary_shut(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """primary_shut: idempotent recovery for restarts mid-shutdown.

        Releases re-acquired lock or rewinds to new primary.

        MDB-41951 race fix: only rewind when candidate promoted (phase == PROMOTED).
        Rewinding to a non-primary candidate (CANDIDATE_ACQUIRED/PRIMARY_SHUT)
        causes a stuck cluster when promote fails.
        """
        # Unexpected restart: release re-acquired lock.
        if obs.lock_holder == obs.my_hostname:
            logging.warning('Switchover primary_shut: unexpectedly holding the lock — releasing')
            return [
                StopPooler(),
                ReleaseLock(wait=5),
            ]

        # Only rewind after successful promote (MDB-41951).
        new_primary = obs.lock_holder
        if new_primary is not None and obs.record.phase == SwitchoverPhase.PROMOTED:
            logging.warning('SWITCHOVER: new primary found, returning to cluster')
            return [
                Log(
                    message='SWITCHOVER: new primary found, returning to cluster',
                    level='warning',
                    event=True,
                ),
                DeleteHostOp(),
                SetSimplePrimarySwitchTry(new_primary),
                ReturnToCluster(
                    new_primary=new_primary,
                    role='primary',
                    is_postgresql_dead=True,
                ),
            ]

        logging.info('Switchover primary_shut: waiting for candidate to promote (phase=%s)', obs.record.phase)
        return []

    def plan_failed(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Start fallback recovery when no primary remains; otherwise clean up."""
        if obs.lock_holder is None:
            if obs.record.hostname == obs.my_hostname:
                return [AcquireLock(allow_queue=False, timeout=0)]
            if obs.primary_alive:
                logging.info('Waiting for switchover primary %s to roll back', obs.record.hostname)
                return []
            return self._plan_fallback()
        if obs.lock_holder == obs.record.selected_candidate:
            logging.warning('SWITCHOVER: waiting for failed candidate %s to resolve primary lock', obs.lock_holder)
            return []
        if obs.lock_holder == obs.record.hostname:
            if obs.my_hostname != obs.record.hostname:
                return []
            if obs.role is None:
                return [StartPostgresql()]
        return self._plan_failed_cleanup(obs)

    def plan_fallback(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Retry fallback initialization or clean up after a primary appears."""
        if obs.lock_holder is None:
            return [InitializeFailover()]
        return self._plan_failed_cleanup(obs)

    @staticmethod
    def _plan_fallback() -> CommandPlan:
        return [
            InitializeFailover(),
            TransitionTo(SwitchoverPhase.FALLBACK),
        ]

    @staticmethod
    def _plan_failed_cleanup(obs: 'SwitchoverObservation') -> CommandPlan:
        plan: CommandPlan = []
        if obs.downtime_started_ts is not None:
            plan.append(StopTimer('downtime'))
        if obs.switchover_started_ts is not None:
            plan.append(StopTimer('switchover', track_as='switchover_failure'))
        plan.append(CleanupSwitchover())
        return plan
