# encoding: utf-8
"""Primary-side switchover state machine (ADR-0005 §3, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. Phase persisted to ZK via TransitionTo before the action,
so restarts resume from the same phase.
"""

import logging
import time
from typing import TYPE_CHECKING, Callable

from ..commands import (
    Checkpoint,
    DeleteHostOp,
    Log,
    Plan as CommandPlan,
    ReleaseLock,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    StartTimer,
    StopPooler,
    StopPostgresql,
    StoreReplicsInfo,
    TransitionTo,
    WriteCandidate,
    WriteSideReplicas,
)
from ..helpers import app_name_from_fqdn
from ..types import ReplicaInfos
from ..types import check_last_failover_time, is_timed_out
from .types import (
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
)

if TYPE_CHECKING:
    from ..zk import Zookeeper


class PrimarySwitchoverMachine:
    """Primary-side switchover state machine (ADR-0005 §3, ADR-0006)."""

    # Phases where primary waits for candidate to promote — timeout gate
    # short-circuits to FAILED after promote_timeout (ADR-0007 §2 analog).
    _PROMOTE_WAIT_PHASES = frozenset({
        SwitchoverPhase.PRIMARY_SHUT,
        SwitchoverPhase.CANDIDATE_ACQUIRED,
    })

    def __init__(
        self,
        zk: 'Zookeeper',
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._zk = zk
        self._cfg = config or SwitchoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Return Command Plan for current observation (pure, no I/O).

        Empty Plan = nothing to do, retry next iteration (ADR-0006 §2).
        """
        # Timeout gate: if candidate didn't promote in time → FAILED.
        if obs.record.phase in self._PROMOTE_WAIT_PHASES and is_timed_out(
            obs.downtime_started_ts, self._cfg.promote_timeout, 'Candidate promote'
        ):
            return [TransitionTo(SwitchoverPhase.FAILED)]

        match obs.record.phase:
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
            if not self._cfg.allow_potential_data_loss:
                logging.warning(
                    'Replica %s cannot be primary for switchover, max allowed lag %sms',
                    candidate, self._cfg.max_allowed_lag_ms,
                )
                return False
            logging.warning('Replica %s has replay lag %s and allow data loss', candidate, replay_lag)
        return True

    # --- Pure gate predicates for plan_scheduled (ADR-0006 §2) ---

    def _hostname_matches(self, obs: 'SwitchoverObservation') -> bool:
        if obs.record.hostname != obs.my_hostname:
            logging.warning(
                'Switchover scheduled: hostname %s differs from current %s, ignoring',
                obs.record.hostname, obs.my_hostname,
            )
            return False
        return True

    def _role_is_primary(self, obs: 'SwitchoverObservation') -> bool:
        if obs.role != 'primary':
            logging.error(
                'Switchover scheduled: current role is %s, ignoring switchover',
                obs.role,
            )
            return False
        return True

    def _timeline_matches(self, obs: 'SwitchoverObservation') -> bool:
        if obs.zk_timeline != obs.record.timeline:
            logging.warning(
                'Switchover scheduled: ZK timeline %s differs from switchover timeline %s, ignoring',
                obs.zk_timeline, obs.record.timeline,
            )
            return False
        return True

    def _failover_state_ok(self, obs: 'SwitchoverObservation') -> bool:
        if obs.failover_state not in ('finished', None):
            logging.error(
                'Switchover scheduled: current failover state is %s, ignoring switchover',
                obs.failover_state,
            )
            return False
        return True

    def _ha_replicas_ok(self, obs: 'SwitchoverObservation') -> bool:
        if obs.ha_replics is None:
            logging.warning('Switchover scheduled: HA replicas are empty, ignoring switchover')
            return False
        return True

    def _last_transition_ok(self, obs: 'SwitchoverObservation') -> bool:
        """Last role transition old enough, or enough replicas alive."""
        # ha_replics is None is already checked by _ha_replicas_ok (called first).
        assert obs.ha_replics is not None
        last_role_transition_ts: float = 0.0
        if obs.last_failover_ts is not None or obs.last_switchover_ts is not None:
            last_role_transition_ts = max(
                x for x in (obs.last_switchover_ts, obs.last_failover_ts) if x is not None
            )
        alive_replics_number = len([i for i in obs.replics_info if i.get('state') == 'streaming'])
        if not check_last_failover_time(last_role_transition_ts, self._cfg.min_failover_timeout) and (
            alive_replics_number < len(obs.ha_replics)
        ):
            logging.warning(
                'Switchover scheduled: last role transition was %.1f seconds ago,'
                ' and alive host count less than HA hosts (HA: %d, alive: %d) ignoring switchover.',
                time.time() - last_role_transition_ts,
                len(obs.ha_replics),
                alive_replics_number,
            )
            return False
        return True

    def plan_scheduled(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """scheduled → sync_set: sanity-check, choose candidate, set sync replication.

        Empty Plan when a gate fails (retry next iteration).
        """
        # --- Sanity gates (each gate is a pure predicate) ---
        if not self._hostname_matches(obs):
            return []
        if not self._role_is_primary(obs):
            return []
        if not self._timeline_matches(obs):
            return []
        if not self._failover_state_ok(obs):
            return []
        if not self._ha_replicas_ok(obs):
            return []
        if not self._last_transition_ok(obs):
            return []

        # --- Choose candidate ---

        candidate = obs.switchover_candidate
        if candidate is None:
            logging.info('Switchover scheduled: no eligible candidate, waiting')
            return []

        # --- Check candidate is in sync ---

        if not self._candidate_is_sync(obs.replics_info, candidate):
            logging.info('Switchover scheduled: candidate %s not yet in sync, waiting', candidate)
            return []

        # --- Action: set sync replication, transition to sync_set ---

        logging.info('Scheduled switchover checks passed OK.')

        plan: CommandPlan = []
        if not obs.switchover_timer_started:  # Idempotent.
            plan.append(StartTimer('switchover'))

        # Persist candidate so plan_sync_set reads it next iteration
        # (anywhere-switchover without destination needs this).
        plan.append(WriteCandidate(candidate=candidate))

        logging.warning('Starting sync replication %s', candidate)
        plan.append(SetSyncReplication(host=candidate))

        plan.append(TransitionTo(SwitchoverPhase.SYNC_SET))  # ADR-0005 §3 fence.
        return plan

    def plan_sync_set(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """sync_set → initiated: fix candidate + side replicas, write initiated.

        Emits TransitionTo(FAILED) if candidate is None.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover sync_set: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        side_replicas = tuple(r for r in obs.streaming_replicas if r != candidate)

        logging.info('Switchover sync_set: candidate=%s side_replicas=%s', candidate, side_replicas)

        return [
            WriteCandidate(candidate=candidate),
            WriteSideReplicas(side_replicas=side_replicas),
            TransitionTo(SwitchoverPhase.INITIATED),
        ]

    def plan_initiated(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """initiated: wait (non-blocking) for candidate to set candidate_found.

        Emits pre-shutdown prep when detected; aborts if candidate is dead.
        No phase transition — candidate writes candidate_found, primary detects it.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover initiated: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        if obs.live_switchover_state == SwitchoverPhase.CANDIDATE_FOUND:
            # Inline pooler stop to avoid wasting an iteration (pgconsul_util.feature:402).
            # Prep commands (StoreReplicsInfo, Checkpoint) must precede StopPooler.
            # Uses _plan_pooler_shutdown (shared with plan_candidate_found) to avoid
            # coupling — candidate is already checked non-None above.
            plan: CommandPlan = [
                Log(
                    message='SWITCHOVER: candidate_found detected, proceeding to shutdown',
                    level='warning',
                    event=True,
                ),
                StoreReplicsInfo(
                    replics_info=obs.replics_info,
                    timeline_match=obs.timeline_match,
                ),
                Checkpoint(),
            ]
            plan.extend(self._plan_pooler_shutdown(obs))
            return plan

        if obs.candidate_alive is not True:
            logging.warning(
                'Switchover initiated: candidate %s is no longer alive, aborting switchover',
                candidate,
            )
            return [TransitionTo(SwitchoverPhase.FAILED)]

        logging.debug(
            'Switchover initiated: waiting for candidate_found (current=%s)',
            obs.live_switchover_state,
        )
        return []

    def _plan_pooler_shutdown(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Shared shutdown sequence: start downtime timer, stop pooler, fence.

        Extracted from plan_candidate_found to avoid inline coupling from
        plan_initiated (ADR-0006 §4). Caller must ensure candidate is non-None.
        """
        plan: CommandPlan = []

        if not obs.downtime_timer_started:  # Idempotent.
            plan.append(StartTimer('downtime'))

        plan.append(StopPooler())
        plan.append(Log(
            message='Cluster closed from user requests (pooler stopped)',
            level='warning',
        ))

        if self._debug_failure('primary_switchover_before_catchup'):  # ADR-0006 §6.
            plan.append(TransitionTo(SwitchoverPhase.FAILED))
            return plan

        plan.append(TransitionTo(SwitchoverPhase.POOLER_STOPPED))  # Idempotency fence.
        return plan

    def plan_candidate_found(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """candidate_found → pooler_stopped: stop pooler, start downtime timer.

        Splits old monolithic handler for granular kill-9 recovery (ADR-0006 §4).
        Sync check moves to plan_pooler_stopped.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover candidate_found: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        return self._plan_pooler_shutdown(obs)

    def plan_pooler_stopped(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """pooler_stopped → pg_stopped: non-blocking sync check, stop PG.

        Empty Plan if not in sync (retry next iteration).
        Catchup timeout gate: if candidate didn't catch up in catchup_timeout → FAILED.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover pooler_stopped: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        if not self._candidate_is_sync(obs.replics_info, candidate):
            if is_timed_out(obs.downtime_started_ts, self._cfg.catchup_timeout, 'Switchover catchup'):
                logging.error('Switchover pooler_stopped: catchup timeout exceeded, aborting')
                return [TransitionTo(SwitchoverPhase.FAILED)]
            logging.info('Switchover pooler_stopped: candidate %s not yet in sync, waiting', candidate)
            return []

        logging.warning('Candidate %s is in sync, stopping PostgreSQL', candidate)

        return [
            StopPostgresql(wait=False, force_async=False),  # Non-blocking first stop.
            TransitionTo(SwitchoverPhase.PG_STOPPED),
        ]

    def plan_pg_stopped(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """pg_stopped → primary_shut: drain WAL, release lock, final PG stop."""
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover pg_stopped: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        plan: CommandPlan = []

        if self._debug_failure('primary_switchover_before_release'):
            plan.append(TransitionTo(SwitchoverPhase.FAILED))
            return plan

        plan.append(TransitionTo(SwitchoverPhase.PRIMARY_SHUT))  # Idempotency fence.

        plan.append(ReleaseLock(wait=5))
        plan.append(StopPostgresql(wait=True, force_async=False))  # Final blocking stop.

        if self._debug_failure('primary_switchover_after_release'):
            return plan

        plan.append(SetSimplePrimarySwitchTry())  # Signal return-to-cluster.
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
                SetSimplePrimarySwitchTry(),
                RewindFromSource(
                    new_primary=new_primary,
                    is_postgresql_dead=True,
                    limit=self._cfg.rollback_timeout,
                ),
            ]

        logging.info('Switchover primary_shut: waiting for candidate to promote (phase=%s)', obs.record.phase)
        return []
