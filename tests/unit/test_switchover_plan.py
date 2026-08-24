"""Unit tests for PrimarySwitchoverMachine plan_* methods (ADR-0006, step E5).

Tests call ``plan(observation)`` and assert on the returned Plan — no MagicMock
context. Decisions, not interactions, are verified.
"""

from src.commands import (
    Log,
    ReleaseLock,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    StartTimer,
    StopPooler,
    StopPostgresql,
    TransitionTo,
    WriteCandidate,
    WriteLocalState,
)
from src.switchover import (
    PrimarySwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)
from src.types import ReplicaInfos


def _make_record(phase, candidate='host2', destination='host2'):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination=destination,
        phase=phase,
        candidate=candidate,
        side_replicas=['host3'],
    )


def _make_obs(
    phase,
    *,
    candidate='host2',
    downtime_timer_started=False,
    downtime_started_ts=None,
    replics_info=None,
    lock_holder=None,
    my_hostname='host1',
    switchover_candidate=None,
    local_phase=None,
):
    """Build a minimal SwitchoverObservation for plan_* tests."""
    if replics_info is None:
        # Default: candidate is in sync (replay_lag=0).
        replics_info = [{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 0}]
    return SwitchoverObservation(
        record=_make_record(phase, candidate=candidate),
        my_hostname=my_hostname,
        role='primary',
        zk_timeline=5,
        failover_state=None,
        last_failover_ts=None,
        last_switchover_ts=None,
        ha_replics=frozenset({'host2', 'host3'}),
        replics_info=replics_info,
        streaming_replicas=('host2', 'host3'),
        live_switchover_state=None,
        candidate_alive=True,
        lock_holder=lock_holder,
        switchover_timer_started=False,
        downtime_timer_started=downtime_timer_started,
        downtime_started_ts=downtime_started_ts,
        candidate=candidate,
        side_replicas=('host3',),
        all_side_replicas_turned=False,
        switchover_primary_info=None,
        switchover_candidate=switchover_candidate,
        local_phase=local_phase,
    )


def _make_machine(debug_failure=None):
    """Create a stub-only machine (no context needed for plan_*)."""
    cfg = SwitchoverMachineConfig()
    return PrimarySwitchoverMachine(None, config=cfg, debug_failure=debug_failure)


class TestLocalPhaseDispatch:
    def test_sync_set_is_resumed_from_local_state(self):
        obs = _make_obs(SwitchoverPhase.SCHEDULED, local_phase=SwitchoverPhase.SYNC_SET)

        plan = _make_machine().plan(obs)

        assert TransitionTo(SwitchoverPhase.INITIATED) in plan
        assert SetSyncReplication(host='host2') not in plan

    def test_pooler_stopped_is_resumed_from_local_state(self):
        obs = _make_obs(
            SwitchoverPhase.CANDIDATE_FOUND,
            local_phase=SwitchoverPhase.POOLER_STOPPED,
        )

        plan = _make_machine().plan(obs)

        assert StopPostgresql(wait=False, force_async=False) in plan
        assert StopPooler() not in plan


# ---------------------------------------------------------------------------
# plan_candidate_found: candidate_found → pooler_stopped
# ---------------------------------------------------------------------------


class TestPlanCandidateFound:
    """candidate_found → pooler_stopped: stop pooler, start timer, transition."""

    def test_stops_pooler_and_transitions_to_pooler_stopped(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        # StartTimer + StopPooler + Log + local POOLER_STOPPED.
        assert StartTimer('downtime') in plan
        assert StopPooler() in plan
        local_transition = WriteLocalState('switchover_primary', SwitchoverPhase.POOLER_STOPPED)
        assert local_transition in plan
        assert plan[-1] == local_transition

    def test_skips_timer_if_already_started(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, downtime_timer_started=True)
        plan = m.plan_candidate_found(obs)
        assert StartTimer('downtime') not in plan
        assert StopPooler() in plan

    def test_aborts_when_candidate_is_none(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, candidate=None)
        plan = m.plan_candidate_found(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_debug_failure_before_catchup_aborts(self):
        debug = lambda name: name == 'primary_switchover_before_catchup'
        m = _make_machine(debug_failure=debug)
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) in plan
        # Should not transition to POOLER_STOPPED
        assert TransitionTo(SwitchoverPhase.POOLER_STOPPED) not in plan

    def test_emits_pooler_stopped_log(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        log_cmds = [c for c in plan if isinstance(c, Log)]
        assert len(log_cmds) == 1
        assert 'pooler stopped' in log_cmds[0].message.lower()


# ---------------------------------------------------------------------------
# plan_pooler_stopped: pooler_stopped → pg_stopped
# ---------------------------------------------------------------------------


class TestPlanPoolerStopped:
    """pooler_stopped → pg_stopped: sync check, stop PG, transition."""

    def test_stops_pg_and_transitions_when_in_sync(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED)
        plan = m.plan_pooler_stopped(obs)
        assert StopPostgresql(wait=False, force_async=False) in plan
        assert plan[-1] == WriteLocalState('switchover_primary', SwitchoverPhase.PG_STOPPED)

    def test_waits_when_candidate_not_in_sync(self):
        m = _make_machine()
        # replay_lag too high → not in sync
        replics_info = [{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 99999}]
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED, replics_info=replics_info)
        plan = m.plan_pooler_stopped(obs)
        assert plan == []

    def test_waits_when_replica_info_missing(self):
        m = _make_machine()
        # No replica info for candidate
        replics_info = [{'application_name': 'other', 'state': 'streaming', 'replay_lag_msec': 0}]
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED, replics_info=replics_info)
        plan = m.plan_pooler_stopped(obs)
        assert plan == []

    def test_aborts_when_candidate_is_none(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED, candidate=None)
        plan = m.plan_pooler_stopped(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_allows_data_loss_when_configured(self):
        cfg = SwitchoverMachineConfig(allow_potential_data_loss=True)
        m = PrimarySwitchoverMachine(None, config=cfg)
        # High lag but data loss allowed → proceeds
        replics_info = [{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 99999}]
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED, replics_info=replics_info)
        plan = m.plan_pooler_stopped(obs)
        assert StopPostgresql(wait=False, force_async=False) in plan


# ---------------------------------------------------------------------------
# plan_pooler_stopped: LSN-based sync check + catchup timeout gate (MDB-41951)
# ---------------------------------------------------------------------------


class TestPlanPoolerStoppedLsnCatchup:
    """Regression tests for pooler_stopped LSN-based sync check (MDB-41951).

    Bug: _candidate_is_sync checked only time-based replay_lag_msec, ignoring
    replay_location_diff=0 (LSN caught up). When pooler is stopped (no new WAL),
    replay_lag_msec freezes and never drops below max_allowed_lag_ms → primary
    stuck in pooler_stopped forever → --block timeout (pgconsul_util.feature:402).
    """

    def test_proceeds_when_lsn_caught_up_despite_frozen_replay_lag(self):
        """replay_location_diff=0 + write_location_diff=0 → in sync even if
        replay_lag_msec=121 > max_allowed_lag_ms=10 (frozen lag after pooler stop).
        """
        m = _make_machine()
        replics_info = [{
            'application_name': 'host2',
            'state': 'streaming',
            'replay_lag_msec': 121,
            'replay_location_diff': 0,
            'write_location_diff': 0,
        }]
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED, replics_info=replics_info)
        plan = m.plan_pooler_stopped(obs)
        assert StopPostgresql(wait=False, force_async=False) in plan
        assert plan[-1] == WriteLocalState('switchover_primary', SwitchoverPhase.PG_STOPPED)

    def test_fails_when_catchup_timeout_exceeded(self):
        """downtime_started_ts in the past + catchup_timeout exceeded → FAILED."""
        import time
        cfg = SwitchoverMachineConfig(catchup_timeout=1.0)
        m = PrimarySwitchoverMachine(None, config=cfg)
        old_ts = time.time() - 10.0  # 10s ago, well past 1s timeout
        replics_info = [{
            'application_name': 'host2',
            'state': 'streaming',
            'replay_lag_msec': 99999,
            'replay_location_diff': 999,
            'write_location_diff': 999,
        }]
        obs = _make_obs(
            SwitchoverPhase.POOLER_STOPPED,
            replics_info=replics_info,
            downtime_started_ts=old_ts,
        )
        plan = m.plan_pooler_stopped(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_waits_when_catchup_timeout_not_exceeded(self):
        """downtime_started_ts recent + not in sync → empty plan (still waiting)."""
        import time
        cfg = SwitchoverMachineConfig(catchup_timeout=300.0)
        m = PrimarySwitchoverMachine(None, config=cfg)
        recent_ts = time.time() - 1.0  # 1s ago, well within 300s timeout
        replics_info = [{
            'application_name': 'host2',
            'state': 'streaming',
            'replay_lag_msec': 99999,
            'replay_location_diff': 999,
            'write_location_diff': 999,
        }]
        obs = _make_obs(
            SwitchoverPhase.POOLER_STOPPED,
            replics_info=replics_info,
            downtime_started_ts=recent_ts,
        )
        plan = m.plan_pooler_stopped(obs)
        assert plan == []

    def test_waits_when_downtime_timer_not_started(self):
        """No downtime timer → no timeout gate, still waiting (empty plan)."""
        m = _make_machine()
        replics_info = [{
            'application_name': 'host2',
            'state': 'streaming',
            'replay_lag_msec': 99999,
            'replay_location_diff': 999,
            'write_location_diff': 999,
        }]
        obs = _make_obs(
            SwitchoverPhase.POOLER_STOPPED,
            replics_info=replics_info,
            downtime_started_ts=None,
        )
        plan = m.plan_pooler_stopped(obs)
        assert plan == []


# ---------------------------------------------------------------------------
# plan_pg_stopped: pg_stopped → primary_shut
# ---------------------------------------------------------------------------


class TestPlanPgStopped:
    """pg_stopped → primary_shut: drain WAL, release lock, final PG stop."""

    def test_releases_lock_and_transitions_to_primary_shut(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        assert TransitionTo(SwitchoverPhase.PRIMARY_SHUT) in plan
        assert ReleaseLock(wait=5) in plan
        assert SetSimplePrimarySwitchTry() in plan

    def test_aborts_when_candidate_is_none(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED, candidate=None)
        plan = m.plan_pg_stopped(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_debug_failure_before_release_aborts(self):
        debug = lambda name: name == 'primary_switchover_before_release'
        m = _make_machine(debug_failure=debug)
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) in plan
        # Lock should not be released
        assert ReleaseLock(wait=5) not in plan

    def test_debug_failure_after_release_stops_before_signal(self):
        debug = lambda name: name == 'primary_switchover_after_release'
        m = _make_machine(debug_failure=debug)
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        # Lock released but return-to-cluster signal not sent
        assert ReleaseLock(wait=5) in plan
        assert SetSimplePrimarySwitchTry() not in plan

    def test_final_pg_stop_is_blocking(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        stop_cmds = [c for c in plan if isinstance(c, StopPostgresql)]
        assert len(stop_cmds) == 1
        assert stop_cmds[0].wait is True

    def test_fence_order_transition_before_release(self):
        """ADR-0006 §5: TransitionTo(PRIMARY_SHUT) precedes ReleaseLock in Plan."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        transition_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PRIMARY_SHUT))
        release_idx = next(i for i, c in enumerate(plan) if isinstance(c, ReleaseLock))
        assert transition_idx < release_idx


# ---------------------------------------------------------------------------
# plan() dispatch: new phases route to correct planner
# ---------------------------------------------------------------------------


class TestPlanDispatch:
    """plan() dispatches new sub-phases to the correct planner."""

    def test_plan_dispatches_pooler_stopped(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED)
        plan = m.plan(obs)
        # Should produce a non-empty plan (sync check passes with default obs)
        assert plan  # non-empty
        assert StopPostgresql(wait=False, force_async=False) in plan

    def test_plan_dispatches_pg_stopped(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan(obs)
        assert ReleaseLock(wait=5) in plan

    def test_plan_returns_empty_for_unknown_phase(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED)
        plan = m.plan(obs)
        assert plan == []


# ---------------------------------------------------------------------------
# Fence invariant: TransitionTo(X) precedes actions of X in Plan (ADR-0006 §5)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# plan_scheduled: anywhere-switchover regression (no destination, no pre-set candidate)
# ---------------------------------------------------------------------------


class TestPlanScheduled:
    """plan_scheduled: sanity-check gates and candidate-write regression."""

    def _make_scheduled_obs(self, switchover_candidate='host2', ha_replics=None, replics_info=None):
        """Build an observation for the SCHEDULED phase with no pre-set candidate/destination."""
        if replics_info is None:
            replics_info = [{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 0}]
        record = SwitchoverRecord(
            hostname='host1',
            timeline=5,
            destination=None,   # anywhere-switchover: no explicit destination
            phase=SwitchoverPhase.SCHEDULED,
            candidate=None,     # not yet written to ZK
            side_replicas=[],
        )
        return SwitchoverObservation(
            record=record,
            my_hostname='host1',
            role='primary',
            zk_timeline=5,
            failover_state=None,
            last_failover_ts=None,
            last_switchover_ts=None,
            ha_replics=frozenset(ha_replics or {'host2', 'host3'}),
            replics_info=replics_info,
            streaming_replicas=('host2', 'host3'),
            live_switchover_state=None,
            candidate_alive=True,
            lock_holder='host1',
            switchover_timer_started=False,
            downtime_timer_started=False,
            downtime_started_ts=None,
            candidate=None,
            side_replicas=(),
            all_side_replicas_turned=False,
            switchover_primary_info=None,
            switchover_candidate=switchover_candidate,
        )

    def test_anywhere_switchover_writes_candidate_before_sync_set(self):
        """Regression: plan_scheduled must emit WriteCandidate before local SYNC_SET.

        Without this, plan_sync_set reads obs.candidate=None and immediately
        emits TransitionTo(FAILED), breaking anywhere-switchover (no destination).
        """
        m = _make_machine()
        obs = self._make_scheduled_obs(switchover_candidate='host2')
        plan = m.plan_scheduled(obs)
        assert WriteCandidate(candidate='host2') in plan
        local_transition = WriteLocalState('switchover_primary', SwitchoverPhase.SYNC_SET)
        assert local_transition in plan
        write_idx = next(i for i, c in enumerate(plan) if isinstance(c, WriteCandidate))
        transition_idx = next(i for i, c in enumerate(plan) if c == local_transition)
        assert write_idx < transition_idx

    def test_anywhere_switchover_emits_set_sync_replication(self):
        """plan_scheduled sets sync replication on the chosen candidate."""
        m = _make_machine()
        obs = self._make_scheduled_obs(switchover_candidate='host2')
        plan = m.plan_scheduled(obs)
        assert SetSyncReplication(host='host2') in plan

    def test_returns_empty_when_no_candidate(self):
        """When switchover_candidate is None, plan is empty (wait next iteration)."""
        m = _make_machine()
        obs = self._make_scheduled_obs(switchover_candidate=None)
        plan = m.plan_scheduled(obs)
        assert plan == []

    def test_returns_empty_when_candidate_not_in_sync(self):
        """When candidate lag exceeds threshold, plan is empty (wait)."""
        m = _make_machine()
        replics_info = [{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 99999}]
        obs = self._make_scheduled_obs(switchover_candidate='host2', replics_info=replics_info)
        plan = m.plan_scheduled(obs)
        assert plan == []

    def test_returns_empty_when_hostname_mismatch(self):
        """Switchover is not for this host — plan is empty."""
        m = _make_machine()
        obs = self._make_scheduled_obs(switchover_candidate='host2')
        # Override: the record's hostname differs from my_hostname.
        obs2 = SwitchoverObservation(
            record=SwitchoverRecord(
                hostname='other-host', timeline=5, destination=None,
                phase=SwitchoverPhase.SCHEDULED, candidate=None, side_replicas=[],
            ),
            my_hostname='host1',
            role='primary',
            zk_timeline=5,
            failover_state=None,
            last_failover_ts=None,
            last_switchover_ts=None,
            ha_replics=frozenset({'host2', 'host3'}),
            replics_info=[{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 0}],
            streaming_replicas=('host2', 'host3'),
            live_switchover_state=None,
            candidate_alive=True,
            lock_holder='host1',
            switchover_timer_started=False,
            downtime_timer_started=False,
            downtime_started_ts=None,
            candidate=None,
            side_replicas=(),
            all_side_replicas_turned=False,
            switchover_primary_info=None,
            switchover_candidate='host2',
        )
        plan = m.plan_scheduled(obs2)
        assert plan == []


class TestFenceInvariant:
    """ADR-0006 §5: TransitionTo(X) must precede the commands that perform X's action."""

    def test_scheduled_sync_replication_before_transition(self):
        """plan_scheduled: SetSyncReplication before local SYNC_SET."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.SCHEDULED, switchover_candidate='host2')
        plan = m.plan_scheduled(obs)
        sync_idx = next(i for i, c in enumerate(plan) if isinstance(c, SetSyncReplication))
        transition_idx = next(i for i, c in enumerate(plan) if c == WriteLocalState('switchover_primary', SwitchoverPhase.SYNC_SET))
        assert sync_idx < transition_idx

    def test_scheduled_write_candidate_before_transition(self):
        """plan_scheduled: WriteCandidate before local SYNC_SET."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.SCHEDULED, switchover_candidate='host2')
        plan = m.plan_scheduled(obs)
        write_idx = next(i for i, c in enumerate(plan) if isinstance(c, WriteCandidate))
        transition_idx = next(i for i, c in enumerate(plan) if c == WriteLocalState('switchover_primary', SwitchoverPhase.SYNC_SET))
        assert write_idx < transition_idx

    def test_sync_set_writes_before_transition(self):
        """plan_sync_set: WriteCandidate + WriteSideReplicas before TransitionTo(INITIATED)."""
        from src.commands import WriteCandidate, WriteSideReplicas
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.SYNC_SET)
        plan = m.plan_sync_set(obs)
        write_cand_idx = next(i for i, c in enumerate(plan) if isinstance(c, WriteCandidate))
        write_side_idx = next(i for i, c in enumerate(plan) if isinstance(c, WriteSideReplicas))
        transition_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.INITIATED))
        assert write_cand_idx < transition_idx
        assert write_side_idx < transition_idx

    def test_candidate_found_pooler_stop_before_local_transition(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        pooler_idx = next(i for i, c in enumerate(plan) if isinstance(c, StopPooler))
        transition_idx = next(i for i, c in enumerate(plan) if c == WriteLocalState('switchover_primary', SwitchoverPhase.POOLER_STOPPED))
        assert pooler_idx < transition_idx

    def test_pooler_stopped_pg_stop_before_local_transition(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED)
        plan = m.plan_pooler_stopped(obs)
        stop_pg_idx = next(i for i, c in enumerate(plan) if isinstance(c, StopPostgresql))
        transition_idx = next(i for i, c in enumerate(plan) if c == WriteLocalState('switchover_primary', SwitchoverPhase.PG_STOPPED))
        assert stop_pg_idx < transition_idx

    def test_pg_stopped_transition_before_release(self):
        """plan_pg_stopped: TransitionTo(PRIMARY_SHUT) before ReleaseLock."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        transition_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PRIMARY_SHUT))
        release_idx = next(i for i, c in enumerate(plan) if isinstance(c, ReleaseLock))
        assert transition_idx < release_idx

    def test_pg_stopped_transition_before_final_pg_stop(self):
        """plan_pg_stopped: TransitionTo(PRIMARY_SHUT) before final StopPostgresql."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan_pg_stopped(obs)
        transition_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PRIMARY_SHUT))
        stop_cmds = [i for i, c in enumerate(plan) if isinstance(c, StopPostgresql)]
        assert all(transition_idx < idx for idx in stop_cmds)


# ---------------------------------------------------------------------------
# plan_primary_shut: recovery handler for kill-9 restarts (ADR-0006 §4)
# ---------------------------------------------------------------------------


class TestPlanPrimaryShut:
    """primary_shut: idempotent recovery — release re-acquired lock or rewind to new primary."""

    def test_releases_lock_when_self_is_holder(self):
        """Kill-9 restart re-acquired the lock — release it."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder='host1', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        assert StopPooler() in plan
        assert ReleaseLock(wait=5) in plan

    def test_rewinds_to_new_primary_when_other_holds_lock(self):
        """New primary took over — rewind to it (only when phase=PROMOTED)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED, lock_holder='host2', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        from src.commands import DeleteHostOp, RewindFromSource
        assert DeleteHostOp() in plan
        assert SetSimplePrimarySwitchTry() in plan
        rewind_cmds = [c for c in plan if isinstance(c, RewindFromSource)]
        assert len(rewind_cmds) == 1
        assert rewind_cmds[0].new_primary == 'host2'
        assert rewind_cmds[0].is_postgresql_dead is True

    def test_waits_when_no_lock_holder(self):
        """No new primary yet — empty plan (retry next iteration)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder=None, my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        assert plan == []

    # ---------------------------------------------------------------------------
    # Bug #3 (MDB-41951): plan_primary_shut must NOT rewind when the candidate
    # holds the lock but has not promoted yet (phase != PROMOTED).
    # Reproduces anywhere_switchover.feature:131 — @switchover_failed_promote.
    #
    # Race: old primary reaches primary_shut, candidate acquires lock but
    # promote fails (sleep 3 && false). Old primary sees lock_holder != None
    # and immediately rewinds to the candidate — which is NOT a primary.
    # The cluster is then stuck with no primary.
    # Fix: add CANDIDATE_ACQUIRED phase; old primary waits for PROMOTED.
    # ---------------------------------------------------------------------------

    def test_does_not_rewind_when_phase_is_primary_shut(self):
        """lock_holder set but phase=primary_shut → wait, not rewind.

        The candidate acquired the lock but hasn't promoted yet.
        Rewinding now is a race: if promote fails, the old primary
        becomes a replica of a non-primary.
        """
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder='host2', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        from src.commands import RewindFromSource
        assert not any(isinstance(c, RewindFromSource) for c in plan), \
            'Must not rewind to candidate during primary_shut — candidate has not promoted yet'

    def test_rewinds_only_when_phase_is_promoted(self):
        """lock_holder set AND phase=promoted → rewind (promote succeeded)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED, lock_holder='host2', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        from src.commands import RewindFromSource
        rewind_cmds = [c for c in plan if isinstance(c, RewindFromSource)]
        assert len(rewind_cmds) == 1
        assert rewind_cmds[0].new_primary == 'host2'

    def test_waits_when_phase_is_candidate_acquired(self):
        """lock_holder set but phase=candidate_acquired → wait, not rewind."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_ACQUIRED, lock_holder='host2', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        from src.commands import RewindFromSource
        assert not any(isinstance(c, RewindFromSource) for c in plan), \
            'Must not rewind during candidate_acquired — promote has not completed'

    def test_rewind_uses_config_rollback_timeout(self):
        """RewindFromSource limit comes from SwitchoverMachineConfig.rollback_timeout."""
        cfg = SwitchoverMachineConfig(rollback_timeout=42.0)
        m = PrimarySwitchoverMachine(None, config=cfg)
        obs = _make_obs(SwitchoverPhase.PROMOTED, lock_holder='host2', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        from src.commands import RewindFromSource
        rewind_cmds = [c for c in plan if isinstance(c, RewindFromSource)]
        assert rewind_cmds[0].limit == 42.0

    def test_emits_log_event_when_new_primary_found(self):
        """Structured log event emitted when new primary is detected."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED, lock_holder='host2', my_hostname='host1')
        plan = m.plan_primary_shut(obs)
        log_cmds = [c for c in plan if isinstance(c, Log)]
        assert len(log_cmds) == 1
        assert log_cmds[0].event is True
        assert 'new primary found' in log_cmds[0].message.lower()

    def test_plan_dispatches_primary_shut(self):
        """plan() dispatches PROMOTED to plan_primary_shut (rewind path)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED, lock_holder='host2', my_hostname='host1')
        plan = m.plan(obs)
        # Should produce a non-empty plan (rewind to new primary)
        assert plan
        from src.commands import RewindFromSource
        assert any(isinstance(c, RewindFromSource) for c in plan)


# ---------------------------------------------------------------------------
# Timeout gate: plan() short-circuits to FAILED when candidate doesn't promote
# in time (ADR-0007 §2 analog). Active in PRIMARY_SHUT / CANDIDATE_ACQUIRED.
# ---------------------------------------------------------------------------


class TestPromoteTimeoutGate:
    """plan() returns TransitionTo(FAILED) when promote_timeout exceeded."""

    def test_fails_when_promote_timeout_exceeded_in_primary_shut(self):
        """downtime_started_ts in the past + phase=PRIMARY_SHUT → FAILED."""
        import time
        cfg = SwitchoverMachineConfig(promote_timeout=1.0)
        m = PrimarySwitchoverMachine(None, config=cfg)
        old_ts = time.time() - 10.0  # 10s ago, well past 1s timeout
        obs = _make_obs(
            SwitchoverPhase.PRIMARY_SHUT,
            lock_holder=None,
            my_hostname='host1',
            downtime_started_ts=old_ts,
        )
        plan = m.plan(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_fails_when_promote_timeout_exceeded_in_candidate_acquired(self):
        """downtime_started_ts in the past + phase=CANDIDATE_ACQUIRED → FAILED."""
        import time
        cfg = SwitchoverMachineConfig(promote_timeout=1.0)
        m = PrimarySwitchoverMachine(None, config=cfg)
        old_ts = time.time() - 10.0
        obs = _make_obs(
            SwitchoverPhase.CANDIDATE_ACQUIRED,
            lock_holder=None,
            my_hostname='host1',
            downtime_started_ts=old_ts,
        )
        plan = m.plan(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_does_not_fail_when_timeout_not_exceeded(self):
        """downtime_started_ts recent → normal plan, no FAILED transition."""
        import time
        cfg = SwitchoverMachineConfig(promote_timeout=300.0)
        m = PrimarySwitchoverMachine(None, config=cfg)
        recent_ts = time.time() - 1.0  # 1s ago, well within 300s timeout
        obs = _make_obs(
            SwitchoverPhase.PRIMARY_SHUT,
            lock_holder=None,
            my_hostname='host1',
            downtime_started_ts=recent_ts,
        )
        plan = m.plan(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan

    def test_does_not_fail_when_downtime_started_ts_is_none(self):
        """No downtime timer started → no timeout gate, normal plan."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.PRIMARY_SHUT,
            lock_holder=None,
            my_hostname='host1',
            downtime_started_ts=None,
        )
        plan = m.plan(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan
