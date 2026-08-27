"""Unit tests for CandidateSwitchoverMachine plan_* methods (ADR-0006, step F).

Tests call ``plan(observation)`` and assert on the returned Plan — no MagicMock
context. Decisions, not interactions, are verified.
"""

from src.commands import (
    AcquireLock,
    ClearLocalState,
    CleanupSwitchover,
    CreateSlots,
    Log,
    Promote,
    ReleaseLock,
    StartTimer,
    StopTimer,
    TransitionTo,
    WriteLastSwitchoverTime,
)
from src.switchover import (
    CandidateSwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _make_record(phase, candidate='host2', side_replicas=None):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination='host2',
        phase=phase,
        candidate=candidate,
        side_replicas=side_replicas if side_replicas is not None else ['host3'],
    )


_SENTINEL = object()


def _make_obs(
    phase,
    *,
    candidate='host2',
    side_replicas=('host3',),
    all_side_replicas_turned=True,
    switchover_primary_info=_SENTINEL,
    downtime_timer_started=False,
    downtime_started_ts=None,
    lock_holder=None,
    role='replica',
):
    """Build a minimal SwitchoverObservation for candidate plan_* tests."""
    if switchover_primary_info is _SENTINEL:
        switchover_primary_info = {'hostname': 'host1'}
    if downtime_timer_started and downtime_started_ts is None:
        import time
        downtime_started_ts = time.time()
    record = _make_record(phase, candidate=candidate, side_replicas=list(side_replicas))
    record.hostname = (switchover_primary_info or {}).get('hostname')
    return SwitchoverObservation(
        record=record,
        my_hostname='host2',
        role=role,
        zk_timeline=5,
        last_role_transition_ts=None,
        ha_replics=frozenset({'host2', 'host3'}),
        replics_info=[],
        streaming_replicas=('host2', 'host3'),
        candidate_alive=True,
        lock_holder=lock_holder,
        switchover_started_ts=None,
        downtime_started_ts=downtime_started_ts,
        all_side_replicas_turned=all_side_replicas_turned,
        current_time=0.0,
        switchover_candidate=None,
    )


def _make_machine(debug_failure=None):
    """Create a stub-only machine (no context needed for plan_*)."""
    cfg = SwitchoverMachineConfig()
    return CandidateSwitchoverMachine(config=cfg, debug_failure=debug_failure)


# ---------------------------------------------------------------------------
# plan_initiated: initiated → candidate_found
# ---------------------------------------------------------------------------


class TestPlanInitiated:
    """initiated → candidate_found: create slots, check side replicas turned."""

    def test_creates_slots_and_transitions_when_all_turned(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=True)
        plan = m.plan_initiated(obs)
        assert CreateSlots(hosts=('host3',)) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) in plan

    def test_creates_slots_only_when_not_turned(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=False)
        plan = m.plan_initiated(obs)
        assert CreateSlots(hosts=('host3',)) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) not in plan

    def test_waits_when_side_replicas_not_turned(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=False)
        plan = m.plan_initiated(obs)
        # Plan is non-empty (CreateSlots) but no transition
        assert plan
        assert not any(isinstance(c, TransitionTo) for c in plan)

    def test_retries_when_not_turned(self):
        """all_side_replicas_turned=False (not turned or read error) → no transition, retry."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=False)
        plan = m.plan_initiated(obs)
        assert CreateSlots(hosts=('host3',)) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) not in plan

    def test_transitions_without_side_replicas(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, side_replicas=())
        plan = m.plan_initiated(obs)
        # Plan includes SWITCHOVER STARTED event + transition (no side replicas).
        assert Log(message='SWITCHOVER STARTED', level='warning', event=True) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) in plan
        assert CreateSlots(hosts=('host3',)) not in plan

    def test_create_slots_before_transition(self):
        """Fence: CreateSlots precedes TransitionTo(CANDIDATE_FOUND)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=True)
        plan = m.plan_initiated(obs)
        slots_idx = next(i for i, c in enumerate(plan) if isinstance(c, CreateSlots))
        transition_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.CANDIDATE_FOUND))
        assert slots_idx < transition_idx


# ---------------------------------------------------------------------------
# plan_candidate_found: candidate_found → promoted
# ---------------------------------------------------------------------------


class TestPlanCandidateFound:
    """candidate_found → promoted: acquire lock, promote, cleanup."""

    def test_acquires_lock_and_promotes(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan
        assert Promote(scope='switchover_candidate', old_primary='host1') in plan
        assert TransitionTo(SwitchoverPhase.PROMOTED) in plan
        assert CleanupSwitchover() in plan
        assert WriteLastSwitchoverTime() in plan
        assert StopTimer('switchover') in plan

    def test_lock_acquisition_is_non_blocking(self):
        """timeout=0: non-blocking, one attempt per iteration."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        acquire_cmds = [c for c in plan if isinstance(c, AcquireLock)]
        assert len(acquire_cmds) == 1
        assert acquire_cmds[0].timeout == 0

    def test_starts_downtime_when_not_already_started(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, downtime_timer_started=False)
        plan = m.plan_candidate_found(obs)
        assert StartTimer('downtime') in plan

    def test_does_not_start_downtime_when_already_started(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, downtime_timer_started=True)
        plan = m.plan_candidate_found(obs)
        assert StartTimer('downtime') not in plan

    def test_returns_empty_when_debug_failure(self):
        debug = lambda name: name == 'candidate_switchover_before_acquire'
        m = _make_machine(debug_failure=debug)
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        assert plan == []

    def test_releases_lock_when_switchover_info_missing(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, switchover_primary_info=None)
        plan = m.plan_candidate_found(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan
        assert ReleaseLock() in plan
        assert not any(isinstance(command, Promote) for command in plan)

    def test_writes_promoted_before_cleanup(self):
        """Fence: TransitionTo(PROMOTED) precedes CleanupSwitchover."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        promoted_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PROMOTED))
        cleanup_idx = next(i for i, c in enumerate(plan) if isinstance(c, CleanupSwitchover))
        assert promoted_idx < cleanup_idx

    def test_promote_before_promoted(self):
        """Promote precedes TransitionTo(PROMOTED) — promotion must succeed first."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        failover_idx = next(i for i, c in enumerate(plan) if isinstance(c, Promote))
        promoted_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PROMOTED))
        assert failover_idx < promoted_idx

    def test_local_state_is_cleared_before_acquire(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        assert plan[:2] == [
            ClearLocalState('switchover_candidate'),
            AcquireLock(allow_queue=True, timeout=0),
        ]


# ---------------------------------------------------------------------------
# plan() dispatch
# ---------------------------------------------------------------------------


class TestCandidatePlanDispatch:
    """plan() dispatches phases to the correct planner."""

    def test_plan_dispatches_initiated(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED)
        plan = m.plan(obs)
        assert CreateSlots(hosts=('host3',)) in plan

    def test_plan_dispatches_candidate_found(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan

    def test_plan_dispatches_primary_shut_to_candidate_found(self):
        """primary_shut maps to plan_candidate_found with blocking acquire (MDB-41951 fix).

        In PRIMARY_SHUT, the old primary guarantees immediate lock release, so
        CandidateSwitchoverMachine uses blocking AcquireLock(timeout=primary_shut_acquire_timeout)
        instead of the non-blocking timeout=0 used in earlier phases.
        """
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT)
        plan = m.plan(obs)
        acquire_cmds = [c for c in plan if isinstance(c, AcquireLock)]
        assert len(acquire_cmds) == 1, "Expected exactly one AcquireLock command in PRIMARY_SHUT plan"
        # PRIMARY_SHUT uses blocking acquire (default primary_shut_acquire_timeout=30.0).
        assert acquire_cmds[0].timeout > 0, (
            "PRIMARY_SHUT should use blocking AcquireLock (timeout > 0), "
            f"got timeout={acquire_cmds[0].timeout}"
        )

    def test_plan_returns_empty_for_unknown_phase(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.SCHEDULED)
        plan = m.plan(obs)
        assert plan == []

    def test_plan_returns_empty_for_failed_phase(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.FAILED)
        plan = m.plan(obs)
        assert plan == []

    def test_plan_promoted_cleans_up_metadata(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED)
        plan = m.plan(obs)
        assert CleanupSwitchover() in plan


# ---------------------------------------------------------------------------
# Bug 1: CandidateSwitchoverMachine must handle PG_STOPPED and POOLER_STOPPED
# Reproduces anywhere_switchover.feature:132 — candidate stuck on pg_stopped.
# ---------------------------------------------------------------------------


class TestCandidateHandlesShutdownPhases:
    """Candidate must not stall when primary transitions through shutdown phases.

    The primary goes candidate_found → pooler_stopped → pg_stopped →
    primary_shut. The candidate observes these intermediate phases in ZK and
    must keep attempting lock acquisition (non-blocking) instead of returning
    an empty plan and logging 'No candidate-side planner'.
    """

    def test_plan_handles_pg_stopped_phase(self):
        """PG_STOPPED must produce a non-empty plan (lock acquisition attempt)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED)
        plan = m.plan(obs)
        assert len(plan) > 0, 'CandidateSwitchoverMachine has no planner for PG_STOPPED'
        assert AcquireLock(allow_queue=True, timeout=0) in plan

    def test_plan_handles_pooler_stopped_phase(self):
        """POOLER_STOPPED must produce a non-empty plan (lock acquisition attempt)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.POOLER_STOPPED)
        plan = m.plan(obs)
        assert len(plan) > 0, 'CandidateSwitchoverMachine has no planner for POOLER_STOPPED'
        assert AcquireLock(allow_queue=True, timeout=0) in plan

    def test_plan_pg_stopped_does_not_promote_without_lock(self):
        """PG_STOPPED with lock held by old primary: AcquireLock is first.

        The plan is declarative — it always contains the full sequence, but
        the executor stops at AcquireLock (non-blocking, timeout=0) when the
        lock is held by the old primary. Promote never executes.
        """
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED, lock_holder='host1')
        plan = m.plan(obs)
        assert isinstance(plan[0], ClearLocalState)
        assert isinstance(plan[1], AcquireLock)
        assert plan[1].timeout == 0
        # No FAILED transition — we are waiting, not aborting.
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan


# ---------------------------------------------------------------------------
# Crash after lock acquisition but before the global phase transition.
# ---------------------------------------------------------------------------


class TestCandidateAcquiredLockRecovery:
    def test_continues_without_reacquiring_own_lock(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host2')

        plan = m.plan_candidate_found(obs)

        assert not any(isinstance(command, AcquireLock) for command in plan)
        assert TransitionTo(SwitchoverPhase.CANDIDATE_ACQUIRED) in plan
        assert Promote(scope='switchover_candidate', old_primary='host1') in plan
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan

    def test_does_not_abort_when_lock_held_by_other(self):
        """Lock held by another host → normal non-blocking acquire, no abort."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host1')
        plan = m.plan_candidate_found(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan
        assert AcquireLock(allow_queue=True, timeout=0) in plan


# ---------------------------------------------------------------------------
# Timeout gate: plan() short-circuits to FAILED when old primary doesn't
# release lock in time. Active in POOLER_STOPPED / PG_STOPPED / PRIMARY_SHUT.
# ---------------------------------------------------------------------------


class TestPrimaryShutTimeoutGate:
    """plan() returns TransitionTo(FAILED) when primary_shut_timeout exceeded."""

    def test_fails_when_primary_shut_timeout_exceeded_in_pooler_stopped(self):
        """downtime_started_ts in the past + phase=POOLER_STOPPED → FAILED."""
        import time
        cfg = SwitchoverMachineConfig(primary_shut_timeout=1.0)
        m = CandidateSwitchoverMachine(config=cfg)
        old_ts = time.time() - 10.0
        obs = _make_obs(
            SwitchoverPhase.POOLER_STOPPED,
            downtime_started_ts=old_ts,
        )
        plan = m.plan(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_fails_when_primary_shut_timeout_exceeded_in_pg_stopped(self):
        """downtime_started_ts in the past + phase=PG_STOPPED → FAILED."""
        import time
        cfg = SwitchoverMachineConfig(primary_shut_timeout=1.0)
        m = CandidateSwitchoverMachine(config=cfg)
        old_ts = time.time() - 10.0
        obs = _make_obs(
            SwitchoverPhase.PG_STOPPED,
            downtime_started_ts=old_ts,
        )
        plan = m.plan(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_fails_when_primary_shut_timeout_exceeded_in_primary_shut(self):
        """downtime_started_ts in the past + phase=PRIMARY_SHUT → FAILED."""
        import time
        cfg = SwitchoverMachineConfig(primary_shut_timeout=1.0)
        m = CandidateSwitchoverMachine(config=cfg)
        old_ts = time.time() - 10.0
        obs = _make_obs(
            SwitchoverPhase.PRIMARY_SHUT,
            downtime_started_ts=old_ts,
        )
        plan = m.plan(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]

    def test_does_not_fail_when_timeout_not_exceeded(self):
        """downtime_started_ts recent → normal plan, no FAILED transition."""
        import time
        cfg = SwitchoverMachineConfig(primary_shut_timeout=300.0)
        m = CandidateSwitchoverMachine(config=cfg)
        recent_ts = time.time() - 1.0
        obs = _make_obs(
            SwitchoverPhase.PG_STOPPED,
            downtime_started_ts=recent_ts,
        )
        plan = m.plan(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan

    def test_does_not_fail_when_downtime_started_ts_is_none(self):
        """No downtime timer → no timeout gate, normal plan."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.PG_STOPPED,
            downtime_started_ts=None,
        )
        plan = m.plan(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan


# ---------------------------------------------------------------------------
# Guard: plan_candidate_found must abort when switchover_primary_info has
# no hostname (old_primary=None). Previously caused KeyError / None passed
# to Promote.
# ---------------------------------------------------------------------------


class TestCandidateOldPrimaryNone:
    """plan_candidate_found releases lock when old_primary hostname is None."""

    def test_releases_lock_when_hostname_is_none(self):
        """switchover_primary_info={'hostname': None} → ReleaseLock, no Promote."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.CANDIDATE_FOUND,
            switchover_primary_info={'hostname': None},
        )
        plan = m.plan_candidate_found(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan
        assert ReleaseLock() in plan
        assert not any(isinstance(command, Promote) for command in plan)
        assert TransitionTo(SwitchoverPhase.PROMOTED) not in plan


class TestPlanFailed:
    def test_primary_resumes_promotion_and_cleans_up(self):
        obs = _make_obs(
            SwitchoverPhase.FAILED,
            lock_holder='host2',
            role='primary',
        )

        plan = _make_machine().plan(obs)

        assert Promote(scope='switchover_candidate', old_primary='host1') in plan
        assert WriteLastSwitchoverTime() in plan
        assert StopTimer('switchover') in plan
        assert CleanupSwitchover() in plan
        assert ReleaseLock() not in plan

    def test_replica_releases_lock_and_clears_local_state(self):
        obs = _make_obs(
            SwitchoverPhase.FAILED,
            lock_holder='host2',
            role='replica',
        )

        plan = _make_machine().plan(obs)

        assert plan == [
            ClearLocalState('switchover_candidate'),
            ReleaseLock(),
        ]

    def test_waits_when_candidate_does_not_hold_lock(self):
        obs = _make_obs(
            SwitchoverPhase.FAILED,
            lock_holder='host1',
        )

        assert _make_machine().plan(obs) == []
