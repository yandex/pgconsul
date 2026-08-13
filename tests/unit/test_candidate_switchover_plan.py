"""Unit tests for CandidateSwitchoverMachine plan_* methods (ADR-0006, step F).

Tests call ``plan(observation)`` and assert on the returned Plan — no MagicMock
context. Decisions, not interactions, are verified.
"""

from src.commands import (
    AcquireLock,
    CleanupSwitchover,
    CreateSlots,
    DoFailover,
    Log,
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
    lock_holder=None,
):
    """Build a minimal SwitchoverObservation for candidate plan_* tests."""
    if switchover_primary_info is _SENTINEL:
        switchover_primary_info = {'hostname': 'host1'}
    return SwitchoverObservation(
        record=_make_record(phase, candidate=candidate, side_replicas=list(side_replicas)),
        my_hostname='host2',
        role='replica',
        zk_timeline=5,
        failover_state=None,
        last_failover_ts=None,
        last_switchover_ts=None,
        ha_replics=frozenset({'host2', 'host3'}),
        replics_info=[],
        streaming_replicas=('host2', 'host3'),
        live_switchover_state=None,
        candidate_alive=True,
        lock_holder=lock_holder,
        switchover_timer_started=False,
        downtime_timer_started=downtime_timer_started,
        candidate=candidate,
        side_replicas=side_replicas,
        all_side_replicas_turned=all_side_replicas_turned,
        switchover_primary_info=switchover_primary_info,
        switchover_candidate=None,
    )


def _make_machine(debug_failure=None):
    """Create a stub-only machine (no context needed for plan_*)."""
    cfg = SwitchoverMachineConfig()
    return CandidateSwitchoverMachine(None, config=cfg, debug_failure=debug_failure)


# ---------------------------------------------------------------------------
# plan_initiated: initiated → candidate_found
# ---------------------------------------------------------------------------


class TestPlanInitiated:
    """initiated → candidate_found: create slots, check side replicas turned."""

    def test_creates_slots_and_transitions_when_all_turned(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=True)
        plan = m.plan_initiated(obs)
        assert CreateSlots(hosts=['host3']) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) in plan

    def test_creates_slots_only_when_not_turned(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=False)
        plan = m.plan_initiated(obs)
        assert CreateSlots(hosts=['host3']) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) not in plan

    def test_waits_when_side_replicas_not_turned(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=False)
        plan = m.plan_initiated(obs)
        # Plan is non-empty (CreateSlots) but no transition
        assert plan
        assert not any(isinstance(c, TransitionTo) for c in plan)

    def test_retries_on_pg_connection_error(self):
        """all_side_replicas_turned=None (read error) → no transition, retry."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=None)
        plan = m.plan_initiated(obs)
        assert CreateSlots(hosts=['host3']) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) not in plan

    def test_transitions_without_side_replicas(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, side_replicas=())
        plan = m.plan_initiated(obs)
        # Plan includes SWITCHOVER STARTED event + transition (no side replicas).
        assert Log(message='SWITCHOVER STARTED', level='warning', event=True) in plan
        assert TransitionTo(SwitchoverPhase.CANDIDATE_FOUND) in plan
        assert CreateSlots(hosts=['host3']) not in plan

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
    """candidate_found → promoted: acquire lock, do_failover, cleanup."""

    def test_acquires_lock_and_promotes(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan
        assert DoFailover(old_primary='host1') in plan
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
        assert DoFailover(old_primary='host1') not in plan

    def test_writes_promoted_before_cleanup(self):
        """Fence: TransitionTo(PROMOTED) precedes CleanupSwitchover."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        promoted_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PROMOTED))
        cleanup_idx = next(i for i, c in enumerate(plan) if isinstance(c, CleanupSwitchover))
        assert promoted_idx < cleanup_idx

    def test_do_failover_before_promoted(self):
        """DoFailover precedes TransitionTo(PROMOTED) — failover must succeed first."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        failover_idx = next(i for i, c in enumerate(plan) if isinstance(c, DoFailover))
        promoted_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.PROMOTED))
        assert failover_idx < promoted_idx

    def test_acquire_lock_is_first_command(self):
        """AcquireLock is the first command — nothing before it."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan_candidate_found(obs)
        assert isinstance(plan[0], AcquireLock)


# ---------------------------------------------------------------------------
# plan() dispatch
# ---------------------------------------------------------------------------


class TestCandidatePlanDispatch:
    """plan() dispatches phases to the correct planner."""

    def test_plan_dispatches_initiated(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED)
        plan = m.plan(obs)
        assert CreateSlots(hosts=['host3']) in plan

    def test_plan_dispatches_candidate_found(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND)
        plan = m.plan(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan

    def test_plan_dispatches_primary_shut_to_candidate_found(self):
        """primary_shut maps to plan_candidate_found (same as candidate_found)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT)
        plan = m.plan(obs)
        assert AcquireLock(allow_queue=True, timeout=0) in plan

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

    def test_plan_returns_empty_for_promoted_phase(self):
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PROMOTED)
        plan = m.plan(obs)
        assert plan == []


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
        lock is held by the old primary. DoFailover never executes.
        """
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PG_STOPPED, lock_holder='host1')
        plan = m.plan(obs)
        # AcquireLock is present and is the first command — executor stops here.
        assert isinstance(plan[0], AcquireLock)
        assert plan[0].timeout == 0
        # No FAILED transition — we are waiting, not aborting.
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan


# ---------------------------------------------------------------------------
# Bug 2: plan_candidate_found must abort when promote already failed.
# Reproduces anywhere_switchover.feature:132 — promote retry loop.
# ---------------------------------------------------------------------------


class TestCandidateFailedPromoteAbort:
    """When the candidate holds the lock but is still in candidate_found /
    primary_shut, a previous DoFailover has failed (the executor stops on
    failure and the lock is never released). The candidate must transition
    to FAILED instead of retrying promote in an infinite loop.
    """

    def test_aborts_when_lock_held_and_still_in_candidate_found(self):
        """Lock held by us + phase candidate_found → FAILED (promote failed)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host2')
        plan = m.plan_candidate_found(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) in plan
        assert DoFailover(old_primary='host1') not in plan

    def test_aborts_when_lock_held_and_still_in_primary_shut(self):
        """Lock held by us + phase primary_shut → FAILED (promote failed)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder='host2')
        plan = m.plan_candidate_found(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) in plan
        assert DoFailover(old_primary='host1') not in plan

    def test_releases_lock_on_failed_promote_abort(self):
        """Abort plan must release the lock before transitioning to FAILED."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host2')
        plan = m.plan_candidate_found(obs)
        from src.commands import ReleaseLock
        assert ReleaseLock() in plan
        # ReleaseLock must come before TransitionTo(FAILED).
        release_idx = next(i for i, c in enumerate(plan) if isinstance(c, ReleaseLock))
        failed_idx = next(i for i, c in enumerate(plan) if c == TransitionTo(SwitchoverPhase.FAILED))
        assert release_idx < failed_idx

    def test_does_not_abort_when_lock_not_held(self):
        """Lock not held (None) → normal promote plan, no FAILED transition."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder=None)
        plan = m.plan_candidate_found(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan
        assert DoFailover(old_primary='host1') in plan

    def test_does_not_abort_when_lock_held_by_other(self):
        """Lock held by another host → normal non-blocking acquire, no abort."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host1')
        plan = m.plan_candidate_found(obs)
        assert TransitionTo(SwitchoverPhase.FAILED) not in plan
        assert AcquireLock(allow_queue=True, timeout=0) in plan
