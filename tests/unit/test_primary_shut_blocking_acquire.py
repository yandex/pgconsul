# encoding: utf-8
"""RED test: MDB-41951 race — candidate cannot acquire lock in PRIMARY_SHUT phase.

Root cause (from logs.local/6, 2026-08-15):
    - Primary transitions to PRIMARY_SHUT (ZK) then calls ReleaseLock immediately after.
    - Candidate observes PRIMARY_SHUT and tries AcquireLock(timeout=0) — non-blocking.
    - If the primary still holds the lock at that moment, candidate gets False
      and must wait for the next iteration (~7-8 seconds under network latency in tests).
    - With CLI timeout of 60 seconds, candidate gets <4 attempts after pg_stopped,
      and the switchover exceeds the timeout → SwitchoverException → behave test fails.

Log timeline (reproduced from logs.local/6/):
    18:52:33 primary: SWITCHOVER PHASE → sync_set
    18:52:39 primary: SWITCHOVER PHASE → initiated
    18:52:45 candidate: SWITCHOVER PHASE → candidate_found
    18:53:01 primary: SWITCHOVER PHASE → pooler_stopped
    18:53:06 primary: SWITCHOVER PHASE → pg_stopped
    18:52:53 candidate: Unable to obtain lock leader within timeout (0 s)  [iteration 1]
    18:53:00 candidate: Unable to obtain lock leader within timeout (0 s)  [iteration 2]
    18:53:08 candidate: Unable to obtain lock leader within timeout (0 s)  [iteration 3]
    18:53:16 candidate: Unable to obtain lock leader within timeout (0 s)  [iteration 4]
    18:53:16 primary: SWITCHOVER PHASE → primary_shut  ← lock released here
    18:53:23 candidate: SWITCHOVER PHASE → candidate_acquired  ← lock acquired 7s later!
    CLI timeout fires at ~18:53:32 (started 18:52:32 + 60s)

Expected behavior after fix:
    In PRIMARY_SHUT phase, primary guarantees immediate lock release (ReleaseLock is
    called right after TransitionTo(PRIMARY_SHUT) in plan_pg_stopped).
    Candidate should use blocking AcquireLock(timeout > 0) in this phase to acquire
    the lock immediately after release, without waiting for the next iteration.
"""
from src.commands import AcquireLock
from src.switchover import (
    CandidateSwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _make_record(phase, candidate='host2'):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination='host2',
        phase=phase,
        candidate=candidate,
        side_replicas=['host3'],
    )


def _make_obs(phase, *, lock_holder='host1', candidate='host2', downtime_started_ts=None):
    """Build a minimal SwitchoverObservation for candidate plan_* tests."""
    return SwitchoverObservation(
        record=_make_record(phase, candidate=candidate),
        my_hostname='host2',
        role='replica',
        zk_timeline=5,
        last_role_transition_ts=None,
        ha_replics=frozenset({'host2', 'host3'}),
        replics_info=[],
        streaming_replicas=('host2', 'host3'),
        candidate_alive=True,
        lock_holder=lock_holder,
        switchover_started_ts=None,
        downtime_started_ts=downtime_started_ts,
        all_side_replicas_turned=True,
        current_time=1.0,
        switchover_candidate=None,
    )


def _make_machine(primary_shut_acquire_timeout=None):
    """Create a CandidateSwitchoverMachine stub for plan_* tests.

    When primary_shut_acquire_timeout is None, SwitchoverMachineConfig is
    created with defaults only — tests 1 and 2 use this to verify current behavior.
    When a value is provided, the new field is passed — tests 3 and 4 use this
    to verify the desired behavior after the fix (TypeError until fix lands).
    """
    if primary_shut_acquire_timeout is not None:
        cfg = SwitchoverMachineConfig(primary_shut_acquire_timeout=primary_shut_acquire_timeout)
    else:
        cfg = SwitchoverMachineConfig()
    return CandidateSwitchoverMachine(config=cfg)


class TestPrimaryShutBlockingAcquire:
    """MDB-41951: candidate must use blocking AcquireLock in PRIMARY_SHUT phase.

    In PRIMARY_SHUT, the primary guarantees immediate lock release (plan:
    TransitionTo(PRIMARY_SHUT) → ReleaseLock(wait=5) → StopPostgresql).
    Using AcquireLock(timeout=0) forces candidate to wait for the next
    iteration (~7-8 seconds under network latency), which with CLI timeout=60s
    causes SwitchoverException after only 4-5 attempts.
    """

    def test_primary_shut_acquire_lock_timeout_is_zero(self):
        """RED TEST: PRIMARY_SHUT uses non-blocking timeout=0.

        This test documents the current broken behavior — AcquireLock(timeout=0) —
        which causes a race condition under slow iterations.
        After the fix this test should become GREEN (timeout > 0).
        """
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder='host1')
        plan = m.plan(obs)

        acquire_cmds = [c for c in plan if isinstance(c, AcquireLock)]
        assert len(acquire_cmds) == 1, "Expected exactly one AcquireLock command"

        acquire = acquire_cmds[0]
        # RED: timeout=0 causes a race under slow iterations.
        # In PRIMARY_SHUT, primary guarantees lock release — a blocking timeout is needed.
        assert acquire.timeout > 0, (
            f"In PRIMARY_SHUT phase, AcquireLock should use timeout > 0 "
            f"to immediately acquire the lock after primary releases it, "
            f"instead of non-blocking timeout=0 (current: timeout={acquire.timeout}). "
            f"With iteration_timeout ~7-8 seconds under network latency, "
            f"timeout=0 gives only 4-5 attempts within CLI timeout of 60 seconds."
        )

    def test_primary_shut_uses_same_plan_as_candidate_found(self):
        """RED TEST: plan_candidate_found does not differentiate PRIMARY_SHUT from other phases.

        In PRIMARY_SHUT, the plan should differ from CANDIDATE_FOUND — it must use
        a blocking acquire since the lock will be released immediately.
        """
        m = _make_machine()

        obs_primary_shut = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder='host1')
        obs_candidate_found = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host1')

        plan_shut = m.plan(obs_primary_shut)
        plan_found = m.plan(obs_candidate_found)

        acquire_shut = next((c for c in plan_shut if isinstance(c, AcquireLock)), None)
        acquire_found = next((c for c in plan_found if isinstance(c, AcquireLock)), None)

        assert acquire_shut is not None, "PRIMARY_SHUT plan must contain AcquireLock"
        assert acquire_found is not None, "CANDIDATE_FOUND plan must contain AcquireLock"

        # RED: both phases use the same timeout=0, but PRIMARY_SHUT should use blocking acquire.
        assert acquire_shut.timeout != acquire_found.timeout, (
            "PRIMARY_SHUT should use a different (larger) timeout for AcquireLock "
            "than CANDIDATE_FOUND, since in PRIMARY_SHUT the lock will be released immediately. "
            f"Current: PRIMARY_SHUT timeout={acquire_shut.timeout}, "
            f"CANDIDATE_FOUND timeout={acquire_found.timeout}"
        )

    def test_primary_shut_with_configured_blocking_timeout(self):
        """GREEN after fix: with primary_shut_acquire_timeout > 0, acquire is blocking."""
        m = _make_machine(primary_shut_acquire_timeout=30.0)
        obs = _make_obs(SwitchoverPhase.PRIMARY_SHUT, lock_holder='host1')
        plan = m.plan(obs)

        acquire_cmds = [c for c in plan if isinstance(c, AcquireLock)]
        assert len(acquire_cmds) == 1, "Expected exactly one AcquireLock command"

        acquire = acquire_cmds[0]
        # After fix: PRIMARY_SHUT uses the configured blocking timeout.
        assert acquire.timeout == 30.0, (
            f"In PRIMARY_SHUT with primary_shut_acquire_timeout=30.0, "
            f"AcquireLock should have timeout=30.0, got: {acquire.timeout}"
        )

    def test_candidate_found_still_non_blocking(self):
        """GREEN after fix: CANDIDATE_FOUND keeps non-blocking acquire."""
        m = _make_machine(primary_shut_acquire_timeout=30.0)
        obs = _make_obs(SwitchoverPhase.CANDIDATE_FOUND, lock_holder='host1')
        plan = m.plan(obs)

        acquire_cmds = [c for c in plan if isinstance(c, AcquireLock)]
        assert len(acquire_cmds) == 1

        acquire = acquire_cmds[0]
        # In CANDIDATE_FOUND, the primary is still active — non-blocking timeout=0 is correct.
        assert acquire.timeout == 0, (
            f"In CANDIDATE_FOUND, AcquireLock should remain non-blocking (timeout=0), "
            f"got: {acquire.timeout}"
        )
