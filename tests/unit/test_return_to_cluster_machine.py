# encoding: utf-8
"""
Unit tests for the ReturnToClusterMachine pure plan() API (MDB-41951, ADR-0006).

Tests cover each phase derivation and the corresponding Command Plan output.
The machine is stateless — phase is derived from the observation, not stored.
"""



from src.commands import (
    CheckDivergence,
    EnsureRestoringWal,
    Log,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    SimplePrimarySwitch,
)
from src.return_to_cluster import (
    ReturnObservation,
    ReturnPhase,
    ReturnToClusterMachine,
)


def _obs(**kwargs) -> ReturnObservation:
    """Build a ReturnObservation with sensible defaults for testing."""
    defaults = dict(
        new_primary='pgconsul_postgresql2_1.pgconsul_pgconsul_net',
        role='replica',
        local_timeline=1,
        zk_timeline=1,
        last_op=None,
        simple_switch_tried=False,
        archive_restore_disabled=False,
        recovery_timeout=60.0,
        is_dead=False,
    )
    defaults.update(kwargs)
    return ReturnObservation(**defaults)


class TestDerivePhase:
    """_derive_phase decides the phase from the observation (pure)."""

    def test_simple_switch_when_no_blockers(self):
        """No blockers → SIMPLE_SWITCH."""
        machine = ReturnToClusterMachine()
        phase = machine._derive_phase(_obs())
        assert phase == ReturnPhase.SIMPLE_SWITCH

    def test_rewind_when_role_is_primary(self):
        """role='primary' → REWIND (easy way not possible)."""
        machine = ReturnToClusterMachine()
        phase = machine._derive_phase(_obs(role='primary'))
        assert phase == ReturnPhase.REWIND

    def test_rewind_when_destructive_op(self):
        """Destructive last_op → REWIND."""
        machine = ReturnToClusterMachine()
        phase = machine._derive_phase(_obs(last_op='rewind'))
        assert phase == ReturnPhase.REWIND

    def test_check_divergence_when_simple_switch_tried(self):
        """simple_switch_tried=True → CHECK_DIVERGENCE."""
        machine = ReturnToClusterMachine()
        phase = machine._derive_phase(_obs(simple_switch_tried=True))
        assert phase == ReturnPhase.CHECK_DIVERGENCE


class TestPlanSimpleSwitch:
    """plan_simple_switch emits SimplePrimarySwitch + CheckDivergence."""

    def test_emits_simple_primary_switch_and_check_divergence(self):
        machine = ReturnToClusterMachine()
        obs = _obs()
        plan = machine.plan(obs)
        assert len(plan) == 2
        assert isinstance(plan[0], SimplePrimarySwitch)
        assert isinstance(plan[1], CheckDivergence)
        assert plan[0].new_primary == obs.new_primary
        assert plan[0].is_dead == obs.is_dead
        assert plan[0].limit == obs.recovery_timeout


class TestPlanCheckDivergence:
    """plan_check_divergence decides retry vs rewind based on timelines."""

    def test_timelines_match_emits_ensure_restoring_wal_and_log(self):
        """Timelines match → EnsureRestoringWal + Log (retry, no rewind)."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=1,
            archive_restore_disabled=True,
        )
        plan = machine.plan(obs)
        # Should contain EnsureRestoringWal (archive restore was disabled).
        assert any(isinstance(c, EnsureRestoringWal) for c in plan)
        # Should NOT contain RewindFromSource.
        assert not any(isinstance(c, RewindFromSource) for c in plan)

    def test_timelines_match_no_archive_disabled_emits_only_log(self):
        """Timelines match, archive not disabled → only Log (no EnsureRestoringWal)."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=1,
            archive_restore_disabled=False,
        )
        plan = machine.plan(obs)
        assert not any(isinstance(c, EnsureRestoringWal) for c in plan)
        assert not any(isinstance(c, RewindFromSource) for c in plan)
        assert any(isinstance(c, Log) for c in plan)

    def test_timelines_diverge_emits_rewind(self):
        """Timelines diverge → RewindFromSource."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=2,
        )
        plan = machine.plan(obs)
        assert any(isinstance(c, RewindFromSource) for c in plan)

    def test_timelines_unknown_emits_rewind(self):
        """Timelines unknown (None) → REWIND (conservative)."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=None,
            zk_timeline=None,
        )
        plan = machine.plan(obs)
        assert any(isinstance(c, RewindFromSource) for c in plan)


class TestPlanRewind:
    """plan_rewind emits SetSimplePrimarySwitchTry + RewindFromSource."""

    def test_emits_set_try_and_rewind(self):
        machine = ReturnToClusterMachine()
        obs = _obs(role='primary')  # forces REWIND phase
        plan = machine.plan(obs)
        assert any(isinstance(c, SetSimplePrimarySwitchTry) for c in plan)
        assert any(isinstance(c, RewindFromSource) for c in plan)
        rewind_cmd = next(c for c in plan if isinstance(c, RewindFromSource))
        assert rewind_cmd.new_primary == obs.new_primary
        assert rewind_cmd.is_postgresql_dead == obs.is_dead
        assert rewind_cmd.limit == obs.recovery_timeout

    def test_rewind_with_archive_disabled_emits_ensure_restoring_wal(self):
        """REWIND with archive_restore_disabled → EnsureRestoringWal before RewindFromSource.

        Regression: side-replica disables archive restore (restore_command=/bin/false)
        during switchover. When timelines diverge and pg_rewind runs with
        --restore-target-wal, it cannot fetch WAL from archive → fatal error.
        EnsureRestoringWal must restore archive recovery BEFORE pg_rewind.
        """
        machine = ReturnToClusterMachine()
        obs = _obs(role='primary', archive_restore_disabled=True)
        plan = machine.plan(obs)
        assert any(isinstance(c, EnsureRestoringWal) for c in plan), \
            "REWIND with archive_restore_disabled must emit EnsureRestoringWal"
        assert any(isinstance(c, RewindFromSource) for c in plan)
        # EnsureRestoringWal must come before RewindFromSource.
        idx_ensure = next(i for i, c in enumerate(plan) if isinstance(c, EnsureRestoringWal))
        idx_rewind = next(i for i, c in enumerate(plan) if isinstance(c, RewindFromSource))
        assert idx_ensure < idx_rewind


class TestTimelinesMatch:
    """timelines_match utility."""

    def test_both_equal(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(1, 1) is True

    def test_both_none(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(None, None) is False

    def test_one_none(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(1, None) is False
        assert timelines_match(None, 1) is False

    def test_different(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(1, 2) is False


class TestPlanCheckDivergenceRetryLoop:
    """Regression test for cascade replication infinite loop (MDB-41951).

    When simple_switch_tried=True and timelines match, the machine logs
    "will retry" but only emits a Log command — no actual SimplePrimarySwitch.
    This causes an infinite loop in _return_to_cluster:

      1. Pass 1 skipped (simple_switch_tried flag is True in ZK)
      2. Pass 2 → CHECK_DIVERGENCE → timelines match → [Log] (no retry)
      3. _return_to_cluster returns None
      4. Outer loop (non_ha_replica_iter) calls _return_to_cluster again
      5. Go to step 1 — infinite loop (362 iterations observed in logs)

    The fix: plan_check_divergence must emit SimplePrimarySwitch to actually
    retry the switch when timelines match.
    """

    def test_retry_includes_simple_primary_switch(self):
        """Timelines match → retry plan must include SimplePrimarySwitch."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=1,
        )
        plan = machine.plan(obs)
        assert any(isinstance(c, SimplePrimarySwitch) for c in plan), \
            "Retry plan must include SimplePrimarySwitch to avoid infinite loop"

    def test_retry_simple_primary_switch_has_correct_params(self):
        """Retry SimplePrimarySwitch must use observation's new_primary/limit."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=1,
        )
        plan = machine.plan(obs)
        switch_cmd = next(c for c in plan if isinstance(c, SimplePrimarySwitch))
        assert switch_cmd.new_primary == obs.new_primary
        assert switch_cmd.is_dead == obs.is_dead
        assert switch_cmd.limit == obs.recovery_timeout

    def test_retry_with_archive_disabled_includes_ensure_restoring_wal(self):
        """Retry with archive_restore_disabled → EnsureRestoringWal before switch."""
        machine = ReturnToClusterMachine()
        obs = _obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=1,
            archive_restore_disabled=True,
        )
        plan = machine.plan(obs)
        assert any(isinstance(c, EnsureRestoringWal) for c in plan)
        assert any(isinstance(c, SimplePrimarySwitch) for c in plan)
        # EnsureRestoringWal must come before SimplePrimarySwitch.
        idx_ensure = next(i for i, c in enumerate(plan) if isinstance(c, EnsureRestoringWal))
        idx_switch = next(i for i, c in enumerate(plan) if isinstance(c, SimplePrimarySwitch))
        assert idx_ensure < idx_switch
