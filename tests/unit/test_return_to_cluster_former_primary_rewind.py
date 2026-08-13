"""
Red test: former primary with dead PG (role=None) gets SIMPLE_SWITCH instead of REWIND.

Reproduces kill_primary.feature:169 — "Destroy primary with primary_switch_restart = yes".
The old primary (postgresql1) is killed by the test, failover elects postgresql2,
then postgresql1 is repaired. dead_iter() calls _return_to_cluster(holder, 'primary',
is_dead=True). ReturnObservation.build() reads role from db.get_state() which returns
role=None (PG is dead). _derive_phase() checks obs.role == 'primary' → None != 'primary'
→ picks SIMPLE_SWITCH instead of REWIND. Simple switch succeeds, pg_rewind is never
called, /tmp/rewind_called is never created → test assertion "was rewinded" fails.

See: implement/46-behave-kill-primary-rewind-skipped.md
"""

from unittest.mock import MagicMock, patch

import pytest

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
        candidate_reachable=True,
        archive_restore_disabled=False,
        recovery_timeout=60.0,
        is_dead=False,
        skip_check=True,
        failover_state='finished',
    )
    defaults.update(kwargs)
    return ReturnObservation(**defaults)


class TestFormerPrimaryDeadPgGetsRewind:
    """
    A former primary returning to cluster with dead PG (role=None) must
    get REWIND phase, not SIMPLE_SWITCH.

    The role parameter passed to _return_to_cluster() is the *previous* role
    (correctly 'primary'), but ReturnObservation.build() reads role from
    db.get_state() which returns None when PG is dead. The state machine
    then cannot distinguish "former primary" from "replica" and picks
    SIMPLE_SWITCH — skipping pg_rewind.
    """

    def test_former_primary_with_role_none_gets_rewind(self):
        """
        role=None (dead PG) + is_dead=True → must be REWIND.

        This is the core bug: _derive_phase() only checks obs.role == 'primary',
        but when PG is dead, role is None even for a former primary.

        After the fix, ReturnObservation will carry a fallback_role field
        (the previous role from dead_iter) so _derive_phase() can detect
        former primaries even when the current role is None.
        """
        machine = ReturnToClusterMachine()
        # Former primary: role=None (dead PG), but fallback_role='primary'
        obs = _obs(role=None, is_dead=True, fallback_role='primary')
        phase = machine._derive_phase(obs)
        # Currently fails: returns SIMPLE_SWITCH (fallback_role not yet implemented)
        # Should return REWIND — former primary must rewind after failover
        assert phase == ReturnPhase.REWIND, (
            'Former primary with dead PG (role=None, fallback_role=primary) '
            'must get REWIND phase, not SIMPLE_SWITCH — pg_rewind is required after failover'
        )

    def test_replica_with_role_none_still_simple_switch(self):
        """
        role=None + fallback_role='replica' → SIMPLE_SWITCH is correct.

        A replica with dead PG should still try simple switch first.
        This ensures the fix doesn't break the replica path.
        """
        machine = ReturnToClusterMachine()
        obs = _obs(role=None, is_dead=True, fallback_role='replica')
        phase = machine._derive_phase(obs)
        assert phase == ReturnPhase.SIMPLE_SWITCH, (
            'Replica with dead PG (role=None, fallback_role=replica) '
            'must still get SIMPLE_SWITCH — no rewind needed for replicas'
        )

    def test_former_primary_with_role_none_and_diverged_timelines(self):
        """
        role=None + fallback_role='primary' + timelines diverge → REWIND.

        Even with diverged timelines, the former primary must get REWIND.
        """
        machine = ReturnToClusterMachine()
        obs = _obs(
            role=None, is_dead=True, fallback_role='primary',
            local_timeline=1, zk_timeline=2,
        )
        phase = machine._derive_phase(obs)
        assert phase == ReturnPhase.REWIND

    def test_explicit_role_primary_still_rewind(self):
        """role='primary' (PG alive) → REWIND — existing behavior unchanged."""
        machine = ReturnToClusterMachine()
        obs = _obs(role='primary', is_dead=False)
        phase = machine._derive_phase(obs)
        assert phase == ReturnPhase.REWIND

    def test_no_fallback_role_defaults_to_none(self):
        """Without fallback_role, behavior is unchanged (backward compat)."""
        machine = ReturnToClusterMachine()
        obs = _obs(role=None, is_dead=True)
        phase = machine._derive_phase(obs)
        # No fallback_role → role is None → SIMPLE_SWITCH (existing behavior)
        assert phase == ReturnPhase.SIMPLE_SWITCH
