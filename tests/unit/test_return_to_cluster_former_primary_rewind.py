"""
Red test: former primary with dead PG (role=None) gets SIMPLE_SWITCH instead of REWIND.

Reproduces kill_primary.feature:169 — "Destroy primary with primary_switch_restart = yes".
The old primary (postgresql1) is killed by the test, failover elects postgresql2,
then postgresql1 is repaired. dead_iter() calls _return_to_cluster(holder, 'primary',
is_dead=True). ReturnObservation.build() reads role from db.get_state() which returns
role=None (PG is dead). decide_return_action() checks obs.role == 'primary' → None != 'primary'
→ picks SIMPLE_SWITCH instead of REWIND. Simple switch succeeds, pg_rewind is never
called, /tmp/rewind_called is never created → test assertion "was rewinded" fails.

See: implement/46-behave-kill-primary-rewind-skipped.md
"""

from src.return_to_cluster import (
    ReturnAction,
    ReturnObservation,
    decide_return_action,
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


class TestFormerPrimaryDeadPgGetsRewind:
    """
    A former primary returning to cluster with dead PG (role=None) must
    get REWIND action, not SIMPLE_SWITCH.

    The role parameter passed to _return_to_cluster() is the *previous* role
    (correctly 'primary'), but ReturnObservation.build() reads role from
    db.get_state() which returns None when PG is dead. The decision function
    then cannot distinguish "former primary" from "replica" and picks
    SIMPLE_SWITCH — skipping pg_rewind.
    """

    def test_former_primary_with_role_none_gets_rewind(self):
        """role=None (dead PG) + fallback_role='primary' → must be REWIND."""
        obs = _obs(role=None, is_dead=True, fallback_role='primary')
        action = decide_return_action(obs)
        assert action == ReturnAction.REWIND, (
            'Former primary with dead PG (role=None, fallback_role=primary) '
            'must get REWIND action, not SIMPLE_SWITCH — pg_rewind is required after failover'
        )

    def test_replica_with_role_none_still_simple_switch(self):
        """role=None + fallback_role='replica' → SIMPLE_SWITCH is correct."""
        obs = _obs(role=None, is_dead=True, fallback_role='replica')
        action = decide_return_action(obs)
        assert action == ReturnAction.SIMPLE_SWITCH, (
            'Replica with dead PG (role=None, fallback_role=replica) '
            'must still get SIMPLE_SWITCH — no rewind needed for replicas'
        )

    def test_former_primary_with_role_none_and_diverged_timelines(self):
        """A former primary waits until the target history is archived."""
        obs = _obs(
            role=None, is_dead=True, fallback_role='primary',
            local_timeline=1, zk_timeline=2,
        )
        action = decide_return_action(obs)
        assert action == ReturnAction.WAIT_HISTORY

    def test_explicit_role_primary_still_rewind(self):
        """role='primary' (PG alive) → REWIND — existing behavior unchanged."""
        obs = _obs(role='primary', is_dead=False)
        action = decide_return_action(obs)
        assert action == ReturnAction.REWIND

    def test_no_fallback_role_defaults_to_simple_switch(self):
        """Without fallback_role, role=None → SIMPLE_SWITCH (backward compat)."""
        obs = _obs(role=None, is_dead=True)
        action = decide_return_action(obs)
        assert action == ReturnAction.SIMPLE_SWITCH
