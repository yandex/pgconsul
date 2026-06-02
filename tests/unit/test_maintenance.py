# encoding: utf-8
"""
Unit tests for maintenance-related pure functions in src/maintenance.py.

Tests for should_stop_pooler_in_maintenance() — the function that decides
whether pooler (odyssey/pgbouncer) and WAL archiving should be stopped
during maintenance mode.

See MDB-43333 for the bug that motivated extracting this logic into a
testable pure function.
"""

from pathlib import Path
import sys

# Add src to path to import the module directly
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from maintenance import should_stop_pooler_in_maintenance


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestShouldStopPoolerInMaintenance:
    """
    Tests for should_stop_pooler_in_maintenance(db_state, zk_timeline).

    This pure function decides whether pooler and WAL archiving should be
    stopped during maintenance mode. The key invariant (MDB-43333):

        If db_state['alive'] is False, the function MUST return False —
        even when db_timeline is None and zk_timeline is set — because
        a None timeline in that case is caused by a connection failure,
        not by a failover.
    """

    # ------------------------------------------------------------------
    # MDB-43333: DB dead → must return False
    # ------------------------------------------------------------------

    def test_db_dead_primary_zk_timeline_set_returns_false(self):
        """Core MDB-43333 regression: DB unavailable, db_timeline=None → False."""
        db_state = {'role': 'primary', 'alive': False, 'timeline': None}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=8) is False

    def test_db_dead_both_timelines_none_returns_false(self):
        """DB dead, both timelines None → False."""
        db_state = {'role': 'primary', 'alive': False, 'timeline': None}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=None) is False

    def test_db_dead_timelines_match_returns_false(self):
        """DB dead even if timelines would match → False (alive check wins)."""
        db_state = {'role': 'primary', 'alive': False, 'timeline': 5}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=5) is False

    def test_missing_alive_key_defaults_to_false(self):
        """Missing 'alive' key defaults to False → no stop."""
        db_state = {'role': 'primary', 'timeline': None}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=8) is False

    # ------------------------------------------------------------------
    # Failover: DB alive, ZK timeline ahead → must return True
    # ------------------------------------------------------------------

    def test_db_alive_zk_ahead_returns_true(self):
        """Real failover: zk_timeline > db_timeline → True."""
        db_state = {'role': 'primary', 'alive': True, 'timeline': 1}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=2) is True

    def test_db_alive_db_timeline_none_zk_set_returns_true(self):
        """DB alive but db_timeline=None while zk_timeline is set → True."""
        db_state = {'role': 'primary', 'alive': True, 'timeline': None}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=5) is True

    # ------------------------------------------------------------------
    # Normal maintenance: timelines match or ZK has no timeline
    # ------------------------------------------------------------------

    def test_db_alive_timelines_equal_returns_false(self):
        """Timelines match → no failover → False."""
        db_state = {'role': 'primary', 'alive': True, 'timeline': 3}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=3) is False

    def test_db_alive_zk_timeline_none_returns_false(self):
        """zk_timeline=None → condition not met → False."""
        db_state = {'role': 'primary', 'alive': True, 'timeline': 3}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=None) is False

    def test_db_alive_zk_behind_returns_false(self):
        """ZK timeline < db_timeline → False."""
        db_state = {'role': 'primary', 'alive': True, 'timeline': 5}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=3) is False

    # ------------------------------------------------------------------
    # Role is not primary
    # ------------------------------------------------------------------

    def test_replica_role_returns_false(self):
        """Replica role → False regardless of timelines."""
        db_state = {'role': 'replica', 'alive': True, 'timeline': 1}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=5) is False

    def test_role_none_returns_false(self):
        """role=None (DB dead/unknown) → False."""
        db_state = {'role': None, 'alive': True, 'timeline': 1}
        assert should_stop_pooler_in_maintenance(db_state, zk_timeline=5) is False
