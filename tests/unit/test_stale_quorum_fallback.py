# encoding: utf-8
"""
Unit tests for ReplicationManager.get_ensured_sync_replica stale-quorum fallback.

Reproduces anywhere_switchover.feature:73 (third switchover, scenario 2):
the primary never finds a switchover candidate because the quorum (SSN)
points to a stopped replica while a different replica is alive and streaming.

Root cause: when switchover is active, primary_iter() returns early (line 522)
and the replication-type/quorum update code (lines 536-558) is skipped. The
quorum stays stale — it lists a dead replica. get_ensured_sync_replica()
filters replica_infos by the stale quorum, finds no intersection, and
returns None → "no eligible candidate" forever.

Fix: when no quorum members are present in replica_infos (stale quorum),
fall back to all streaming replicas instead of returning None.
"""
from unittest.mock import MagicMock, patch

from src.replication_manager import ReplicationManager, ReplicationManagerConfig
from src.types import DurabilityConfig


def _make_replication_manager():
    """Create a ReplicationManager with mocked dependencies."""
    config = ReplicationManagerConfig(
        priority=100,
        primary_unavailability_timeout=0.0,
        change_replication_metric='count',
        weekday_change_hours='',
        weekend_change_hours='',
        overload_sessions_ratio=0.0,
        before_async_unavailability_timeout=0.0,
        quorum_removal_delay=0.0,
    )
    db = MagicMock()
    zk = MagicMock()
    with patch('src.replication_manager.SsnManager'):
        rm = ReplicationManager(config, db, zk)
    return rm


# Host FQDNs
_PG2_FQDN = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
_PG3_FQDN = 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'

# App names (as seen in pg_stat_replication.application_name)
_PG2_APP = 'pgconsul_postgresql2_1_pgconsul_pgconsul_net'
_PG3_APP = 'pgconsul_postgresql3_1_pgconsul_pgconsul_net'


def _make_replica_info(app_name, priority, write_location_diff=0, client_hostname=None):
    """Build a single replica info dict as returned by pg_stat_replication."""
    if client_hostname is None:
        # Derive FQDN from app_name: replace underscores with dots in the host part
        client_hostname = app_name.replace('_', '.')
    return {
        'application_name': app_name,
        'client_hostname': client_hostname,
        'state': 'streaming',
        'sync_state': 'async',
        'write_location_diff': write_location_diff,
        'priority': priority,
        'replay_lag_msec': 0,
    }


class TestGetEnsuredSyncReplicaStaleQuorum:
    """get_ensured_sync_replica must not return None when quorum is stale.

    When the quorum (SSN) lists a replica that is no longer streaming (dead),
    but another HA replica IS streaming, the method must fall back to the
    streaming replica instead of returning None.
    """

    def test_fallback_when_quorum_points_to_dead_replica(self):
        """Quorum lists postgresql3 (dead), but postgresql2 is streaming.

        Reproduces anywhere_switchover.feature:73 (third switchover):
        - postgresql3 was the only sync replica, then was stopped
        - postgresql2 came back and is streaming
        - quorum (SSN) still says ANY 1(postgresql3) — stale
        - get_ensured_sync_replica must return postgresql2, not None

        Before the fix: quorum_info is empty (postgresql3 not in replica_infos)
        → get_oldest_replica([]) returns None → method returns None.
        After the fix: falls back to all replica_infos → returns postgresql2.
        """
        rm = _make_replication_manager()
        # Quorum (SSN) points to postgresql3 only — stale, postgresql3 is dead.
        rm._zk.get_durability_config.return_value = DurabilityConfig.build(
            ['primary', _PG3_FQDN],
        )
        # HA hosts include both postgresql2 and postgresql3 (for fallback mapping).
        rm._zk.get_ha_hosts.return_value = [_PG2_FQDN, _PG3_FQDN]

        # replica_infos: only postgresql2 is streaming (postgresql3 is dead).
        replica_infos = [
            _make_replica_info(_PG2_APP, priority=1),
        ]

        result = rm.get_ensured_sync_replica(replica_infos)

        assert result is not None, (
            "Expected postgresql2 as candidate when quorum points to dead "
            "postgresql3 but postgresql2 is streaming, but got None — "
            "the primary would log 'no eligible candidate' forever"
        )
        assert result == _PG2_FQDN

    def test_normal_case_quorum_member_is_streaming(self):
        """When quorum member IS streaming, return it (no fallback needed)."""
        rm = _make_replication_manager()
        rm._zk.get_durability_config.return_value = DurabilityConfig.build(
            ['primary', _PG3_FQDN],
        )

        replica_infos = [
            _make_replica_info(_PG3_APP, priority=3),
        ]

        result = rm.get_ensured_sync_replica(replica_infos)

        assert result == _PG3_FQDN

    def test_fallback_picks_oldest_when_multiple_streaming(self):
        """When quorum is stale and multiple replicas stream, pick oldest.

        postgresql3 (in quorum, dead) and postgresql2 (not in quorum, streaming).
        With the fallback, postgresql2 should be returned as the only candidate.
        """
        rm = _make_replication_manager()
        # Quorum points to postgresql3 (dead) only.
        rm._zk.get_durability_config.return_value = DurabilityConfig.build(
            ['primary', _PG3_FQDN],
        )
        rm._zk.get_ha_hosts.return_value = [_PG2_FQDN, _PG3_FQDN]

        # Only postgresql2 is streaming.
        replica_infos = [
            _make_replica_info(_PG2_APP, priority=1, write_location_diff=0),
        ]

        result = rm.get_ensured_sync_replica(replica_infos)

        assert result == _PG2_FQDN

    def test_returns_none_when_no_replicas_streaming(self):
        """When no replicas are streaming at all, return None (genuine)."""
        rm = _make_replication_manager()
        rm._zk.get_durability_config.return_value = DurabilityConfig.build(
            ['primary', _PG3_FQDN],
        )

        replica_infos = []

        result = rm.get_ensured_sync_replica(replica_infos)

        assert result is None
