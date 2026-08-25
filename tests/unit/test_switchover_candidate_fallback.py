# encoding: utf-8
"""
Unit tests for _get_switchover_candidate fallback to db_state.

Reproduces the flaky anywhere_switchover.feature:73 failure where the primary
logs "Switchover scheduled: no eligible candidate, waiting" forever.

Root cause: _get_extended_replica_infos reads replica info from the global ZK
node (zk.get_replics_info()). When that node is stale or empty (e.g. the
primary has not yet persisted the latest replics_info), the candidate
selection returns None even though db_state['replics_info'] — read directly
from pg_stat_replication on the same iteration — contains fresh, valid data.

The fix: fall back to db_state['replics_info'] when the ZK global node is
None, so the candidate can be chosen from the live DB state.
"""

import pytest
from unittest.mock import MagicMock, patch

from src.switchover import SwitchoverRecord


_RECORD = SwitchoverRecord(destination=None)


def _make_pgconsul():
    """Create a pgconsul instance bypassing __init__ entirely."""
    from src.main import PgconsulConfig
    with patch('src.main.pgconsul.__init__', return_value=None):
        from src.main import Pgconsul
        inst = Pgconsul.__new__(Pgconsul)

    inst.db = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='',
        working_dir='/tmp',
        iteration_timeout=0.0,
        quorum_commit=False,
        use_lwaldump=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='100',
        stream_from=None,
        autofailover=False,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        do_consecutive_primary_switch=False,
        max_allowed_switchover_lag_ms=0,
        allow_potential_data_loss=False,
        close_detached_after=0.0,
        start_pooler=False,
        recovery_timeout=0.0,
        can_delayed=False,
        primary_switch_disable_archive_restore=False,
        primary_switch_checks=0,
        primary_switch_restart=False,
        primary_unavailability_timeout=0.0,
        walreceiver_disable_timeout=0.0,
        min_failover_timeout=0.0,
        change_replication_type=False,
        sync_replication_in_maintenance=False,
        promote_checkpoint_sql=None,
        failure_name=None,
        failure_count=100000000,
        sleep_before_disable_walreceiver=0.0,
        election_lsn_read_sleep=0.0,
        election_loser_timeout=0,
    )
    inst._timings = MagicMock()
    inst._maintenance = MagicMock()
    inst._replication_manager = MagicMock()
    return inst


# Replica info as seen by pg_stat_replication on the primary (fresh DB state).
# postgresql3 has priority 3, postgresql2 has priority 1.
_DB_REPLICS_INFO = [
    {
        'pid': 184,
        'application_name': 'pgconsul_postgresql3_1_pgconsul_pgconsul_net',
        'client_hostname': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
        'client_addr': '192.168.233.16',
        'state': 'streaming',
        'primary_location': '0/30317D8',
        'sent_location_diff': 0,
        'write_location_diff': 0,
        'replay_location_diff': 0,
        'replay_lag_msec': 0,
        'backend_start_ts': 1786419507,
        'reply_time_ms': 1786419514264,
        'sync_state': 'quorum',
    },
]

_QUORUM_HOSTS = [
    'pgconsul_postgresql2_1.pgconsul_pgconsul_net',
    'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
]

_HA_HOSTS = [
    'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
    'pgconsul_postgresql2_1.pgconsul_pgconsul_net',
    'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
]


class TestGetSwitchoverCandidateFallback:
    """_get_switchover_candidate must fall back to db_state when ZK is stale.

    Reproduces anywhere_switchover.feature:73 (flaky): the primary never finds
    a candidate because zk.get_replics_info() returns None while
    db_state['replics_info'] has valid streaming replicas.
    """

    def test_candidate_found_when_zk_replics_info_is_none(self):
        """When ZK global replics_info is None, use db_state as fallback.

        This is the red test: before the fix, _get_extended_replica_infos
        returns None (because zk.get_replics_info() is None), so
        _get_switchover_candidate returns None → "no eligible candidate".
        After the fix, it falls back to db_state['replics_info'] and finds
        the candidate.
        """
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        # ZK global replics_info is None (stale / not yet written).
        inst.zk.get_replics_info.return_value = None
        # Switchover record exists, no explicit destination (anywhere switchover).
        inst.zk.get_quorum.return_value = _QUORUM_HOSTS
        inst.zk.get_ha_hosts.return_value = _HA_HOSTS
        # Priority for postgresql3 is 3, for postgresql2 is 1.
        inst.zk.get_host_prio.side_effect = lambda host=None: {
            'pgconsul_postgresql3_1.pgconsul_pgconsul_net': '3',
            'pgconsul_postgresql2_1.pgconsul_pgconsul_net': '1',
        }.get(host)

        # db_state has fresh replics_info from pg_stat_replication.
        db_state = {'replics_info': _DB_REPLICS_INFO}

        # ReplicationManager.get_ensured_sync_replica should return postgresql3
        # (highest priority, in quorum, streaming).
        from src.helpers import get_oldest_replica, app_name_from_fqdn
        expected_app = get_oldest_replica([
            {**_DB_REPLICS_INFO[0], 'priority': 3},
        ])
        expected_fqdn = {
            app_name_from_fqdn(h): h for h in _QUORUM_HOSTS
        }.get(expected_app)
        inst._replication_manager.get_ensured_sync_replica.return_value = expected_fqdn

        result = inst._get_switchover_candidate(_RECORD, db_state=db_state)

        assert result is not None, (
            "Expected a switchover candidate when db_state has valid replics_info, "
            "but got None — the primary would log 'no eligible candidate' forever"
        )
        assert result == 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'

    def test_candidate_found_when_zk_replics_info_is_empty_list(self):
        """When ZK global replics_info is an empty list [], use db_state fallback.

        Reproduces anywhere_switchover.feature:73 (third switchover): the
        primary never finds a candidate because zk.get_replics_info() returns
        [] (stale/empty) while db_state['replics_info'] has a valid streaming
        replica. The fallback must trigger on empty lists too, not just None.
        """
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        # ZK global replics_info is an empty list (stale / cleared).
        inst.zk.get_replics_info.return_value = []
        inst.zk.get_quorum.return_value = _QUORUM_HOSTS
        inst.zk.get_ha_hosts.return_value = _HA_HOSTS
        inst.zk.get_host_prio.side_effect = lambda host=None: {
            'pgconsul_postgresql3_1.pgconsul_pgconsul_net': '3',
            'pgconsul_postgresql2_1.pgconsul_pgconsul_net': '1',
        }.get(host)

        db_state = {'replics_info': _DB_REPLICS_INFO}

        from src.helpers import get_oldest_replica, app_name_from_fqdn
        expected_app = get_oldest_replica([
            {**_DB_REPLICS_INFO[0], 'priority': 3},
        ])
        expected_fqdn = {
            app_name_from_fqdn(h): h for h in _QUORUM_HOSTS
        }.get(expected_app)
        inst._replication_manager.get_ensured_sync_replica.return_value = expected_fqdn

        result = inst._get_switchover_candidate(_RECORD, db_state=db_state)

        assert result is not None, (
            "Expected a switchover candidate when ZK returns [] but db_state "
            "has valid replics_info, but got None — the primary would log "
            "'no eligible candidate' forever"
        )
        assert result == 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'

    def test_candidate_none_when_both_zk_and_db_are_empty(self):
        """When both ZK and db_state are empty, candidate is genuinely None."""
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        inst.zk.get_replics_info.return_value = None
        inst.zk.get_quorum.return_value = _QUORUM_HOSTS
        inst.zk.get_ha_hosts.return_value = _HA_HOSTS
        inst.zk.get_host_prio.return_value = None

        db_state = {'replics_info': []}

        result = inst._get_switchover_candidate(_RECORD, db_state=db_state)

        assert result is None

    def test_fresh_db_state_wins_over_stale_non_empty_zk(self):
        """autofailover.feature:63: do not select a stopped stale replica."""
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        inst.zk.get_replics_info.return_value = [{
            'application_name': 'pgconsul_postgresql2_1_pgconsul_pgconsul_net',
            'state': 'streaming',
        }]
        inst.zk.get_ha_hosts.return_value = _HA_HOSTS
        inst.zk.get_host_prio.side_effect = lambda host=None: {
            'pgconsul_postgresql2_1.pgconsul_pgconsul_net': '1',
            'pgconsul_postgresql3_1.pgconsul_pgconsul_net': '3',
        }.get(host)

        def choose_live_replica(replica_infos):
            assert [info['application_name'] for info in replica_infos] == [
                'pgconsul_postgresql3_1_pgconsul_pgconsul_net',
            ]
            return 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'

        inst._replication_manager.get_ensured_sync_replica.side_effect = choose_live_replica

        assert inst._get_switchover_candidate(_RECORD, {'replics_info': _DB_REPLICS_INFO}) == (
            'pgconsul_postgresql3_1.pgconsul_pgconsul_net'
        )

    def test_fresh_empty_db_state_does_not_fall_back_to_stale_zk(self):
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        inst.zk.get_replics_info.return_value = [{
            'application_name': 'pgconsul_postgresql2_1_pgconsul_pgconsul_net',
            'state': 'streaming',
        }]
        inst.zk.get_ha_hosts.return_value = _HA_HOSTS

        assert inst._get_extended_replica_infos({'replics_info': []}) == []
