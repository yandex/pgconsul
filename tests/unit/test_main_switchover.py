# encoding: utf-8
"""
Unit tests for switchover and failover methods in src/main.py.

Tests cover:
  - _all_side_replicas_turned_to_the_candidate: DB error handling
  - switchover routing
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock


def _make_pgconsul():
    """
    Create a pgconsul instance bypassing __init__ entirely.

    We patch __init__ to do nothing, then inject the minimal attributes
    needed by the methods under test.
    """
    from src.main import PgconsulConfig
    with patch('src.main.pgconsul.__init__', return_value=None):
        from src.main import Pgconsul
        inst = Pgconsul.__new__(Pgconsul)

    # Minimal mocks required by _candidate_is_sync_with_primary
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

    return inst


# ---------------------------------------------------------------------------
# Tests: _all_side_replicas_turned_to_the_candidate
# ---------------------------------------------------------------------------

class TestAllSideReplicasTurnedToTheCandidate:
    """_all_side_replicas_turned_to_the_candidate returns False on DB error."""

    def _make(self):
        from src.main import Pgconsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = Pgconsul.__new__(Pgconsul)
        inst.db = MagicMock()
        return inst

    def test_returns_true_when_all_replicas_turned(self):
        """Returns True when all side replicas are streaming from the candidate."""
        inst = self._make()
        inst.db.get_replics_info.return_value = [
            {'application_name': 'replica2', 'state': 'streaming'},
        ]
        with patch('src.helpers.app_name_from_fqdn', side_effect=lambda x: x.split('.')[0]):
            result = inst._all_side_replicas_turned_to_the_candidate(['replica2.example.com'])
        assert result is True

    def test_returns_false_on_connection_error(self):
        """PostgresConnectionError → return False (await_for will retry)."""
        from src.exceptions import PostgresConnectionError
        inst = self._make()
        inst.db.get_replics_info.side_effect = PostgresConnectionError("db down")
        with patch('src.helpers.app_name_from_fqdn', side_effect=lambda x: x.split('.')[0]):
            result = inst._all_side_replicas_turned_to_the_candidate(['replica2.example.com'])
        assert result is False


class TestGetStreamingReplicas:

    def _make(self):
        from src.main import Pgconsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = Pgconsul.__new__(Pgconsul)
        inst.db = MagicMock()
        inst.zk = MagicMock()
        return inst

    def test_raises_on_connection_error(self):
        from src.exceptions import PostgresConnectionError
        inst = self._make()
        inst.db.get_replics_info.side_effect = PostgresConnectionError("db down")
        with pytest.raises(PostgresConnectionError):
            inst._get_streaming_replicas()

    def test_returns_streaming_hosts(self):
        inst = self._make()
        inst.db.get_replics_info.return_value = [{'application_name': 'host1'}]
        inst.zk.get_members.return_value = ['host1.example.com', 'host2.example.com']
        with patch('src.helpers.app_name_from_fqdn', side_effect=lambda x: x.split('.')[0]):
            result = inst._get_streaming_replicas()
        assert result == ['host1.example.com']


class TestCheckArchiveRecovery:

    def _make(self):
        from src.main import Pgconsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = Pgconsul.__new__(Pgconsul)
        inst.db = MagicMock()
        return inst

    def test_returns_true_when_streaming(self):
        inst = self._make()
        with patch.object(inst, '_check_postgresql_streaming', return_value=True), \
             patch('src.main.helpers.await_for_value', return_value=True):
            result = inst._check_archive_recovery('primary.example.com', limit=10)
        assert result is True

    def test_returns_none_when_not_replaying(self):
        inst = self._make()
        inst.db.get_role.return_value = 'replica'
        inst.db.is_replaying_wal.return_value = False
        with patch.object(inst, '_check_postgresql_streaming', return_value=False), \
             patch.object(inst, '_acquire_replication_source_slot_lock'), \
             patch('src.main.helpers.await_for_value', return_value=None):
            result = inst._check_archive_recovery('primary.example.com', limit=10)
        assert result is None


# TestMakeElection removed — _make_election and FailoverElection are deprecated (ADR-0007 §7).


# ---------------------------------------------------------------------------
# Tests: _check_postgresql_streaming
# ---------------------------------------------------------------------------

class TestCheckPostgresqlStreaming:
    """_check_postgresql_streaming returns None (not raises) when check_walreceiver raises PostgresConnectionError."""

    def _make(self):
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        return inst

    def test_returns_none_when_check_walreceiver_raises_connection_error(self):
        """If check_walreceiver raises PostgresConnectionError, function returns None instead of propagating."""
        from src.exceptions import PostgresConnectionError

        inst = self._make()

        # DB is alive and in terminal state
        inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
        # Role is replica — passes the role check
        inst.db.get_role.return_value = 'replica'
        # check_walreceiver raises connection error
        inst.db.check_walreceiver.side_effect = PostgresConnectionError("connection lost")

        # Build replica_infos so that _is_caught_up returns True
        replica_info = {'application_name': 'myhost', 'state': 'streaming'}

        with patch('src.main.helpers.app_name_from_fqdn', return_value='myhost'), \
             patch('src.main.helpers.get_hostname', return_value='myhost.example.com'), \
             patch.object(inst, '_acquire_replication_source_slot_lock'), \
             patch.object(inst, '_get_replics_info_from_zk', return_value=[replica_info]):
            result = inst._check_postgresql_streaming('primary.example.com')

        assert result is None

    def test_does_not_raise_when_check_walreceiver_raises_connection_error(self):
        """PostgresConnectionError from check_walreceiver must never propagate out of _check_postgresql_streaming."""
        from src.exceptions import PostgresConnectionError

        inst = self._make()
        inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
        inst.db.get_role.return_value = 'replica'
        inst.db.check_walreceiver.side_effect = PostgresConnectionError("db gone")

        replica_info = {'application_name': 'myhost', 'state': 'streaming'}

        with patch('src.main.helpers.app_name_from_fqdn', return_value='myhost'), \
             patch('src.main.helpers.get_hostname', return_value='myhost.example.com'), \
             patch.object(inst, '_acquire_replication_source_slot_lock'), \
             patch.object(inst, '_get_replics_info_from_zk', return_value=[replica_info]):
            # Must not raise
            try:
                inst._check_postgresql_streaming('primary.example.com')
            except PostgresConnectionError:
                pytest.fail("PostgresConnectionError propagated out of _check_postgresql_streaming")

    def test_returns_true_when_streaming_normally(self):
        """Sanity check: returns True when _is_caught_up and check_walreceiver both succeed."""
        inst = self._make()
        inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
        inst.db.get_role.return_value = 'replica'
        inst.db.check_walreceiver.return_value = True

        replica_info = {'application_name': 'myhost', 'state': 'streaming'}

        with patch('src.main.helpers.app_name_from_fqdn', return_value='myhost'), \
             patch('src.main.helpers.get_hostname', return_value='myhost.example.com'), \
             patch.object(inst, '_acquire_replication_source_slot_lock'), \
             patch.object(inst, '_get_replics_info_from_zk', return_value=[replica_info]):
            result = inst._check_postgresql_streaming('primary.example.com')

        assert result is True

    def test_live_runtime_source_wins_over_stale_sender_zk(self):
        """cascade.feature:360: blocked switchover leaves sender stats stale."""
        inst = self._make()
        inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
        inst.db.get_role.return_value = 'replica'
        inst.db.get_primary_fqdn.return_value = 'primary.example.com'
        inst.db.check_walreceiver.return_value = True

        with patch.object(inst, '_acquire_replication_source_slot_lock'), \
             patch.object(inst, '_get_replics_info_from_zk', return_value=[]) as get_replics_info:
            result = inst._check_postgresql_streaming('primary.example.com')

        assert result is True
        get_replics_info.assert_not_called()


class TestAllSideReplicasTurnedToCandidate:
    """_all_side_replicas_turned_to_the_candidate catches PostgresConnectionError (CR-4)."""

    def _make(self):
        inst = _make_pgconsul()
        inst.zk = MagicMock()
        return inst

    def test_returns_false_on_connection_error(self):
        """PostgresConnectionError from get_replics_info → return False (§2 compensating action)."""
        from src.exceptions import PostgresConnectionError

        inst = self._make()
        inst.db.get_replics_info.side_effect = PostgresConnectionError('db down')

        with patch('src.main.helpers.app_name_from_fqdn', return_value='side1'):
            result = inst._all_side_replicas_turned_to_the_candidate(['side1.example.com'])

        assert result is False


class TestHandleSwitchoverRouting:
    def test_failed_candidate_holding_lock_runs_candidate_machine(self):
        from src.main import Pgconsul

        inst = Pgconsul.__new__(Pgconsul)
        inst.zk = MagicMock()
        inst.zk.SWITCHOVER_RECORD_PATH = '/switchover/record'
        inst.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
        inst.zk.TIMELINE_INFO_PATH = 'timeline'
        inst._sw_machine = MagicMock()
        inst._cand_machine = MagicMock()
        inst._executor = MagicMock()
        observation = object()
        inst._build_switchover_observation = MagicMock(return_value=observation)
        zk_state = {
            inst.zk.SWITCHOVER_RECORD_PATH: {
                'hostname': 'host1',
                'timeline': 5,
                'destination': 'host2',
                'phase': 'failed',
                'candidate': 'host2',
            },
            inst.zk.SWITCHOVER_VERSION_KEY: 7,
            'lock_holder': 'host2',
        }

        with patch('src.main.helpers.get_hostname', return_value='host2'):
            handled = inst.handle_switchover({'role': 'replica'}, zk_state)

        assert handled is True
        inst._build_switchover_observation.assert_called_once()
        assert inst._build_switchover_observation.call_args.kwargs['route'].value == 'candidate'
        inst._executor.run.assert_called_once_with(inst._cand_machine, observation)
