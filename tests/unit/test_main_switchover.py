# encoding: utf-8
"""
Unit tests for switchover and failover methods in src/main.py.

Tests cover:
  - _wait_candidate_is_sync_with_primary: uses is_alive() instead of None-check
  - _candidate_is_sync_with_primary: replay lag logic
  - _accept_failover: PostgresConnectionError returns None; unexpected errors propagate
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock


def _make_pgconsul():
    """
    Create a pgconsul instance bypassing __init__ entirely.

    We patch __init__ to do nothing, then inject the minimal attributes
    needed by the methods under test.
    """
    with patch('src.main.pgconsul.__init__', return_value=None):
        from src.main import pgconsul as PgConsul
        inst = PgConsul.__new__(PgConsul)

    # Minimal mocks required by _wait_candidate_is_sync_with_primary
    inst.db = MagicMock()
    inst.config = MagicMock()
    # iteration_timeout controls sleep between attempts
    inst.config.getfloat.return_value = 0.0  # instant sleep in tests
    # max_allowed_switchover_lag_ms — used by _candidate_is_sync_with_primary
    inst.config.getint.return_value = 0
    inst.config.getboolean.return_value = False

    return inst


# ---------------------------------------------------------------------------
# Tests: _candidate_is_sync_with_primary
# ---------------------------------------------------------------------------

class TestCandidateIsSyncWithPrimary:
    """_candidate_is_sync_with_primary checks replay lag for the candidate."""

    def _make(self):
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
        inst.config = MagicMock()
        return inst

    def _replica_info(self, app_name='replica1', replay_lag_msec=0):
        return {
            'application_name': app_name,
            'state': 'streaming',
            'replay_lag_msec': replay_lag_msec,
        }

    def test_returns_true_when_lag_within_limit(self):
        """Returns True when replay lag is within the allowed limit."""
        inst = self._make()
        inst.config.getint.return_value = 100  # 100ms allowed
        inst.config.getboolean.return_value = False

        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._candidate_is_sync_with_primary(
                [self._replica_info('replica1', replay_lag_msec=50)],
                'replica1.example.com',
            )
        assert result is True

    def test_returns_false_when_lag_exceeds_limit(self):
        """Returns False when lag exceeds limit and data loss not allowed."""
        inst = self._make()
        inst.config.getint.return_value = 100  # 100ms allowed
        inst.config.getboolean.return_value = False  # allow_potential_data_loss=False

        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._candidate_is_sync_with_primary(
                [self._replica_info('replica1', replay_lag_msec=200)],
                'replica1.example.com',
            )
        assert result is False

    def test_returns_true_when_lag_exceeds_but_data_loss_allowed(self):
        """Returns True when lag is high but allow_potential_data_loss=True."""
        inst = self._make()
        inst.config.getint.return_value = 100
        inst.config.getboolean.return_value = True  # allow_potential_data_loss=True

        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._candidate_is_sync_with_primary(
                [self._replica_info('replica1', replay_lag_msec=999)],
                'replica1.example.com',
            )
        assert result is True

    def test_returns_false_when_candidate_not_in_replics_info(self):
        """Returns False when candidate is not in replics_info."""
        inst = self._make()
        inst.config.getint.return_value = 100
        inst.config.getboolean.return_value = False

        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._candidate_is_sync_with_primary(
                [],  # empty list — no replicas
                'replica1.example.com',
            )
        assert result is False

    def test_returns_false_when_replay_lag_is_none(self):
        """Returns False when replay_lag_msec is missing."""
        inst = self._make()
        inst.config.getint.return_value = 100
        inst.config.getboolean.return_value = False

        info = {'application_name': 'replica1', 'state': 'streaming', 'replay_lag_msec': None}
        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._candidate_is_sync_with_primary([info], 'replica1.example.com')
        assert result is False


# ---------------------------------------------------------------------------
# Tests: _wait_candidate_is_sync_with_primary
# ---------------------------------------------------------------------------

class TestWaitCandidateIsSyncWithPrimary:
    """
    _wait_candidate_is_sync_with_primary should:
    - Return True when candidate catches up
    - Return True when primary becomes unreachable after max_attempts
    - Return False when timeout expires without sync
    """

    def _make(self):
        inst = _make_pgconsul()
        return inst

    def _replica_info(self, app_name='replica1', replay_lag_msec=0):
        return {
            'application_name': app_name,
            'state': 'streaming',
            'replay_lag_msec': replay_lag_msec,
        }

    def test_returns_true_when_candidate_synced(self):
        """Returns True immediately when candidate is in sync."""
        inst = self._make()
        inst.db.is_alive.return_value = True
        inst.db.get_replics_info.return_value = [self._replica_info('replica1', 0)]
        inst.config.getint.return_value = 1000  # 1000ms allowed lag
        inst.config.getboolean.return_value = False

        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'), \
             patch('time.time', side_effect=[0.0, 0.0] + [100.0] * 20):  # deadline far away
            result = inst._wait_candidate_is_sync_with_primary(
                'replica1.example.com', timeout=60
            )
        assert result is True

    def test_returns_true_when_primary_unreachable_after_max_attempts(self):
        """Returns True when primary is unreachable for max_attempts iterations."""
        inst = self._make()
        # is_alive always returns False — primary is unreachable
        inst.db.is_alive.return_value = False
        inst.config.getint.return_value = 0  # not used in this path
        inst.config.getboolean.return_value = False

        # Provide enough time values for the loop to run max_attempts=5 times
        time_values = [0.0] * 20 + [1000.0]  # deadline always far
        with patch('time.time', side_effect=time_values):
            result = inst._wait_candidate_is_sync_with_primary(
                'replica1.example.com', timeout=60, max_attempts=3
            )
        assert result is True

    def test_returns_false_when_timeout_expires(self):
        """Returns False when candidate never syncs within timeout."""
        inst = self._make()
        # primary is alive but lag is too high
        inst.db.is_alive.return_value = True
        inst.db.get_replics_info.return_value = [
            self._replica_info('replica1', replay_lag_msec=99999)
        ]
        inst.config.getint.return_value = 0  # 0ms allowed — lag always exceeds
        inst.config.getboolean.return_value = False

        # Simulate timeout expiry: first calls are 0.0, then beyond deadline.
        # Extra values absorb time.time() calls made by logging on some Python versions.
        with patch('time.time', side_effect=[0.0, 0.0] + [100.0] * 20), \
             patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._wait_candidate_is_sync_with_primary(
                'replica1.example.com', timeout=50, max_attempts=5
            )
        assert result is False

    def test_primary_alive_calls_get_replics_info(self):
        """When primary is alive, get_replics_info is called to check sync."""
        inst = self._make()
        inst.db.is_alive.return_value = True
        inst.db.get_replics_info.return_value = [self._replica_info('replica1', 0)]
        inst.config.getint.return_value = 9999
        inst.config.getboolean.return_value = False

        with patch('src.helpers.app_name_from_fqdn', return_value='replica1'), \
             patch('time.time', side_effect=[0.0, 0.0] + [100.0] * 20):
            inst._wait_candidate_is_sync_with_primary('replica1.example.com', timeout=60)

        inst.db.get_replics_info.assert_called_once_with('primary')

    def test_primary_dead_does_not_call_get_replics_info(self):
        """When primary is unreachable, get_replics_info is never called."""
        inst = self._make()
        inst.db.is_alive.return_value = False
        inst.config.getint.return_value = 0
        inst.config.getboolean.return_value = False

        time_values = [0.0] * 20 + [1000.0]
        with patch('time.time', side_effect=time_values):
            inst._wait_candidate_is_sync_with_primary(
                'replica1.example.com', timeout=60, max_attempts=1
            )

        inst.db.get_replics_info.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: _all_side_replicas_turned_to_the_candidate
# ---------------------------------------------------------------------------

class TestAllSideReplicasTurnedToTheCandidate:
    """_all_side_replicas_turned_to_the_candidate returns False on DB error."""

    def _make(self):
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
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


# ---------------------------------------------------------------------------
# Tests: _wait_candidate_is_sync_with_primary — DB error path
# ---------------------------------------------------------------------------

class TestWaitCandidateConnectionError:
    """_wait_candidate_is_sync_with_primary treats DB error like primary unreachable."""

    def _make(self):
        inst = _make_pgconsul()
        return inst

    def test_db_error_increments_attempt_like_dead_primary(self):
        """PostgresConnectionError from get_replics_info → attempt++, eventually returns True."""
        from src.exceptions import PostgresConnectionError
        inst = self._make()
        inst.db.is_alive.return_value = True  # primary appears alive via is_alive
        inst.db.get_replics_info.side_effect = PostgresConnectionError("db down")
        inst.config.getint.return_value = 0
        inst.config.getboolean.return_value = False

        # Enough time values to run max_attempts=1 iteration
        time_values = [0.0] * 10 + [1000.0]
        with patch('time.time', side_effect=time_values), \
             patch('src.helpers.app_name_from_fqdn', return_value='replica1'):
            result = inst._wait_candidate_is_sync_with_primary(
                'replica1.example.com', timeout=60, max_attempts=1
            )
        assert result is True


# ---------------------------------------------------------------------------
# Tests: _accept_failover — PostgresConnectionError must not kill the process
# ---------------------------------------------------------------------------

class TestAcceptFailoverConnectionError:
    """_accept_failover must not call sys.exit on PostgresConnectionError."""

    def _make(self):
        from src.main import pgconsul as PgConsul
        from src.exceptions import PostgresConnectionError
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
        inst.db = MagicMock()
        inst.zk = MagicMock()
        inst.config = MagicMock()
        inst.config.getfloat.return_value = 0.0
        inst.config.getint.return_value = 0
        inst.config.getboolean.return_value = False
        inst._master_lost_ts = 0.0
        return inst

    def test_returns_none_on_postgres_connection_error(self):
        """PostgresConnectionError during failover checks → return None, no sys.exit."""
        from src.exceptions import PostgresConnectionError
        inst = self._make()
        inst._can_do_failover = MagicMock(side_effect=PostgresConnectionError("db down"))

        with patch('sys.exit') as mock_exit:
            result = inst._accept_failover(switchover_in_progress=False)

        assert result is None
        mock_exit.assert_not_called()

    def test_propagates_unexpected_exception(self):
        """Unexpected Exception (not PostgresConnectionError) → propagates to run_iteration()."""
        inst = self._make()
        inst._can_do_failover = MagicMock(side_effect=RuntimeError("unexpected"))

        with patch('sys.exit') as mock_exit:
            with pytest.raises(RuntimeError):
                inst._accept_failover(switchover_in_progress=False)

        mock_exit.assert_not_called()


class TestGetStreamingReplicas:

    def _make(self):
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
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
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
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


class TestMakeElection:

    def _make(self):
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
        inst.db = MagicMock()
        inst.zk = MagicMock()
        inst.config = MagicMock()
        inst.config.getint.return_value = 10
        inst._replication_manager = MagicMock()
        return inst

    def test_returns_false_without_sys_exit_on_election_error(self):
        from src.failover_election import ElectionError
        inst = self._make()
        inst.db.get_wal_receive_lsn.return_value = 0
        inst.zk.get_alive_hosts.return_value = []
        with patch('src.main.helpers.make_current_replics_quorum', return_value=[]), \
             patch('src.main.FailoverElection') as MockElection:
            MockElection.return_value.make_election.side_effect = ElectionError("election failed")
            with patch('sys.exit') as mock_exit:
                result = inst._make_election(replica_infos=[], allow_data_loss=False)
        assert result is False
        mock_exit.assert_not_called()


class TestDoPrimarySwitchoverCosmetic:
    """_do_primary_switchover continues when cosmetic operations raise PostgresConnectionError."""

    def _make(self):
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
        inst.db = MagicMock()
        inst.zk = MagicMock()
        inst.config = MagicMock()
        inst.config.getfloat.return_value = 0.0
        inst.config.getint.return_value = 0
        inst.config.getboolean.return_value = False
        inst._replication_manager = MagicMock()
        return inst

    def test_switchover_continues_when_checkpoint_raises(self):
        """checkpoint() raises PostgresConnectionError → switchover continues, pgpooler('stop') is called."""
        from src.exceptions import PostgresConnectionError
        inst = self._make()

        inst._replication_manager.change_replication_to_sync_host.return_value = True
        inst.zk.write_switchover_candidate.return_value = True
        inst.zk.write_switchover_side_replicas.return_value = True
        inst.zk.get_switchover_state.return_value = 'candidate_found'
        inst.db.get_replics_info.side_effect = PostgresConnectionError("db down")
        inst.db.checkpoint.side_effect = PostgresConnectionError("db down")
        # abort early after pgpooler to avoid mocking further steps
        inst._debug_failure = MagicMock(return_value=True)

        db_state = {'replics_info': []}
        zk_state = {}

        with patch('src.main.log_event'), \
             patch('src.main.helpers.await_for', return_value=True), \
             patch.object(inst, '_start_timing'), \
             patch.object(inst, '_get_streaming_replicas', return_value=[]), \
             patch.object(inst, '_store_replics_info'):
            inst._do_primary_switchover('replica1.example.com', db_state, zk_state)

        inst.db.pgpooler.assert_called_with('stop')

    def test_switchover_continues_when_replics_info_update_raises(self):
        """get_replics_info raises in cosmetic block → switchover continues, checkpoint is still attempted."""
        from src.exceptions import PostgresConnectionError
        inst = self._make()

        inst._replication_manager.change_replication_to_sync_host.return_value = True
        inst.zk.write_switchover_candidate.return_value = True
        inst.zk.write_switchover_side_replicas.return_value = True
        inst.zk.get_switchover_state.return_value = 'candidate_found'
        inst.db.get_replics_info.side_effect = PostgresConnectionError("db down")
        inst.db.checkpoint.return_value = True
        inst._debug_failure = MagicMock(return_value=True)

        db_state = {'replics_info': []}
        zk_state = {}

        with patch('src.main.log_event'), \
             patch('src.main.helpers.await_for', return_value=True), \
             patch.object(inst, '_start_timing'), \
             patch.object(inst, '_get_streaming_replicas', return_value=[]), \
             patch.object(inst, '_store_replics_info'):
            inst._do_primary_switchover('replica1.example.com', db_state, zk_state)

        inst.db.checkpoint.assert_called_once()
        inst.db.pgpooler.assert_called_with('stop')


# ---------------------------------------------------------------------------
# Tests: _check_postgresql_streaming
# ---------------------------------------------------------------------------

class TestCheckPostgresqlStreaming:
    """_check_postgresql_streaming returns None (not raises) when check_walreceiver raises PostgresConnectionError."""

    def _make(self):
        from src.main import pgconsul as PgConsul
        with patch('src.main.pgconsul.__init__', return_value=None):
            inst = PgConsul.__new__(PgConsul)
        inst.db = MagicMock()
        inst.zk = MagicMock()
        inst.config = MagicMock()
        inst.config.getboolean.return_value = False  # replication_slots_polling=False
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
