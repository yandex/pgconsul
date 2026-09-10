# encoding: utf-8
"""
Unit tests for replica_iter switchover phase gate.

Reproduces the flaky anywhere_switchover.feature:67 failure where the
HA replica (sw2_replica) returns to the switchover candidate during the
SYNC_SET phase — before the candidate has created replication slots —
causing "replication slot does not exist" errors and a fallback to
pg_rewind.

Root cause: replica_iter() returns to the candidate as soon as
sw_record.candidate is set (written during SCHEDULED via WriteCandidate),
without checking that the switchover has advanced to INITIATED or later.
The candidate only creates replication slots in plan_initiated (phase
INITIATED, via CreateSlots command), so connecting during SYNC_SET races
with slot creation.

The non-HA path (_accept_switchover_non_ha) already gates on
initiated/candidate_found; the HA path in replica_iter does not.

The fix: add a phase gate in replica_iter — only return to the candidate
when the switchover phase is INITIATED or later.
"""

from unittest.mock import MagicMock, patch

from src.switchover import SwitchoverPhase, SwitchoverRecord


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
        switchover_replica_turn_timeout=0.0,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        election_timeout=0,
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
    inst._is_single_node = False
    inst.zk = MagicMock()
    inst._executor = MagicMock()
    # ZK path constants must be real strings for dict lookups.
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover/state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover/master'
    inst.zk.SWITCHOVER_CANDIDATE = 'switchover/candidate'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'switchover/side_replicas'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'
    inst.zk.PRIMARY_LOCK_PATH = 'master'
    return inst


# Hostnames in the test cluster.
_REPLICA_FQDN = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
_CANDIDATE_FQDN = 'pgconsul_postgresql3_1.pgconsul_pgconsul_net'
_PRIMARY_FQDN = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'

_REPLICA_APP = 'pgconsul_postgresql2_1_pgconsul_pgconsul_net'


def _build_zk_state():
    """Build a zk_state dict with the keys replica_iter reads."""
    return {
        'alive': True,
        'lock_holder': _PRIMARY_FQDN,
        'replics_info': [
            {'application_name': _REPLICA_APP, 'state': 'streaming'},
        ],
    }


def _build_db_state():
    return {
        'replics_info': [
            {'application_name': _REPLICA_APP, 'state': 'streaming'},
        ],
    }


class TestReplicaSwitchoverPhaseGate:
    """replica_iter must not return to candidate before INITIATED phase.

    Reproduces anywhere_switchover.feature:67 (flaky): the HA replica
    connects to the candidate during SYNC_SET, before slots are created,
    gets "replication slot does not exist", and falls back to pg_rewind.
    """

    @patch('src.main.helpers.get_hostname', return_value=_REPLICA_FQDN)
    @patch('src.main.helpers.app_name_from_fqdn', return_value=_REPLICA_APP)
    @patch('src.main.SwitchoverRecord.from_zk_state')
    def test_replica_waits_when_phase_is_sync_set(self, mock_from_zk, _app, _host):
        """During SYNC_SET, the candidate has not yet created slots.

        The HA replica must NOT return to the candidate until the
        switchover advances to INITIATED (when the candidate creates
        slots via plan_initiated). Returning early races with slot
        creation and causes "replication slot does not exist" → rewind.
        """
        inst = _make_pgconsul()
        inst._check_replica_switchover = MagicMock(return_value=True)
        inst.write_host_stat = MagicMock()
        inst._return_to_cluster = MagicMock()

        # Switchover record: phase SYNC_SET, candidate already chosen.
        mock_from_zk.return_value = SwitchoverRecord(
            hostname=_PRIMARY_FQDN,
            timeline=1,
            destination=None,
            phase=SwitchoverPhase.SYNC_SET,
            candidate=_CANDIDATE_FQDN,
            side_replicas=[],
        )

        # Lock holder is the old primary (still alive).
        inst.zk.get_current_lock_holder.return_value = _PRIMARY_FQDN

        inst.replica_iter(_build_db_state(), _build_zk_state())

        # The replica must NOT return to the candidate during SYNC_SET.
        inst._return_to_cluster.assert_not_called()

    @patch('src.main.helpers.get_hostname', return_value=_REPLICA_FQDN)
    @patch('src.main.helpers.app_name_from_fqdn', return_value=_REPLICA_APP)
    @patch('src.main.SwitchoverRecord.from_zk_state')
    def test_replica_returns_to_candidate_when_phase_is_initiated(self, mock_from_zk, _app, _host):
        """During INITIATED, the candidate has created slots and is ready.

        The HA replica SHOULD return to the candidate once the switchover
        has advanced to INITIATED. This is the normal switchover flow.
        """
        inst = _make_pgconsul()
        inst._check_replica_switchover = MagicMock(return_value=True)
        inst.write_host_stat = MagicMock()
        inst._return_to_cluster = MagicMock()

        mock_from_zk.return_value = SwitchoverRecord(
            hostname=_PRIMARY_FQDN,
            timeline=1,
            destination=None,
            phase=SwitchoverPhase.INITIATED,
            candidate=_CANDIDATE_FQDN,
            side_replicas=[],
        )

        inst.zk.get_current_lock_holder.return_value = _PRIMARY_FQDN

        inst.replica_iter(_build_db_state(), _build_zk_state())

        # The replica SHOULD return to the candidate during INITIATED.
        inst._return_to_cluster.assert_called_once_with(
            _CANDIDATE_FQDN, 'replica', is_dead=False, skip_check=True,
        )
