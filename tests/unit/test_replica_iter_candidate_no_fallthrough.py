# encoding: utf-8
"""
Red unit test: replica_iter must return after running the candidate state machine.

Reproduces the stuck targeted_switchover.feature scenario "Check targeted
switchover": after the candidate (postgresql2) promotes itself (phase=PROMOTED),
replica_iter runs the candidate machine via ``self._executor.run(self._cand_machine, obs)``
but does NOT return. Execution falls through into the "Not the candidate" block
(lines ~991-1002) where ``sw_record.candidate == my_hostname`` (postgresql2 is
both the candidate and the new primary), so it calls
``_return_to_cluster(myself, 'replica')`` — the new primary tries to become a
replica of itself.

This triggers pg_rewind from itself (Connection refused), which fails and leaves
``host_op='rewind'`` in ZK. On subsequent iterations
``is_op_destructive('rewind')`` (helpers.py:321) blocks lock acquisition forever,
so the cluster is left without a primary — the test hangs until timeout.

Root cause: the return-to-cluster refactoring (staged changes) removed the
``return`` statement after ``self._executor.run(self._cand_machine, obs)`` in
replica_iter. In HEAD (commit 8982b9a) the return was present.

The fix: restore ``return`` after the candidate machine execution in replica_iter.
The same bug exists in primary_iter (missing return after _sw_machine) and
_run_failover_step (missing return after participant machine) — see the report.
"""

from unittest.mock import MagicMock, patch

from src.switchover import SwitchoverPhase, SwitchoverRecord


# ---------------------------------------------------------------------------
# Hostnames in the test cluster.
# ---------------------------------------------------------------------------
_PRIMARY_FQDN = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
_CANDIDATE_FQDN = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
_CANDIDATE_APP = 'pgconsul_postgresql2_1'


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
    )
    inst._timings = MagicMock()
    inst._maintenance = MagicMock()
    inst._replication_manager = MagicMock()
    inst._is_single_node = False
    inst.zk = MagicMock()
    inst._executor = MagicMock()
    inst._cand_machine = MagicMock()
    inst._sw_machine = MagicMock()
    # ZK path constants must be real strings for dict lookups.
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover/state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover/master'
    inst.zk.SWITCHOVER_CANDIDATE = 'switchover/candidate'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'switchover/side_replicas'
    inst.zk.TIMELINE_INFO_PATH = 'timeline'
    inst.zk.PRIMARY_LOCK_PATH = 'master'
    inst.zk.FAILOVER_STATE_PATH = 'failover/state'
    inst.zk.CURRENT_PROMOTING_HOST = 'current_promoting_host'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    return inst


def _build_zk_state():
    """Build a zk_state dict with the keys replica_iter reads."""
    return {
        'alive': True,
        'lock_holder': _PRIMARY_FQDN,
        'replics_info': [
            {'application_name': _CANDIDATE_APP, 'state': 'streaming'},
        ],
    }


def _build_db_state():
    return {
        'replics_info': [
            {'application_name': _CANDIDATE_APP, 'state': 'streaming'},
        ],
        'primary_fqdn': _PRIMARY_FQDN,
    }


class TestReplicaIterCandidateNoFallthrough:
    """replica_iter must return immediately after running the candidate machine.

    Reproduces: targeted_switchover.feature — "Check targeted switchover" hangs.
    Bug: after ``self._executor.run(self._cand_machine, obs)`` (candidate side,
    phase=PROMOTED) there is no ``return``, so execution falls through into the
    "Not the candidate" block where ``sw_record.candidate == my_hostname`` and
    ``_return_to_cluster(myself, 'replica')`` is called — the new primary tries
    to become a replica of itself.
    """

    @patch('src.main.helpers.get_hostname', return_value=_CANDIDATE_FQDN)
    @patch('src.main.helpers.app_name_from_fqdn', return_value=_CANDIDATE_APP)
    @patch('src.main.SwitchoverRecord.from_zk_state')
    def test_replica_iter_returns_after_candidate_machine_promoted(self, mock_from_zk, _app, _host):
        """After candidate machine runs (phase=PROMOTED), replica_iter must NOT
        fall through to _return_to_cluster.

        With the bug (missing return), _return_to_cluster IS called because
        sw_record.candidate == my_hostname and phase PROMOTED is in the
        phase-gate list — the new primary tries to rewind from itself.
        """
        inst = _make_pgconsul()
        inst._check_replica_switchover = MagicMock(return_value=True)
        inst._check_failover_fallback = MagicMock(return_value=inst._NO_FALLBACK)
        inst.write_host_stat = MagicMock()
        inst._build_switchover_observation = MagicMock(return_value=MagicMock())
        inst._return_to_cluster = MagicMock()

        # Switchover record: phase PROMOTED, candidate is this host (postgresql2).
        mock_from_zk.return_value = SwitchoverRecord(
            hostname=_PRIMARY_FQDN,
            timeline=1,
            destination=None,
            phase=SwitchoverPhase.PROMOTED,
            candidate=_CANDIDATE_FQDN,
            side_replicas=[],
        )

        # Lock holder is still the old primary (switchover not fully cleaned up).
        inst.zk.get_current_lock_holder.return_value = _PRIMARY_FQDN

        inst.replica_iter(_build_db_state(), _build_zk_state())

        # The candidate machine MUST be executed.
        inst._executor.run.assert_called_once_with(inst._cand_machine, inst._build_switchover_observation.return_value)

        # _return_to_cluster must NOT be called — we are the candidate/primary,
        # not a replica that needs to return to the cluster.
        inst._return_to_cluster.assert_not_called()

    @patch('src.main.helpers.get_hostname', return_value=_CANDIDATE_FQDN)
    @patch('src.main.helpers.app_name_from_fqdn', return_value=_CANDIDATE_APP)
    @patch('src.main.SwitchoverRecord.from_zk_state')
    def test_replica_iter_returns_after_candidate_machine_candidate_found(self, mock_from_zk, _app, _host):
        """Same fall-through bug during CANDIDATE_FOUND phase.

        When the candidate machine runs during CANDIDATE_FOUND (acquiring lock),
        replica_iter must return and not fall through to _return_to_cluster.
        """
        inst = _make_pgconsul()
        inst._check_replica_switchover = MagicMock(return_value=True)
        inst._check_failover_fallback = MagicMock(return_value=inst._NO_FALLBACK)
        inst.write_host_stat = MagicMock()
        inst._build_switchover_observation = MagicMock(return_value=MagicMock())
        inst._return_to_cluster = MagicMock()

        mock_from_zk.return_value = SwitchoverRecord(
            hostname=_PRIMARY_FQDN,
            timeline=1,
            destination=None,
            phase=SwitchoverPhase.CANDIDATE_FOUND,
            candidate=_CANDIDATE_FQDN,
            side_replicas=[],
        )

        inst.zk.get_current_lock_holder.return_value = _PRIMARY_FQDN

        inst.replica_iter(_build_db_state(), _build_zk_state())

        inst._executor.run.assert_called_once()
        # Must NOT fall through to _return_to_cluster.
        inst._return_to_cluster.assert_not_called()
