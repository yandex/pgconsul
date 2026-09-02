# coding: utf8
"""Regression test for the infinite failover restart loop.

Reproduces MDB-41951 behave hang: async.feature:46 scenario
"No automatic failover without durability -- @1.1 without replication slots".

When sync_quorum is empty (async mode),
the promote-safe gate permanently fails. The cycle is:

  1. Failover initialization persists its first phase.
  2. The promote-safe gate fails in a later phase.
  3. Cleanup removes failover state.
  4. Next iteration: primary still dead → phase=None → goto 1.

This creates an infinite loop: detected → failed → cleanup → detected → ...
In behave, the test hangs for 360s until timeout.

Failover entry checks promote safety before writing the first phase, so
failover never starts and failover_state stays absent.

The fix: check promote-safe before `_start_failover` persists its first phase.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.failover import (
    FailoverMachine,
    FailoverObservation,
    FailoverRequest,
)


def _make_instance():
    from src.main import PgconsulConfig, Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='',
        working_dir='/tmp',
        iteration_timeout=0.0,
        quorum_commit=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='2',
        stream_from=None,
        autofailover=True,
        switchover_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        do_consecutive_primary_switch=False,
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
    inst._master_lost_ts = 0.0
    inst._is_single_node = False
    inst._replication_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    inst._debug_failure = MagicMock(return_value=False)
    inst._failover_machine = FailoverMachine()
    inst._executor = MagicMock()
    inst._executor.set_iteration_state = MagicMock()
    return inst


class TestFailoverInfiniteRestart:
    """async mode + no data loss → failover must not restart infinitely.

    Entry checks must reject the operation before its first persistent phase.
    """

    def test_detected_is_not_written_when_promote_is_unsafe(self):
        inst = _make_instance()
        inst.zk.FAILOVER_STATE_PATH = 'failover_state'
        inst.zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
        inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
        observation = FailoverObservation(
            phase=None,
            my_hostname='host1',
            role='replica',
            lock_holder=None,
            is_coordinator=True,
            election_winner=None,
            votes={},
            alive_hosts=['host2', 'host3'],
            replics_info=[{'application_name': 'host2', 'state': 'streaming'}],
            host_priority=2,
            last_failover_ts=None,
            last_primary_availability_ts=0.0,
            is_primary_unreachable=True,
            is_replaying_wal=False,
            failover_started_ts=None,
            downtime_started_ts=None,
            zk_timeline=5,
            local_timeline=5,
            quorum_size=2,
            autofailover=True,
            durability=None,
            current_time=9_999_999_999.0,
        )
        inst._build_failover_observation = MagicMock(return_value=observation)
        inst.zk.get_current_lock_holder.return_value = None
        db_state = {'role': 'replica', 'timeline': 5}
        zk_state = {}

        for _ in range(3):
            # The missing-primary iteration is claimed, but no persistent
            # failover phase may be created while promotion is unsafe.
            assert inst._start_failover(db_state, zk_state) is False

        inst.zk.write_failover_state.assert_not_called()
        inst._executor.run.assert_not_called()

    @pytest.mark.parametrize('with_data_loss, expected', [(False, False), (True, True)])
    def test_only_explicit_manual_data_loss_freezes_ha_members(
        self, with_data_loss, expected,
    ):
        """Only an operator request may bypass absent durability metadata."""
        inst = _make_instance()
        inst.zk.FAILOVER_STATE_PATH = 'failover_state'
        inst.zk.ELECTION_MANAGER_LOCK_PATH = 'election_manager'
        inst.zk.PRIMARY_LOCK_PATH = 'leader'
        inst.zk.ELECTION_VOTES_PATH = 'votes'
        inst.zk.ELECTION_WINNER_PATH = 'winner'
        inst.zk.FAILOVER_PARTICIPANTS_PATH = 'participants'
        inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
        inst._failover_machine = MagicMock()
        inst._failover_machine.can_start.return_value = True
        inst.zk.get_current_lock_holder.return_value = None
        inst.zk.is_lock_holder.return_value = True
        inst.zk.delete.return_value = True
        inst.zk.write_failover_members.return_value = True
        inst.zk.write_failover_version.return_value = True
        inst.zk.write_failover_state.return_value = True
        inst.zk.get_desired_primary.return_value = (None, None)
        inst.zk.write_desired_primary.return_value = 0
        inst.zk.get_ha_hosts.return_value = ['old-primary', 'host1', 'host2']
        observation = MagicMock(
            durability=None,
        )
        inst._build_failover_observation = MagicMock(return_value=observation)

        request = FailoverRequest(
            'old-primary', 'operation-1', with_data_loss=with_data_loss,
            winner='host1' if with_data_loss else None,
        )
        assert inst._initialize_failover(
            {'role': 'replica', 'primary_fqdn': 'old-primary'},
            {},
            automatic=False,
            manual_request=request,
        ) is expected

        if expected:
            inst.zk.write_failover_members.assert_called_once_with(['host1', 'host2'])
        else:
            inst.zk.write_failover_members.assert_not_called()
