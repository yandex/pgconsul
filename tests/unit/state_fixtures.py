"""State scenarios with real domain collaborators and fake external adapters."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.main import Pgconsul, PgconsulConfig
from src.pg import Postgres
from src.replication_manager import ReplicationManager, ReplicationManagerConfig
from src.slot_manager import ReplicationSlotManager, ReplicationSlotManagerConfig
from src.timings import TimingTracker
from src.types import DbState, ReplicationState
from src.zk import Zookeeper, ZookeeperConfig


@pytest.fixture
def state_cluster(monkeypatch):
    records, holders = {}, {}
    transport = MagicMock()

    def children(path):
        prefix = path + '/'
        return sorted({key[len(prefix):].split('/')[0] for key in records.keys() | holders.keys() if key.startswith(prefix)})

    def delete(path, recursive=False):
        for key in list(records):
            if key == path or (recursive and key.startswith(path + '/')):
                del records[key]
        return True

    def lock(path, owner):
        name = path.removeprefix('/test/')
        handle = MagicMock()
        handle.contenders.side_effect = lambda: list(holders.get(name, []))

        def acquire(**kwargs):
            owners = holders.setdefault(name, [])
            if owner not in owners:
                owners.append(owner)
            return True

        def release():
            owners = holders.get(name, [])
            if owner in owners:
                owners.remove(owner)
            return True

        handle.acquire.side_effect = acquire
        handle.release.side_effect = release
        return handle

    transport.get.side_effect = records.get
    transport.write.side_effect = lambda path, value, **kwargs: records.__setitem__(path, value) or True
    transport.ensure_path.side_effect = lambda path: records.setdefault(path, '') or True
    transport.exists.side_effect = lambda path: path in records
    transport.delete.side_effect = delete
    transport.get_children.side_effect = children
    transport.is_alive.return_value = True
    transport.is_connected.return_value = True
    transport.reconnect.return_value = True
    transport.make_lock.side_effect = lock
    transport.make_read_lock.side_effect = lock
    monkeypatch.setattr('src.helpers.get_hostname', lambda: 'me')
    monkeypatch.setattr('src.main.get_hostname', lambda: 'me')
    monkeypatch.setattr('time.sleep', lambda seconds: None)
    monkeypatch.setattr('signal.signal', lambda *args: None)
    monkeypatch.delenv('NOTIFY_SOCKET', raising=False)
    zk = Zookeeper(transport, ZookeeperConfig(False, 0.0, '/test/', 'me'))
    return SimpleNamespace(zk=zk, records=records, holders=holders, transport=transport)


@pytest.fixture
def state_consul(state_cluster, tmp_path):
    config = PgconsulConfig(
        welcome_message='', working_dir=str(tmp_path), iteration_timeout=0.0,
        quorum_commit=False, use_lwaldump=False, update_prio_in_zk=False,
        use_replication_slots=False, replication_slots_polling=False, priority='100',
        stream_from=None, autofailover=False, switchover_replica_turn_timeout=0.0,
        switchover_rollback_timeout=0.0, switchover_catchup_timeout=0.0,
        max_rewind_retries=0, election_timeout=0, do_consecutive_primary_switch=False,
        max_allowed_switchover_lag_ms=0, allow_potential_data_loss=False,
        close_detached_after=0.0, start_pooler=False, recovery_timeout=0.0,
        can_delayed=False, primary_switch_disable_archive_restore=False,
        primary_switch_checks=0, primary_switch_restart=False,
        primary_unavailability_timeout=0.0, walreceiver_disable_timeout=0.0,
        min_failover_timeout=0.0, change_replication_type=False,
        sync_replication_in_maintenance=False, promote_checkpoint_sql=None,
        failure_name=None, failure_count=100000000,
        sleep_before_disable_walreceiver=0.0, election_lsn_read_sleep=0.0,
        election_loser_timeout=0,
    )
    db = MagicMock(spec=Postgres)
    db.role = 'primary'
    db.get_prev_state.return_value = None
    db.is_alive.return_value = True
    db.is_alive_and_in_terminal_state.return_value = (True, True)
    db.get_role.return_value = 'primary'
    db.get_timeline.return_value = 7
    db.get_state.return_value = DbState(alive=True, role='primary', timeline=7, replics_info=[])
    db.get_replics_info.return_value = []
    db.get_replication_state.return_value = ReplicationState('async', None)
    db.pgpooler.side_effect = lambda action: (True, True) if action == 'status' else 0
    db.stop_postgresql.return_value = 0
    db.recovery_conf.return_value = 0
    db.get_database_cluster_state.return_value = 'in archive recovery'
    db.is_host_unreachable.return_value = False
    db.change_replication_type.return_value = True
    zk = state_cluster.zk
    replication = ReplicationManager(
        ReplicationManagerConfig(100, 0.0, 'count', '0-0', '0-0', 100.0, 0.0, 0.0), db, zk,
    )
    slots = ReplicationSlotManager(db, zk, ReplicationSlotManagerConfig(False, False, 0))
    return Pgconsul(config, db, zk, MagicMock(), replication, slots, TimingTracker(zk, None))
