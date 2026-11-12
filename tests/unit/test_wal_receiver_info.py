"""Receiver normalization at model, persistence and decision boundaries."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul
from src.types import DbState, WalReceiverInfo, ZkState
from tests.unit.state_samples import DB_DEFAULTS
from tests.unit.state_fixtures import state_cluster, state_consul  # noqa: F401


DEFAULTS = {'pid': 0, 'status': '', 'slot_name': '', 'last_msg_receipt_time_msec': 0, 'conninfo': ''}


@pytest.mark.parametrize('payload', [
    {}, dict.fromkeys(DEFAULTS), {'status': 'streaming'},
    {'pid': -1, 'last_msg_receipt_time_msec': -1000},
    {'pid': 123, 'status': 'streaming', 'slot_name': 'slot',
     'last_msg_receipt_time_msec': 123456, 'conninfo': 'host=primary'},
])
def test_receiver_preserves_legacy_record(payload):
    receiver = WalReceiverInfo.from_dict(payload)
    expected = payload
    assert bool(receiver) == bool(payload)
    assert receiver.to_dict() == expected
    assert WalReceiverInfo.from_dict(json.loads(json.dumps(receiver.to_dict()))) == receiver


@pytest.mark.parametrize('slot_name', [None, '', 'slot'])
@pytest.mark.parametrize('conninfo', [None, '', 'host=primary'])
def test_receiver_preserves_nullable_strings(slot_name, conninfo):
    receiver = WalReceiverInfo.from_dict({'slot_name': slot_name, 'conninfo': conninfo})
    assert receiver.slot_name == slot_name
    assert receiver.conninfo == conninfo


def test_direct_receiver_and_later_updates_are_serialized():
    receiver = WalReceiverInfo()
    assert receiver
    assert receiver.to_dict() == DEFAULTS
    receiver.pid = 123
    receiver.status = 'streaming'
    assert receiver.to_dict() == {**DEFAULTS, 'pid': 123, 'status': 'streaming'}
    assert WalReceiverInfo(pid=123, status='streaming').to_dict() == receiver.to_dict()


@pytest.mark.parametrize('payload, expected', [
    ({}, {}), ({'wal_receiver': None}, {'wal_receiver': None}),
    ({'wal_receiver': {}}, {'wal_receiver': {}}),
    ({'wal_receiver': {'status': None}}, {'wal_receiver': {'status': None}}),
])
def test_db_state_preserves_receiver_and_previous_state(payload, expected):
    state = DbState.from_dict(payload)
    assert state.to_dict() == expected
    assert (state.wal_receiver is None) == (expected.get('wal_receiver') is None)
    assert DbState.from_dict({**payload, 'prev_state': payload}).to_dict() == {**expected, 'prev_state': payload}


@pytest.mark.parametrize('payload, expected', [
    ({}, {}), (None, None), ({'status': None}, {'status': None}),
    ({'status': 'streaming'}, {'status': 'streaming'}),
])
def test_zk_receiver_read_and_explicit_write(state_cluster, payload, expected):
    path = 'all_hosts/upstream/wal_receiver'
    state_cluster.records[path] = json.dumps(payload)
    receiver = state_cluster.zk.get_host_wal_receiver('upstream')
    assert (receiver is None) == (expected is None)
    assert state_cluster.zk.write_host_wal_receiver(receiver, 'upstream')
    assert json.loads(state_cluster.records[path]) == expected


@pytest.mark.parametrize('payload', [None, {}, {'status': None}])
def test_host_stat_skips_absent_receiver_but_publishes_partial_record(state_consul, state_cluster, payload):
    path = 'all_hosts/me/wal_receiver'
    previous = {'status': 'streaming', 'pid': 123}
    state_cluster.records[path] = json.dumps(previous)
    state_consul.write_host_stat('me', DbState.from_dict({'wal_receiver': payload}))
    assert json.loads(state_cluster.records[path]) == (previous if payload is None else payload)


@pytest.mark.parametrize('last_write', [0, 95])
@pytest.mark.parametrize('payload, stale', [
    (None, True), ({}, True), ({'last_msg_receipt_time_msec': None}, True),
    ({'last_msg_receipt_time_msec': 0}, True), ({'last_msg_receipt_time_msec': 99000}, False),
])
def test_detached_replica_uses_receiver_time_without_normalization(payload, stale, last_write):
    instance = Pgconsul.__new__(Pgconsul)
    instance.config = SimpleNamespace(close_detached_after=10)
    instance.last_zk_host_stat_write = last_write
    instance.db = MagicMock()
    with patch('src.main.time.time', return_value=100):
        if payload == {'last_msg_receipt_time_msec': None} and last_write == 0:
            with pytest.raises(TypeError):
                instance.handle_detached_replica(DbState.from_dict({'wal_receiver': payload}))
            return
        instance.handle_detached_replica(DbState.from_dict({'wal_receiver': payload}))
    if stale and last_write == 0:
        instance.db.pgpooler.assert_called_once_with('stop')
    else:
        instance.db.pgpooler.assert_not_called()


@pytest.mark.parametrize('local_payload', [None, {}, {'status': None}])
@pytest.mark.parametrize('source_payload', [None, {}, {'status': None}, {'status': 'streaming'}])
def test_cascade_distinguishes_receiver_presence_from_source_streaming(state_consul, state_cluster, local_payload, source_payload):
    cluster = state_cluster
    state_consul.config.stream_from = 'upstream'
    state_consul.config.replication_slots_polling = True
    cluster.holders.update({'quorum/members/me': ['me'], 'replication_sources/old': ['me']})
    cluster.records.update({
        'replication_sources/old': '', 'replication_sources/upstream': '',
        'all_hosts/upstream/replics_info': '[{"application_name":"me","state":"streaming"}]',
        'all_hosts/upstream/wal_receiver': json.dumps(source_payload),
    })
    # The external DB remains in recovery after its upstream is reconfigured.
    state_consul.db.get_role.return_value = 'replica'
    state_consul.non_ha_replica_iter(
        DbState.from_dict({'wal_receiver': local_payload}), ZkState(alive=True, lock_holder='primary', replics_info=[]),
    )
    switches_upstream = not local_payload and source_payload == {'status': 'streaming'}
    assert cluster.holders.get('replication_sources/upstream', []) == (['me'] if local_payload or switches_upstream else [])
    assert cluster.holders['quorum/members/me'] == (['me'] if local_payload else [])
    if local_payload or switches_upstream:
        assert cluster.holders['replication_sources/old'] == []
    if switches_upstream:
        state_consul.db.recovery_conf.assert_called_with('create', 'upstream')
        state_consul.db.reload.assert_called()
        state_consul.db.ensure_replaying_wal.assert_called()
    else:
        state_consul.db.recovery_conf.assert_not_called()
