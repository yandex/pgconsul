"""Wire compatibility for typed records, including absent and nullable fields."""
import json

from src.types import ReplicationState

import pytest

from src.types import (
    DbState, MaintenanceState, ReplicaInfo, SsnInfo,
    SwitchoverPrimaryInfo, WalReceiverInfo, ZkState,
)
from tests.unit.test_pg import _make_postgres
from tests.unit.state_samples import DB_DEFAULTS
from tests.unit.test_switchover_records import cluster  # noqa: F401


REPLICA_DEFAULTS = {
    'pid': 0, 'application_name': '', 'client_hostname': '', 'state': '',
    'primary_location': None, 'write_location_diff': None, 'replay_lag_msec': None,
    'reply_time_ms': None, 'sync_state': '', 'priority': 0,
}


@pytest.mark.parametrize('payload', [
    {},
    {'pid': 1, 'application_name': 'replica', 'client_hostname': '',
     'primary_location': None, 'write_location_diff': None, 'reply_time_ms': None},
    {'application_name': 'replica', 'priority': 0},
])
def test_replica_serialization_fills_missing_fields(payload):
    expected = {**REPLICA_DEFAULTS, **payload}
    assert json.loads(json.dumps(ReplicaInfo.from_dict(payload).to_dict())) == expected
    assert ReplicaInfo(**payload).to_dict() == expected


@pytest.mark.parametrize('payload, expected', [
    ({}, 0), ({'priority': None}, 0), ({'priority': 0}, 0),
    ({'priority': 200}, 200), ({'priority': -10}, -10),
])
def test_replica_priority_normalizes_legacy_null(payload, expected):
    replica = ReplicaInfo.from_dict(payload)
    assert replica.priority == expected
    assert replica.to_dict()['priority'] == expected


@pytest.mark.parametrize('payload, expected', [
    ({}, ''), ({'state': None}, ''), ({'state': ''}, ''),
    ({'state': 'streaming'}, 'streaming'), ({'state': 'catchup'}, 'catchup'),
])
def test_replica_state_normalizes_legacy_null(payload, expected):
    replica = ReplicaInfo.from_dict(payload)
    assert replica.state == expected
    assert replica.to_dict()['state'] == expected


@pytest.mark.parametrize('record, payload', [
    (WalReceiverInfo, {'pid': 1, 'status': 'streaming', 'slot_name': '',
                       'last_msg_receipt_time_msec': 0, 'conninfo': 'host=leader'}),
    (DbState, {}),
    (DbState, {'alive': False, 'running': True, 'role': None,
               'connection_timed_out': True}),
    (DbState, {'replics_info': None, 'wal_receiver': None, 'replication_state': None}),
    (DbState, {'replics_info': [], 'wal_receiver': None, 'replication_state': {'mode': 'async', 'synchronous_standby_names': None}}),
    (DbState, {'replics_info': [{**REPLICA_DEFAULTS, 'application_name': 'replica'}],
               'wal_receiver': {'pid': 0, 'status': '', 'slot_name': '', 'last_msg_receipt_time_msec': 0, 'conninfo': ''},
               'replication_state': {'mode': 'sync', 'synchronous_standby_names': 'replica'}}),
])
def test_wire_round_trip(record, payload):
    restored = record.from_dict(payload)
    expected = {**DB_DEFAULTS, **payload} if record is DbState else payload
    assert json.loads(json.dumps(restored.to_dict())) == expected


@pytest.mark.parametrize('legacy_fields', [
    {}, {'prev_state': None}, {'prev_state': {}},
    {'prev_state': {'role': 'primary', 'timeline': 7}},
    {'prev_state': {'prev_state': {'wal_receiver': {'status': None}}}},
])
def test_db_state_ignores_legacy_previous_snapshot(legacy_fields):
    expected = {**DB_DEFAULTS, 'running': True, 'connection_timed_out': True}
    state = DbState.from_dict({**expected, **legacy_fields})
    assert state.to_dict() == expected


@pytest.mark.parametrize('state, payload', [
    (MaintenanceState(), {'status': None, 'ts': None}),
    (MaintenanceState(status='enable', ts='123.4'), {'status': 'enable', 'ts': '123.4'}),
])
def test_state_serialization_preserves_wire_format(state, payload):
    assert json.loads(json.dumps(state.to_dict())) == payload


@pytest.mark.parametrize('state, overrides', [
    (ZkState(), {'single_node': False, 'maintenance': {'status': None, 'ts': None}}),
    (ZkState(switchover=SwitchoverPrimaryInfo(), single_node=False, maintenance=MaintenanceState()),
     {'switchover': {'hostname': None, 'timeline': None, 'destination': None}, 'single_node': False, 'maintenance': {'status': None, 'ts': None}}),
    (ZkState(replics_info=[], synchronous_standby_names={'host': SsnInfo(value='', last_update='123')}),
     {'replics_info': [], 'synchronous_standby_names': {'host': {'value': '', 'last_update': '123'}}}),
])
def test_zk_state_has_fixed_wire_schema(state, overrides):
    expected = {
        'alive': False, 'replics_info': None, 'last_failover_time': None,
        'last_switchover_time': None, 'failover_state': None, 'failover_must_be_reset': False,
        'current_promoting_host': None, 'lock_holder': None,
        'single_node': False, 'timeline': None, 'switchover': None,
        'switchover/candidate': None, 'switchover/side_replicas': None, 'switchover/state': None,
        'maintenance': {'status': None, 'ts': None}, 'last_leader': '', 'synchronous_standby_names': {},
    }
    expected.update(overrides)
    assert json.loads(json.dumps(state.to_dict())) == expected


def test_switchover_plan_returns_independent_copy(cluster):
    assert cluster.switch.plan_switchover()
    plan = cluster.switch.plan()
    assert plan.primary == 'leader'
    assert plan.timeline == 7
    plan.primary = 'other'
    plan.timeline = 99
    assert cluster.switch.plan().primary == 'leader'
    assert cluster.switch.plan().timeline == 7
    assert cluster.switch.perform(block=False)
    assert json.loads(cluster.records['switchover/master']) == {
        'hostname': 'leader', 'timeline': 7, 'destination': None,
    }


def test_replica_missing_priority_matches_explicit_null_and_serializes_updates():
    absent = ReplicaInfo.from_dict({'application_name': 'replica'})
    explicit_null = ReplicaInfo.from_dict({'application_name': 'replica', 'priority': None})
    assert absent == explicit_null
    assert absent.to_dict() == explicit_null.to_dict()
    absent.priority = 7
    assert absent.to_dict()['priority'] == 7


def test_db_state_serialization_fills_nested_replica_defaults():
    state = DbState.from_dict({'replics_info': [{'application_name': 'replica'}]})
    assert state.to_dict() == {**DB_DEFAULTS, 'replics_info': [{**REPLICA_DEFAULTS, 'application_name': 'replica'}]}


def test_switchover_serializes_direct_values_and_updates():
    info = SwitchoverPrimaryInfo(hostname='leader', timeline=7)
    assert info.to_dict() == {'hostname': 'leader', 'timeline': 7, 'destination': None}
    info.destination = 'replica'
    assert info.to_dict() == {'hostname': 'leader', 'timeline': 7, 'destination': 'replica'}


@pytest.mark.parametrize('content', [None, '', '{}', 'null', 'not json'])
def test_empty_missing_and_invalid_cache_mean_no_previous_state(tmp_path, content):
    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    if content is not None:
        tmp_path.joinpath('.pgconsul_db_state.cache').write_text(content)
    assert pg.get_prev_state() is None


@pytest.mark.parametrize('legacy_fields', [{}, {'prev_state': None}, {'prev_state': {}},
                                           {'prev_state': {'role': 'primary', 'pgdata': '/older'}}])
def test_previous_cache_becomes_model_and_drops_legacy_snapshot(tmp_path, legacy_fields):
    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    payload = {'role': 'replica', 'pgdata': '/old', 'primary_fqdn': None,
               'wal_receiver': None, 'replication_state': ['sync', 'replica']}
    tmp_path.joinpath('.pgconsul_db_state.cache').write_text(json.dumps({**payload, **legacy_fields}))
    state = pg.get_prev_state()
    assert isinstance(state, DbState)
    assert state.role == 'replica'
    assert state.replication_state == ReplicationState(mode='sync', synchronous_standby_names='replica')
    pg.save_state(state)
    assert json.loads(tmp_path.joinpath('.pgconsul_db_state.cache').read_text()) == {
        **DB_DEFAULTS, **payload, 'replication_state': {'mode': 'sync', 'synchronous_standby_names': 'replica'},
    }


def test_empty_external_wal_receiver_is_absent_but_direct_model_is_present():
    assert DbState.from_dict({'wal_receiver': {}}).wal_receiver is None
    assert WalReceiverInfo.from_dict({})
    assert WalReceiverInfo.from_dict({'status': None})
