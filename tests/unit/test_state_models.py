"""Wire compatibility for typed records, including absent and nullable fields."""
import json
from unittest.mock import patch

import pytest

from src.types import (
    DbState, MaintenanceState, ReplicaInfo, SwitchoverPlan,
    SwitchoverPrimaryInfo, WalReceiverInfo, ZkState,
)
from tests.unit.test_pg import _make_postgres


@pytest.mark.parametrize('record, payload', [
    (ReplicaInfo, {}),
    (ReplicaInfo, {'pid': 1, 'application_name': 'replica', 'client_hostname': None,
                   'primary_location': None, 'write_location_diff': None, 'reply_time_ms': None}),
    (ReplicaInfo, {'application_name': 'replica', 'priority': None}),
    (ReplicaInfo, {'application_name': 'replica', 'priority': 0}),
    (WalReceiverInfo, {'pid': 1, 'status': 'streaming', 'slot_name': None,
                       'last_msg_receipt_time_msec': 0, 'conninfo': 'host=leader'}),
    (DbState, {}),
    (DbState, {'alive': False, 'running': True, 'role': None,
               'connection_timed_out': True, 'prev_state': {}}),
    (DbState, {'alive': False, 'connection_timed_out': True,
               'prev_state': {'role': 'replica', 'primary_fqdn': 'leader', 'pgdata': '/old'}}),
    (DbState, {'replics_info': None, 'wal_receiver': None, 'replication_state': None}),
    (DbState, {'replics_info': [], 'wal_receiver': None, 'replication_state': ['async', None]}),
    (DbState, {'replics_info': [{'application_name': 'replica', 'write_location_diff': None}],
               'wal_receiver': {'slot_name': None}, 'replication_state': ['sync', 'replica']}),
    (SwitchoverPrimaryInfo, {}),
    (SwitchoverPrimaryInfo, {'hostname': 'leader', 'timeline': 42, 'destination': None}),
    (SwitchoverPrimaryInfo, {'primary': 'legacy', 'timeline': '42'}),
])
def test_wire_round_trip(record, payload):
    restored = record.from_dict(payload)
    assert json.loads(json.dumps(restored.to_dict())) == payload


@pytest.mark.parametrize('state, payload', [
    (MaintenanceState(), {'status': None, 'ts': None}),
    (MaintenanceState(status='enable', ts='123.4'), {'status': 'enable', 'ts': '123.4'}),
])
def test_state_serialization_preserves_wire_format(state, payload):
    assert json.loads(json.dumps(state.to_dict())) == payload


@pytest.mark.parametrize('state, overrides', [
    (ZkState(), {'single_node': False, 'maintenance': {'status': None, 'ts': None}}),
    (ZkState(switchover=SwitchoverPrimaryInfo(), single_node=False, maintenance=MaintenanceState()),
     {'switchover': {}, 'single_node': False, 'maintenance': {'status': None, 'ts': None}}),
    (ZkState(replics_info=[], synchronous_standby_names={'host': (None, '123')}),
     {'replics_info': [], 'synchronous_standby_names': {'host': [None, '123']}}),
])
def test_zk_state_has_fixed_wire_schema(state, overrides):
    expected = {
        'alive': False, 'replics_info': None, 'last_failover_time': None,
        'last_switchover_time': None, 'failover_state': None, 'failover_must_be_reset': False,
        'current_promoting_host': None, 'lock_version': None, 'lock_holder': None,
        'single_node': False, 'timeline': None, 'switchover': None,
        'switchover/candidate': None, 'switchover/side_replicas': None, 'switchover/state': None,
        'maintenance': {'status': None, 'ts': None}, 'last_leader': None, 'synchronous_standby_names': {},
    }
    expected.update(overrides)
    assert json.loads(json.dumps(state.to_dict())) == expected


def test_switchover_plan_returns_independent_copy():
    with patch('src.read_config', create=True):
        from src.utils import Switchover

    switch = Switchover.__new__(Switchover)
    switch._plan = SwitchoverPlan(primary='leader', timeline=None)
    plan = switch.plan()
    assert plan.primary == 'leader'
    assert plan.timeline is None
    plan.primary = 'other'
    assert switch._plan.primary == 'leader'


def test_presence_is_private_and_does_not_change_equality():
    absent = ReplicaInfo.from_dict({'application_name': 'replica'})
    explicit_null = ReplicaInfo.from_dict({'application_name': 'replica', 'priority': None})
    assert absent == explicit_null
    assert repr(absent) == repr(explicit_null)
    assert absent.to_dict() != explicit_null.to_dict()
    absent.priority = 7
    absent._present_fields.add('priority')
    assert absent.to_dict()['priority'] == 7


def test_empty_switchover_keeps_legacy_truthiness():
    assert not SwitchoverPrimaryInfo.from_dict({})
    assert SwitchoverPrimaryInfo.from_dict({'hostname': None})


@pytest.mark.parametrize('content', [None, '', '{}', 'not json'])
def test_empty_missing_and_invalid_cache_mean_no_previous_state(tmp_path, content):
    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    if content is not None:
        tmp_path.joinpath('.pgconsul_db_state.cache').write_text(content)
    assert pg.get_prev_state() is None


def test_previous_cache_becomes_model_and_reserializes_without_added_fields(tmp_path):
    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    payload = {'role': 'replica', 'pgdata': '/old', 'primary_fqdn': None,
               'wal_receiver': None, 'replication_state': ['sync', 'replica']}
    tmp_path.joinpath('.pgconsul_db_state.cache').write_text(json.dumps(payload))
    state = pg.get_prev_state()
    assert isinstance(state, DbState)
    assert state.role == 'replica'
    assert state.replication_state == ('sync', 'replica')
    pg.save_state(state)
    assert json.loads(tmp_path.joinpath('.pgconsul_db_state.cache').read_text()) == payload


def test_empty_wal_receiver_keeps_false_truth_value():
    assert not WalReceiverInfo.from_dict({})
    assert WalReceiverInfo.from_dict({'status': None})
