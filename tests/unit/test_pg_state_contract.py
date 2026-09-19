import json
from unittest.mock import patch

import pytest

from src.pg import Postgres
from tests.unit.test_pg import _make_postgres


def _wire(state):
    return state if isinstance(state, dict) else state.to_dict()


@pytest.mark.parametrize('role', ['primary', 'replica'])
@pytest.mark.parametrize('final_alive', [True, False])
def test_state_and_cache_preserve_wire_contract(tmp_path, role, final_alive):
    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    old_cache = '{"role": "primary", "pgdata": "/old"}'
    tmp_path.joinpath('.pgconsul_db_state.cache').write_text(old_cache)
    with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(True, True)), \
         patch.object(pg, 'is_alive', return_value=final_alive), \
         patch.object(pg, 'get_role', return_value=role), \
         patch.object(pg, '_get_pgdata_path', return_value='/data'), \
         patch.object(pg, 'pgpooler', return_value=(True, False)), \
         patch.object(pg, 'get_timeline', return_value=7), \
         patch.object(pg, '_get_wal_receiver_info', return_value=None), \
         patch.object(pg, 'get_replics_info', return_value=[]), \
         patch.object(pg, 'get_replication_state', return_value=('async', None)), \
         patch.object(pg, 'get_sessions_ratio', return_value=2.5), \
         patch.object(pg, 'get_primary_fqdn', return_value='leader'):
        result = _wire(pg.get_state())
    expected = dict(alive=final_alive, running=True, role=role if final_alive else None,
                    pgdata='/data', opened=False, timeline=7, wal_receiver=None, replics_info=[])
    if role == 'primary':
        expected.update(replication_state=('async', None), sessions_ratio=2.5)
    else:
        expected['primary_fqdn'] = 'leader'
    assert result == expected
    cache = tmp_path.joinpath('.pgconsul_db_state.cache').read_text()
    assert json.loads(cache) == json.loads(json.dumps(expected)) if final_alive else cache == old_cache


@pytest.mark.parametrize('probe, expected', [
    ((False, True), {'alive': False, 'running': False, 'role': None}),
    ((False, False), {'alive': False, 'running': True, 'role': None}),
])
def test_dead_state_has_no_collected_fields(probe, expected):
    pg = _make_postgres()
    with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=probe), \
         patch.object(pg, '_collect_db_state') as collect, patch.object(pg, 'save_state') as save:
        assert _wire(pg.get_state()) == expected
    collect.assert_not_called()
    save.assert_not_called()
