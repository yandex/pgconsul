"""Replication settings survive cache upgrades and use named fields in CLI output."""
import json
from configparser import ConfigParser
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from tests.unit.test_pg import _make_postgres
from tests.unit.state_fixtures import state_cluster  # noqa: F401


@pytest.mark.parametrize('raw', [
    ['sync', 'ANY 1 (replica)'],
    {'mode': 'sync', 'synchronous_standby_names': 'ANY 1 (replica)'},
])
def test_saved_replication_settings_upgrade_without_live_database(tmp_path, raw):
    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    cache = tmp_path / '.pgconsul_db_state.cache'
    cache.write_text(json.dumps({'role': 'primary', 'pgdata': '/data', 'replication_state': raw}))
    state = pg.get_prev_state()
    assert state is not None
    pg.save_state(state)
    assert json.loads(cache.read_text())['replication_state'] == {
        'mode': 'sync', 'synchronous_standby_names': 'ANY 1 (replica)',
    }


@pytest.mark.parametrize('raw, expected', [
    (['async', None], {'mode': 'async', 'synchronous_standby_names': None}),
    (['sync', 'replica'], {'mode': 'sync', 'synchronous_standby_names': 'replica'}),
    ({'mode': 'sync', 'synchronous_standby_names': 'replica'},
     {'mode': 'sync', 'synchronous_standby_names': 'replica'}),
    (None, None),
])
def test_cli_normalizes_cached_settings_without_losing_other_fields(tmp_path, capsys, monkeypatch, state_cluster, raw, expected):
    with patch('src.read_config', create=True), patch('src.init_logging', create=True):
        from src import cli
    conf = ConfigParser()
    conf['global'] = {'working_dir': str(tmp_path)}
    cache = tmp_path / '.pgconsul_db_state.cache'
    original = json.dumps({'replication_state': raw, 'legacy_extra': {'keep': True}})
    cache.write_text(original)
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: state_cluster.zk)
    cli.show_info(SimpleNamespace(short=False, json=True), conf)
    result = json.loads(capsys.readouterr().out)
    assert result['replication_state'] == expected
    assert result['legacy_extra'] == {'keep': True}
    assert cache.read_text() == original
