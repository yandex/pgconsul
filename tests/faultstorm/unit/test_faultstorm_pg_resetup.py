import importlib.util
from configparser import RawConfigParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def load_pg_resetup():
    path = ROOT / 'docker/pgconsul/pg_resetup.py'
    spec = importlib.util.spec_from_file_location('pg_resetup', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resetup_keeps_flag_when_pgconsul_restart_fails(tmp_path, monkeypatch):
    module = load_pg_resetup()
    flag = tmp_path / '.pgconsul_rewind_fail.flag'
    flag.touch()
    monkeypatch.setattr(module, 'FLAG_FILE', str(flag))
    monkeypatch.setattr(module, 'LOCK_FILE', str(tmp_path / '.pg_resetup.lock'))
    monkeypatch.setattr(module, 'get_hosts_from_status_file', lambda: ['primary'])
    monkeypatch.setattr(module, 'find_primary', lambda hosts: 'primary')
    monkeypatch.setattr(module, 'rebuild_from_primary', lambda primary, pgdata: None)

    def supervisorctl(action, service, *, check=False):
        assert check is True
        if (action, service) == ('start', 'pgconsul'):
            raise RuntimeError('pgconsul: ERROR (spawn error)')

    monkeypatch.setattr(module, 'supervisorctl', supervisorctl)

    module.check_and_resetup()

    assert flag.exists()


def test_supervisor_stops_the_pgconsul_process_group():
    config = RawConfigParser()
    config.read(ROOT / 'docker/pgconsul/supervisor.conf')

    assert config.getboolean('program:pgconsul', 'stopasgroup') is True
    assert config.getboolean('program:pgconsul', 'killasgroup') is True
    assert config.getint('program:pgconsul', 'startretries') == 300
