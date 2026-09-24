import importlib.util
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]


def load_pg_resetup():
    path = ROOT / 'docker/pgconsul/pg_resetup.py'
    spec = importlib.util.spec_from_file_location('pg_resetup', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_basebackup_is_bounded_so_resetup_can_retry_after_primary_change(monkeypatch):
    module = load_pg_resetup()
    calls = []
    basebackup_timeout = 120

    def run_cmd(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    monkeypatch.setattr(module, 'run_cmd', run_cmd)

    module.run_basebackup('primary', '/pgdata')

    command, kwargs = calls[0]
    assert command[:3] == ['timeout', '--kill-after=5s', f'{basebackup_timeout}s']
    assert command[3:7] == ['su', '-', 'postgres', '-c']
    assert kwargs['timeout'] == basebackup_timeout + 10
