import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace


def load_main(monkeypatch):
    config = ModuleType('faultstorm_config')
    config.build_pgconsul_dc_map = lambda value: {}
    config.create_pgconsul_registry = lambda: object()
    config.get_default_config = lambda: None
    config.get_quick_config = lambda: None
    client = ModuleType('faultstorm_pg_client')
    client.PgConsulClient = object
    cluster = ModuleType('faultstorm.cluster')
    cluster.ClusterManager = SimpleNamespace(exec_on_node=lambda *args, **kwargs: 't')
    runner = ModuleType('faultstorm.runner')
    runner.TestRunner = object
    for name, module in [
        ('faultstorm_config', config),
        ('faultstorm_pg_client', client),
        ('faultstorm.cluster', cluster),
        ('faultstorm.runner', runner),
    ]:
        monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).resolve().parents[3] / 'docker/faultstorm/main.py'
    spec = importlib.util.spec_from_file_location('faultstorm_main', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_next_session_waits_until_all_nodes_have_stable_roles(monkeypatch):
    module = load_main(monkeypatch)
    calls = []

    def exec_on_node(node, command, timeout):
        calls.append(node)
        return 'f' if node == 'postgresql1' else 't'

    module.ClusterManager.exec_on_node = exec_on_node
    config = SimpleNamespace(
        db_nodes=['postgresql1', 'postgresql2', 'postgresql3'],
        operations_log='/missing/operations.log',
        scenario_log='/missing/scenario.log',
    )
    db_client = SimpleNamespace(setup=lambda node: None)
    runner = SimpleNamespace(run=lambda: SimpleNamespace(valid=True))

    assert module._run_sessions(config, db_client, object(), {}, 2, lambda *args, **kwargs: runner)
    assert calls == ['postgresql1', 'postgresql2', 'postgresql3']
