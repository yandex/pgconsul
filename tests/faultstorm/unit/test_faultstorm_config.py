import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace


ROOT = Path(__file__).resolve().parents[3]


def load_config(monkeypatch):
    config = ModuleType('faultstorm.config')
    config.TestConfig = lambda **kwargs: SimpleNamespace(**kwargs)
    cluster = ModuleType('faultstorm.cluster')
    cluster.ClusterManager = object
    actions = ModuleType('faultstorm.faults.actions')
    actions.FaultRegistry = object
    actions.create_default_registry = lambda: object()
    for name, module in [
        ('faultstorm.config', config),
        ('faultstorm.cluster', cluster),
        ('faultstorm.faults.actions', actions),
        ('faultstorm_switchover', ModuleType('faultstorm_switchover')),
        ('faultstorm_resetup', ModuleType('faultstorm_resetup')),
        ('faultstorm_maintenance', ModuleType('faultstorm_maintenance')),
    ]:
        monkeypatch.setitem(sys.modules, name, module)
    sys.modules['faultstorm_switchover'].SwitchoverAction = object
    sys.modules['faultstorm_resetup'].ResetupAction = object
    sys.modules['faultstorm_maintenance'].MaintenanceAction = object
    path = ROOT / 'docker/faultstorm/faultstorm_config.py'
    spec = importlib.util.spec_from_file_location('faultstorm_config', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_random_faults_exclude_resetup(monkeypatch):
    config = load_config(monkeypatch).get_default_config()

    assert 'resetup' not in config.fault_types
