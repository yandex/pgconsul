import importlib
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch


def _load_helpers():
    docker = ModuleType('docker')
    docker.from_env = MagicMock(return_value=MagicMock())
    docker_errors = ModuleType('docker.errors')
    docker_errors.APIError = type('APIError', (Exception,), {})
    with patch.dict(sys.modules, {'docker': docker, 'docker.errors': docker_errors}):
        return importlib.import_module('tests.steps.helpers')


def test_timing_log_allows_additional_entries():
    """kill_primary.feature:102: contains means required timings are a subset."""
    helpers = _load_helpers()
    context = MagicMock()
    context.containers.get.return_value = MagicMock()

    with patch.object(helpers, 'container_file_exists', return_value=True), patch.object(
        helpers,
        'container_get_filecontent',
        return_value=b'failover_promote: 1\ndowntime: 2\nfailover: 3\n',
    ):
        assert helpers.check_timing_log(context, ['failover', 'downtime'], 'postgresql3')

