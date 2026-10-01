import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest


def _step_decorator(*args, **kwargs):
    def decorate(func):
        return func
    return decorate


behave = ModuleType('behave')
behave.given = _step_decorator
behave.then = _step_decorator
behave.when = _step_decorator
behave.register_type = MagicMock()
behave.use_step_matcher = MagicMock()

parse_type = ModuleType('parse_type')
parse_type.TypeBuilder = MagicMock()

config = ModuleType('tests.steps.config')
helpers = ModuleType('tests.steps.helpers')
helpers.LOG = MagicMock()
helpers.container_get_host = MagicMock()
helpers.container_get_tcp_port = MagicMock()
helpers.retry_on_assert = lambda func: func
zk = ModuleType('tests.steps.zk')
database = ModuleType('tests.steps.database')
database.Postgres = MagicMock()
latency = ModuleType('tests.steps.latency')
latency.apply_latency = MagicMock()


def _get_cluster_module():
    with patch.dict(sys.modules, {
        'behave': behave,
        'parse_type': parse_type,
        'tests.steps.config': config,
        'tests.steps.helpers': helpers,
        'tests.steps.zk': zk,
        'tests.steps.database': database,
        'tests.steps.latency': latency,
    }):
        from tests.steps import cluster
    return cluster


def _run_step(wait_side_effect, conn=None):
    cluster = _get_cluster_module()
    conn = conn or MagicMock()
    cluster.helpers.container_get_host.return_value = 'localhost'
    cluster.helpers.container_get_tcp_port.return_value = 5432

    with patch.object(cluster, '_get_container', return_value=MagicMock()), \
         patch.object(cluster.psycopg2, 'connect', return_value=conn), \
         patch.object(cluster, 'wait_async_operation', side_effect=wait_side_effect) as wait, \
         patch.object(cluster.time, 'monotonic', side_effect=[10.0, 20.0, 25.0]):
        cluster.step_create_table_expect_timeout(MagicMock(), 'postgresql1', '5000')

    return conn, wait


def test_create_table_connection_timeout_fails_step():
    conn = MagicMock()
    with pytest.raises(TimeoutError):
        _run_step(TimeoutError, conn)

    conn.cursor.assert_not_called()
    conn.close.assert_called_once_with()


def test_create_table_query_timeout_passes_step():
    conn, wait = _run_step([None, TimeoutError])

    conn.cursor.return_value.execute.assert_called_once_with('CREATE TABLE race_probe (ts timestamp)')
    assert wait.call_args_list[0].args[1] == 15.0
    assert wait.call_args_list[1].args[1] == 25.0
    conn.close.assert_called_once_with()


def test_create_table_completed_query_fails_step():
    conn = MagicMock()
    with pytest.raises(AssertionError, match='CREATE TABLE unexpectedly completed'):
        _run_step([None, None], conn)

    conn.close.assert_called_once_with()

