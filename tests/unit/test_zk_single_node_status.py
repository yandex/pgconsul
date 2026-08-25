import pytest
from unittest.mock import MagicMock

from src.zk import ZookeeperException
from src.zk_client import ZkClientError


def test_single_node_status_rejects_partial_ha_host_read(zk):
    """coordinator.feature:49: a partial ZK read must not look like single-node mode."""
    zk._zk_client.get_children = MagicMock(return_value=['primary', 'replica'])
    zk._zk_client.exists = MagicMock(side_effect=[True, ZkClientError('connection lost')])
    zk._zk_client.ensure_path = MagicMock()

    with pytest.raises(ZookeeperException):
        zk.update_single_node_status('primary')

    zk._zk_client.ensure_path.assert_not_called()
