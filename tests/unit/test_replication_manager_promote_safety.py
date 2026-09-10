"""Unit tests for promotion safety checks."""

import importlib
from unittest.mock import MagicMock


ReplicationManager = importlib.import_module('src.replication_manager').ReplicationManager


def test_empty_quorum_allows_safe_promotion():
    """Async replication has no synchronous quorum requirement."""
    manager = object.__new__(ReplicationManager)
    manager._zk = MagicMock()
    manager._zk.get_quorum.return_value = []

    assert manager.is_promote_safe(['replica1'], []) is True
