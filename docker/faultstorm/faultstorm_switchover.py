"""
Switchover fault action for pgconsul faultstorm tests.

Executes pgconsul-util switchover on a random DB node.
"""

import logging
import random
import threading
from typing import List, Optional

from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultAction

logger = logging.getLogger(__name__)


class SwitchoverAction(FaultAction):
    """Execute switchover on a random DB node.

    Serialized format: ``<ordinal> <node>``
    """

    name = "switchover"

    def __init__(self, db_nodes: List[str], extra_nodes: List[str],
                 ordinal: int = 0,
                 node: Optional[str] = None,
                 command: Optional[List[str]] = None):
        """Initialize.

        Args:
            db_nodes: Database node names
            extra_nodes: Extra infrastructure node names
            ordinal: Sequential fault number (ignored by switchover)
            node: Specific node (None = pick random on execute)
            command: Custom switchover command.
                     Defaults to ["timeout", "10", "pgconsul-util", "switchover", "-y"].
        """
        super().__init__(db_nodes, extra_nodes, ordinal)
        self.node = node
        self.command = command or ["timeout", "10", "pgconsul-util", "switchover", "-y"]

    def execute(self, stop_event: Optional[threading.Event] = None) -> None:
        if self.node is None:
            self.node = random.choice(self.db_nodes)
        logger.info("Switchover on %s", self.node)
        try:
            ClusterManager.exec_on_node(self.node, self.command, timeout=15)
        except Exception as e:
            logger.warning("Switchover on %s failed: %s", self.node, e)

    def serialize(self) -> str:
        return f"{self.ordinal} {self.node or ''}"

    @classmethod
    def deserialize(cls, params: str, db_nodes: List[str],
                    extra_nodes: List[str]) -> 'SwitchoverAction':
        parts = params.strip().split()
        ordinal = int(parts[0])
        node = parts[1] if len(parts) > 1 else None
        return cls(db_nodes, extra_nodes, ordinal, node=node)
