"""
Maintenance fault action for pgconsul faultstorm tests.

Enables pgconsul maintenance mode on a random DB node via
``pgconsul-util maintenance -m enable``. Healing disables it
with ``pgconsul-util maintenance -m disable``.
"""

import logging
import random
import threading
from typing import Dict, List, Optional

from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultAction

logger = logging.getLogger(__name__)


class MaintenanceAction(FaultAction):
    """Enable pgconsul maintenance mode on a random DB node.

    Execute picks a random DB node (or uses the specified one) and runs
    ``pgconsul-util maintenance -m enable``.  Healing runs
    ``pgconsul-util maintenance -m disable`` on the same node.

    Serialized format: ``<ordinal> <node>``
    """

    name = "maintenance"
    healable = True

    def __init__(self, db_nodes: List[str], extra_nodes: List[str],
                 ordinal: int = 0,
                 dc_map: Optional[Dict[str, List[str]]] = None,
                 node: Optional[str] = None):
        """Initialize.

        Args:
            db_nodes: Database node names
            extra_nodes: Extra infrastructure node names
            ordinal: Sequential fault number
            dc_map: DC-to-nodes mapping (not used)
            node: Specific node to target (None = pick random DB node)
        """
        super().__init__(db_nodes, extra_nodes, ordinal,
                         dc_map=dc_map)
        self.node = node

    def execute(self, stop_event: Optional[threading.Event] = None) -> None:
        if self.node is None:
            self.node = random.choice(self.db_nodes)
        logger.info("Enabling maintenance on %s", self.node)
        try:
            ClusterManager.exec_on_node(
                self.node,
                ["pgconsul-util", "maintenance", "-m", "enable"],
                timeout=15,
            )
        except Exception as e:
            logger.warning("Maintenance enable on %s failed: %s", self.node, e)

    def heal(self) -> None:
        logger.info("Disabling maintenance on %s", self.node)
        try:
            ClusterManager.exec_on_node(
                self.node,
                ["pgconsul-util", "maintenance", "-m", "disable"],
                timeout=15,
            )
        except Exception as e:
            logger.warning("Maintenance disable on %s failed: %s", self.node, e)

    def serialize(self) -> str:
        return f"{self.ordinal} {self.node or ''}"

    @classmethod
    def deserialize(cls, params: str, db_nodes: List[str],
                    extra_nodes: List[str],
                    dc_map: Optional[Dict[str, List[str]]] = None) -> 'MaintenanceAction':
        parts = params.strip().split()
        ordinal = int(parts[0])
        node = parts[1] if len(parts) > 1 else None
        return cls(db_nodes, extra_nodes, ordinal,
                   dc_map=dc_map, node=node)
