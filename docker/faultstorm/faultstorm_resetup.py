"""
Resetup fault action for pgconsul faultstorm tests.

Deletes PGDATA on a random DB node and creates the
.pgconsul_rewind_fail.flag so that the pg_resetup daemon
rebuilds the instance from the current primary.
"""

import logging
import random
import threading
from typing import Dict, List, Optional

from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultAction

logger = logging.getLogger(__name__)

FLAG_FILE = '/tmp/.pgconsul_rewind_fail.flag'
PGDATA_TEMPLATE = '/var/lib/postgresql/{pg_major}/main/*'


class ResetupAction(FaultAction):
    """Delete PGDATA on a random DB node and set the rewind-fail flag.

    The pg_resetup daemon running inside the container will detect the
    flag and rebuild the database via pg_basebackup from the primary.

    This is a fire-and-forget action (not healable).  Recovery is
    handled by the pg_resetup daemon.

    Serialized format: ``<ordinal> <node>``
    """

    name = "resetup"
    host_targetable = True

    def __init__(self, db_nodes: List[str], extra_nodes: List[str],
                 ordinal: int = 0,
                 load_node: Optional[str] = None,
                 dc_map: Optional[Dict[str, List[str]]] = None,
                 node: Optional[str] = None):
        super().__init__(db_nodes, extra_nodes, ordinal, load_node=load_node,
                         dc_map=dc_map)
        self.node = node

    def execute(self, stop_event: Optional[threading.Event] = None) -> None:
        if self.node is None:
            self.node = random.choice(self.db_nodes)
        logger.info("Resetup on %s: deleting PGDATA and setting rewind-fail flag", self.node)

        try:
            ClusterManager.exec_on_node(
                self.node,
                ["bash", "-c",
                 f"rm -rf {PGDATA_TEMPLATE.format(pg_major='*')}; echo $(date +%s) > {FLAG_FILE}"],
                timeout=30,
            )
        except Exception as e:
            logger.warning("Resetup on %s failed: %s", self.node, e)

    def serialize(self) -> str:
        return f"{self.ordinal} {self.node or ''}"

    @classmethod
    def deserialize(cls, params: str, db_nodes: List[str],
                    extra_nodes: List[str],
                    load_node: Optional[str] = None,
                    dc_map: Optional[Dict[str, List[str]]] = None) -> 'ResetupAction':
        parts = params.strip().split()
        ordinal = int(parts[0])
        node = parts[1] if len(parts) > 1 else None
        return cls(db_nodes, extra_nodes, ordinal, load_node=load_node,
                   dc_map=dc_map, node=node)
