"""
Resetup fault action for pgconsul faultstorm tests.

Freezes outbound traffic to other DB nodes, kills postgres,
deletes PGDATA, drops the frozen packets, restores original
delays, and creates the .pgconsul_rewind_fail.flag so that the
pg_resetup daemon rebuilds the instance from the current primary.
"""

import logging
import random
import threading
from typing import Dict, List, Optional, Set

from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultAction
from faultstorm.network_latency import freeze_drop_restore_for_ips

logger = logging.getLogger(__name__)

FLAG_FILE = '/tmp/.pgconsul_rewind_fail.flag'
PGDATA_TEMPLATE = '/var/lib/postgresql/{pg_major}/main/*'

# Seconds to wait between freezing outbound traffic and killing postgres.
# During this window postgres may commit transactions whose WAL will be
# frozen in the netem queue and later dropped — simulating data in flight
# at the moment of a real crash.
_PRE_KILL_SLEEP = 5.0


class ResetupAction(FaultAction):
    """Kill postgres on a DB node and wipe PGDATA with proper network handling.

    The full sequence is:

    1. Parse ``tc`` rules on the target node to find netem bands that
       route traffic to **other DB nodes**.
    2. Change those bands to a huge delay (effectively freezing new
       outbound packets to other DBs).
    3. Sleep for :data:`_PRE_KILL_SLEEP` seconds — during this window
       postgres may generate WAL that enters the frozen netem queue.
    4. Kill postgres, delete PGDATA contents.
    5. Delete the frozen netem child qdiscs (drops all queued packets)
       and re-create them with the **original** delay values.
    6. Set the rewind-fail flag so ``pg_resetup`` rebuilds the database.

    Traffic to ZK nodes and other non-DB destinations is **not**
    affected — their netem bands stay untouched throughout.

    This is a fire-and-forget action (not healable).  Recovery is
    handled by the pg_resetup daemon.

    Serialized format: ``<ordinal> <node>``
    """

    name = "resetup"
    host_targetable = True
    destructive = True

    def __init__(self, db_nodes: List[str], extra_nodes: List[str],
                 ordinal: int = 0,
                 dc_map: Optional[Dict[str, List[str]]] = None,
                 node: Optional[str] = None):
        super().__init__(db_nodes, extra_nodes, ordinal,
                         dc_map=dc_map)
        self.node = node

    def _get_other_db_ips(self) -> Set[str]:
        """Get IPs of all DB nodes except the target node."""
        ips: Set[str] = set()
        for other in self.db_nodes:
            if other != self.node:
                try:
                    ips.add(ClusterManager.get_node_ip(other))
                except Exception as e:
                    logger.warning("Cannot get IP for %s: %s", other, e)
        return ips

    def _kill_and_wipe(self) -> None:
        """Kill postgres and delete PGDATA contents on the target node."""
        try:
            ClusterManager.exec_on_node(
                self.node,
                ["bash", "-c",
                 f"rm -rf {PGDATA_TEMPLATE.format(pg_major='*')};"
                 f" pkill postgres;"
                 f" echo $(date +%s) > {FLAG_FILE}"],
                timeout=30,
            )
        except Exception as e:
            logger.warning("Kill/wipe on %s failed: %s", self.node, e)

    def execute(self, stop_event: Optional[threading.Event] = None) -> None:
        if self.node is None:
            self.node = random.choice(self.db_nodes)

        logger.info("Resetup on %s: freeze → sleep → kill → drop → restore", self.node)

        other_db_ips = self._get_other_db_ips()

        if other_db_ips:
            freeze_drop_restore_for_ips(
                node=self.node,
                target_ips=other_db_ips,
                pre_kill_sleep=_PRE_KILL_SLEEP,
                kill_callback=self._kill_and_wipe,
            )
        else:
            # No other DB nodes (single-node setup) — just kill and wipe
            logger.info("No other DB nodes, skipping freeze/drop sequence")
            self._kill_and_wipe()

    def serialize(self) -> str:
        return f"{self.ordinal} {self.node or ''}"

    @classmethod
    def deserialize(cls, params: str, db_nodes: List[str],
                    extra_nodes: List[str],
                    dc_map: Optional[Dict[str, List[str]]] = None) -> 'ResetupAction':
        parts = params.strip().split()
        ordinal = int(parts[0])
        node = parts[1] if len(parts) > 1 else None
        return cls(db_nodes, extra_nodes, ordinal,
                   dc_map=dc_map, node=node)
