"""
Behave environment for pgconsul-specific faultstorm action tests.

Adds the pgconsul docker/faultstorm directory to sys.path so that
the action modules (faultstorm_switchover, faultstorm_resetup,
faultstorm_maintenance) can be imported.

Uses the same Docker Compose stack as faultstorm-compose.yml:
  - postgresql1, postgresql2, postgresql3 (DB nodes)
  - zookeeper1, zookeeper2, zookeeper3   (extra nodes)
  - faultstorm                           (load node)

Docker-dependent scenarios are tagged @docker.
"""

import os
import sys
import logging

PGCONSUL_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
FAULTSTORM_DIR = os.path.join(PGCONSUL_ROOT, "docker", "faultstorm")

# Add docker/faultstorm to sys.path at module level so that step files
# can import faultstorm_switchover, faultstorm_resetup, etc.
if FAULTSTORM_DIR not in sys.path:
    sys.path.insert(0, FAULTSTORM_DIR)

from faultstorm.cluster import ClusterManager  # noqa: E402

logger = logging.getLogger(__name__)


def before_all(context):
    """Configure ClusterManager for pgconsul faultstorm tests."""
    # Set default logging level to DEBUG with timestamps
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    # Configure Docker container naming for pgconsul
    ClusterManager.container_template = "pgconsul_{node}_1"
    ClusterManager.network_name = "pgconsul_pgconsul_net"

    context.pg_major = os.environ.get("PG_MAJOR", "14")
    context.db_nodes = ["postgresql1", "postgresql2", "postgresql3"]
    context.extra_nodes = ["zookeeper1", "zookeeper2", "zookeeper3"]
    context.load_node = "faultstorm"
    context.dc_map = {
        "dc1": ["postgresql1", "zookeeper1"],
        "dc2": ["postgresql2", "zookeeper2"],
        "dc3": ["postgresql3", "zookeeper3"],
    }
