"""
Behave environment for pgconsul-specific faultstorm tests.

Adds the pgconsul docker/faultstorm directory to sys.path so that
the action modules (faultstorm_switchover, faultstorm_resetup,
faultstorm_maintenance) can be imported.

Uses the same Docker Compose stack as faultstorm-compose.yml:
  - postgresql1, postgresql2, postgresql3 (DB nodes)
  - zookeeper1, zookeeper2, zookeeper3   (extra nodes)
  - faultstorm                           (load node)

Docker-dependent scenarios are tagged @docker.
Replay scenarios are tagged @docker @replay.
"""

import os
import subprocess
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
from faultstorm.network_latency import NetworkLatencyManager  # noqa: E402

logger = logging.getLogger(__name__)


def before_all(context):
    """Configure ClusterManager for pgconsul faultstorm tests."""
    # Set default logging level to DEBUG with timestamps
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    # Add a FileHandler to the 'faultstorm' logger so that all log records
    # (including DEBUG from load_generator, runner, engine, etc.) are written
    # to the logs directory.  We attach to the *named* logger rather than the
    # root logger because behave's --logcapture mode replaces the root
    # logger's handlers during step execution, which would swallow our records.
    logs_dir = os.path.join(PGCONSUL_ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(logs_dir, "faultstorm_debug.log"), mode="w",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'),
    )
    faultstorm_logger = logging.getLogger('faultstorm')
    faultstorm_logger.setLevel(logging.DEBUG)
    faultstorm_logger.addHandler(file_handler)

    # Configure Docker container naming for pgconsul
    ClusterManager.container_template = "pgconsul_{node}_1"
    ClusterManager.network_name = "pgconsul_pgconsul_net"

    context.logs_dir = logs_dir
    context.pg_major = os.environ.get("PG_MAJOR", "14")
    context.db_nodes = ["postgresql1", "postgresql2", "postgresql3"]
    context.extra_nodes = ["zookeeper1", "zookeeper2", "zookeeper3"]
    context.load_node = "faultstorm"
    context.dc_map = {
        "dc1": ["postgresql1", "zookeeper1"],
        "dc2": ["postgresql2", "zookeeper2"],
        "dc3": ["postgresql3", "zookeeper3"],
    }


def after_scenario(context, scenario):
    """Clean up resources after each scenario."""
    # Remove network latency if it was applied during the scenario
    if hasattr(context, 'latency_manager') and context.latency_manager is not None:
        try:
            context.latency_manager.remove()
        except Exception as e:
            logger.warning("Failed to remove network latency: %s", e)
        context.latency_manager = None

    # Heal any remaining faults from replay engine
    engine = getattr(context, "replay_engine", None)
    if engine is not None:
        try:
            engine.heal_all()
        except Exception as e:
            logger.warning("Failed to heal remaining faults: %s", e)

    # If the write process is somehow still running (e.g. a previous step
    # failed), wait briefly for it to finish on its own.  Only send
    # SIGTERM as a last resort so we don't mask timing issues.
    try:
        result = subprocess.run(
            ["docker", "exec", "pgconsul_faultstorm_1",
             "pgrep", "-f", "load_worker.py write"],
            timeout=10, capture_output=True,
        )
        if result.returncode == 0:
            logger.warning(
                "Write load still running during cleanup, "
                "sending SIGTERM as last resort"
            )
            subprocess.run(
                ["docker", "exec", "pgconsul_faultstorm_1",
                 "pkill", "-f", "load_worker.py write"],
                timeout=10, capture_output=True,
            )
    except Exception:
        pass
