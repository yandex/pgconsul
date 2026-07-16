"""
Network latency helpers for behave tests.

Parses the NETWORK_LATENCY environment variable and applies cross-DC
latency rules to Docker containers using faultstorm's NetworkLatencyManager.

Environment variable format:
    NETWORK_LATENCY='dc1-dc2:5,dc1-dc3:8,dc2-dc3:3'

Each entry is a DC pair separated by '-' with delay in ms after ':'.
Multiple entries are separated by ','.
"""

import logging
import os
from typing import Dict, Tuple

from faultstorm.cluster import ClusterManager
from faultstorm.config import TestConfig
from faultstorm.network_latency import NetworkLatencyManager

LOG = logging.getLogger('helpers')


def parse_network_latency(raw: str) -> Dict[Tuple[str, str], int]:
    """Parse NETWORK_LATENCY env var into a dict of DC-pair delays.

    Args:
        raw: Comma-separated string of 'dcA-dcB:delay_ms' entries.
             Example: 'dc1-dc2:5,dc1-dc3:8,dc2-dc3:3'

    Returns:
        Dict mapping (dc_a, dc_b) tuples to delay in ms.
    """
    delays = {}
    for entry in raw.split(','):
        entry = entry.strip()
        if not entry:
            continue
        pair_str, ms_str = entry.split(':')
        dcs = pair_str.strip().split('-')
        if len(dcs) != 2:
            raise ValueError(
                f"Invalid DC pair '{pair_str}', expected format 'dcA-dcB'"
            )
        dc_a, dc_b = dcs[0].strip(), dcs[1].strip()
        delays[(dc_a, dc_b)] = int(ms_str.strip())
    return delays


def init_latency_context(context):
    """Read NETWORK_LATENCY from environment and store parsed config in context.

    Called from before_all().  If the variable is not set, latency is disabled.

    Sets:
        context.network_latency_delays: parsed delays dict or None
        context.latency_manager: None (created per-scenario)
    """
    raw = os.environ.get('NETWORK_LATENCY', '').strip()
    context.latency_manager = None
    if not raw:
        context.network_latency_delays = None
        return

    # Configure ClusterManager for behave test container naming.
    # Behave tests create containers with names matching compose service keys
    # (e.g. 'postgresql1', 'zookeeper1'), not 'pgconsul_postgresql1_1'.
    ClusterManager.container_template = "{node}"
    ClusterManager.network_name = "pgconsul_net"

    context.network_latency_delays = parse_network_latency(raw)
    LOG.info('Network latency configured: %s', context.network_latency_delays)


def _is_container_running(container) -> bool:
    """Check if a Docker container is running and has a network IP."""
    try:
        container.reload()
        return container.status.strip().lower() == 'running'
    except Exception:
        return False


def apply_latency(context):
    """Apply network latency rules to all active containers.

    Should be called after the cluster is fully created and running,
    and again after any container restart or network reconnect (tc rules
    are lost when a container's network interface is recreated).

    Uses Docker labels (faultstorm.dc) to determine DC membership.
    Skips containers that are not running (stopped/disconnected).

    Safe to call multiple times — nodes that already have tc rules
    will have their band mapping recovered from live tc output
    (the root qdisc add fails and _apply_node falls back to discovery).
    """
    if not getattr(context, 'network_latency_delays', None):
        return

    # Only include containers that are currently running
    running = {
        name for name, container in context.containers.items()
        if _is_container_running(container)
    }
    db_nodes = [n for n in running if 'postgresql' in n]
    extra_nodes = [n for n in running if 'zookeeper' in n]

    if not db_nodes:
        return

    all_nodes = db_nodes + extra_nodes

    # Build DC map from Docker labels (faultstorm.dc)
    dc_map = ClusterManager.build_dc_map(all_nodes)
    if not dc_map:
        LOG.warning('No DC labels found on containers, skipping latency')
        return

    config = TestConfig(
        db_nodes=db_nodes,
        extra_nodes=extra_nodes,
        cross_dc_delays=context.network_latency_delays,
    )

    manager = NetworkLatencyManager(config)
    manager.apply(dc_map)
    context.latency_manager = manager
    LOG.info('Network latency applied to nodes: %s', all_nodes)


def cleanup_latency(context):
    """Remove latency rules during scenario teardown.

    Called from after_scenario().  Since containers are killed anyway,
    this is best-effort — failures are logged but not raised.
    """
    if getattr(context, 'latency_manager', None) is not None:
        try:
            context.latency_manager.remove()
        except Exception as e:
            LOG.warning('Failed to remove network latency: %s', e)
        context.latency_manager = None
