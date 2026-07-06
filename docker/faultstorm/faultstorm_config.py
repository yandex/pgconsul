"""
pgconsul-specific configuration presets and registry setup for faultstorm tests.

Provides TestConfig presets for pgconsul clusters and a helper to create
a FaultRegistry that includes the pgconsul-specific SwitchoverAction
and ResetupAction alongside all built-in fault actions.
"""

import random
from typing import Any, Dict, List, Tuple

from faultstorm.config import TestConfig
from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultRegistry, create_default_registry

from faultstorm_switchover import SwitchoverAction
from faultstorm_resetup import ResetupAction
from faultstorm_maintenance import MaintenanceAction


# pgconsul DC names
_DCS = ["dc1", "dc2", "dc3"]


def generate_random_cross_dc_delays(
    dcs: List[str] = _DCS,
    max_delay_ms: int = 10,
) -> Dict[Tuple[str, str], int]:
    """Generate random cross-DC delays for every DC pair.

    Args:
        dcs: List of datacenter names.
        max_delay_ms: Upper bound (inclusive) for random delay in ms.

    Returns:
        Dict mapping (dc_a, dc_b) → delay_ms for every pair.
    """
    delays: Dict[Tuple[str, str], int] = {}
    for i, a in enumerate(dcs):
        for b in dcs[i + 1:]:
            delays[(a, b)] = random.randint(1, max_delay_ms)
    return delays


# ---- pgconsul-specific presets ----

def get_pgconsul_config(name: str = "default", **overrides: Any) -> TestConfig:
    """Get a TestConfig preset for pgconsul clusters.

    Args:
        name: Configuration name
        **overrides: Override any TestConfig field

    Returns:
        TestConfig configured for pgconsul
    """
    defaults: Dict[str, Any] = dict(
        name=name,
        db_nodes=["postgresql1", "postgresql2", "postgresql3"],
        extra_nodes=["zookeeper1", "zookeeper2", "zookeeper3"],
        load_node="faultstorm",
        fault_types=[
            "partition_random_halves",
            "partition_majorities_ring",
            "partition_random_node",
            "partition_random_subnet",
            "partition_random_dc",
            "kill",
            "switchover",
            "resetup",
            "maintenance",
            "freeze_processes",
            "freeze_processes_group",
        ],
        action_params={
            "freeze_processes": {
                "processes": ["bin/postgres",
                              "postgres: startup",
                              "postgres: checkpointer",
                              "postgres: background writer",
                              "postgres: wal",
                              "bin/pgconsul",
                              "zookeeper"],
                "freeze_duration_range": (100, 60000),
            },
            "freeze_processes_group": {
                "processes": ["bin/postgres",
                              "postgres: startup",
                              "postgres: checkpointer",
                              "postgres: background writer",
                              "postgres: wal",
                              "bin/pgconsul",
                              "zookeeper"],
            },
        },
        cross_dc_delays=generate_random_cross_dc_delays(),
        db_zk_delay_ms=0,
        add_interval=0,
        fault_active_duration=120,
    )
    defaults.update(overrides)
    return TestConfig(**defaults)


def get_default_config() -> TestConfig:
    """Get default pgconsul test configuration."""
    return get_pgconsul_config("default")


def get_quick_config() -> TestConfig:
    """Get quick pgconsul test configuration for fast testing."""
    return get_pgconsul_config(
        "quick",
        write_phase_duration=1200,
        read_phase_duration=300,
        fault_active_duration=120,
        fault_pause_duration=60,
    )


# ---- DC map ----

def build_pgconsul_dc_map(config: TestConfig) -> Dict[str, List[str]]:
    """Build DC mapping from Docker container labels.

    Reads the ``faultstorm.dc`` label from each node's container
    and groups nodes by their DC value.

    Args:
        config: Test configuration (uses db_nodes + extra_nodes)

    Returns:
        Dict mapping DC name to list of node names.
    """
    return ClusterManager.build_dc_map(config.all_nodes)


# ---- Registry with pgconsul-specific actions ----

def create_pgconsul_registry() -> FaultRegistry:
    """Create a FaultRegistry with all built-in actions plus pgconsul-specific ones.

    The base faultstorm library provides partition, kill, wait, and heal actions.
    This function adds the pgconsul-specific SwitchoverAction and ResetupAction.

    Returns:
        FaultRegistry with all fault types including switchover and resetup
    """
    registry = create_default_registry()
    registry.register(SwitchoverAction)
    registry.register(ResetupAction)
    registry.register(MaintenanceAction)
    return registry
