"""
pgconsul-specific configuration presets and registry setup for faultstorm tests.

Provides TestConfig presets for pgconsul clusters and a helper to create
a FaultRegistry that includes the pgconsul-specific SwitchoverAction
alongside all built-in fault actions.
"""

from typing import Any, Dict, List

from faultstorm.config import TestConfig
from faultstorm.cluster import ClusterManager
from faultstorm.faults.actions import FaultRegistry, create_default_registry

from faultstorm_switchover import SwitchoverAction


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
        write_phase_duration=120,
        read_phase_duration=60,
        add_interval=0.1,
    )


def get_intensive_config() -> TestConfig:
    """Get intensive pgconsul test configuration with more faults."""
    return get_pgconsul_config(
        "intensive",
        write_phase_duration=3600,
        fault_active_duration=30,
        fault_pause_duration=30,
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


# ---- Registry with switchover ----

def create_pgconsul_registry() -> FaultRegistry:
    """Create a FaultRegistry with all built-in actions plus SwitchoverAction.

    The base faultstorm library provides partition, kill, wait, and heal actions.
    This function adds the pgconsul-specific SwitchoverAction on top.

    Returns:
        FaultRegistry with all fault types including switchover
    """
    registry = create_default_registry()
    registry.register(SwitchoverAction)
    return registry
