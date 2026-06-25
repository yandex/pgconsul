"""
Entry point for pgconsul FaultStorm tests.

Configures pgconsul-specific cluster settings, creates a PgConsulClient,
registers built-in fault actions, and runs the test via TestRunner.
"""

import sys
import logging

from faultstorm_config import get_default_config, get_quick_config, get_intensive_config
from faultstorm_config import create_pgconsul_registry
from faultstorm_pg_client import PgConsulClient
from faultstorm.cluster import ClusterManager
from faultstorm.runner import TestRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='FaultStorm tests for pgconsul'
    )
    parser.add_argument(
        '--config',
        choices=['default', 'quick', 'intensive'],
        default='default',
        help='Test configuration to use',
    )
    parser.add_argument(
        '--write-duration',
        type=int,
        help='Write phase duration in seconds',
    )
    parser.add_argument(
        '--read-duration',
        type=int,
        help='Read phase duration in seconds',
    )
    parser.add_argument(
        '--replay-scenario',
        type=str,
        default=None,
        help='Path to scenario file to replay (instead of random faults)',
    )
    parser.add_argument(
        '--scenario-log',
        type=str,
        default=None,
        help='Path to write scenario log (default: logs/scenario.log)',
    )

    args = parser.parse_args()

    # Get config
    if args.config == 'quick':
        config = get_quick_config()
    elif args.config == 'intensive':
        config = get_intensive_config()
    else:
        config = get_default_config()

    # Override durations if specified
    if args.write_duration:
        config.write_phase_duration = args.write_duration
    if args.read_duration:
        config.read_phase_duration = args.read_duration

    # Override scenario options
    if args.replay_scenario:
        config.replay_scenario = args.replay_scenario
    if args.scenario_log:
        config.scenario_log = args.scenario_log

    # Configure Docker container naming for pgconsul
    ClusterManager.container_template = "pgconsul_{node}_1"
    ClusterManager.network_name = "pgconsul_pgconsul_net"

    # Create pgconsul-specific components
    db_client = PgConsulClient(config.db_nodes)
    registry = create_pgconsul_registry()

    # Run test
    runner = TestRunner(config, db_client, registry)
    passed = runner.run_and_print()

    # Exit with appropriate code
    sys.exit(0 if passed else 1)


if __name__ == '__main__':
    main()
