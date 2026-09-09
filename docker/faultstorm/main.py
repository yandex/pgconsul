"""
Entry point for pgconsul FaultStorm tests.

Configures pgconsul-specific cluster settings, creates a PgConsulClient,
registers built-in fault actions, and runs the test via TestRunner.
"""

import logging
import os
import sys
from typing import Callable

from faultstorm_config import get_default_config, get_quick_config
from faultstorm_config import create_pgconsul_registry, build_pgconsul_dc_map
from faultstorm_pg_client import PgConsulClient
from faultstorm.cluster import ClusterManager
from faultstorm.runner import TestRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


def _cleanup_successful_session(config, db_client) -> None:  # type: ignore[no-untyped-def]
    """Clear data and transient logs after a validated session."""
    last_error = None
    for node in config.db_nodes:
        try:
            db_client.setup(node)
            break
        except Exception as exc:
            last_error = exc
    else:
        raise RuntimeError("Could not clear the test table after a successful session") from last_error

    for path in (config.operations_log, config.scenario_log):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _run_sessions(config, db_client, registry, dc_map, sessions: int, runner_factory: Callable = TestRunner) -> bool:  # type: ignore[no-untyped-def]
    """Run bounded sessions and retain logs only for the first failed one."""
    for session in range(1, sessions + 1):
        logger.info("Starting faultstorm session %d/%d", session, sessions)
        runner = runner_factory(config, db_client, registry, dc_map=dc_map)
        result = runner.run()
        if not result.valid:
            logger.error("Faultstorm session %d/%d failed; retaining its logs", session, sessions)
            return False

        _cleanup_successful_session(config, db_client)
        logger.info("Faultstorm session %d/%d passed and was cleaned up", session, sessions)

    return True


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
        '--sessions',
        type=int,
        default=1,
        help='Number of independent fault sessions to run',
    )
    parser.add_argument(
        '--fault-cycles',
        type=int,
        default=None,
        help='Random fault cycles per session',
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
    else:
        config = get_default_config()

    # Override durations if specified
    if args.write_duration:
        config.write_phase_duration = args.write_duration
    if args.read_duration:
        config.read_phase_duration = args.read_duration
    elif not args.replay_scenario:
        config.read_phase_duration = 10

    if args.sessions < 1 or args.fault_cycles is not None and args.fault_cycles < 1:
        parser.error('--sessions and --fault-cycles must be positive')

    if args.fault_cycles is not None and not args.replay_scenario and not args.write_duration:
        config.write_phase_duration = args.fault_cycles * (config.fault_active_duration + config.fault_pause_duration)

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
    dc_map = build_pgconsul_dc_map(config)

    logger.info("DC map: %s", dc_map)

    # Run bounded sessions. A failed session keeps its logs for diagnostics;
    # a successful session is independent of the next one.
    passed = _run_sessions(config, db_client, registry, dc_map, args.sessions)

    # Exit with appropriate code
    sys.exit(0 if passed else 1)


if __name__ == '__main__':
    main()
