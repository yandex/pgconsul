"""
Entry point for pgconsul FaultStorm tests.

Configures pgconsul-specific cluster settings, creates a PgConsulClient,
registers built-in fault actions, and runs the test via TestRunner.
"""

import json
import logging
import os
import re
import sys
import threading
import time
from typing import Callable

from faultstorm_config import get_default_config, get_quick_config
from faultstorm_config import create_pgconsul_registry, build_pgconsul_dc_map
from faultstorm_pg_client import PgConsulClient
from faultstorm.checker import check_consistency
from faultstorm.cluster import ClusterManager
from faultstorm.faults.engine import FaultEngine
from faultstorm.load_generator import LoadGenerator
from faultstorm.runner import TestRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


def _wait_for_recovered_cluster(config, db_client, timeout: float = 600) -> str:  # type: ignore[no-untyped-def]
    """Wait for one primary and all replicas to stream synchronously."""
    deadline = time.monotonic() + timeout
    last_status = None
    while time.monotonic() < deadline:
        states = {}
        errors = {}
        for node in config.db_nodes:
            try:
                states[node] = db_client.get_node_state(node)
            except Exception as exc:
                errors[node] = str(exc)

        primaries = [node for node, (in_recovery, _, _) in states.items() if not in_recovery]
        replicas = [node for node, (in_recovery, _, _) in states.items() if in_recovery]
        status = {'roles': {node: 'replica' if state[0] else 'primary' for node, state in states.items()}, 'errors': errors}
        if not errors and len(primaries) == 1 and len(replicas) == len(config.db_nodes) - 1:
            primary = primaries[0]
            _, ssn, synchronous = states[primary]
            expected = {db_client.replication_application_name(node) for node in replicas}
            ssn_members = set(re.findall(r'[A-Za-z_][A-Za-z0-9_]*', ssn))
            status.update({'ssn': ssn, 'synchronous_streaming': sorted(synchronous)})
            if expected <= ssn_members and expected <= synchronous:
                logger.info("Cluster recovered: primary=%s, replicas=%s, SSN=%s", primary, replicas, ssn)
                return primary

        if status != last_status:
            logger.info("Waiting for cluster recovery: %s", status)
            last_status = status
        time.sleep(min(2, max(0, deadline - time.monotonic())))

    raise RuntimeError("Cluster did not recover before reading within %ss; last state: %s" % (timeout, last_status))


def _read_all_hosts(config, db_client, ops_log, primary: str, timeout: float = 120) -> None:  # type: ignore[no-untyped-def]
    """Wait for identical snapshots on every host and record each final read."""
    deadline = time.monotonic() + timeout
    last_status = None
    while time.monotonic() < deadline:
        snapshots = {}
        errors = {}
        for node in config.db_nodes:
            try:
                snapshots[node] = db_client.read(node)
            except Exception as exc:
                errors[node] = str(exc)

        status = {'row_counts': {node: len(values) for node, values in snapshots.items()}, 'errors': errors}
        if not errors and len(snapshots) == len(config.db_nodes):
            reference = snapshots[primary]
            differences = {node: {'missing': len(reference - values), 'extra': len(values - reference)}
                           for node, values in snapshots.items() if values != reference}
            if not differences:
                for node, values in snapshots.items():
                    now = time.time()
                    ops_log.write(json.dumps({'type': 'ok', 'action': 'read', 'timestamp': now,
                                             'pretty_ts': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)),
                                             'value': sorted(values), 'node': node}) + '\n')
                ops_log.flush()
                logger.info("Validated final read on every host: %s", status['row_counts'])
                return
            status['differences_from_primary'] = differences

        if status != last_status:
            logger.info("Waiting for equal final reads: %s", status)
            last_status = status
        time.sleep(min(2, max(0, deadline - time.monotonic())))

    raise RuntimeError("Final reads did not converge on every host within %ss; last state: %s" % (timeout, last_status))


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


class PgConsulTestRunner(TestRunner):
    """FaultStorm runner with a recovery gate and final reads from every host."""

    def _run_phases(self):  # type: ignore[no-untyped-def]
        with open(self.config.operations_log, 'w') as ops_log:
            load_gen = LoadGenerator(self.config, self.db_client)
            engine = FaultEngine(self.config, self.fault_registry, dc_map=self.dc_map)

            logger.info("Setting up test table...")
            load_gen.setup()

            logger.info("Phase 1: Write + Faults started")
            if self.config.replay_scenario:
                fault_thread = threading.Thread(
                    target=engine.run_replay,
                    args=(self.config.replay_scenario, self.config.scenario_log),
                )
            else:
                fault_thread = threading.Thread(
                    target=engine.run_random,
                    args=(self.config.write_phase_duration, self.config.scenario_log),
                )
            fault_thread.start()
            load_gen.run_write_phase(self.config.write_phase_duration, ops_log)
            engine.stop()
            fault_thread.join()
            engine.heal_all()
            logger.info("Phase 1 completed")

            primary = _wait_for_recovered_cluster(self.config, self.db_client)
            logger.info("Phase 2: Read validation started")
            load_gen.run_read_phase(self.config.read_phase_duration, ops_log)
            load_gen.stop()
            _read_all_hosts(self.config, self.db_client, ops_log, primary)
            logger.info("Phase 2 completed")

        logger.info("Checking consistency...")
        return check_consistency(self.config.operations_log)


def _run_sessions(config, db_client, registry, dc_map, sessions: int, runner_factory: Callable = PgConsulTestRunner) -> bool:  # type: ignore[no-untyped-def]
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
