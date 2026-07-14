#!/usr/bin/env python3
"""
Load worker for running inside the faultstorm Docker container.

Provides subcommands to set up the test table, run the write phase,
and run the read phase.  The operations log is written inside the
container and can be retrieved via ``docker cp``.

Usage from the host::

    # Setup
    docker exec pgconsul_faultstorm_1 python3 /root/load_worker.py setup

    # Start writers (blocks for --duration seconds, then exits)
    docker exec pgconsul_faultstorm_1 python3 /root/load_worker.py write \
        --duration 3600 --ops-log /tmp/ops.log

    # Read phase
    docker exec pgconsul_faultstorm_1 python3 /root/load_worker.py read \
        --duration 60 --ops-log /tmp/ops.log

    # Copy results back
    docker cp pgconsul_faultstorm_1:/tmp/ops.log ./ops.log
"""

import argparse
import logging
import signal
import sys

sys.path.insert(0, "/root")

from faultstorm.config import TestConfig
from faultstorm.load_generator import LoadGenerator
from faultstorm_pg_client import PgConsulClient

LOG_FILE = "/tmp/load.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Also write all log records to a file so the test harness can
# retrieve them from the container after the run.
_file_handler = logging.FileHandler(LOG_FILE, mode="a")
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"),
)
logging.getLogger().addHandler(_file_handler)

logger = logging.getLogger(__name__)

DB_NODES = ["postgresql1", "postgresql2", "postgresql3"]


def _make_load_gen(args: argparse.Namespace) -> LoadGenerator:
    config = TestConfig(
        name="replay_worker",
        db_nodes=DB_NODES,
        add_interval=getattr(args, "add_interval", 0),
        read_interval=getattr(args, "read_interval", 1.0),
        operation_timeout=getattr(args, "operation_timeout", 5.0),
        writers_per_node=getattr(args, "writers_per_node", 2),
    )
    client = PgConsulClient(DB_NODES)
    return LoadGenerator(config, client)


def cmd_setup(args: argparse.Namespace) -> None:
    """Set up the test table."""
    load_gen = _make_load_gen(args)
    load_gen.setup()
    logger.info("Setup complete")


def cmd_write(args: argparse.Namespace) -> None:
    """Run the write phase for --duration seconds."""
    load_gen = _make_load_gen(args)

    # Allow graceful stop via SIGTERM (sent by ``docker exec`` timeout
    # or by the host test harness killing the exec process).
    def _sigterm(signum, frame):
        logger.info("Received SIGTERM, stopping writers")
        load_gen.stop()

    signal.signal(signal.SIGTERM, _sigterm)

    with open(args.ops_log, "a") as f:
        load_gen.run_write_phase(args.duration, f)

    logger.info("Write phase finished")


def cmd_read(args: argparse.Namespace) -> None:
    """Run the read phase for --duration seconds."""
    load_gen = _make_load_gen(args)
    with open(args.ops_log, "a") as f:
        load_gen.run_read_phase(args.duration, f)
    logger.info("Read phase finished")


def main() -> None:
    parser = argparse.ArgumentParser(description="Faultstorm load worker")
    sub = parser.add_subparsers(dest="command")

    # setup
    sub.add_parser("setup", help="Create the test table")

    # write
    wp = sub.add_parser("write", help="Run write phase")
    wp.add_argument("--duration", type=int, required=True)
    wp.add_argument("--ops-log", required=True)
    wp.add_argument("--add-interval", type=float, default=0)
    wp.add_argument("--operation-timeout", type=float, default=5.0)
    wp.add_argument("--writers-per-node", type=int, default=2)

    # read
    rp = sub.add_parser("read", help="Run read phase")
    rp.add_argument("--duration", type=int, required=True)
    rp.add_argument("--ops-log", required=True)
    rp.add_argument("--read-interval", type=float, default=1.0)
    rp.add_argument("--operation-timeout", type=float, default=5.0)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    {"setup": cmd_setup, "write": cmd_write, "read": cmd_read}[args.command](args)


if __name__ == "__main__":
    main()
