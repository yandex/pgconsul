"""Step definitions for deterministic faultstorm replay tests.

Provides steps to:
  - Set up the pgconsul cluster for replay testing
  - Apply faultstorm actions with concurrent write load
  - Validate data consistency and write availability

The load generator (setup, write, read) runs inside the ``faultstorm``
Docker container so it has direct network access to the PG nodes.
The operations log is copied back to the host for consistency checking.

The "I apply faultstorm actions" step starts the write load
automatically.  The write duration is set to the sum of all ``wait``
durations in the inline scenario so that the writers finish on their
own shortly after the fault replay completes — no SIGTERM is needed.
"""

import logging
import os
import re
import subprocess
import time

from behave import given, when, then

from faultstorm.checker import check_consistency
from faultstorm.cluster import ClusterManager
from faultstorm.config import TestConfig
from faultstorm.faults.engine import FaultEngine
from faultstorm.network_latency import NetworkLatencyManager

from faultstorm_config import create_pgconsul_registry, build_pgconsul_dc_map

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Container name for the faultstorm load node
FAULTSTORM_CONTAINER = "pgconsul_faultstorm_1"

# Path inside the faultstorm container for the operations log
CONTAINER_OPS_LOG = "/tmp/faultstorm_ops.log"

# Path inside the faultstorm container for the load_worker Python log
CONTAINER_LOAD_LOG = "/tmp/load.log"

# Regex to strip optional timestamp prefix from scenario lines
_TIMESTAMP_RE = re.compile(r"^\[[\d\-T:.]+\]\s*")


def _docker_exec(command, timeout=600):
    """Run a command inside the faultstorm container.

    Args:
        command: List of command parts to run
        timeout: Timeout in seconds

    Returns:
        stdout string

    Raises:
        subprocess.CalledProcessError: on non-zero exit
        subprocess.TimeoutExpired: on timeout
    """
    docker_cmd = ["docker", "exec", FAULTSTORM_CONTAINER] + command
    logger.debug("docker exec: %s", " ".join(docker_cmd))
    result = subprocess.run(
        docker_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    logger.debug("docker exec: %s %s", result.stdout, result.stderr)
    return result.stdout


def _docker_exec_bg(command):
    """Start a command inside the faultstorm container in background.

    Returns a Popen object.
    """
    docker_cmd = ["docker", "exec", FAULTSTORM_CONTAINER] + command
    logger.debug("docker exec (bg): %s", " ".join(docker_cmd))
    return subprocess.Popen(
        docker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _docker_cp_from(container_path, host_path):
    """Copy a file from the faultstorm container to the host."""
    src = f"{FAULTSTORM_CONTAINER}:{container_path}"
    logger.debug("docker cp %s -> %s", src, host_path)
    subprocess.run(
        ["docker", "cp", src, host_path],
        check=True,
        timeout=30,
    )


def _find_primary(db_nodes):
    """Find the current PG primary among db_nodes.

    Returns the node name of the primary, or None if no primary found.
    """
    for node in db_nodes:
        try:
            out = ClusterManager.exec_on_node(
                node,
                ["sudo", "-u", "postgres", "psql", "-tAc",
                 "SELECT NOT pg_is_in_recovery()"],
                timeout=5,
            )
            if out.strip() == "t":
                return node
        except Exception:
            continue
    return None


# ---- Setup ----

@given('the pgconsul cluster is ready for replay testing')
def step_cluster_ready(context):
    """Initialize test infrastructure for replay tests.

    Sets up:
      - TestConfig with pgconsul cluster nodes
      - FaultRegistry with all pgconsul-specific actions
      - DC map from context
      - Temporary directory for host-side logs
      - Detects current primary node
      - Runs load_worker.py setup inside the faultstorm container
    """
    # Create a persistent directory under logs/ for host-side logs so they
    # survive after the test and are not overwritten by save_logs.sh.
    scenario_name = re.sub(r'[^\w]+', '_', context.scenario.name).strip('_').lower()
    replay_logs_dir = os.path.join(context.logs_dir, "faultstorm_replay", scenario_name)
    os.makedirs(replay_logs_dir, exist_ok=True)
    context.replay_tmpdir = replay_logs_dir

    context.ops_log_path = os.path.join(replay_logs_dir, "operations.log")
    context.scenario_log_path = os.path.join(replay_logs_dir, "scenario.log")

    context.replay_config = TestConfig(
        name="replay_test",
        db_nodes=list(context.db_nodes),
        extra_nodes=list(context.extra_nodes),
        load_node=context.load_node,
        read_phase_duration=60,
        add_interval=0,
        read_interval=1.0,
        operation_timeout=5.0,
        writers_per_node=2,
        operations_log=context.ops_log_path,
        scenario_log=context.scenario_log_path,
        cross_dc_delays={},
    )

    context.replay_registry = create_pgconsul_registry()

    # Detect primary
    context.replay_primary = _find_primary(context.db_nodes)
    assert context.replay_primary is not None, (
        "Could not find a primary among db nodes"
    )
    logger.info("Detected primary: %s", context.replay_primary)

    # Initialize tracking attributes
    context.replay_engine = None
    context.replay_check_result = None

    context.latency_mgr = None

    # Clean any stale logs inside the container
    try:
        _docker_exec(["rm", "-f", CONTAINER_OPS_LOG, CONTAINER_LOAD_LOG], timeout=10)
    except Exception:
        pass

    # Set up the test table inside the container
    _docker_exec(["python3", "/root/load_worker.py", "setup"])
    logger.info("Test table created via faultstorm container")


# ---- Helpers ----

def _compute_total_wait(scenario_text):
    """Sum all ``wait`` durations in a scenario text.

    Parses the inline scenario and returns the total number of seconds
    from all ``wait <ordinal> <seconds>`` lines.  Lines with a leading
    ``+`` or ``-`` prefix are stripped before matching, so only bare
    ``wait`` actions (which are never healable) are counted.

    Args:
        scenario_text: Raw scenario text (after placeholder substitution).

    Returns:
        Total wait duration in seconds.
    """
    total = 0
    for raw_line in scenario_text.strip().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip optional timestamp prefix
        line = _TIMESTAMP_RE.sub("", line)
        if not line:
            continue
        # Strip +/- prefix
        if line.startswith("+") or line.startswith("-"):
            line = line[1:]
        parts = line.split()
        if parts and parts[0] == "wait" and len(parts) >= 3:
            try:
                total += int(parts[2])
            except ValueError:
                pass
    return total


def _find_max_ordinal(scenario_text):
    """Find the maximum ordinal number across all action lines.

    Each action line has the format::

        [+/-]action_name <ordinal> [params...]

    The ordinal is always the second token after stripping the optional
    ``+``/``-`` prefix and timestamp.

    Args:
        scenario_text: Raw scenario text (after placeholder substitution).

    Returns:
        Maximum ordinal found, or 0 if none found.
    """
    max_ord = 0
    for raw_line in scenario_text.strip().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip optional timestamp prefix
        line = _TIMESTAMP_RE.sub("", line)
        if not line or line.startswith("#"):
            continue
        # Strip +/- prefix
        if line.startswith("+") or line.startswith("-"):
            line = line[1:]
        parts = line.split()
        if len(parts) >= 2:
            try:
                ordinal = int(parts[1])
                max_ord = max(max_ord, ordinal)
            except ValueError:
                pass
    return max_ord


def _shift_ordinals(scenario_text, offset):
    """Shift all ordinal numbers in a scenario text by *offset*.

    Each non-comment, non-empty line is expected to contain an ordinal as
    the second token (after stripping optional ``+``/``-`` prefix).  The
    ordinal is incremented by *offset* and the line is reassembled.

    Args:
        scenario_text: Raw scenario text (one action per line).
        offset: Integer to add to every ordinal.

    Returns:
        New scenario text with shifted ordinals.
    """
    if offset == 0:
        return scenario_text

    shifted_lines = []
    for raw_line in scenario_text.strip().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            shifted_lines.append(line)
            continue

        # Strip optional timestamp prefix
        stripped = _TIMESTAMP_RE.sub("", line)
        if not stripped or stripped.startswith("#"):
            shifted_lines.append(line)
            continue

        # Detect +/- prefix
        prefix = ""
        action_line = stripped
        if stripped.startswith("+") or stripped.startswith("-"):
            prefix = stripped[0]
            action_line = stripped[1:]

        parts = action_line.split()
        if len(parts) >= 2:
            try:
                ordinal = int(parts[1])
                parts[1] = str(ordinal + offset)
                shifted_lines.append(prefix + " ".join(parts))
            except ValueError:
                shifted_lines.append(line)
        else:
            shifted_lines.append(line)

    return "\n".join(shifted_lines)


def _build_repeated_scenario(scenario_text, n):
    """Build a scenario that repeats *scenario_text* N times with shifted ordinals.

    On each iteration the ordinals are shifted so that they don't collide
    with previous iterations.  The shift for iteration *i* equals
    ``i * max_ordinal`` where *max_ordinal* is the largest ordinal found
    in the original text.

    Args:
        scenario_text: Raw scenario text (after placeholder substitution).
        n: Number of repetitions (must be > 0).

    Returns:
        Combined scenario text with shifted ordinals.
    """
    max_ordinal = _find_max_ordinal(scenario_text)
    assert max_ordinal > 0, (
        "Could not find any ordinals in scenario text"
    )

    parts = []
    for iteration in range(n):
        offset = iteration * max_ordinal
        shifted = _shift_ordinals(scenario_text, offset)
        parts.append(shifted)

    return "\n".join(parts)


# ---- Apply faultstorm actions (with write load) ----


def _apply_faultstorm_scenario(context, scenario_text):
    """Common implementation for applying a faultstorm scenario.

    Starts write load, replays the given scenario text, and waits for
    replay completion.

    Args:
        context: Behave context with replay_config, replay_registry, etc.
        scenario_text: Fully resolved scenario text (placeholders already
                       substituted, ordinals already shifted if needed).
    """
    # Compute write duration from the scenario wait actions
    write_duration = _compute_total_wait(scenario_text)
    assert write_duration > 0, (
        "Scenario must contain at least one 'wait' action to define "
        "the write duration"
    )
    # Store for use in "write-load is stopped"
    context.replay_config.write_phase_duration = write_duration
    logger.info("Computed write duration from scenario waits: %ds", write_duration)

    context.latency_mgr = NetworkLatencyManager(context.replay_config)
    context.latency_mgr.remove(force_all_nodes=True)
    context.latency_mgr.apply(build_pgconsul_dc_map(context.replay_config))

    logger.info("Applyed cross-dc-delays")

    # Write scenario to temp file
    replay_path = os.path.join(context.replay_tmpdir, "replay_input.log")
    with open(replay_path, "w") as f:
        f.write("# Deterministic replay scenario\n\n")
        for line in scenario_text.strip().splitlines():
            line = line.strip()
            if line:
                f.write(line + "\n")

    logger.info("Replay scenario written to %s", replay_path)

    # Start write load in background
    config = context.replay_config
    _docker_exec_bg([
        "python3", "/root/load_worker.py", "write",
        "--duration", str(config.write_phase_duration),
        "--ops-log", CONTAINER_OPS_LOG,
        "--add-interval", str(config.add_interval),
        "--operation-timeout", str(config.operation_timeout),
        "--writers-per-node", str(config.writers_per_node),
    ])
    logger.info("Write load started in faultstorm container (duration=%ds)",
                config.write_phase_duration)

    # Create fault engine and run replay
    engine = FaultEngine(
        config, context.replay_registry,
        dc_map=context.dc_map,
    )
    context.replay_engine = engine

    logger.info("Starting faultstorm replay...")
    engine.run_replay(replay_path, config.scenario_log)
    engine.heal_all()
    logger.info("Faultstorm replay completed")


@when('I apply cross-dc delays {dc1_dc2:d}ms {dc2_dc3:d}ms {dc1_dc3:d}ms')
def step_apply_cross_dc_delays(context, dc1_dc2, dc2_dc3, dc1_dc3):
    context.replay_config.cross_dc_delays={("dc1", "dc2"): dc1_dc2, ("dc2", "dc3"): dc2_dc3, ("dc1", "dc3"): dc1_dc3}


@when('I apply faultstorm actions')
def step_apply_faultstorm_actions(context):
    """Start write load, replay inline faultstorm scenario, and wait.

    This step combines write-load startup and fault-action replay.
    The write duration is computed as the sum of all ``wait`` durations
    in the inline scenario so that the writers finish naturally once the
    replay is done — no SIGTERM is required.

    The scenario text uses the standard faultstorm log format:
      +action_name ordinal params   (enable healable action)
      -action_name ordinal params   (heal action)
      action_name ordinal params    (fire-and-forget action)

    The special placeholder {primary} is replaced with the actual
    primary node name detected during setup.

    This step blocks until all actions are executed **and** the write
    load process exits on its own.
    """
    assert context.text is not None, "No scenario text provided in docstring"

    # Replace {primary} placeholder with actual primary node
    scenario_text = context.text.replace("{primary}", context.replay_primary)

    _apply_faultstorm_scenario(context, scenario_text)


@when('I apply faultstorm actions repeated {n:d} times')
def step_apply_faultstorm_actions_repeated(context, n):
    """Start write load, replay inline faultstorm scenario N times, and wait.

    Works like "I apply faultstorm actions" but repeats the inline
    scenario *n* times.  On each iteration all ordinals in the action
    lines are shifted so that they never collide with ordinals from
    previous iterations.

    The shift is computed as follows:

    1. Find the maximum ordinal *X* in the original scenario text.
    2. Iteration 0 adds offset 0 (ordinals unchanged).
    3. Iteration 1 adds offset X.
    4. Iteration 2 adds offset 2·X, etc.

    For example, given the scenario::

        +maintenance 1 {primary}
        wait 2 10
        resetup 3 {primary}
        wait 4 90
        -maintenance 1 {primary}
        wait 5 60

    With *n=2*, the maximum ordinal is 5.  Iteration 0 keeps ordinals
    1–5; iteration 1 shifts them to 6–10.

    The total write duration is the sum of all ``wait`` durations across
    all iterations.
    """
    assert context.text is not None, "No scenario text provided in docstring"
    assert n > 0, "Number of repetitions must be positive"

    # Replace {primary} placeholder with actual primary node
    scenario_text = context.text.replace("{primary}", context.replay_primary)

    # Build repeated scenario with shifted ordinals
    repeated_text = _build_repeated_scenario(scenario_text, n)
    logger.info("Built repeated scenario: %d iterations", n)

    _apply_faultstorm_scenario(context, repeated_text)


# ---- Stop write load ----

def _wait_for_process_exit(process_pattern, timeout=60):
    """Wait until a process matching *process_pattern* exits on its own.

    Polls ``pgrep -f`` inside the faultstorm container until the process
    is no longer found.  Does **not** send any signals — the process is
    expected to finish naturally.

    Args:
        process_pattern: Pattern passed to ``pgrep -f``
        timeout: Maximum seconds to wait for the process to exit

    Raises:
        TimeoutError: If the process is still running after *timeout* seconds
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", FAULTSTORM_CONTAINER,
             "pgrep", "-f", process_pattern],
            capture_output=True, timeout=10,
        )
        if result.returncode != 0:
            # pgrep returns non-zero when no matching process exists
            return
        time.sleep(1)

    raise TimeoutError(
        f"Process matching '{process_pattern}' did not exit "
        f"within {timeout}s"
    )


@when('write-load is stopped')
def step_stop_write_load(context):
    """Wait for background writers to finish, run the read phase, and
    fetch results.

    The writers are expected to terminate on their own once their
    ``--duration`` (derived from scenario wait totals) expires.
    This step simply waits for exit, then proceeds to the read phase.
    """
    config = context.replay_config
    logger.info("Waiting for write load to finish on its own")

    # Allow extra margin beyond the write duration for the process to
    # wind down (thread joins, final flushes, etc.).
    wait_timeout = config.write_phase_duration + 120
    _wait_for_process_exit("load_worker.py write", timeout=wait_timeout)
    logger.info("Write load finished")

    # Run read phase inside the container
    _docker_exec([
        "python3", "/root/load_worker.py", "read",
        "--duration", str(config.read_phase_duration),
        "--ops-log", CONTAINER_OPS_LOG,
        "--read-interval", str(config.read_interval),
        "--operation-timeout", str(config.operation_timeout),
    ], timeout=config.read_phase_duration + 60)
    logger.info("Read phase completed")

    # Copy operations log from container to host
    _docker_cp_from(CONTAINER_OPS_LOG, config.operations_log)
    logger.info("Operations log copied to %s", config.operations_log)

    # Copy load_worker Python log from container to host
    host_load_log = os.path.join(context.replay_tmpdir, "load.log")
    try:
        _docker_cp_from(CONTAINER_LOAD_LOG, host_load_log)
        logger.info("Load worker log copied to %s", host_load_log)
    except Exception as e:
        logger.warning("Could not copy load worker log: %s", e)

    # Run consistency check
    context.replay_check_result = check_consistency(config.operations_log)

    result = context.replay_check_result
    logger.info(
        "Check result: valid=%s total=%d successful=%d failed=%d "
        "availability=%.2f%% lost=%d unexpected=%d recovered=%d",
        result.valid, result.total_attempts, result.successful_adds,
        result.failed_adds, result.write_availability * 100,
        len(result.lost), len(result.unexpected), len(result.recovered),
    )


# ---- Assertions ----

@then('there was no data lost')
def step_no_data_lost(context):
    """Assert that no confirmed writes were lost."""
    result = context.replay_check_result
    assert result is not None, "No check result available (did you stop write-load?)"
    assert len(result.lost) == 0, (
        f"DATA LOSS: {len(result.lost)} confirmed writes were lost: "
        f"{sorted(result.lost)[:20]}"
    )
    assert len(result.unexpected) == 0, (
        f"CORRUPTION: {len(result.unexpected)} unexpected values found: "
        f"{sorted(result.unexpected)[:20]}"
    )

@then('some data was lost')
def step_no_data_lost(context):
    """Assert that no confirmed writes were lost."""
    result = context.replay_check_result
    assert result is not None, "No check result available (did you stop write-load?)"
    assert len(result.lost) != 0 or len(result.unexpected) != 0, (
        "No data was lost, hmmm..."
    )


@then('cluster was available at least {threshold:g} of the time')
def step_check_availability(context, threshold):
    """Assert that write availability met the threshold.

    Args:
        threshold: Minimum acceptable write availability (0.0 to 1.0)
    """
    result = context.replay_check_result
    assert result is not None, "No check result available (did you stop write-load?)"
    assert result.write_availability >= threshold, (
        f"Write availability {result.write_availability:.2%} "
        f"is below threshold {threshold:.2%}"
    )
