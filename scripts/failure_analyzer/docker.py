"""Docker integration helpers.

Isolates all ``docker`` subprocess calls behind small, injectable functions
so the analyzer and CLI don't shell out directly and tests can fake them.
"""

from __future__ import annotations

import logging
import subprocess
from typing import Protocol

from .config import Config
from .models import DockerFinding
from .patterns import PGCONSUL_PATTERNS, POSTGRES_PATTERNS, ZOOKEEPER_PATTERNS
from .scanner import Scanner
from .sources.docker_source import DockerLogSource, DockerRunner, SubprocessDockerRunner
from .utils import normalize_container_name

log = logging.getLogger(__name__)


class DockerRunnerFactory(Protocol):
    """Builds a :class:`DockerRunner` (injected for tests)."""

    def __call__(self) -> DockerRunner:
        ...


def docker_is_available(timeout: int = 5) -> bool:
    """Return True if docker is in PATH and the daemon is reachable."""
    try:
        r = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
        return r.returncode == 0
    except (OSError, subprocess.SubprocessError) as exc:
        log.debug("docker not available: %s", exc)
        return False


def docker_list_containers(label: str, timeout: int = 10) -> list[str]:
    """Return names of running containers carrying the given docker label.

    The pgconsul test containers are named ``postgresql1``, ``zookeeper1`` etc.
    (without a common name prefix), so they are identified by the shared
    ``pgconsul_tests`` label rather than by name.
    """
    try:
        r = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}", "--filter", f"label={label}"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("docker ps failed: %s", exc)
        return []
    return [n.strip() for n in r.stdout.splitlines() if n.strip()]


def build_docker_sources(
    containers: list[str],
    runner: DockerRunner,
    config: Config,
) -> list[DockerLogSource]:
    """Build :class:`DockerLogSource` objects for the given containers."""
    sources: list[DockerLogSource] = []
    for container in containers:
        short = normalize_container_name(container, config.docker_project)

        if any(short.startswith(p) for p in config.docker_postgresql_containers):
            for log_path in config.docker_pgconsul_paths:
                sources.append(
                    DockerLogSource(container, "pgconsul", log_path, runner, config.docker_tail_lines)
                )
            for log_path in config.docker_postgres_paths:
                sources.append(
                    DockerLogSource(container, "postgresql", log_path, runner, config.docker_tail_lines)
                )
        elif any(short.startswith(p) for p in config.docker_zookeeper_containers):
            zk_log = f"/var/log/zookeeper/zookeeper--server-{container}.log"
            sources.append(
                DockerLogSource(container, "zookeeper", zk_log, runner, config.docker_zk_tail_lines)
            )
    return sources


def scan_docker_containers(
    containers: list[str],
    scanner: Scanner,
    runner: DockerRunner,
    config: Config,
) -> list[DockerFinding]:
    """Scan running pgconsul docker containers for known failure patterns."""
    sources = build_docker_sources(containers, runner, config)
    findings: list[DockerFinding] = []
    for source in sources:
        if source.log_type == "pgconsul":
            patterns = PGCONSUL_PATTERNS
        elif source.log_type == "postgresql":
            patterns = POSTGRES_PATTERNS
        elif source.log_type == "zookeeper":
            patterns = ZOOKEEPER_PATTERNS
        else:
            continue
        findings.extend(scanner.scan_docker(source, patterns))
    return findings


def pg_containers(containers: list[str], config: Config) -> list[str]:
    """Return the postgresql containers among *containers*."""
    return [
        c for c in containers
        if any(
            normalize_container_name(c, config.docker_project).startswith(p)
            for p in config.docker_postgresql_containers
        )
    ]
