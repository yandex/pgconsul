"""Configuration for the failure analyzer.

All magic numbers and hardcoded paths from the original script live here as a
single :class:`Config` dataclass. The analyzer and its collaborators receive a
``Config`` instance via dependency injection, so behavior is adjustable and
testable without monkey-patching module globals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Config:
    """Tunable settings for the analysis pipeline."""

    # Maximum file size to read fully (50 MB). Larger files (e.g. postgresql.log
    # can be 180+ MB) are streamed line-by-line with DEBUG filtering.
    max_full_read_size: int = 50 * 1024 * 1024

    # How many lines to tail from docker container logs.
    docker_tail_lines: int = 5000
    docker_zk_tail_lines: int = 2000

    # Subprocess timeouts (seconds).
    docker_info_timeout: int = 5
    docker_list_timeout: int = 10
    docker_read_timeout: int = 30

    # How many findings to show by default (non-verbose).
    default_findings_limit: int = 15

    # Minimum number of matches for a stuck pattern to be reported.
    stuck_min_occurrences: int = 5

    # Docker project name used by docker-compose in pgconsul tests.
    docker_project: str = "pgconsul"

    # Docker label used to identify test-environment containers.
    # The test containers are named ``postgresql1``, ``zookeeper1`` etc. (without
    # the ``pgconsul_`` prefix), so filtering by name does not work. All of them
    # carry the ``pgconsul_tests`` label, which we use instead.
    docker_label_filter: str = "pgconsul_tests"

    # Container name prefixes (matched with startswith after normalization).
    docker_postgresql_containers: tuple[str, ...] = (
        "postgresql1", "postgresql2", "postgresql3",
    )
    docker_zookeeper_containers: tuple[str, ...] = (
        "zookeeper1", "zookeeper2", "zookeeper3",
    )

    # Log files to read from containers.
    docker_pgconsul_paths: tuple[str, ...] = (
        "/var/log/pgconsul/pgconsul.log",
    )
    # PostgreSQL log file name depends on the major version (e.g.
    # postgresql-14-main.log). We try both the versioned name and the
    # generic name; missing files are silently skipped by the docker source.
    docker_postgres_paths: tuple[str, ...] = (
        "/var/log/postgresql/postgresql-14-main.log",
        "/var/log/postgresql/postgresql.log",
    )
    # Supervisor log — captures pgconsul startup crashes (tracebacks printed
    # to stderr before pgconsul's own logging initialises) and ZK output.
    docker_supervisor_paths: tuple[str, ...] = (
        "/var/log/supervisor.log",
    )

    # Auto-discovery candidate roots (relative to CWD).
    auto_discover_roots: tuple[str, ...] = ("logs", "logs.local")

    # Use the external `grep` binary for large-file pre-filtering. When False,
    # a pure-Python reader is used (slower but portable).
    use_grep: bool = True

    # Extra roots to scan (populated by the CLI).
    log_dirs: tuple[str, ...] = field(default_factory=tuple)

    def auto_discover_paths(self) -> list[Path]:
        return [Path(p) for p in self.auto_discover_roots]
