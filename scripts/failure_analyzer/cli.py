"""Command-line interface for the failure analyzer.

This is a thin entry point: it parses arguments, assembles the collaborators
(:class:`Config`, :class:`Analyzer`, :class:`Reporter`) and runs them. All
heavy lifting lives in the package modules; the CLI just wires them together.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional, TextIO

from .analyzer import Analyzer
from .config import Config
from .docker import (
    docker_is_available,
    docker_list_containers,
    pg_containers,
    scan_docker_containers,
)
from .reporting import JsonReporter, Reporter, TextReporter
from .scanner import Scanner
from .sources.docker_source import SubprocessDockerRunner
from .utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze failed behave test logs and pinpoint root cause."
    )
    parser.add_argument(
        "log_dirs",
        nargs="*",
        help="Log directories to search (default: auto-discover logs/ and logs.local/)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show all findings, not just the top 15",
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        default=False,
        help=(
            "Also scan live docker containers (requires running test environment). "
            "Enabled automatically when no saved container logs are found."
        ),
    )
    parser.add_argument(
        "--no-docker",
        dest="docker",
        action="store_false",
        help="Disable docker container scanning (overrides auto-detect).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--no-grep",
        dest="use_grep",
        action="store_false",
        help="Use a pure-Python reader instead of the external grep binary.",
    )
    return parser


def make_reporter(fmt: str, verbose: bool, config: Config) -> Reporter:
    if fmt == "json":
        return JsonReporter()
    return TextReporter(verbose=verbose, findings_limit=config.default_findings_limit)


def main(
    argv: Optional[list[str]] = None,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> int:
    args = build_parser().parse_args(argv)
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    configure_logging(verbose=args.verbose)

    config = Config(
        log_dirs=tuple(args.log_dirs),
        use_grep=args.use_grep,
    )
    analyzer = Analyzer(config=config, scanner=Scanner())
    reporter = make_reporter(args.format, args.verbose, config)

    # Informational messages go to stderr so stdout stays pure for the report
    # (text or JSON) and can be piped/parsed by CI.
    info_stream = err if args.format == "json" else out
    print(
        f"Searching log roots: {[str(r) for r in args.log_dirs] or list(config.auto_discover_roots)}",
        file=info_stream,
    )

    result = analyzer.analyze(list(args.log_dirs))
    reporter.render(result, out)

    # Docker container scan.
    # Auto-enable if no saved container logs were found (e.g. stuck test,
    # logs not yet extracted) and --no-docker was not explicitly passed.
    use_docker = args.docker
    if not use_docker and not result.container_logs and "--no-docker" not in (argv or sys.argv):
        if docker_is_available(timeout=config.docker_info_timeout):
            use_docker = True
            print("\n⚠️  No saved container logs found. Auto-enabling docker scan...", file=out)

    if use_docker:
        _run_docker_scan(args, config, out, err)

    return 0


def _run_docker_scan(args, config: Config, out: TextIO, err: TextIO) -> None:
    print("\n" + "=" * 80, file=out)
    print("DOCKER CONTAINER LOG SCAN (live containers)", file=out)
    print("=" * 80, file=out)

    if not docker_is_available(timeout=config.docker_info_timeout):
        print("  ❌ Docker is not available or daemon is not running.", file=out)
        print("=" * 80, file=out)
        return

    containers = docker_list_containers(config.docker_label_filter, timeout=config.docker_list_timeout)
    if not containers:
        print(f"  ❌ No running containers found (filter: label={config.docker_label_filter}).", file=out)
        print("     Make sure the test environment is still running.", file=out)
        print("=" * 80, file=out)
        return

    runner = SubprocessDockerRunner(timeout=config.docker_read_timeout)
    scanner = Scanner()
    findings = scan_docker_containers(containers, scanner, runner, config)

    if isinstance(_reporter_for_docker(args, config), TextReporter):
        _reporter_for_docker(args, config).render_docker(
            findings, containers, out, pg_containers(containers, config)
        )
    else:
        # JSON path: emit docker findings as a separate JSON document.
        import json
        payload = {
            "docker_findings": [
                {
                    "container": f.container,
                    "log_path": f.log_path,
                    "pattern": f.pattern_name,
                    "weight": f.weight,
                    "line": f.line,
                }
                for f in findings
            ],
            "containers": containers,
        }
        json.dump(payload, out, indent=2, default=str)
        out.write("\n")
    print("=" * 80, file=out)


def _reporter_for_docker(args, config: Config) -> Reporter:
    return make_reporter(args.format, args.verbose, config)


if __name__ == "__main__":
    raise SystemExit(main())
