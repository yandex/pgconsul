#!/usr/bin/env python3
"""Dump all ZooKeeper records under a given path prefix.

Designed to be copied into a pgconsul postgresql container for debugging
integration tests. The container already has ``python3-kazoo`` and the ZK
SSL certificates (``/etc/zk-ssl/``), so the script has zero external
dependencies beyond the standard library and ``kazoo``.

Connection parameters are read from the pgconsul config file
(``/etc/pgconsul.conf`` by default) — the same file the daemon uses — so the
script connects exactly the same way pgconsul does (SSL + digest auth).

Usage (inside a postgresql container)::

    # Auto-read /etc/pgconsul.conf, dump everything under the configured prefix
    python3 dump_zk.py

    # Use a different config file
    python3 dump_zk.py --config /path/to/pgconsul.conf

    # Override the path prefix (relative to the configured zk_lockpath_prefix)
    python3 dump_zk.py --path /pgconsul/postgresql/

    # Machine-readable JSON output (for piping to jq / diff)
    python3 dump_zk.py --format json

    # Dump only the tree structure (paths, no values)
    python3 dump_zk.py --tree

    # Connect without reading a config — pass everything explicitly
    python3 dump_zk.py --hosts host:2281 --prefix /pgconsul/postgresql/ \\
        --ssl --cert /etc/zk-ssl/server.crt --key /etc/zk-ssl/server.key \\
        --ca /etc/zk-ssl/ca.cert.pem --auth --user user1 --password testpassword123

Copy into a running container::

    docker cp scripts/dump_zk.py pgconsul_postgresql1_1:/tmp/dump_zk.py
    docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from configparser import RawConfigParser, NoOptionError, NoSectionError
from dataclasses import dataclass, field
from typing import Any

try:
    from kazoo.client import KazooClient
    from kazoo.exceptions import NoNodeError
except ImportError:  # pragma: no cover - handled at runtime with a clear message
    KazooClient = None  # type: ignore[assignment, misc]
    NoNodeError = Exception  # type: ignore[assignment, misc]


_LOG = logging.getLogger("dump_zk")

# Default paths matching the pgconsul test container layout.
DEFAULT_CONFIG = "/etc/pgconsul.conf"
DEFAULT_PREFIX = "/pgconsul/postgresql/"


@dataclass
class ZkConnectionParams:
    """All parameters needed to create a KazooClient — mirrors ZkClientConfig."""

    hosts: str
    prefix: str
    timeout: float = 1.0
    auth: bool = False
    username: str | None = None
    password: str | None = None
    ssl: bool = False
    cert: str | None = None
    key: str | None = None
    ca: str | None = None
    verify_certs: bool = True


@dataclass
class ZkNode:
    """A single ZK node snapshot for serialisation."""

    path: str
    value: str | None
    children: list[str] = field(default_factory=list)


def load_params_from_config(config_path: str, prefix_override: str | None = None) -> ZkConnectionParams:
    """Parse pgconsul config into connection params — same logic as create_zk_client."""
    parser = RawConfigParser()
    if not parser.read(config_path):
        raise FileNotFoundError(f"Config file not found or empty: {config_path}")

    section = "global"
    if not parser.has_section(section):
        raise ValueError(f"Section [{section}] not found in {config_path}")

    def _get(key: str, fallback: str | None = None) -> str | None:
        try:
            return parser.get(section, key)
        except (NoOptionError, NoSectionError):
            return fallback

    def _getbool(key: str, fallback: bool = False) -> bool:
        try:
            return parser.getboolean(section, key)
        except (NoOptionError, NoSectionError, ValueError):
            return fallback

    def _getfloat(key: str, fallback: float = 1.0) -> float:
        try:
            return parser.getfloat(section, key)
        except (NoOptionError, NoSectionError, ValueError):
            return fallback

    hosts = _get("zk_hosts")
    if not hosts:
        raise ValueError(f"zk_hosts is required in [{section}] of {config_path}")

    prefix = prefix_override or _get("zk_lockpath_prefix") or DEFAULT_PREFIX

    params = ZkConnectionParams(hosts=hosts, prefix=prefix, timeout=_getfloat("iteration_timeout", 1.0))

    params.auth = _getbool("zk_auth")
    if params.auth:
        params.username = _get("zk_username")
        params.password = _get("zk_password")
        if not params.username or not params.password:
            raise ValueError("zk_auth is enabled but zk_username/zk_password are missing")

    params.ssl = _getbool("zk_ssl")
    if params.ssl:
        params.cert = _get("certfile")
        params.key = _get("keyfile")
        params.ca = _get("ca_cert")
        if not params.cert or not params.key or not params.ca:
            raise ValueError("zk_ssl is enabled but certfile/keyfile/ca_cert are missing")

    params.verify_certs = _getbool("verify_certs", True)
    return params


def build_kazoo_client(params: ZkConnectionParams) -> Any:
    """Create a KazooClient configured exactly like pgconsul does."""
    if KazooClient is None:
        raise SystemExit(
            "kazoo is not installed. In a pgconsul postgresql container run:\n"
            "  apt-get install -y python3-kazoo\n"
            "Or copy the script into a postgresql container (it has python3-kazoo preinstalled)."
        )

    args: dict[str, Any] = {
        "hosts": params.hosts,
        "timeout": params.timeout,
    }

    if params.auth:
        from kazoo.security import make_digest_acl

        acl = make_digest_acl(params.username, params.password, all=True)
        args["default_acl"] = [acl]
        args["auth_data"] = [
            ("digest", f"{params.username}:{params.password}"),
        ]

    if params.ssl:
        args["use_ssl"] = True
        args["certfile"] = params.cert
        args["keyfile"] = params.key
        args["ca"] = params.ca
        args["verify_certs"] = params.verify_certs

    return KazooClient(**args)


def dump_tree(client: Any, root: str, include_values: bool = True) -> list[ZkNode]:
    """Recursively walk the ZK tree from *root* and collect all nodes.

    Returns a flat list ordered by path (depth-first), which keeps parent
    nodes before their children — convenient for reading and diffing.
    """
    results: list[ZkNode] = []

    def _walk(path: str) -> None:
        try:
            data, _stat = client.get(path)
        except NoNodeError:
            _LOG.debug("No node at %s — skipping", path)
            return

        value: str | None = None
        if include_values and data is not None:
            value = data.decode("utf-8", errors="replace")

        try:
            children = client.get_children(path)
        except NoNodeError:
            children = []

        results.append(ZkNode(path=path, value=value, children=sorted(children)))

        for child in sorted(children):
            child_path = f"{path.rstrip('/')}/{child}"
            _walk(child_path)

    _walk(root)
    return results


def format_text(nodes: list[ZkNode], tree_only: bool = False) -> str:
    """Render nodes as a human-readable indented tree."""
    if not nodes:
        return "(no nodes found)\n"

    lines: list[str] = []
    for node in nodes:
        depth = node.path.strip("/").count("/")
        indent = "  " * depth
        name = node.path.rsplit("/", 1)[-1] or node.path
        if tree_only:
            lines.append(f"{indent}{name}/")
        else:
            value_repr = _format_value(node.value)
            lines.append(f"{indent}{name}  =  {value_repr}")
    return "\n".join(lines) + "\n"


def _format_value(value: str | None) -> str:
    """Render a node value: pretty-print JSON, show empty as <empty>, truncate long blobs."""
    if value is None:
        return "<empty>"
    if value == "":
        return "<empty>"
    # Try to pretty-print JSON values for readability.
    stripped = value.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
            return json.dumps(parsed, indent=2, ensure_ascii=False, sort_keys=True)
        except (json.JSONDecodeError, ValueError):
            pass
    if len(value) > 500:
        return f"{value[:500]}... <truncated, {len(value)} bytes total>"
    return value


def format_json(nodes: list[ZkNode]) -> str:
    """Render nodes as a JSON array of {path, value} objects."""
    payload = [{"path": n.path, "value": n.value} for n in nodes]
    return json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump all ZooKeeper records under a path prefix. "
        "Designed to run inside a pgconsul postgresql container.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--config", default=DEFAULT_CONFIG,
        help=f"Path to pgconsul config (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--path", default=None,
        help=f"ZK path prefix to dump (default: from config, e.g. {DEFAULT_PREFIX})",
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--tree", action="store_true",
        help="Show only the tree structure (paths, no values)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging on stderr",
    )

    # Explicit connection overrides (skip config parsing entirely).
    parser.add_argument("--hosts", default=None, help="ZK hosts (e.g. host:2281,...)")
    parser.add_argument("--prefix", default=None, help="ZK path prefix (with --hosts)")
    parser.add_argument("--ssl", action="store_true", help="Use SSL for ZK connection")
    parser.add_argument("--cert", default=None, help="SSL cert file")
    parser.add_argument("--key", default=None, help="SSL key file")
    parser.add_argument("--ca", default=None, help="SSL CA cert file")
    parser.add_argument("--no-verify", action="store_true", help="Disable SSL cert verification")
    parser.add_argument("--auth", action="store_true", help="Use digest auth for ZK")
    parser.add_argument("--user", default=None, help="ZK auth username")
    parser.add_argument("--password", default=None, help="ZK auth password")

    return parser.parse_args(argv)


def resolve_params(args: argparse.Namespace) -> ZkConnectionParams:
    """Build connection params either from explicit CLI args or from config."""
    if args.hosts:
        if not args.prefix:
            raise SystemExit("--prefix is required when using --hosts (no config file)")
        params = ZkConnectionParams(
            hosts=args.hosts,
            prefix=args.prefix,
            ssl=args.ssl,
            cert=args.cert,
            key=args.key,
            ca=args.ca,
            verify_certs=not args.no_verify,
            auth=args.auth,
            username=args.user,
            password=args.password,
        )
        if params.ssl and not (params.cert and params.key and params.ca):
            raise SystemExit("--cert, --key, --ca are required when --ssl is set")
        if params.auth and not (params.username and params.password):
            raise SystemExit("--user and --password are required when --auth is set")
        return params

    return load_params_from_config(args.config, prefix_override=args.path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        stream=sys.stderr,
        format="%(levelname)s: %(message)s",
    )

    params = resolve_params(args)
    _LOG.debug("Connecting to ZK: hosts=%s prefix=%s ssl=%s auth=%s", params.hosts, params.prefix, params.ssl, params.auth)

    client = build_kazoo_client(params)
    try:
        client.start(timeout=params.timeout + 10)
    except Exception as exc:
        print(f"ERROR: failed to connect to ZK ({params.hosts}): {exc}", file=sys.stderr)
        return 2

    try:
        nodes = dump_tree(client, params.prefix, include_values=not args.tree)
    finally:
        client.stop()
        client.close()

    if args.format == "json":
        sys.stdout.write(format_json(nodes))
    else:
        sys.stdout.write(format_text(nodes, tree_only=args.tree))

    _LOG.info("Dumped %d nodes from %s", len(nodes), params.prefix)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
