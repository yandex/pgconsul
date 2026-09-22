"""Analyze saved FaultStorm logs without Docker or third-party dependencies."""

import argparse
import json
from pathlib import Path

from .analyzer import analyze
from .reporting import markdown


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog='Exit 0: analysis completed (even for failed tests); exit 2: invalid arguments or report output error.')
    parser.add_argument('log_dir', nargs='?', default='logs.local/faultstorm', type=Path, help='Saved log directory (default: logs.local/faultstorm)')
    parser.add_argument('--format', choices=('markdown', 'json'), default='markdown', help='Report format; stdout contains only the report')
    parser.add_argument('--output', type=Path, help='Create a report file; existing files are never overwritten')
    parser.add_argument('--limit', type=int, default=80, help='Latest Markdown timeline events, shown chronologically; 0 means all retained events (default: 80)')
    parser.add_argument('--full-node-logs', action='store_true', help='Scan PostgreSQL logs larger than 32 MiB; may take substantial time')
    args = parser.parse_args(argv)
    if not args.log_dir.is_dir():
        parser.error(f'Log directory does not exist: {args.log_dir}')
    if args.limit < 0:
        parser.error('--limit must be non-negative')
    if args.output and args.output.exists():
        parser.error(f'Report already exists: {args.output}')
    report = analyze(args.log_dir, max_node_log_bytes=None if args.full_node_logs else 32 * 1024 * 1024)
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + '\n' if args.format == 'json' else markdown(report, args.limit)
    if args.output:
        try:
            with args.output.open('x', encoding='utf-8') as stream:
                stream.write(rendered)
        except OSError as error:
            parser.error(str(error))
    else:
        print(rendered, end='')
    return 0
