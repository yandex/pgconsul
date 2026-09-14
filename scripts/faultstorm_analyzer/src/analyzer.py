"""Combine observed outcomes, timelines and gaps in the saved evidence."""

from collections import deque
from pathlib import Path
import re
from typing import Any

from .parsing import evidence, feature_result, lines, operation_summary, session_results, timestamp
from .scanner import LOG_NAMES, scan_log


def analyze(root):
    root = root.resolve()
    report: dict[str, Any] = {'schema_version': 1, 'root': str(root), 'features': [], 'sessions': [], 'operations': [],
                             'files': [], 'findings': [], 'timeline': [], 'truncated_events': 0}

    def finding(code, level, message, refs=()):
        report['findings'].append({'code': code, 'level': level, 'message': message, 'evidence': list(refs)})

    def read(reader, path):
        try:
            return reader(path)
        except OSError as error:
            finding('unreadable_log', 'gap', f'Could not read {path}: {error}')
            return None

    for path in sorted(root.glob('faultstorm_*.log')):
        if path.name == 'faultstorm_debug.log':
            continue
        if result := read(feature_result, path):
            report['features'].append(result)
    if (root / 'faultstorm.log').is_file():
        if result := read(session_results, root / 'faultstorm.log'):
            report['sessions'], actions, dropped = result
            report['timeline'].extend(actions)
            report['truncated_events'] += dropped
    operation_paths = {root / 'faultstorm' / 'operations.log', root / 'operations.log'}
    operation_paths.update(root.glob('faultstorm_replay/*/operations.log'))
    operation_paths.update(root.glob('behave_*/faultstorm/faultstorm_ops.log'))
    for path in sorted(operation_paths):
        if path.is_file():
            if result := read(operation_summary, path):
                report['operations'].append(result)
                counts = result['counts']
                refs = result['read_errors'] or [result[key] for key in ('first', 'last') if key in result]
                if not counts.get('read.ok', 0):
                    finding('no_successful_reads', 'fact', f'{path.relative_to(root)}: no successful reads; data loss has not been established.', refs)
                if result['invalid_lines'] or result['ignored_records']:
                    finding('partial_operations', 'gap', f'{path.relative_to(root)}: malformed lines {result["invalid_lines"]}, '
                            f'unrecognized records {result["ignored_records"]}; statistics are incomplete.', result['invalid_samples'])
                if result['invalid_timestamps']:
                    finding('invalid_timestamps', 'gap', f'{path.relative_to(root)}: records without a valid timestamp: '
                            f'{result["invalid_timestamps"]}; time coverage is incomplete.')
    if not report['operations']:
        finding('missing_operations', 'gap', 'No operations.log / faultstorm_ops.log found; client outcomes cannot be verified.')

    directories = [root]
    directories.extend(root / ('behave_' + Path(feature['path']).stem.removeprefix('faultstorm_'))
                       for feature in report['features'] if feature['status'] == 'failed')
    for directory in directories:
        node_dirs = sorted(path for path in directory.iterdir() if path.is_dir() and re.fullmatch(r'(postgresql|zookeeper)\d+', path.name)) if directory.is_dir() else []
        for node_dir in node_dirs:
            for name in LOG_NAMES:
                path = node_dir / name
                if not path.is_file():
                    if name == 'pgconsul.log' and node_dir.name.startswith('postgresql'):
                        finding('missing_pgconsul_log', 'gap', f'Missing {path.relative_to(root)}.')
                    continue
                if result := read(scan_log, path):
                    report['files'].append(result)
                    for group in result['groups']:
                        refs = [group['first']] if group['first'] == group['last'] else [group['first'], group['last']]
                        for ref in refs:
                            report['timeline'].append(dict(ref, kind=group['kind'], node=node_dir.name, count=group['count']))
                        if group['kind'] == 'startup_zk_failure':
                            finding('startup_zk_failure', 'fact', f'{node_dir.name}: ZooKeeper connection failures during startup: {group["count"]}. '
                                    'Check subsequent daemon startup and supervisor state; process exit alone does not prove supervisor entered FATAL state.', refs)
                        if group['kind'] == 'no_primary':
                            finding('resetup_no_primary', 'fact', f'{node_dir.name}: resetup could not find a recovery source ({group["count"]} occurrences).', refs)
    if not report['files']:
        finding('missing_node_logs', 'gap', 'No node logs found; client failures cannot be correlated with cluster state.')
    latest = next((item for item in report['operations'] if item['path'] == str(root / 'faultstorm' / 'operations.log')), None)
    if latest is None:
        latest = next((item for item in report['operations'] if item['path'] == str(root / 'operations.log')), None)
    for file in report['files']:
        path = Path(file['path'])
        if path.name == 'pgconsul.log' and path.parent.parent == root and latest and latest.get('first'):
            if file['last'] is None or file['last']['time'] < latest['first']['time']:
                finding('stale_pgconsul_log', 'gap', f'{path.relative_to(root)} contains no timestamps from the current workload. '
                        'Missing log entries do not establish process death or primary lock ownership.',
                        [file['last'], latest['first']] if file['last'] else [latest['first']])
        if path.name == 'pg_resetup.log':
            stop = next((group for group in file['groups'] if group['kind'] == 'supervisor_stopped'), None)
            if not stop or not stop['last']['time']:
                continue
            freezer = next((item for item in report['files'] if Path(item['path']) == path.with_name('process_freezer.log')), None)
            for group in freezer['groups'] if freezer else []:
                if group['kind'].startswith('pgconsul_process:') and group['first']['time'] and group['last']['time']:
                    if group['first']['time'] < stop['last']['time'] < group['last']['time']:
                        finding('process_after_stop', 'hypothesis', f'{path.parent.name}: PID {group["kind"].split(":")[1]} '
                                'was observed before and after supervisor reported stopped. A child process may have survived; check PID/PPID/PGID and subsequent startup.',
                                [group['first'], stop['last'], group['last']])
    if latest and latest.get('first'):
        load = root / 'faultstorm' / 'load.log'
        if load.is_file() and (result := read(scan_log, load)):
            report['files'].append(result)
            if result['last'] and result['last']['time'] < latest['first']['time']:
                finding('stale_load_log', 'gap', 'load.log belongs to an earlier workload; use operations.log and faultstorm.log.',
                        [result['last'], latest['first']])
    scenario_paths = [root / 'faultstorm' / 'scenario.log', root / 'scenario.log', *sorted(root.glob('faultstorm_replay/*/scenario.log'))]
    for path in scenario_paths:
        if not path.is_file():
            continue
        try:
            tail: deque[dict[str, Any]] = deque(maxlen=500)
            for number, text in lines(path):
                if timestamp(text):
                    if len(tail) == tail.maxlen:
                        report['truncated_events'] += 1
                    tail.append(dict(evidence(path, number, text, timestamp(text)), kind='action', node='scenario'))
            report['timeline'].extend(tail)
        except OSError as error:
            finding('unreadable_log', 'gap', f'Could not read {path}: {error}')
    if not any(path.is_file() for path in scenario_paths):
        finding('missing_scenario', 'gap', 'No scenario.log found; the exact fault sequence for replay is unavailable.')
    if not report['sessions'] and not report['features']:
        finding('missing_results', 'gap', 'No runner/Behave results found; run status is unknown.')
    report['timeline'].sort(key=lambda item: (item['time'] or '9999', item['path'], item['line']))
    return report
