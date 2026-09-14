"""Parse runner, Behave and client operation logs incrementally."""

from collections import Counter, deque
from datetime import datetime, timezone
import json
import re
from typing import Any


TIME = re.compile(r'^\[?(\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d)(?:[,.](\d{1,6}))?')
SUMMARY = re.compile(r'(\d+) steps? passed, (\d+) failed, (\d+) skipped')
SESSION = re.compile(r'(Starting faultstorm session|Faultstorm session) (\d+)/(\d+)(.*)')


def timestamp(text):
    match = TIME.match(text)
    if match:
        return match[1].replace('T', ' ') + '.' + (match[2] or '').ljust(6, '0')
    return None


def evidence(path, line, text, time=None) -> dict[str, Any]:
    return {'path': str(path), 'line': line, 'time': time, 'text': text.strip()[:350]}


def lines(path):
    with path.open(encoding='utf-8', errors='replace') as stream:
        yield from enumerate(stream, 1)


def feature_result(path):
    result: dict[str, Any] = {'path': str(path), 'status': 'unknown', 'scenarios': [], 'failed_steps': [], 'checker': []}
    scenario = None
    last_step = None
    in_failures = False
    for number, text in lines(path):
        ref = evidence(path, number, text, timestamp(text))
        if re.match(r'^\s*Scenario(?: Outline)?:', text):
            scenario = {'name': text.strip().split('#')[0].strip(), 'expects_loss': False, 'source': ref}
            result['scenarios'].append(scenario)
        if re.match(r'^\s+(Given|When|Then|And|But) ', text):
            last_step = ref
            if scenario and 'some data was lost' in text:
                scenario['expects_loss'] = True
        if re.search(r'Assertion(?:Error| Failed)|^\s*Traceback \(most recent call last\):', text) and last_step:
            if last_step not in result['failed_steps']:
                result['failed_steps'].append(last_step)
        if text.startswith('Failing scenarios:'):
            in_failures = True
        if in_failures:
            result.setdefault('failure_summary', []).append(ref)
        if 'Consistency check FAILED:' in text:
            result['checker'].append({'source': ref, 'scenario': scenario})
        if match := SUMMARY.search(text):
            passed, failed, skipped = map(int, match.groups())
            result.update(status='failed' if failed else 'passed' if passed else 'skipped',
                          passed=passed, failed=failed, skipped=skipped, summary=ref)
            in_failures = False
    for check in result['checker']:
        check['expected_loss'] = bool(check['scenario'] and check['scenario']['expects_loss'])
    return result


def session_results(path):
    sessions = []
    actions: deque[dict[str, Any]] = deque(maxlen=500)
    truncated = 0
    for number, text in lines(path):
        ref = evidence(path, number, text, timestamp(text))
        if match := SESSION.search(text):
            if match[1] == 'Starting faultstorm session':
                sessions.append({'number': int(match[2]), 'total': int(match[3]), 'status': 'incomplete', 'start': ref})
            else:
                current = next((item for item in reversed(sessions) if item['number'] == int(match[2])), None)
                if current is None:
                    current = {'number': int(match[2]), 'total': int(match[3]), 'status': 'incomplete'}
                    sessions.append(current)
                current.update(status='failed' if 'failed' in match[4] else 'passed' if 'passed' in match[4] else 'incomplete', end=ref)
        if re.search(r'INFO (?:Resetup on|Killing |Partition |Healing |Enabling maintenance|Disabling maintenance|Phase [12]:)|ERROR|WARNING', text):
            if len(actions) == actions.maxlen:
                truncated += 1
            actions.append(dict(ref, kind='runner', node='runner'))
    return sessions, list(actions), truncated


def operation_summary(path):
    counts: Counter[str] = Counter()
    writes: Counter[str] = Counter()
    bounds: dict[str, tuple[float, int, str]] = {}
    errors: dict[str, dict[str, Any]] = {}
    invalid: list[dict[str, Any]] = []
    invalid_count = ignored = invalid_timestamps = total = 0
    for total, text in lines(path):
        if not text.strip():
            continue
        try:
            item = json.loads(text)
            if not isinstance(item, dict):
                raise ValueError('not an object')
            if item.get('action') == 'read' and item.get('type') == 'ok' and not isinstance(item.get('value'), list):
                raise ValueError('read result is not a snapshot')
            if item.get('action') == 'add':
                value = item.get('value')
                if value is None:
                    raise ValueError('missing write value')
                int(value)
        except (ValueError, TypeError, OverflowError, RecursionError):
            invalid_count += 1
            if len(invalid) < 3:
                invalid.append(evidence(path, total, text))
            continue
        action, status = item.get('action'), item.get('type')
        if action not in ('add', 'read') or status not in ('invoke', 'ok', 'fail', 'info'):
            ignored += 1
            continue
        counts[f'{action}.{status}'] += 1
        node = str(item.get('node', '?'))
        if node not in writes and len(writes) >= 32:
            node = '(other nodes)'
        if action == 'add' and status == 'ok':
            writes[node] += 1
        when = item.get('timestamp')
        if isinstance(when, (int, float)) and not isinstance(when, bool) and 0 <= when <= 253402300799:
            sample = (when, total, f'{node}: {action} {status}')
            if 'first' not in bounds or when < bounds['first'][0]:
                bounds['first'] = sample
            if 'last' not in bounds or when > bounds['last'][0]:
                bounds['last'] = sample
            if action == 'add' and status == 'ok' and ('last_write' not in bounds or when > bounds['last_write'][0]):
                bounds['last_write'] = sample
        else:
            invalid_timestamps += 1
        if action == 'read' and status in ('fail', 'info'):
            key = node if node in errors or len(errors) < 10 else '(other nodes)'
            errors.setdefault(key, evidence(path, total, str(item.get('error', 'Read error'))))
    anchors = {key: evidence(path, number, text, datetime.fromtimestamp(when, timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'))
               for key, (when, number, text) in bounds.items()}
    return dict(path=str(path), lines=total, counts=dict(counts), writes_by_node=dict(writes),
                invalid_lines=invalid_count, invalid_samples=invalid, ignored_records=ignored, invalid_timestamps=invalid_timestamps,
                read_errors=list(errors.values()), **anchors)
