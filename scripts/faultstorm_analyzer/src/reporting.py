"""Render analysis as an English Markdown report."""

from pathlib import Path
from urllib.parse import quote

from .scanner import RULES


def markdown(report, limit):
    def link(ref):
        path = Path(ref['path'])
        label = str(path.relative_to(report['root']))
        target = quote(str(path), safe='/') + ':' + str(ref['line'])
        return f'[{label}:{ref["line"]}]({target})'

    output = ['# FaultStorm analysis', '', f'Archive: `{report["root"]}`', '',
              'Observations from saved logs. A stop signal does not prove process termination; '
              'root cause and lock ownership require direct evidence.', '',
              'Operation Unix timestamps are converted to UTC. Text timestamps are preserved as logged; '
              'time correlation assumes that nodes use UTC.', '', '## Results', '']
    for item in report['sessions']:
        output.append(f'- Session {item["number"]}/{item["total"]}: **{item["status"]}** — {link(item.get("end") or item["start"])}.')
    for item in report['features']:
        source = ' — ' + link(item['summary']) if item.get('summary') else ''
        output.append(f'- `{Path(item["path"]).name}`: **{item["status"]}**{source}.')
        for ref in item['failed_steps']:
            output.append(f'  - Step preceding the error: {ref["text"]} — {link(ref)}.')
        for check in item['checker']:
            meaning = 'checker reported data loss; check the scenario outcome'
            if check['expected_loss']:
                meaning = 'expected data loss; scenario passed' if item['status'] == 'passed' else 'scenario expects data loss; the overall feature outcome does not confirm its success'
            output.append(f'  - {meaning} — {link(check["source"])}.')
    output.extend(['', '## Client operations', '', 'Each file is shown separately. Snapshots of the same run may overlap; their counters are not summed.', ''])
    for item in report['operations']:
        counts = item['counts']
        output.append(f'- `{Path(item["path"]).relative_to(report["root"])}`: successful writes **{counts.get("add.ok", 0)}**, '
                      f'successful reads **{counts.get("read.ok", 0)}**, read errors **{counts.get("read.fail", 0) + counts.get("read.info", 0)}**.')
        if ref := item.get('last_write'):
            output.append(f'  - Last successful write: {ref["time"]}, {ref["text"]} — {link(ref)}.')
    output.extend(['', '## Findings and evidence gaps', ''])
    levels = {'fact': 'Fact', 'gap': 'Gap', 'hypothesis': 'Hypothesis'}
    for item in report['findings']:
        output.append(f'- **{levels[item["level"]]}:** {item["message"]}')
        if item['evidence']:
            output.append('  ' + ', '.join(link(ref) for ref in item['evidence']) + '.')
    output.extend(['', '## Timeline', '', 'For repeated events, the first and last entries are shown; the count covers the entire file.', ''])
    events = report['timeline'] if limit == 0 else report['timeline'][-limit:]
    omitted = len(report['timeline']) - len(events) + report['truncated_events']
    if omitted:
        output.append(f'Omitted events: {omitted}. `--limit 0` shows all retained events; the latest 500 actions from the runner and each scenario.log are retained.\n')
    for item in events:
        label = RULES.get(item['kind'], ('Action / process observation',))[0]
        text = item['text'].replace('`', "'")
        output.append(f'- {item["time"] or "timestamp unavailable"} · {item["node"]} · {label}: `{text}` — {link(item)}.')
    return '\n'.join(output) + '\n'
