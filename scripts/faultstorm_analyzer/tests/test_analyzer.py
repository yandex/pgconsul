import json
from pathlib import Path
import subprocess
import sys

import pytest

from faultstorm_analyzer import analyze
from faultstorm_analyzer.src.cli import main
from faultstorm_analyzer.src.parsing import operation_summary
from faultstorm_analyzer.src.reporting import markdown


SCRIPT = Path(__file__).resolve().parents[2] / 'analyze_faultstorm_failure.py'


def write(root, name, text):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def operations(root, records, name='faultstorm/operations.log'):
    return write(root, name, '\n'.join(json.dumps(item) for item in records) + '\n')


def codes(report):
    return {item['code'] for item in report['findings']}


def test_expected_data_loss_does_not_hide_failed_random_session(tmp_path):
    write(tmp_path, 'faultstorm_maintenance_resetup.log', '''Scenario: Resetup primary
2026-09-13 09:56:34,588 ERROR Consistency check FAILED: 2590 lost, 0 unexpected
    Then some data was lost
4 steps passed, 0 failed, 0 skipped
''')
    write(tmp_path, 'faultstorm.log', '''2026-09-13 10:00:03,068 INFO Starting faultstorm session 1/12
2026-09-13 10:09:28,098 INFO Faultstorm session 1/12 passed and was cleaned up
2026-09-13 10:09:28,098 INFO Starting faultstorm session 2/12
2026-09-13 10:19:27,041 ERROR Faultstorm session 2/12 failed; retaining its logs
''')

    report = analyze(tmp_path)

    assert report['features'][0]['status'] == 'passed'
    assert report['features'][0]['checker'][0]['expected_loss'] is True
    assert [session['status'] for session in report['sessions']] == ['passed', 'failed']
    assert report['sessions'][1]['end']['line'] == 4
    assert 'expected data loss' in markdown(report, 0)


def test_loss_expectation_belongs_to_its_scenario(tmp_path):
    write(tmp_path, 'faultstorm_actions.log', '''Scenario: Expected loss
    Then some data was lost
Scenario: Unexpected loss
2026-09-13 09:56:34,588 ERROR Consistency check FAILED: 1 lost, 0 unexpected
    Then there was no data lost
      Assertion Failed: one write lost
2 steps passed, 1 failed, 0 skipped
''')

    report = analyze(tmp_path)

    feature = report['features'][0]
    assert feature['status'] == 'failed'
    assert feature['checker'][0]['expected_loss'] is False
    assert feature['failed_steps'][0]['line'] == 5


@pytest.mark.parametrize('passed,failed,errors,status', [(22, 0, 1, 'failed'), (0, 0, 2, 'failed'), (22, 0, 0, 'passed'), (0, 0, 0, 'skipped'), (22, 1, 0, 'failed')])
def test_behave_error_summary_includes_failed_feature_node_logs(tmp_path, passed, failed, errors, status):
    write(tmp_path, 'faultstorm_actions.log', f'''Scenario: Network latency survives resetup
    And pg_resetup service is stopped on "postgresql3"
      Traceback (most recent call last):
        subprocess.TimeoutExpired: supervisorctl stop pg_resetup timed out
Errored scenarios:
  features/actions.feature:15  Network latency survives resetup
{passed} steps passed, {failed} failed, {errors} error, 5 skipped
''')
    node_log = write(tmp_path, 'behave_actions/postgresql3/pg_resetup.log',
                     '2026-09-14 08:22:53,971 [pg_resetup] INFO pg_resetup completed successfully\n')

    report = analyze(tmp_path)

    feature = report['features'][0]
    assert feature['status'] == status
    assert feature['errors'] == errors
    assert feature['failed_steps'][0]['line'] == 2
    assert feature['failure_summary'][0]['line'] == 5
    if status == 'failed':
        assert any(item['path'] == str(node_log) for item in report['files'])
        assert 'missing_node_logs' not in codes(report)
        assert any(item['kind'] == 'resetup_complete' for item in report['timeline'])


def test_no_successful_reads_means_unknown_data_loss(tmp_path):
    operations(tmp_path, [
        {'type': 'ok', 'action': 'add', 'node': 'postgresql1', 'timestamp': 100, 'value': 1},
        {'type': 'ok', 'action': 'add', 'node': 'postgresql1', 'timestamp': 99, 'value': 2},
        {'type': 'invoke', 'action': 'read', 'node': 'postgresql1', 'timestamp': 110},
        {'type': 'fail', 'action': 'read', 'node': 'postgresql1', 'timestamp': 111, 'error': 'Connection refused'},
    ])

    report = analyze(tmp_path)

    summary = report['operations'][0]
    assert summary['counts']['add.ok'] == 2
    assert summary['last_write']['line'] == 1
    assert summary['first']['line'] == 2
    assert 'no_successful_reads' in codes(report)
    assert 'data loss has not been established' in markdown(report, 0)
    assert 'lost' not in summary


def test_startup_failures_before_session_and_stale_logs_remain_visible(tmp_path):
    write(tmp_path, 'postgresql1/pgconsul.log', '''2026-09-13 10:08:42,000 ERROR Could not initialize ZooKeeper connection
2026-09-13 10:08:51,000 ERROR Could not initialize ZooKeeper connection
''')
    write(tmp_path, 'faultstorm/load.log', '2026-09-13 09:58:59,000 INFO All writers stopped\n')
    operations(tmp_path, [{'type': 'ok', 'action': 'add', 'timestamp': 1789294170, 'node': 'postgresql1', 'value': 1}])

    report = analyze(tmp_path)

    assert {'startup_zk_failure', 'stale_pgconsul_log', 'stale_load_log'} <= codes(report)
    startup = next(item for item in report['findings'] if item['code'] == 'startup_zk_failure')
    assert [ref['line'] for ref in startup['evidence']] == [1, 2]
    assert any(item['kind'] == 'startup_zk_failure' for item in report['timeline'])


def test_same_python_pid_after_supervisor_stop_is_only_a_hypothesis(tmp_path):
    write(tmp_path, 'postgresql3/pg_resetup.log', '''2026-09-13 10:07:30,069 DEBUG Command succeeded: stdout=pgconsul: stopped
2026-09-13 10:09:33,550 WARNING Command exited with code 7: stdout=pgconsul: ERROR (spawn error)
''')
    write(tmp_path, 'postgresql3/process_freezer.log', '''2026-09-13 10:06:43 [process_freezer] SIGSTOP -> PID 210 (/opt/yandex/pgconsul/bin/python /usr/local/bin/pgconsul -f yes)
2026-09-13 10:09:53 [process_freezer] SIGSTOP -> PID 210 (/opt/yandex/pgconsul/bin/python /usr/local/bin/pgconsul -f yes)
''')
    write(tmp_path, 'postgresql3/pgconsul.log', 'Already running!\n')

    report = analyze(tmp_path)

    finding = next(item for item in report['findings'] if item['code'] == 'process_after_stop')
    assert finding['level'] == 'hypothesis'
    assert len(finding['evidence']) == 3
    already_running = next(item for item in report['timeline'] if item['kind'] == 'already_running')
    assert already_running['time'] is None


def test_new_process_after_stop_is_not_reported_as_survivor(tmp_path):
    write(tmp_path, 'postgresql3/pg_resetup.log', '2026-09-13 10:07:30,069 DEBUG Command succeeded: stdout=pgconsul: stopped\n')
    write(tmp_path, 'postgresql3/process_freezer.log', '''2026-09-13 10:06:43 [process_freezer] SIGSTOP -> PID 210 (/opt/yandex/pgconsul/bin/python /usr/local/bin/pgconsul -f yes)
2026-09-13 10:09:53 [process_freezer] SIGSTOP -> PID 211 (/opt/yandex/pgconsul/bin/python /usr/local/bin/pgconsul -f yes)
''')

    assert 'process_after_stop' not in codes(analyze(tmp_path))


def test_empty_and_truncated_archives_are_reported_as_incomplete(tmp_path):
    assert {'missing_results', 'missing_operations'} <= codes(analyze(tmp_path))
    path = operations(tmp_path, [{'type': 'ok', 'action': 'read', 'timestamp': 100, 'value': []}, ['wrong shape']])
    with path.open('a') as stream:
        stream.write('{"type": "ok"\n')
        stream.write('{"action": "new-format"}\n')

    report = analyze(tmp_path)

    assert 'partial_operations' in codes(report)
    assert report['operations'][0]['invalid_lines'] == 2
    assert report['operations'][0]['ignored_records'] == 1
    assert 'no_successful_reads' not in codes(report)


def test_operations_are_streamed_and_error_samples_are_bounded(tmp_path, monkeypatch):
    path = operations(tmp_path, [
        {'type': 'fail', 'action': 'read', 'node': f'node{i}', 'timestamp': i, 'error': f'failure {i}'}
        for i in range(200)
    ])

    def no_whole_file_read(*args, **kwargs):
        raise AssertionError('read the operations log incrementally')

    monkeypatch.setattr(Path, 'read_text', no_whole_file_read)
    result = operation_summary(path)

    assert result['counts']['read.fail'] == 200
    assert len(result['read_errors']) <= 11


def test_cli_json_is_clean_and_existing_output_is_preserved(tmp_path):
    result = subprocess.run([sys.executable, str(SCRIPT), str(tmp_path), '--format', 'json'], capture_output=True, text=True, timeout=10)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['schema_version'] == 1
    target = write(tmp_path, 'report.md', 'keep me')
    result = subprocess.run([sys.executable, str(SCRIPT), str(tmp_path), '--output', str(target)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 2
    assert target.read_text() == 'keep me'


def test_markdown_timeline_limit_keeps_source_line_links(tmp_path, capsys):
    write(tmp_path, 'faultstorm/scenario.log', '''[2026-09-13T10:17:01.221] resetup 1 postgresql1
[2026-09-13T10:17:06.763] wait 2 14
''')
    assert main([str(tmp_path), '--limit', '1']) == 0
    output = capsys.readouterr().out
    assert 'Omitted events: 1' in output
    assert 'scenario.log:2]' in output
    assert 'scenario.log:1]' not in output


def test_read_ok_without_snapshot_is_not_successful(tmp_path):
    operations(tmp_path, [{'type': 'ok', 'action': 'read', 'timestamp': 100}])

    report = analyze(tmp_path)

    assert 'no_successful_reads' in codes(report)
    assert report['operations'][0]['invalid_lines'] == 1


def test_invalid_timestamp_does_not_abort_analysis(tmp_path):
    operations(tmp_path, [{'type': 'ok', 'action': 'add', 'timestamp': 10**400, 'value': 1}])

    report = analyze(tmp_path)

    assert report['operations'][0]['counts']['add.ok'] == 1
    assert 'invalid_timestamps' in codes(report)


def test_expected_loss_survives_failure_in_later_scenario(tmp_path):
    write(tmp_path, 'faultstorm_actions.log', '''Scenario: Expected loss
2026-09-13 09:56:34,588 ERROR Consistency check FAILED: 1 lost, 0 unexpected
    Then some data was lost
Scenario: Next scenario
    Given the cluster is ready
      Assertion Failed: no primary
2 steps passed, 1 failed, 1 skipped
''')

    report = analyze(tmp_path)

    assert report['features'][0]['checker'][0]['expected_loss'] is True
    assert report['features'][0]['status'] == 'failed'
    assert 'scenario passed' not in markdown(report, 0)


def test_embedded_traceback_without_summary_does_not_prove_test_failure(tmp_path):
    write(tmp_path, 'faultstorm_actions.log', '''Scenario: In progress
    When I kill postgres
    Traceback (most recent call last):
      PostgresConnectionError: database unavailable
''')

    assert analyze(tmp_path)['features'][0]['status'] == 'unknown'


def test_event_limit_keeps_actions_at_the_end_of_the_run(tmp_path):
    noise = '2026-09-13 10:00:00,000 WARNING expected fault\n' * 510
    write(tmp_path, 'faultstorm.log', noise + '2026-09-13 10:17:01,000 INFO Resetup on postgresql1\n')
    write(tmp_path, 'faultstorm/scenario.log', '[2026-09-13T10:00:00.000] wait 1 10\n' * 2100 +
          '[2026-09-13T10:17:01.000] resetup 2 postgresql1\n')

    report = analyze(tmp_path)

    assert any('Resetup on' in item['text'] for item in report['timeline'])
    assert any('resetup 2' in item['text'] for item in report['timeline'])
    assert report['truncated_events'] > 0


def test_unrelated_replay_does_not_hide_behave_operations(tmp_path):
    record = {'type': 'ok', 'action': 'read', 'timestamp': 100, 'value': []}
    operations(tmp_path, [record], 'faultstorm_replay/first/operations.log')
    operations(tmp_path, [record], 'behave_second/faultstorm/faultstorm_ops.log')

    assert len(analyze(tmp_path)['operations']) == 2
