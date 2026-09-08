import shlex
import subprocess
import sys
from unittest.mock import MagicMock, patch

from src import helpers


def test_subprocess_call_redirects_large_stdout_and_stderr_to_file(tmp_path):
    output = tmp_path / 'command.log'
    program = (
        "import sys; "
        "sys.stdout.write('o' * 1048576); "
        "sys.stderr.write('e' * 1048576)"
    )
    command = '{} -c {}'.format(
        shlex.quote(sys.executable), shlex.quote(program),
    )

    assert helpers.subprocess_call(command, output_file=str(output)) == 0

    value = output.read_text()
    assert 'o' * 1048576 in value
    assert 'e' * 1048576 in value


def test_subprocess_call_uses_communicate_before_inspecting_output():
    proc = MagicMock(returncode=0)
    proc.communicate.return_value = (b'output', b'')

    with patch('src.helpers.subprocess_popen', return_value=proc):
        assert helpers.subprocess_call('command', timeout=7) == 0

    proc.communicate.assert_called_once_with(timeout=7)
    proc.wait.assert_not_called()


def test_subprocess_call_terminates_process_group_on_timeout():
    proc = MagicMock(pid=123, returncode=None)
    proc.communicate.side_effect = [
        subprocess.TimeoutExpired('command', 7),
        (b'', b'timed out'),
    ]

    with patch('src.helpers.subprocess_popen', return_value=proc), \
         patch('src.helpers.os.killpg') as killpg:
        assert helpers.subprocess_call('command', timeout=7) == 124

    killpg.assert_called_once_with(123, helpers.signal.SIGKILL)


def test_subprocess_call_kills_command_when_process_group_cannot_be_persisted():
    proc = MagicMock(pid=123, returncode=None)
    proc.communicate.return_value = (b'', b'')

    with patch('src.helpers.subprocess_popen', return_value=proc), \
         patch('src.helpers.os.killpg') as killpg:
        assert helpers.subprocess_call(
            'command', on_started=MagicMock(side_effect=RuntimeError),
        ) == 1

    killpg.assert_called_once_with(123, helpers.signal.SIGKILL)
    proc.communicate.assert_called_once_with()


def test_rewind_process_group_is_parsed_and_checked():
    assert helpers.rewind_process_group_from_op('rewind:123') == 123
    assert helpers.rewind_process_group_from_op('rewind') is None
    assert helpers.rewind_process_group_from_op('rewind:bad') is None
    assert helpers.is_op_destructive('rewind:123') is True

    with patch('src.helpers.os.killpg', side_effect=ProcessLookupError):
        assert helpers.is_process_group_running(123) is False
