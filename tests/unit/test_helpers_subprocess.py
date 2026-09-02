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
