import shlex
import sys

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
