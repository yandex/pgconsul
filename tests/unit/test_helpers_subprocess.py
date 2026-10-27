from unittest.mock import patch

from src import helpers


def test_subprocess_call_returns_failure_when_process_cannot_start():
    with patch('src.helpers.subprocess_popen', return_value=None):
        assert helpers.subprocess_call('missing command') == 1
