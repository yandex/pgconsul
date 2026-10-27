from unittest.mock import patch

import pytest

from src import helpers


def test_subprocess_call_raises_os_error_when_process_cannot_start():
    with patch('src.helpers.subprocess_popen', return_value=None):
        with pytest.raises(OSError, match='Could not start subprocess'):
            helpers.subprocess_call('missing command')
