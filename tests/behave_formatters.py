"""Behave output formatters used by the pgconsul test suite."""

from datetime import datetime

from behave.formatter.pretty import PrettyFormatter


class TimestampedPrettyFormatter(PrettyFormatter):
    """Pretty formatter that shows wall-clock start time of every step."""

    name = 'timestamped'
    description = 'Pretty formatter with wall-clock step start time'

    def __init__(self, stream_opener, config):
        super().__init__(stream_opener, config)
        # Timestamp is rendered in the location column, so it has to be printed.
        self.show_source = True
        self._started_at = None

    def match(self, match):
        self._started_at = datetime.now()
        super().match(match)

    def result(self, step):
        super().result(step)
        self._started_at = None

    def print_step(self, status, arguments, location, proceed):
        if self._started_at is not None:
            location = '%s %s' % (self._started_at.strftime('%H:%M:%S.%f')[:-3], location)
        super().print_step(status, arguments, location, proceed)
