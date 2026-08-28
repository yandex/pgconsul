# encoding: utf-8
"""PostgreSQL timeline-history parsing and divergence checks."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TimelineSwitch:
    timeline: int
    switch_lsn: int


def parse_lsn(value: str) -> int:
    high, low = value.split('/', maxsplit=1)
    return (int(high, 16) << 32) + int(low, 16)


def parse_timeline_history(value: str, target_timeline: int) -> tuple[TimelineSwitch, ...]:
    """Parse the complete ancestry stored in ``<target>.history``."""
    switches = []
    for line in value.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        fields = line.split(maxsplit=2)
        if len(fields) < 2:
            raise ValueError(f'Invalid timeline history line: {line!r}')
        timeline = int(fields[0])
        switch_lsn = parse_lsn(fields[1])
        if timeline >= target_timeline:
            raise ValueError(
                f'Invalid ancestor timeline {timeline} for target {target_timeline}'
            )
        switches.append(TimelineSwitch(timeline, switch_lsn))
    if target_timeline > 1 and not switches:
        raise ValueError(f'Empty history for timeline {target_timeline}')
    if any(left.timeline >= right.timeline for left, right in zip(switches, switches[1:])):
        raise ValueError('Timeline history is not strictly ordered')
    return tuple(switches)


def timeline_requires_rewind(
    local_timeline: int,
    local_lsn: int,
    target_timeline: int,
    history: tuple[TimelineSwitch, ...],
) -> bool:
    """Return whether local data is outside the target timeline ancestry."""
    if local_timeline == target_timeline:
        return False
    for switch in history:
        if switch.timeline == local_timeline:
            return local_lsn > switch.switch_lsn
    return True


def wal_filename_before_switch(
    switch: TimelineSwitch,
    segment_size: int,
) -> str:
    """Return the archived partial WAL file containing the switchpoint."""
    if switch.switch_lsn <= 0 or segment_size <= 0:
        raise ValueError('Invalid switch LSN or WAL segment size')
    segments_per_log = 0x100000000 // segment_size
    segment_number = (switch.switch_lsn - 1) // segment_size
    log = segment_number // segments_per_log
    segment = segment_number % segments_per_log
    return f'{switch.timeline:08X}{log:08X}{segment:08X}.partial'


def wal_filenames_before_switch(
    switch: TimelineSwitch,
    segment_size: int,
) -> tuple[str, str]:
    """Return both archive names PostgreSQL may use for the fork segment."""
    partial = wal_filename_before_switch(switch, segment_size)
    return partial.removesuffix('.partial'), partial
