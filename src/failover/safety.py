"""Pure diagnostics for an operator-selected failover candidate."""

from dataclasses import dataclass

from ..types import DurabilityConfig


Vote = tuple[int, int, int]  # LSN, priority, timeline


@dataclass(frozen=True)
class CandidateSafety:
    safe: bool
    reasons: tuple[str, ...]
    notes: tuple[str, ...] = ()


def sort_votes(votes: dict[str, Vote]) -> list[tuple[str, Vote]]:
    """Highest timeline, LSN and priority first; hostname breaks ties."""
    return sorted(
        votes.items(),
        key=lambda item: (-item[1][2], -item[1][0], -item[1][1], item[0]),
    )


def assess_candidate(
    candidate: str,
    votes: dict[str, Vote],
    stable: DurabilityConfig | None,
    configs: tuple[DurabilityConfig, ...],
    failed_primary: str,
    expected_timeline: int | None,
    wal_sources_fenced: bool = True,
) -> CandidateSafety:
    """Explain whether the normal durability proof accepts candidate."""
    vote = votes.get(candidate)
    if vote is None:
        return CandidateSafety(False, ('host did not vote in this failover',))

    lsn, _, timeline = vote
    reasons: list[str] = []
    notes: list[str] = []

    if not wal_sources_fenced:
        reasons.append(
            'restore_command and walreceiver were not fenced; vote positions may change'
        )

    if stable is None or not configs:
        reasons.append('stable durability quorum is unavailable')
    elif not any(candidate in config.members for config in configs):
        reasons.append('host is outside durability failover quorums')

    if expected_timeline is None:
        reasons.append('cluster timeline is unknown')
    elif timeline != expected_timeline:
        reasons.append(
            f'host timeline {timeline} differs from cluster timeline {expected_timeline}'
        )

    same_timeline = {
        host: host_vote for host, host_vote in votes.items()
        if host_vote[2] == expected_timeline
    }
    for config in configs:
        replicas = set(config.members) - {failed_primary}
        required = len(replicas) - config.required + 1
        present = replicas & set(same_timeline)
        if len(present) < required:
            reasons.append(
                f'not enough votes for {sorted(config.members)}: '
                f'{len(present)} < {required}'
            )
            continue
        dominated = sum(
            1 for host in present if same_timeline[host][0] <= lsn
        )
        if dominated < required:
            reasons.append(
                f'LSN is not ahead of a read quorum for '
                f'{sorted(config.members)}: {dominated} < {required}'
            )

    same_timeline_lsns = [
        host_vote[0] for host_vote in votes.values() if host_vote[2] == timeline
    ]
    if same_timeline_lsns and lsn < max(same_timeline_lsns):
        notes.append('host does not have the maximum LSN on its timeline')
    highest_timeline = max(host_vote[2] for host_vote in votes.values())
    if timeline < highest_timeline:
        reasons.append(
            f'host timeline {timeline} is older than voted timeline {highest_timeline}'
        )

    return CandidateSafety(not reasons, tuple(dict.fromkeys(reasons)), tuple(notes))


def format_lsn(lsn: int) -> str:
    return f'{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}'
