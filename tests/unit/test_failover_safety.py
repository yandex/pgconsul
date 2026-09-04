from src.failover.safety import assess_candidate, format_lsn, sort_votes
from src.types import DurabilityConfig


def test_votes_are_sorted_by_timeline_lsn_priority_and_hostname():
    votes = {
        'b': (100, 1, 2),
        'a': (100, 2, 2),
        'c': (200, 9, 1),
    }

    assert [host for host, _ in sort_votes(votes)] == ['a', 'b', 'c']


def test_candidate_reports_missing_votes_and_wrong_timeline():
    durability = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    result = assess_candidate(
        'a',
        {'a': (100, 1, 2)},
        durability,
        (durability,),
        'primary',
        1,
    )

    assert result.safe is False
    assert any('differs from cluster timeline' in reason for reason in result.reasons)
    assert any('not enough votes' in reason for reason in result.reasons)


def test_non_maximum_lsn_can_still_be_proven_safe():
    durability = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    result = assess_candidate(
        'b',
        {
            'a': (110, 1, 1),
            'b': (100, 1, 1),
            'c': (90, 1, 1),
        },
        durability,
        (durability,),
        'primary',
        1,
    )

    assert result.safe is True
    assert result.notes == ('host does not have the maximum LSN on its timeline',)
    assert format_lsn(0x100000002) == '1/2'


def test_target_only_candidate_can_be_proven_safe_during_transition():
    source = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    target = DurabilityConfig.build(['primary', 'b', 'c', 'd'])

    result = assess_candidate(
        'd',
        {
            'a': (100, 1, 1),
            'b': (110, 1, 1),
            'c': (90, 1, 1),
            'd': (120, 1, 1),
        },
        source,
        (source, target),
        'primary',
        1,
    )

    assert result.safe is True


def test_unfenced_votes_are_reported_unsafe():
    durability = DurabilityConfig.build(['primary', 'a'])

    result = assess_candidate(
        'a',
        {'a': (100, 1, 1)},
        durability,
        (durability,),
        'primary',
        1,
        wal_sources_fenced=False,
    )

    assert result.safe is False
    assert any('not fenced' in reason for reason in result.reasons)
