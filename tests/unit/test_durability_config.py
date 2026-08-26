from src.types import DurabilityConfig


def test_zk_value_contains_members_and_required():
    config = DurabilityConfig.build(['replica2', 'primary', 'replica1'], required=1)

    assert config.to_dict() == {
        'members': ['primary', 'replica1', 'replica2'],
        'required': 1,
    }


def test_default_required_is_derived_from_standby_count():
    assert DurabilityConfig.build(['primary', 'r1', 'r2', 'r3']).required == 2


def test_rejects_required_larger_than_available_standbys():
    try:
        DurabilityConfig.build(['primary', 'replica'], required=2)
    except ValueError:
        return
    raise AssertionError('invalid durability config was accepted')
