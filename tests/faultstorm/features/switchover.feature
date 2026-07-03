Feature: SwitchoverAction
  Pgconsul-specific action that triggers switchover via pgconsul-util.

  Scenario: Switchover action flags
    Given a switchover action
    Then it is not healable
    And it is not destructive
    And it is not host_targetable

  Scenario: Switchover serialization round-trip
    Given a switchover action with ordinal 5 and node "postgresql2"
    When I serialize and deserialize the switchover action
    Then the deserialized switchover action has ordinal 5 and node "postgresql2"

  Scenario: Switchover serialization round-trip without node
    Given a switchover action with ordinal 3 and no node
    When I serialize and deserialize the switchover action
    Then the deserialized switchover action has ordinal 3 and no node

  @docker
  Scenario: Switchover picks a random node when none specified
    Given a switchover action with no node
    And the current primary node is recorded
    When I execute the switchover action
    Then the switchover action node is one of the db nodes
    When I wait up to 180 seconds for the primary to change
    Then the primary has changed
  