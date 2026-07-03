Feature: MaintenanceAction
  Pgconsul-specific action that enables/disables maintenance mode
  via pgconsul-util on a random DB node.

  Scenario: Maintenance action flags
    Given a maintenance action
    Then it is healable
    And it is not destructive
    And it is not host_targetable

  Scenario: Maintenance serialization round-trip
    Given a maintenance action with ordinal 4 and node "postgresql1"
    When I serialize and deserialize the maintenance action
    Then the deserialized maintenance action has ordinal 4 and node "postgresql1"

  Scenario: Maintenance serialization round-trip without node
    Given a maintenance action with ordinal 9 and no node
    When I serialize and deserialize the maintenance action
    Then the deserialized maintenance action has ordinal 9 and no node

  @docker
  Scenario: Maintenance enable and heal (disable) verifiable via pgconsul-util
    Given a maintenance action with node "postgresql1"
    When I execute the maintenance action
    Then pgconsul-util maintenance show on "postgresql1" reports "enabled"
    When I heal the maintenance action
    Then pgconsul-util maintenance show on "postgresql1" reports "disabled"

  @docker
  Scenario: Maintenance picks a random node when none specified
    Given a maintenance action with no node
    When I execute the maintenance action
    Then the maintenance action node is one of the db nodes
    When I heal the maintenance action
    Then pgconsul-util maintenance show on "postgresql1" reports "disabled"
