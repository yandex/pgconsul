Feature: Pgconsul-specific actions

  @docker
  Scenario: Resetup rebuilds PGDATA on a replica node
    Given the node "postgresql3" is a replica
    And pg_resetup service is stopped on "postgresql3"
    And a marker file is placed in PGDATA on "postgresql3"
    When I execute a resetup action on "postgresql3"
    And pg_resetup service is started on "postgresql3"
    And I wait up to 60 seconds for the marker file to disappear on "postgresql3"
    Then the marker file is gone on "postgresql3"
    And postgres is running on "postgresql3"

  @docker
  Scenario: Network latency survives resetup
    Given a cross-DC latency of 50ms between "dc1" and "dc2" is applied
    And the node "postgresql3" is a replica
    And pg_resetup service is stopped on "postgresql3"
    When I execute a resetup action on "postgresql3"
    And pg_resetup service is started on "postgresql3"
    And I wait up to 120 seconds for postgres to be running on "postgresql3"
    Then ping from "postgresql1" to "postgresql2" takes at least 50ms
    And ping from "postgresql2" to "postgresql1" takes at least 50ms

  @docker
  Scenario: Switchover picks a random node when none specified
    Given a switchover action with no node
    And the current primary node is recorded
    When I execute the switchover action
    Then the switchover action node is one of the db nodes
    When I wait up to 180 seconds for the primary to change
    Then the primary has changed

  @docker
  Scenario: Maintenance enable and heal (disable) verifiable via pgconsul-util
    Given a maintenance action with node "postgresql1"
    When I execute the maintenance action
    Then pgconsul-util maintenance show on "postgresql1" reports "enabled"
    When I heal the maintenance action
    Then pgconsul-util maintenance show on "postgresql1" reports "disabled"
