Feature: ResetupAction
  Pgconsul-specific action that deletes PGDATA and sets the
  rewind-fail flag so that pg_resetup rebuilds the instance.

  Scenario: Resetup action flags
    Given a resetup action
    Then it is not healable
    And it is destructive
    And it is host_targetable

  Scenario: Resetup serialization round-trip
    Given a resetup action with ordinal 7 and node "postgresql3"
    When I serialize and deserialize the resetup action
    Then the deserialized resetup action has ordinal 7 and node "postgresql3"

  Scenario: Resetup serialization round-trip without node
    Given a resetup action with ordinal 2 and no node
    When I serialize and deserialize the resetup action
    Then the deserialized resetup action has ordinal 2 and no node

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
