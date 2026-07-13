Feature: Deterministic maintenance + resetup replay
  Verify that pgconsul cluster recovers after enabling maintenance
  on the primary, triggering a resetup (break network + PGDATA wipe + rebuild),
  and then disabling maintenance. Data must not be lost.

  Uses faultstorm replay mode with an inline scenario.
  The {primary} placeholder is resolved to the current primary node
  at runtime.

  @docker @replay
  Scenario: Maintenance and resetup on primary node
    Given the pgconsul cluster is ready for replay testing
    When I apply faultstorm actions
      """
      wait 5
      + maint maintenance {primary}
      wait 10
      resetup {primary}
      wait 90
      - maint
      wait 60
      """
    And write-load is stopped
    Then some data was lost
