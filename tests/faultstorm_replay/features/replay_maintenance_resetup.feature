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
      wait 1 5
      +maintenance 2 {primary}
      wait 3 10
      resetup 4 {primary}
      wait 5 90
      -maintenance 2 {primary}
      wait 6 60
      """
    And write-load is stopped
    Then some data was lost
