Feature: Switchover availability

  @docker @replay
  Scenario: Switchover availability
    Given the pgconsul cluster is ready for replay testing
    When I apply faultstorm actions
      """
      switchover
      wait 60
      """
    And write-load is stopped
    Then there was no data lost
    And cluster was available at least 0.8 of the time
