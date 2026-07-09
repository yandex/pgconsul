Feature: Deterministic freeze_processes_group replay
  Verify that pgconsul cluster survives periodic SIGSTOP/SIGCONT
  of ZooKeeper processes on all extra nodes with no data loss and
  acceptable write availability.

  Uses faultstorm replay mode with an inline scenario.

  @docker @replay @skip
  Scenario: Freeze zookeeper on all ZK nodes for the bulk of the test
    Given the pgconsul cluster is ready for replay testing
    When I apply faultstorm actions
      """
      +freeze_processes_group 1 extra zookeeper 100-3000 100-10000
      wait 2 6
      -freeze_processes_group 1 extra zookeeper 100-3000 100-3000
      wait 3 6
      """
    And write-load is stopped
    Then there was no data lost
    And cluster was available at least 0.95 of the time
