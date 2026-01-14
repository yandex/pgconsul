Feature: Targeted switchover

   @switchover
   Scenario Outline: Check switchover with debug failure "<failure_name>" continues until success
       Given a "pgconsul" container common config
       """
           pgconsul.conf:
               global:
                   priority: 0
                   use_replication_slots: 'yes'
                   quorum_commit: '<quorum_commit>'
               primary:
                   change_replication_type: 'yes'
                   primary_switch_checks: 3
               replica:
                   allow_potential_data_loss: 'no'
                   primary_unavailability_timeout: 1
                   primary_switch_checks: 3
                   min_failover_timeout: 120
                   primary_unavailability_timeout: 2
               commands:
                   generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
               debug:
                   failure_name: '<failure_name>'
                   failure_count: <failure_count>
       """
       Given a following cluster with "<lock_type>" with replication slots
       """
           postgresql1:
               role: primary
               config:
                   pgconsul.conf:
                       global:
                           priority: 3
           postgresql2:
               role: replica
               config:
                   pgconsul.conf:
                       global:
                           priority: 1
           postgresql3:
               role: replica
               config:
                   pgconsul.conf:
                       global:
                           priority: 2
       """
       Then container "postgresql3" is in <replication_type> group
       When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
       And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
       And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
       # And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
       Then container "postgresql2" became a primary
       And container "postgresql3" is a replica of container "postgresql2"
       And container "postgresql1" is a replica of container "postgresql2"
       And container "postgresql1" is in <replication_type> group
       And postgresql in container "postgresql3" was not rewinded
       And postgresql in container "postgresql1" was rewinded
       And timing log contains "switchover,downtime"
   Examples: <lock_type>, <lock_host>
      |   lock_type   |   lock_host    | quorum_commit | replication_type | failure_name                        | failure_count |
      |   zookeeper   |   zookeeper1   |      yes      |      quorum      | candidate_switchover_before_acquire | 1             |
      |   zookeeper   |   zookeeper1   |      yes      |      quorum      | before_promote                      | 1             |
      |   zookeeper   |   zookeeper1   |      no       |      sync        | candidate_switchover_before_acquire | 1             |
      |   zookeeper   |   zookeeper1   |      no       |      sync        | before_promote                      | 1             |


    @switchover
    Scenario Outline: Check switchover with debug failure "<failure_name>" rolls back
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
                debug:
                    failure_name: '<failure_name>'
                    failure_count: <failure_count>
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
                config:
                    pgconsul.conf:
                        global:
                            priority: 3
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2

        """
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql1" became a primary
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is in <replication_type> group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql2" was not rewinded


    Examples: <lock_type>, <lock_host>
       |   lock_type   |   lock_host    | quorum_commit | replication_type | failure_name                        | failure_count |
       |   zookeeper   |   zookeeper1   |      yes      |      quorum      | primary_switchover_before_catchup   | 1             |
       |   zookeeper   |   zookeeper1   |      yes      |      quorum      | primary_switchover_before_release   | 1             |
       |   zookeeper   |   zookeeper1   |      yes      |      quorum      | primary_switchover_after_release    | 1             |
       |   zookeeper   |   zookeeper1   |      no       |      sync        | primary_switchover_before_catchup   | 1             |
       |   zookeeper   |   zookeeper1   |      no       |      sync        | primary_switchover_before_release   | 1             |
       |   zookeeper   |   zookeeper1   |      no       |      sync        | primary_switchover_after_release    | 1             |
