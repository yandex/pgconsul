Feature: Testing min_failover_timeout setting

    @failover
    Scenario Outline: Destroy primary and wait min_failover_timeout seconds
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 240
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" <with_slots> replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <destroy> container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        When we <repair> container "postgresql1"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is a replica of container "postgresql2"
        Then postgresql in container "postgresql1" was rewinded
        When we <destroy> container "postgresql2"
        Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
        When we wait until "10.0" seconds to failover of "postgresql3" left in <lock_type> "<lock_host>"
        Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
        When we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has one of holders "pgconsul_postgresql1_1.pgconsul_pgconsul_net,pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then one of the containers "postgresql1,postgresql3" became a primary, and we remember it
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then <lock_type> "<lock_host>" has "1" values for key "/pgconsul/postgresql/replics_info"
        When we <repair> container "postgresql2"
        Then <lock_type> "<lock_host>" has "2" values for key "/pgconsul/postgresql/replics_info"

    Examples: <lock_type>, <sync_state>hronous replication <with_slots> slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots | quorum_commit | replication_type |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      no       |       sync       |


    Scenario Outline: Destroy primary and wait min_failover_timeout seconds with async replication
      Given a "pgconsul" container common config
          """
              pgconsul.conf:
                  global:
                      priority: 0
                      use_replication_slots: '<use_slots>'
                  primary:
                      change_replication_type: 'no'
                      primary_switch_checks: 1
                  replica:
                      allow_potential_data_loss: 'yes'
                      primary_unavailability_timeout: 1
                      primary_switch_checks: 1
                      min_failover_timeout: 240
                      primary_unavailability_timeout: 2
                  commands:
                      generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
          """
      Given a following cluster with "<lock_type>" <with_slots> replication slots
          """
              postgresql1:
                  role: primary
              postgresql2:
                  role: replica
                  config:
                      pgconsul.conf:
                          global:
                              priority: 2
              postgresql3:
                  role: replica
                  config:
                      pgconsul.conf:
                          global:
                              priority: 1
          """
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
      Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
          """
            - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
            - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
          """
      When we <destroy> container "postgresql1"
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
      Then container "postgresql2" became a primary
      Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
      Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
          """
            - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
          """
      Then container "postgresql3" is a replica of container "postgresql2"
      Then postgresql in container "postgresql3" was not rewinded
      When we <repair> container "postgresql1"
      Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
          """
            - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
            - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
          """
      Then container "postgresql1" is a replica of container "postgresql2"
      Then postgresql in container "postgresql1" was rewinded
      When we <destroy> container "postgresql2"
      Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
      When we wait until "10.0" seconds to failover of "postgresql3" left in <lock_type> "<lock_host>"
      Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
      When we wait "10.0" seconds
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
      Then container "postgresql3" became a primary
      Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
      Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
      Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
          """
            - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
          """
      When we <repair> container "postgresql2"
      Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
          """
            - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
            - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
              state: streaming
              sync_state: async
          """

      Examples: <lock_type>, <sync_state>hronous replication <with_slots> slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |
