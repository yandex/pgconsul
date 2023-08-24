Feature: Check availability on coordinator failure

    @coordinator_fail
    Scenario Outline: Kill coordinator
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
                    min_failover_timeout: 1
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
        When we disconnect from network container "zookeeper1"
        And we disconnect from network container "zookeeper2"
        And we disconnect from network container "zookeeper3"
        And we wait "10.0" seconds
        Then pgbouncer is running in container "postgresql1"
        And pgbouncer is running in container "postgresql2"
        And pgbouncer is running in container "postgresql3"

    Examples: <lock_type>, <with_slots> slots
        | lock_type | lock_host  | with_slots | use_slots | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 |   with     |    yes    |      no       |       sync       |

    @coordinator_fail
    Scenario Outline: Kill coordinator and both replicas
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
                    min_failover_timeout: 1
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
        When we disconnect from network container "zookeeper1"
        And we disconnect from network container "zookeeper2"
        And we disconnect from network container "zookeeper3"
        And we disconnect from network container "postgresql2"
        And we disconnect from network container "postgresql3"
        And we wait "10.0" seconds
        Then pgbouncer is not running in container "postgresql1"

    Examples: <lock_type>, <with_slots> slots
        | lock_type | lock_host  | with_slots | use_slots | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 |   with     |    yes    |      no       |       sync       |
