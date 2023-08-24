Feature: Destroy non HA replica in various scenarios

    Scenario Outline: Destroy non HA replica
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
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
                stream_from: postgresql1
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
        When we <destroy> container "postgresql3"
        Then container "postgresql1" is primary
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <repair> container "postgresql3"
        Then container "postgresql3" is a replica of container "postgresql1"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is primary
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql3" is a replica of container "postgresql1"
        Then pgconsul in container "postgresql3" is connected to zookeeper

    Examples: <lock_type>, <with_slots> replication slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |      no       |       sync       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      no       |       sync       |

