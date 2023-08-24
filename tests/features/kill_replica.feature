Feature: Destroy synchronous replica in various scenarios


    Scenario Outline: Destroy synchronous replica
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
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
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
        Then container "postgresql3" is in <replication_type> group
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

    Scenario Outline: Loss zookeeper connectivity
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: <quorum_commit>
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
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: <replication_type>
        """
        When we kill "pgconsul" in container "postgresql2" with signal "SIGKILL"
        And we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        When we start "pgconsul" in container "postgresql2" 
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: <replication_type>
        """

    Examples: <lock_type>, <with_slots> replication slots, <destroy>/<repair>
        | lock_type | lock_host  | with_slots | use_slots | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 |   with     |    yes    |      no       |       sync       |


    Scenario Outline: Loss connect to last quorum replica 
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: <quorum_commit>
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
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: <replication_type>
        """
        When we disconnect from network container "postgresql2"
        And we wait "35.0" seconds
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
        """
        When we connect to network container "postgresql2" 
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: <replication_type>
        """

    Examples: <lock_type>, <with_slots> replication slots, <destroy>/<repair>
        | lock_type | lock_host  | with_slots | use_slots | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |  without   |    no     |      yes      |      quorum      |
        | zookeeper | zookeeper1 |   with     |    yes    |      yes      |      quorum      |
        | zookeeper | zookeeper1 |  without   |    no     |      no       |       sync       |
        | zookeeper | zookeeper1 |   with     |    yes    |      no       |       sync       |


    @pause_replication
    Scenario Outline: Pause replication on replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
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
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we pause replaying WAL in container "postgresql2"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is replaying WAL
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we pause replaying WAL in container "postgresql3"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then container "postgresql3" is replaying WAL
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """

    Examples: <lock_type>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |
