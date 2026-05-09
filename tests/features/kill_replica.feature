Feature: Destroy synchronous replica in various scenarios


    Scenario Outline: Destroy synchronous replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
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
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" <with_slots> replication slots
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
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        When we <destroy> container "postgresql3"
        Then container "postgresql1" is primary
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        When we <repair> container "postgresql3"
        Then container "postgresql3" is a replica of container "postgresql1"
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        Then container "postgresql1" is primary
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql3" is a replica of container "postgresql1"
        Then pgconsul in container "postgresql3" is connected to zookeeper

    Examples: <with_slots> replication slots, <destroy>/<repair>
        |          destroy        |       repair       | with_slots | use_slots |
        |           stop          |        start       |  without   |    no     |
        |           stop          |        start       |   with     |    yes    |
        | disconnect from network | connect to network |  without   |    no     |
        | disconnect from network | connect to network |   with     |    yes    |

    Scenario Outline: Loss zookeeper connectivity
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: yes
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
        Given a following cluster with "zookeeper" <with_slots> replication slots
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
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: quorum
        """
        When we kill "pgconsul" in container "postgresql2" with signal "SIGKILL"
        And we wait "10.0" seconds
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        When we start "pgconsul" in container "postgresql2" 
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in quorum group
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: quorum
        """

    Examples: <with_slots> replication slots
        | with_slots | use_slots |
        |  without   |    no     |
        |   with     |    yes    |


    Scenario Outline: Loss connect to last quorum replica 
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: yes
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                    before_async_unavailability_timeout: 10
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" <with_slots> replication slots
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
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: quorum
        """
        When we disconnect from network container "postgresql2"
        And we wait "5.0" seconds
        Then container "postgresql1" replication state is "sync"
        When we wait "35.0" seconds
        Then container "postgresql1" replication state is "async"
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
        """
        When we connect to network container "postgresql2" 
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in quorum group
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: quorum
        """

    Examples: <with_slots> replication slots
        | with_slots | use_slots |
        |  without   |    no     |
        |   with     |    yes    |


    @pause_replication
    Scenario: Paused replication on replica automatically resumed in next iterations
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
        Given a following cluster with "zookeeper" with replication slots
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
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        When we pause replaying WAL in container "postgresql2"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is replaying WAL
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        When we pause replaying WAL in container "postgresql3"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then container "postgresql3" is replaying WAL
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"

    @detached_replica
    Scenario Outline: Disconnecting replica pgbouncer behaviour
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
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
                    close_detached_after: 30
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" <with_slots> replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        When we disconnect from network container "postgresql2"
        And we wait "5.0" seconds
        Then pgbouncer is running in container "postgresql2"
        When we wait "30.0" seconds
        Then pgbouncer is not running in container "postgresql2"
        When we connect to network container "postgresql2"
        Then container "postgresql1" is primary
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql2" is in quorum group
        Then pgbouncer is running in container "postgresql2"
        Then container "postgresql2" is streaming from container "postgresql1"

    Examples: <with_slots> replication slots
        | with_slots | use_slots |
        |  without   |    no     |
        |   with     |    yes    |
