Feature: Interacting with coordinator

    @failover
    Scenario Outline: Destroying most of the cluster (including ZK quorum)
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
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
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
          And a following cluster with "zookeeper" with replication slots
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
        And container "postgresql2" is in <replication_type> group
         And zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" replication state is "sync"
        When we <destroy> container "zookeeper1"
         And we <destroy> container "zookeeper2"
         And we <destroy> container "postgresql1"
        Then pgbouncer is not running in container "postgresql1"
        When we <repair> container "zookeeper1"
         And we <repair> container "zookeeper2"
        Then zookeeper "zookeeper3" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
         And container "postgresql2" became a primary
         And zookeeper "zookeeper3" has value "finished" for key "/pgconsul/postgresql/failover_state"
        And container "postgresql3" is in <replication_type> group
         And zookeeper "zookeeper3" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
         And container "postgresql3" is a replica of container "postgresql2"
        When we <repair> container "postgresql1"
        Then zookeeper "zookeeper3" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
         And container "postgresql1" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        Then postgresql in container "postgresql1" was rewinded
    Examples: <destroy>/<repair>
        |          destroy        |       repair       | quorum_commit | replication_type |
        |           stop          |        start       |      yes      |      quorum      |
        | disconnect from network | connect to network |      yes      |      quorum      |
        |           stop          |        start       |      no       |       sync       |
        | disconnect from network | connect to network |      no       |       sync       |
