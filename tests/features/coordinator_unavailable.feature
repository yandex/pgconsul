Feature: Unavailable coordinator

    @failover_when_zk_return
    Scenario: Failover works After repair ZK
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
         And zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" replication state is "sync"
        #  And we wait "10.0" seconds
        When we stop container "postgresql1"
        When we stop container "zookeeper2"
         And we wait "10.0" seconds
        When we disconnect from network container "postgresql2"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in zookeeper "zookeeper3"
        When we disconnect from network container "zookeeper3"
        # When we disconnect from network container "zookeeper3"
        #  And we wait "10.0" seconds
         And we wait "30.0" seconds
        When we connect to network container "zookeeper3"
         And we wait "30.0" seconds
         And we wait "30.0" seconds
        # When we connect to network container "postgresql2"
        #  And we wait "30.0" seconds
        #  And we wait "30.0" seconds
        # When we connect to network container "postgresql2"
        Then "pgconsul" is running in container "postgresql2"
        Then "pgconsul" is running in container "postgresql3"
        Then "postgresql" is running in container "postgresql2"
        Then "postgresql" is running in container "postgresql3"
        Then container "postgresql2" became a primary
        # Then container "postgresql2" became a primary
        # When we wait "120.0" seconds
