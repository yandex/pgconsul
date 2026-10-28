Feature: Switchover WAL barrier

    @switchover
    Scenario: Old primary stays open until the candidate acknowledges the WAL barrier
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 2
                    min_failover_timeout: 120
                debug:
                    wal_barrier_wait_file: /tmp/allow_wal_barrier
        """
        Given a following cluster with "zookeeper" with replication slots
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
        """
        When we do targeted switchover from container "postgresql1" to container "postgresql2"
        Then container "postgresql1" pgconsul log contains "Waiting for WAL barrier injection file"
        When we clear primary_conninfo in container "postgresql2"
        Then container "postgresql2" walreceiver is stopped within "30" seconds
        When we run following command on host "postgresql1"
        """
            touch /tmp/allow_wal_barrier
        """
        Then WAL barrier in container "postgresql1" waits for sync replica within "30" seconds
        When we wait "5" seconds
        Then WAL barrier in container "postgresql1" waits for sync replica within "5" seconds
        And container "postgresql1" is primary
        And pgbouncer is running in container "postgresql1"
        And zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we restore primary_conninfo in container "postgresql2"
        Then container "postgresql2" became a primary
        And container "postgresql1" is a replica of container "postgresql2"
