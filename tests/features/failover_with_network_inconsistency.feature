Feature: Failover with network inconsistency

    @failover
    Scenario: Failover will happen
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    max_rewind_retries: 3
                    election_timeout: 5
                    update_prio_in_zk: 'yes'
                    autofailover: 'yes'
                    quorum_commit: 'yes'
                    use_lwaldump: 'yes'
                primary:
                    change_replication_type: 'yes'
                    change_replication_metric: 'count'
                    primary_switch_checks: 6
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 10
                    min_failover_timeout: 1
                    primary_switch_restart: 'no'
                plugins:
                    wals_to_upload: 100
                debug:
                    election_loser_timeout: 20
            postgresql.conf:
                synchronous_commit: 'on'
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
        # Run load testing
        When we run load testing
        """
        host: postgresql1
        pgbench:
          clients: 2
          jobs: 4
          time: 600
        """
        When we wait "30" seconds
        When we disconnect from ZK container "postgresql1"
        When we block postgres traffic from "postgresql1" to "postgresql3"
        When we disable archive restore in container "postgresql3"
        Then postgresql in container "postgresql3" has value "/bin/false" for option "restore_command"
        When we wait "3" seconds
        When we block postgres traffic from "postgresql1" to "postgresql2"
        # Wait until Election is done
        Then zookeeper "zookeeper1" has value "done" for key "/pgconsul/postgresql/election_status"
        # Return connectivity between postgresql1 and postgresql3. Host postgresql3 will stay a replica
        When we unblock postgres traffic from "postgresql1" to "postgresql3"
        Then container "postgresql2" became a primary
        Then container "postgresql3" is a replica of container "postgresql2" and streaming
        When we connect to ZK container "postgresql1"
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -F"
        """
        Then container "postgresql1" is a replica of container "postgresql2" and streaming
        Then container "postgresql3" is a replica of container "postgresql2" and streaming

    @failover
    Scenario: Replicas vote only after their walreceivers stop
        # Clearing primary_conninfo does not stop a receiver until startup handles
        # the reload. With startup frozen, the old primary can still get a sync ack,
        # so neither replica may vote. A test-only unlimited wait lets us release
        # startup explicitly instead of racing the five-second production timeout.
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    max_rewind_retries: 3
                    election_timeout: 30
                    update_prio_in_zk: 'yes'
                    autofailover: 'yes'
                    quorum_commit: 'yes'
                    use_lwaldump: 'yes'
                primary:
                    change_replication_type: 'yes'
                    change_replication_metric: 'count'
                    primary_switch_checks: 6
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 10
                    min_failover_timeout: 1
                    primary_switch_restart: 'no'
                    walreceiver_disable_timeout: -1
                plugins:
                    wals_to_upload: 100
                debug:
                    sleep_before_disable_walreceiver: 60
                    election_lsn_read_sleep: 10
            postgresql.conf:
                synchronous_commit: 'on'
                wal_retrieve_retry_interval: '1s'
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
        When we wait "30" seconds
        When we disable archiving in "postgresql1"
        # Kill the old primary so replicas start failover
        When we gracefully stop "pgconsul" in container "postgresql1"
        When we gracefully stop "postgres" in container "postgresql1"
        # Wait until both replicas have entered _can_do_failover and are sleeping
        # right before disabling walreceiver
        Then container "postgresql2" pgconsul log contains "Sleep for test purposes before disabling walreceiver"
        Then container "postgresql3" pgconsul log contains "Sleep for test purposes before disabling walreceiver"
        # Old primary comes back before either replica has disabled its walreceiver
        When we start "postgres" in container "postgresql1"
        Then container "postgresql2" walreceiver is streaming from container "postgresql1"
        Then container "postgresql3" walreceiver is streaming from container "postgresql1"
        When we run following command on host "postgresql2"
        """
        pkill -STOP -f 'postgres: startup'
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql3"
        """
        pkill -STOP -f 'postgres: startup'
        """
        Then command exit with return code "0"
        Then postgresql in container "postgresql2" has empty option "primary_conninfo"
        And postgresql in container "postgresql3" has empty option "primary_conninfo"
        And container "postgresql2" walreceiver is streaming from container "postgresql1"
        And container "postgresql3" walreceiver is streaming from container "postgresql1"
        # Before #199, replicas published their LSN after a ten-second debug delay.
        When we wait "15" seconds
        Then zookeeper "zookeeper1" has no listed keys now
        """
        /pgconsul/postgresql/election_vote/pgconsul_postgresql2_1.pgconsul_pgconsul_net/lsn
        /pgconsul/postgresql/election_vote/pgconsul_postgresql3_1.pgconsul_pgconsul_net/lsn
        """
        When we run following command on host "postgresql2"
        """
        pkill -CONT -f 'postgres: startup'
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql3"
        """
        pkill -CONT -f 'postgres: startup'
        """
        Then command exit with return code "0"
        Then container "postgresql2" walreceiver is stopped within "30" seconds
        And container "postgresql3" walreceiver is stopped within "30" seconds
        Then zookeeper "zookeeper1" has key "/pgconsul/postgresql/election_vote/pgconsul_postgresql2_1.pgconsul_pgconsul_net/lsn"
        Then zookeeper "zookeeper1" has key "/pgconsul/postgresql/election_vote/pgconsul_postgresql3_1.pgconsul_pgconsul_net/lsn"
        # Only after both receivers stop and vote is a sync ack impossible.
        When we create a table in container "postgresql1" and expect it does not complete within "5000" ms

    @failover
    Scenario: Replicas do not vote if walreceiver shutdown times out
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    max_rewind_retries: 3
                    election_timeout: 30
                    update_prio_in_zk: 'yes'
                    autofailover: 'yes'
                    quorum_commit: 'yes'
                    use_lwaldump: 'yes'
                primary:
                    change_replication_type: 'yes'
                    change_replication_metric: 'count'
                    primary_switch_checks: 6
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 10
                    min_failover_timeout: 1
                    primary_switch_restart: 'no'
                    walreceiver_disable_timeout: 5
                plugins:
                    wals_to_upload: 100
                debug:
                    sleep_before_disable_walreceiver: 60
            postgresql.conf:
                synchronous_commit: 'on'
                wal_retrieve_retry_interval: '1s'
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
        When we wait "30" seconds
        When we disable archiving in "postgresql1"
        When we gracefully stop "pgconsul" in container "postgresql1"
        When we gracefully stop "postgres" in container "postgresql1"
        Then container "postgresql2" pgconsul log contains "Sleep for test purposes before disabling walreceiver"
        And container "postgresql3" pgconsul log contains "Sleep for test purposes before disabling walreceiver"
        When we start "postgres" in container "postgresql1"
        Then container "postgresql2" walreceiver is streaming from container "postgresql1"
        And container "postgresql3" walreceiver is streaming from container "postgresql1"
        When we run following command on host "postgresql2"
        """
        pkill -STOP -f 'postgres: startup'
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql3"
        """
        pkill -STOP -f 'postgres: startup'
        """
        Then command exit with return code "0"
        Then postgresql in container "postgresql2" has empty option "primary_conninfo"
        And postgresql in container "postgresql3" has empty option "primary_conninfo"
        And container "postgresql2" walreceiver is streaming from container "postgresql1"
        And container "postgresql3" walreceiver is streaming from container "postgresql1"
        Then container "postgresql2" pgconsul log contains "Walreceiver did not stop within 5.0s after disable."
        And container "postgresql3" pgconsul log contains "Walreceiver did not stop within 5.0s after disable."
        # Timeout must abort the vote; while receivers stream, old-primary writes
        # may still succeed, so this scenario does not assert that writes block.
        When we wait "10" seconds
        Then zookeeper "zookeeper1" has no listed keys now
        """
        /pgconsul/postgresql/election_vote/pgconsul_postgresql2_1.pgconsul_pgconsul_net/lsn
        /pgconsul/postgresql/election_vote/pgconsul_postgresql3_1.pgconsul_pgconsul_net/lsn
        """
