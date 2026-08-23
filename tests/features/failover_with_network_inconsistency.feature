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
                plugins:
                    wals_to_upload: 100
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
    Scenario: Old primary can not get a write acknowledged after voting for a new primary has started
        # Regression test for a race between disabling walreceiver (which is only
        # actually enacted by the replica's startup process, asynchronously) and
        # reading wal_receive_lsn for the failover election vote. We simulate the
        # worst case of that race (startup frozen with SIGSTOP, so the disable
        # never takes effect) combined with the old primary coming back mid-election,
        # and check that it is still impossible to get a synchronous write
        # acknowledged by the old primary once voting has begun.
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
        # Freeze startup for a bounded window (auto-CONT after N seconds).
        # On code before https://github.com/yandex/pgconsul/pull/199 replicas vote while startup is still frozen and walreceiver
        # stays alive, so the subsequent CREATE TABLE can be sync-acked (test fails).
        # After a fix that waits for walreceiver to actually stop before reading LSN,
        # replicas only vote after auto-CONT; by then walreceiver is gone and CREATE TABLE
        # must time out (test passes).
        # N must cover: remaining sleep_before_disable_walreceiver + vote + CREATE TABLE.
        When we freeze process "postgres: startup" in container "postgresql2" for "60" seconds
        And we freeze process "postgres: startup" in container "postgresql3" for "60" seconds
        # Wait until both replicas have captured their LSN and voted
        Then zookeeper "zookeeper1" has key "/pgconsul/postgresql/election_vote/pgconsul_postgresql2_1.pgconsul_pgconsul_net/lsn"
        Then zookeeper "zookeeper1" has key "/pgconsul/postgresql/election_vote/pgconsul_postgresql3_1.pgconsul_pgconsul_net/lsn"
        # The old primary must not be able to get a synchronous write acknowledged
        # by any replica once voting has started
        When we create a table in container "postgresql1" and expect it does not complete within "5000" ms
