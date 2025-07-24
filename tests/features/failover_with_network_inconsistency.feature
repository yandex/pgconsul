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
