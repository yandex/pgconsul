Feature: Failover with network inconsistency

    @failover
    Scenario: Failover will happen
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                    max_rewind_retries: 3
                    election_timeout: 5
                    update_prio_in_zk: 'yes'
                    autofailover: 'yes'
                    quorum_commit: 'yes'
                    use_lwaldump: 'yes'
                    election_loser_timeout: 20
                    append_primary_conn_string: 'port=6432 dbname=postgres user=repl password=repl connect_timeout=1'
                primary:
                    change_replication_type: 'yes'
                    change_replication_metric: 'count'
                    primary_switch_checks: 6
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 10
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    primary_switch_restart: 'no'
                    # recovery_timeout: 15
                plugins:
                    wals_to_upload: 100
            postgresql.conf:
                synchronous_commit: 'on'
                log_min_messages: 'info'
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
        # Prepare to load testing
        When we create database "db1" on "postgresql1"
        When we run following command on host "postgresql1"
        """
        su - postgres -c "psql -d db1 -c 'CREATE TABLE test (ts timestamp);'"
        """
        When we run following command on host "postgresql1"
        """
        su - postgres -c "echo 'INSERT INTO test VALUES(now());' > /tmp/insert.sql"
        """
        # Test load
        When we run following command on host "postgresql1" nowait
        """
        su - postgres -c "pgbench -n -f /tmp/insert.sql -c 1 -j 1 -T 36000 -h postgresql1 -p 6432 db1"
        """
        When we wait "30" seconds
        # Close postgresql1 from ZK
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -I OUTPUT -m tcp -p tcp --dport 2281 -j DROP"
        """
        # Close port 6432 for postgresql3 + postgresql2
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -I INPUT -p tcp -m tcp -s 192.168.233.16/32 --dport 6432 -j DROP; iptables -I INPUT -p tcp -m tcp -s 192.168.233.15/32 --dport 6432 -j DROP"
        """
        # Close port 5432 for postgresql3 + postgresql2
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -I INPUT -p tcp -m tcp -s 192.168.233.16/32 --dport 5432 -j DROP; sleep 3; iptables -I INPUT -p tcp -m tcp -s 192.168.233.15/32 --dport 5432 -j DROP"
        """
        # Wait until Election is done
        Then zookeeper "zookeeper1" has value "done" for key "/pgconsul/postgresql/election_status"
        # Return connectivity between postgresql1 and postgresql3. Host postgresql3 will stay a replica
        When we run following command on host "postgresql1" nowait
        """
        sh -c "iptables -D INPUT -p tcp -m tcp -s 192.168.233.16/32 --dport 5432 -j DROP"
        """
        Then container "postgresql2" became a primary
        Then container "postgresql3" is in quorum group
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -F"
        """
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is a replica of container "postgresql2"
