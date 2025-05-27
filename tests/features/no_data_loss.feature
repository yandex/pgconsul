Feature: Long promote

    @failover
    Scenario: Long promote not lead to exit pgconsul
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
                    append_primary_conn_string: 'port=6432 dbname=postgres user=repl password=repl connect_timeout=1'
                    iteration_timeout: 5.0
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
                plugins:
                    wals_to_upload: 100
                commands:
                    generate_recovery_conf: sleep 5; /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
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
        When we create database "db1" on "postgresql1"
        # Init
        When we run following command on host "postgresql1"
        """
        su - postgres -c "psql -d db1 -c 'CREATE TABLE test (ts timestamp);'"
        """
        When we run following command on host "postgresql1" nowait
        """
        su - postgres -c "while true; do psql -d db1 -c 'INSERT INTO test VALUES(now());'; done"
        """
        # When we run following command on host "postgresql1"
        # """
        # su - postgres -c "pgbench db1 -i -s 50 -h postgresql1"
        # """
        # # Benchmark
        #  And we run following command on host "postgresql1" nowait
        # """
        # su - postgres -c "pgbench db1 -c 5 -j 1 -P 5 -T 180 -h postgresql1"
        # """
        # Close from ZK
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -I OUTPUT -m tcp -p tcp --dport 2281 -j DROP"
        """
        # Close from postgresql2
        When we run following command on host "postgresql1" nowait
        """
        sh -c "iptables -I INPUT -p tcp -m tcp -s 192.168.233.15/32 --dport 6432 -j DROP"
        """
        # Close from postgresql3
        When we run following command on host "postgresql1" nowait
        """
        sh -c "iptables -I INPUT -p tcp -m tcp -s 192.168.233.16/32 --dport 6432 -j DROP"
        """
        # Close from postgresql2 + postgresql3
        When we run following command on host "postgresql1" nowait
        """
        sh -c "iptables -I INPUT -p tcp -m tcp -s 192.168.233.15/32 --dport 5432 -j DROP; iptables -I INPUT -p tcp -m tcp -s 192.168.233.16/32 --dport 5432 -j DROP"
        """
        # Wait until Election done
        Then zookeeper "zookeeper1" has value "done" for key "/pgconsul/postgresql/election_status"
        # Close from ZK postgresql3 to prevent primary_switch
        When we run following command on host "postgresql3"
        """
        sh -c "iptables -I OUTPUT -m tcp -p tcp --dport 2281 -j DROP"
        """
        # Delete rule for postgresql3 Replica
        When we run following command on host "postgresql1" nowait
        """
        sh -c "iptables -D INPUT -p tcp -m tcp -s 192.168.233.16/32 --dport 5432 -j DROP"
        """
        Then container "postgresql2" became a primary
        # Open ZK postgresql1
        When we run following command on host "postgresql1"
        """
        sh -c "iptables -D OUTPUT 1"
        """
        # Open ZK postgresql3
        When we run following command on host "postgresql3"
        """
        sh -c "iptables -D OUTPUT 1"
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        Then container "postgresql1" is a replica of container "postgresql2"
        When we wait "30.0" seconds
        When we wait "30.0" seconds
        When we wait "30.0" seconds
        When we wait "30.0" seconds
        When we wait "36000.0" seconds
