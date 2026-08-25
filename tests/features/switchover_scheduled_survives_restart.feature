Feature: Switchover survives pgconsul restart in scheduled phase

    @switchover
    Scenario: Switchover in scheduled phase survives pgconsul restart
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 10
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
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
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then container "postgresql3" is in quorum group
        # Initiate switchover by writing scheduled state to ZK (no destination — pgconsul picks the replica)
        When we lock "/pgconsul/postgresql/switchover/lock" in zookeeper "zookeeper1"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': null, 'phase': 'scheduled', 'candidate': null, 'side_replicas': []}" for key "/pgconsul/postgresql/switchover/record" in zookeeper "zookeeper1"
        And we release lock "/pgconsul/postgresql/switchover/lock" in zookeeper "zookeeper1"
        # Restart pgconsul on the primary while switchover is in scheduled phase
        When we gracefully stop "pgconsul" in container "postgresql1"
        And we wait "1" seconds
        And we start "pgconsul" in container "postgresql1"
        # Switchover must resume and complete after restart
        Then we remember which of "postgresql2,postgresql3" became primary as "sw_primary" and the other as "sw_replica"
        And container "sw_replica" is a replica of container "sw_primary"
        And container "postgresql1" is a replica of container "sw_primary"
        And container "postgresql1" is in quorum group
        And postgresql in container "sw_replica" was not rewinded
        And postgresql in container "postgresql1" was rewinded
        And timing log in container "sw_primary" contains "switchover,downtime"
